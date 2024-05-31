# class for representing a foreign currency to cover dividend payments, fees, quellensteuer, etc.
# these are separated from FIFO treatments of foreign currencies
import math
import datetime
import decimal
from datetime import datetime
from decimal import Decimal

from utils import to_decimal, round_decimal


# base class for handling curency-valued items
# e.g. for keeping track of tax payments or
# bookkeeping of other monetary items
class Forex:
    def __init__(self, currency: str, date: datetime, amount: Decimal, comment: str):
        self.currency = currency
        self.date = date
        self.amount = amount
        # after conversion into domestic currency with
        # daily or monthly average currency exchange rate
        self.amount_eur_daily = None
        self.amount_eur_monthly = None
        self.comment = comment

    def __repr__(self):
        return f"{self.currency}(Date: {self.date}, Amount: {self.amount:.2f})"


# base class representing an arbitrary asset subject to FIFO treatment
class FIFOObject:
    def __init__(
        self,
        symbol: str,
        quantity: Decimal,
        buy_date: datetime,
        buy_price: Decimal,
        currency: str,
    ):
        self.symbol = symbol
        self.currency = currency
        self.quantity = quantity
        self.buy_date = buy_date
        self.sell_date = None
        self.buy_price = buy_price
        self.buy_price_eur_daily = None
        self.buy_price_eur_monthly = None
        self.sell_price = None
        self.sell_price_eur_daily = None
        self.sell_price_eur_monthly = None
        self.gain_eur_daily = None
        self.gain_eur_monthly = None


# class representing a FOREX object subject to FIFO treatment
class FIFOForex(FIFOObject):
    def __init__(
        self, currency: str, quantity: Decimal, buy_date: datetime, source: str
    ):
        # "money" is always a single unit "money"
        super().__init__(currency, quantity, buy_date, 1, currency)
        self.source = source  # e.g. "sale", "deposit", "dividend", etc.

    def __repr__(self):
        return f"{self.symbol}(Quantity: {self.quantity:.2f}, Buy-Date: {self.buy_date:%Y-%b-%d})"


# class representing a share subject to FIFO treatment
class FIFOShare(FIFOObject):
    def __init__(self, symbol, quantity, buy_date, buy_price, currency):
        super().__init__(symbol, quantity, buy_date, buy_price, currency)

    def __repr__(self):
        return f"{self.symbol}(Quantity: {self.quantity}, Buy-Date: {self.buy_date:%Y-%b-%d}, Buy-Price: {self.buy_price:.2f} {self.currency})"


class ReportEvent:
    def __init__(self, date: datetime):
        self.date = date

    @staticmethod
    def from_report_row(row):
        raise NotImplementedError

    @classmethod
    def from_report(cls, df):
        events = []
        for _, row in df.iterrows():
            events.append(cls.from_report_row(row))
        return events

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} on {self.date.date()}"


class DepositEvent(ReportEvent):
    def __init__(self, date: datetime, symbol: str, received_shares: FIFOShare):
        self.date = date
        self.symbol = symbol
        self.received_shares = received_shares

    @staticmethod
    def from_report_row(row):
        recv_share = FIFOShare(
            buy_date=row.date,
            symbol=row.symbol,
            buy_price=to_decimal(row.fair_market_value),
            quantity=to_decimal(row.net_quantity),
            currency=row.currency,
        )

        return DepositEvent(
            row.date,
            row.symbol,
            recv_share,
        )


class DividendEvent(ReportEvent):
    def __init__(
        self,
        date: datetime,
        currency: str,
        received_dividend: Forex,
        received_net_dividend: FIFOForex,
        withheld_tax: Forex,
    ):
        self.date = date
        self.currency = currency
        self.received_dividend = received_dividend
        self.received_net_dividend = received_net_dividend
        self.withheld_tax = withheld_tax

    @staticmethod
    def from_report_row(row):
        gross_amount = to_decimal(row.amount)
        tax_amount = to_decimal(row.tax_withholding)
        net_amount = gross_amount - tax_amount
        assert 0 <= tax_amount <= gross_amount, "Expected non-negative Tax Withholding!"
        assert 0 <= net_amount <= gross_amount, "Expected non-negative Net Amount"
        div = Forex(
            currency=row.currency,
            date=row.date,
            amount=gross_amount,
            comment=f"Dividend Payment ({row.symbol})",
        )
        net_div = FIFOForex(
            currency=row.currency,
            buy_date=row.date,
            quantity=net_amount,
            source=f"Received Net Dividend Payment ({row.symbol})",
        )
        tax = Forex(
            currency=row.currency,
            date=row.date,
            amount=tax_amount,
            comment=f"Withheld Tax on Dividends ({row.symbol})",
        )
        return DividendEvent(row.date, row.currency, div, net_div, tax)


class BuyEvent(ReportEvent):
    def __init__(
        self,
        date: datetime,
        symbol: str,
        received_shares: FIFOShare,
        cost_of_shares: decimal,
        paid_fees: Forex,
        currency: str,
    ):
        self.date = date
        self.symbol = symbol
        self.received_shares = received_shares
        self.cost_of_shares = cost_of_shares
        self.paid_fees = paid_fees
        self.currency = currency

    @staticmethod
    def from_report_row(row):
        buy_price = to_decimal(row.buy_price)
        quantity = to_decimal(row.quantity)
        fees = to_decimal(row.fees)

        recv_shares = FIFOShare(
            buy_date=row.date,
            symbol=row.symbol,
            buy_price=buy_price,
            quantity=quantity,
            currency=row.currency,
        )

        paid_fees = Forex(
            currency=row.currency,
            date=row.date,
            amount=fees,
            comment=f"Fees for Buy Order ({quantity:.2f} x {row.symbol})",
        )

        # schwab sometimes has sub-cent share prices
        # e.g. 100.7438, the deposited value, however,
        # is rounded to full cents
        # TODO: check if rounding mode matches schwab
        cost_of_shares = round_decimal(buy_price * quantity - fees)
        return BuyEvent(
            row.date,
            row.symbol,
            recv_shares,
            cost_of_shares,
            paid_fees,
            currency=row.currency,
        )


class SellEvent(ReportEvent):
    def __init__(
        self,
        date: datetime,
        symbol: str,
        currency: str,
        quantity: decimal,
        sell_price: decimal,
        received_forex: FIFOForex,
        paid_fees: Forex,
    ):
        self.date = date
        self.symbol = symbol
        self.currency = currency
        self.quantity = quantity
        self.sell_price = sell_price
        self.received_forex = received_forex
        self.paid_fees = paid_fees

    @staticmethod
    def from_report_row(row):
        sell_price = to_decimal(row.sell_price)
        quantity = to_decimal(row.quantity)
        fees = to_decimal(row.fees)
        # schwab sometimes has sub-cent share prices
        # e.g. 100.7438, the deposited value, however,
        # is rounded to full cents
        # TODO: check if rounding mode matches schwab
        net_proceeds = sell_price * quantity - fees
        net_proceeds = round_decimal(net_proceeds)
        assert (
            net_proceeds >= 0
        ), "Expected non-negative net proceeds from sale of shares!"
        new_forex = FIFOForex(
            currency=row.currency,
            quantity=net_proceeds,
            buy_date=row.date,
            source="Sales Proceeds",
        )
        fees = Forex(
            currency=row.currency,
            date=row.date,
            amount=fees,
            comment=f"Fees for Sell Order ({quantity:.2f} x {row.symbol})",
        )

        return SellEvent(
            row.date, row.symbol, row.currency, quantity, sell_price, new_forex, fees
        )


class CurrencyConversionEvent(ReportEvent):
    def __init__(
        self,
        date: datetime,
        foreign_amount: decimal,
        source_fees: Forex,
        source_currency: str,
        target_currency: str,
    ):
        self.date = date
        self.foreign_amount = foreign_amount
        self.source_fees = source_fees
        self.source_currency = source_currency
        self.target_currency = target_currency

    @staticmethod
    def from_report_row(row):
        source_fees = Forex(
            currency=row.source_currency,
            date=row.date,
            amount=to_decimal(row.source_fees),
            comment=f"Fees for converting {row.source_currency} to {row.target_currency}",
        )
        return CurrencyConversionEvent(
            row.date,
            to_decimal(row.foreign_amount),
            source_fees,
            row.source_currency,
            row.target_currency,
        )


class StockSplitEvent(ReportEvent):
    def __init__(self, date: datetime, symbol: str, shares_after_split: Decimal):
        self.date = date
        self.symbol = symbol
        self.shares_after_split = shares_after_split

    @staticmethod
    def from_report_row(row):
        return StockSplitEvent(
            date=row.date,
            symbol=row.symbol,
            shares_after_split=to_decimal(row.shares_after_split),
        )


class FIFOQueue:
    def __init__(self):
        self.assets: list[FIFOObject] = []
        self.total_quantity: decimal = to_decimal(0)

    def apply_split(self, shares_after_split: decimal):
        self.total_quantity = to_decimal(0)
        for asset in self.assets:
            asset.quantity = asset.quantity * shares_after_split
            asset.buy_price = asset.buy_price / shares_after_split
            self.total_quantity = self.total_quantity + asset.quantity

    def push(self, asset: FIFOObject):
        if self.is_empty():
            self.assets = [asset]
        else:
            # insert based on buy date ("first in")
            idx = 0
            while (idx < len(self.assets)) and (
                asset.buy_date > self.assets[idx].buy_date
            ):
                idx += 1
            self.assets.insert(idx, asset)
        self.total_quantity += asset.quantity

    def is_empty(self):
        return len(self.assets) == 0

    def peek(self):
        if self.is_empty():
            raise ValueError("Cannot peek first element from an empty queue.")
        return self.assets[0]

    def pop(self, quantity: decimal, sell_price: decimal, sell_date: datetime):
        if math.isclose(quantity, 0, abs_tol=1e-10):
            return []

        if quantity < 0:
            raise ValueError("Cannot sell negative amount of equity")

        if quantity > 0 and self.is_empty():
            raise ValueError(f"Cannot sell equities because there isn't any owned")

        if (
            not math.isclose(quantity, self.total_quantity)
            and quantity > self.total_quantity
        ):
            symbol = self.peek().symbol
            asset_type = self.peek().__class__.__name__
            if asset_type == "FIFOShare":
                raise ValueError(
                    f"Cannot sell more {symbol} shares ({quantity:.2f}) than owned overall ({self.total_quantity:.2f})."
                )
            elif asset_type == "FIFOForex":
                raise ValueError(
                    f"Cannot convert more {symbol} ({quantity:.2f}) than owned overall ({self.total_quantity:.2f})."
                )
            else:
                raise ValueError(
                    f"Cannot pop quantity ({quantity:.2f}) larger than all quantities ({self.total_quantity:.2f}) in FIFOQueue!"
                )

        if self.peek().buy_date > sell_date:
            # Relying on the fact that "assets" are sorted by date
            symbol = self.peek().symbol
            raise ValueError(
                f"Cannot sell the requested {symbol} equity because on the sell transaction date "
                f"({sell_date.strftime('%Y-%m-%d')}) the requested amount is not available"
            )

        front_quantity = self.peek().quantity
        if quantity < front_quantity:
            pop_asset = from_asset(self.peek(), quantity)
            pop_asset.sell_price = sell_price
            pop_asset.sell_date = sell_date
            self.peek().quantity -= quantity
            self.total_quantity -= quantity
            return [pop_asset]
        elif math.isclose(quantity, front_quantity):
            self.total_quantity -= front_quantity
            pop_asset = self.assets.pop(0)
            pop_asset.sell_date = sell_date
            pop_asset.sell_price = sell_price
            return [pop_asset]
        else:
            # quantity is larger
            # pop first item, then call pop on remaining quantity
            pop_asset = self.assets.pop(0)
            pop_asset.sell_price = sell_price
            pop_asset.sell_date = sell_date
            self.total_quantity -= pop_asset.quantity
            remaining_quantity = quantity - pop_asset.quantity
            return [pop_asset] + self.pop(remaining_quantity, sell_price, sell_date)

    def __repr__(self):
        return self.assets.__repr__()


def from_asset(asset, quantity):
    # intended for usage in "FIFOQueue"
    if asset.__class__.__name__ == "FIFOForex":
        new_asset = FIFOForex(asset.currency, quantity, asset.buy_date, asset.source)

    elif asset.__class__.__name__ == "FIFOShare":
        new_asset = FIFOShare(
            asset.symbol, quantity, asset.buy_date, asset.buy_price, asset.currency
        )

    else:
        asset_type = type(asset)
        raise ValueError(
            f"asset is of unsupported type, got {asset_type}, expected 'FIFOForex' or 'FIFOShare'"
        )

    return new_asset
