# class for representing a foreign currency to cover dividend payments, fees, quellensteuer, etc.
# these are separated from FIFO treatments of foreign currencies
import math
import decimal

from utils import to_decimal, round_decimal


class Forex:
    def __init__(self, currency, date, amount, comment):
        self.currency = currency
        self.date = date
        self.amount = amount
        # after conversion into domestic currency with
        # daily or monthly average currency exchange rate
        self.amount_eur_daily = None
        self.amount_eur_monthly = None
        self.comment = comment

    @staticmethod
    def from_dividends_row(row):
        gross_amount = to_decimal(row.amount)
        tax_amount = to_decimal(row.tax_withholding)
        net_amount = gross_amount - tax_amount
        assert 0 <= tax_amount <= gross_amount, "Expected non-negative Tax Withholding!"
        assert 0 <= net_amount <= gross_amount, "Expected non-negative Net Amount"
        new_div = Forex(
            currency=row.currency,
            date=row.date,
            amount=gross_amount,
            comment="Dividend Payment",
        )
        new_tax = Forex(
            currency=row.currency,
            date=row.date,
            amount=tax_amount,
            comment="Withheld Tax on Dividends",
        )
        return row.symbol, new_div, new_tax

    def __repr__(self):
        return f"{self.currency}(Date: {self.date}, Amount: {self.amount:.2f})"


class StockSplit:
    def __init__(self, symbol, date, shares_after_split):
        self.symbol = symbol
        self.date = date
        # for each 1 existing shares, you receive shares_after_slit
        # in case e.g. a split was 3:2, this corresponds to 1.5
        # reverse splits would be ratios < 1.0
        self.shares_after_split = shares_after_split

    @staticmethod
    def from_split_csv_row(row):
        return StockSplit(
            symbol=row.symbol, date=row.date, shares_after_split=row.shares_after_split
        )


# base class representing an arbitrary asset subject to FIFO treatment
class FIFOObject:
    def __init__(self, symbol, quantity, buy_date, buy_price, currency):
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
    def __init__(self, currency, quantity, buy_date, source):
        # "money" is always a single unit "money"
        super().__init__(currency, quantity, buy_date, 1, currency)
        self.source = source  # e.g. "sale", "deposit", "dividend", etc.

    @staticmethod
    def from_dividends_row(row):
        gross_quantity = to_decimal(row.amount)
        tax_quantity = to_decimal(row.tax_withholding)
        net_quantity = gross_quantity - tax_quantity
        assert gross_quantity > 0, "Expected positive Gross Quantity!"
        assert (
            0 <= tax_quantity <= gross_quantity
        ), "Expected non-negative Tax Withholding!"
        assert 0 <= net_quantity <= gross_quantity, "Expected non-negative Net Quantity"
        # if quantity > 0, all assets share the same, i.e. just duplicate them
        new_forex = FIFOForex(
            currency=row.currency,
            quantity=net_quantity,
            buy_date=row.date,
            source="Dividend Payment",
        )

        return row.currency, new_forex

    @staticmethod
    def from_share_sale(row):
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

        return row.currency, new_forex

    def __repr__(self):
        return f"{self.symbol}(Quantity: {self.quantity:.2f}, Buy-Date: {self.buy_date:%Y-%b-%d})"


# class representing a share subject to FIFO treatment
class FIFOShare(FIFOObject):
    def __init__(self, symbol, quantity, buy_date, buy_price, currency):
        super().__init__(symbol, quantity, buy_date, buy_price, currency)

    @staticmethod
    def from_deposits_row(row):
        if row.net_quantity <= 0:
            raise ValueError("Expected positive quantity of assets!")
        # if quantity > 0, all assets share the same, i.e. just duplicate them
        new_asset = FIFOShare(
            symbol=row.symbol,
            quantity=to_decimal(row.net_quantity),
            buy_date=row.date,
            buy_price=to_decimal(row.fmv_or_buy_price),
            currency=row.currency,
        )
        return row.symbol, new_asset

    def __repr__(self):
        return f"{self.symbol}(Quantity: {self.quantity}, Buy-Date: {self.buy_date:%Y-%b-%d}, Buy-Price: {self.buy_price:.2f} {self.currency})"


class FIFOQueue:
    def __init__(self):
        self.assets = []
        self.total_quantity = to_decimal(0)

    def apply_split(self, split):
        self.total_quantity = to_decimal(0)
        for asset in self.assets:
            if asset.buy_date <= split.date:
                asset.quantity = asset.quantity * to_decimal(split.shares_after_split)
                asset.buy_price = asset.buy_price / to_decimal(split.shares_after_split)
            self.total_quantity = self.total_quantity + asset.quantity

    def push(self, asset):
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
        return self.assets[0]

    def pop(self, quantity, sell_date):
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
            symbol = self.assets[0].symbol
            asset_type = self.assets[0].__class__.__name__
            if asset_type == "FIFOShare":
                raise ValueError(
                    f"Cannot sell more {symbol} shares ({quantity}) than owned overall ({self.total_quantity})."
                )
            elif asset_type == "FIFOForex":
                raise ValueError(
                    f"Cannot convert more {symbol} ({quantity}) than owned overall ({self.total_quantity})."
                )
            else:
                raise ValueError(
                    f"Cannot pop quantity ({quantity}) larger than all quantities ({self.total_quantity}) in FIFOQueue!"
                )

        if self.assets[0].buy_date > sell_date:
            # Relying on the fact that "assets" are sorted by date
            symbol = self.assets[0].symbol
            raise ValueError(
                f"Cannot sell the requested {symbol} equity because on the sell transaction date "
                f"({sell_date.strftime('%Y-%m-%d')}) the requested amount is not available"
            )

        front_quantity = self.assets[0].quantity
        if quantity < front_quantity:
            pop_asset = from_asset(self.assets[0], quantity)
            self.assets[0].quantity -= quantity
            self.total_quantity -= quantity
            return [pop_asset]
        elif math.isclose(quantity, front_quantity):
            self.total_quantity -= front_quantity
            return [self.assets.pop(0)]
        else:
            # quantity is larger
            # pop first item, then call pop on remaining quantity
            pop_asset = self.assets.pop(0)
            self.total_quantity -= pop_asset.quantity
            remaining_quantity = quantity - pop_asset.quantity
            return [pop_asset] + self.pop(remaining_quantity, sell_date)

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
