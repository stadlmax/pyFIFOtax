import decimal
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pyfifotax.data_structures_fifo import FIFOForex, FIFOShare, Forex
from pyfifotax.data_structures_dataframe import (
    ESPPRow,
    RSURow,
    BuyOrderRow,
    SellOrderRow,
    DividendRow,
    CurrencyConversionRow,
    StockSplitRow,
)
from pyfifotax.utils import to_decimal, round_decimal


class ReportEvent:
    def __init__(self, date: datetime, priority: int):
        self.date = date
        # add priority to make sure that buy/deposit transactions
        # are processed before sell transactions
        # espp/rsu deposit/dividend: prio 0
        # buy: prio 1
        # sell: prio 2
        # stocksplit: 3 (at the end as assumed after market-close)
        # currency conversions: 4
        self.priority = priority

    @staticmethod
    def from_df_row(row):
        raise NotImplementedError

    @classmethod
    def from_report(cls, df):
        events = []
        for _, row in df.iterrows():
            events.append(cls.from_df_row(row))
        return events

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} on {self.date.date()}"


class RSUEvent(ReportEvent):
    def __init__(
        self,
        date: datetime,
        symbol: str,
        received_shares: FIFOShare,
        withheld_shares: FIFOShare,
    ):
        super().__init__(date, 0)
        self.symbol = symbol
        self.received_shares = received_shares
        self.withheld_shares = withheld_shares

    @staticmethod
    def from_df_row(row):
        row = RSURow.from_df_row(row)
        recv_share = FIFOShare(
            buy_date=row.date,
            symbol=row.symbol,
            buy_price=to_decimal(row.fair_market_value),
            quantity=to_decimal(row.net_quantity),
            currency=row.currency,
        )

        withheld_shares = FIFOShare(
            buy_date=row.date,
            symbol=row.symbol,
            buy_price=to_decimal(row.fair_market_value),
            quantity=to_decimal(row.gross_quantity) - to_decimal(row.net_quantity),
            currency=row.currency,
        )

        return RSUEvent(
            row.date,
            row.symbol,
            recv_share,
            withheld_shares,
        )


class DividendEvent(ReportEvent):
    def __init__(
        self,
        date: datetime,
        currency: str,
        received_dividend: Forex,
        received_net_dividend: FIFOForex,
        withheld_tax: Optional[Forex],
    ):
        super().__init__(date, 0)
        self.currency = currency
        self.received_dividend = received_dividend
        self.received_net_dividend = received_net_dividend
        self.withheld_tax = withheld_tax

    @staticmethod
    def from_df_row(row):
        row = DividendRow.from_df_row(row)
        gross_amount = to_decimal(row.amount)
        tax_amount = to_decimal(row.tax_withholding)
        net_amount = gross_amount - tax_amount
        if gross_amount < 0:
            raise ValueError(
                f"Expected Capital Gains to be non-negative but got {net_amount}."
            )
        if not (0 <= tax_amount <= gross_amount):
            raise ValueError(
                f"Expected Tax Withholding {tax_amount} to be non-negative and smaller than the capital gains {gross_amount}."
            )
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
        if tax_amount > 0:
            tax = Forex(
                currency=row.currency,
                date=row.date,
                amount=tax_amount,
                comment=f"Withheld Tax on Dividends ({row.symbol})",
            )
        else:
            tax = None
        return DividendEvent(row.date, row.currency, div, net_div, tax)


class ESPPEvent(ReportEvent):
    def __init__(
        self,
        date: datetime,
        symbol: str,
        currency: str,
        received_shares: FIFOShare,
        contribution: decimal,
        bonus: decimal,
    ):
        super().__init__(date, 0)
        self.symbol = symbol
        self.currency = currency

        self.contribution = contribution
        self.bonus = bonus
        self.received_shares = received_shares

    @staticmethod
    def from_df_row(row):
        row = ESPPRow.from_df_row(row)
        quantity = to_decimal(row.quantity)
        buy_price = to_decimal(row.buy_price)
        contribution = buy_price * quantity
        fair_market_value = to_decimal(row.fair_market_value)
        total_value = fair_market_value * quantity

        bonus = total_value - contribution

        received_shares = FIFOShare(
            symbol=row.symbol,
            quantity=quantity,
            buy_date=row.date,
            buy_price=fair_market_value,
            currency=row.currency,
        )

        return ESPPEvent(
            date=row.date,
            symbol=row.symbol,
            currency=row.currency,
            received_shares=received_shares,
            contribution=contribution,
            bonus=bonus,
        )


class BuyEvent(ReportEvent):
    def __init__(
        self,
        date: datetime,
        symbol: str,
        received_shares: FIFOShare,
        cost_of_shares: decimal,
        paid_fees: Optional[Forex],
        currency: str,
    ):
        super().__init__(date, 1)
        self.symbol = symbol
        self.received_shares = received_shares
        self.cost_of_shares = cost_of_shares
        self.paid_fees = paid_fees
        self.currency = currency

    @staticmethod
    def from_df_row(row):
        row = BuyOrderRow.from_df_row(row)
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

        if fees < 0:
            msg = f"For Transaction on {row.date}, fee of {fees} {row.currency} is negative."
            raise ValueError(msg)

        if fees > 0:
            paid_fees = Forex(
                currency=row.currency,
                date=row.date,
                amount=fees,
                comment=f"Fees for Buy Order ({quantity:.2f} x {row.symbol})",
            )
        else:
            paid_fees = None

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
        paid_fees: Optional[Forex],
    ):
        super().__init__(date, 2)
        self.symbol = symbol
        self.currency = currency
        self.quantity = quantity
        self.sell_price = sell_price
        self.received_forex = received_forex
        self.paid_fees = paid_fees

    @staticmethod
    def from_df_row(row):
        row = SellOrderRow.from_df_row(row)
        sell_price = to_decimal(row.sell_price)
        quantity = to_decimal(row.quantity)
        fees = to_decimal(row.fees)
        # schwab sometimes has sub-cent share prices
        # e.g. 100.7438, the deposited value, however,
        # is rounded to full cents
        # TODO: check if rounding mode matches schwab
        net_proceeds = sell_price * quantity - fees
        net_proceeds = round_decimal(net_proceeds)
        if not (net_proceeds >= 0):
            raise ValueError(
                f"Expected non-negative net proceeds from sale of shares but got {net_proceeds}"
            )
        new_forex = FIFOForex(
            currency=row.currency,
            quantity=net_proceeds,
            buy_date=row.date,
            source="Sales Proceeds",
        )
        if fees < 0:
            msg = f"For Transaction on {row.date}, fee of {fees} {row.currency} is negative."
            raise ValueError(msg)

        if fees > 0:
            fees = Forex(
                currency=row.currency,
                date=row.date,
                amount=fees,
                comment=f"Fees for Sell Order ({quantity:.2f} x {row.symbol})",
            )
        else:
            fees = None

        return SellEvent(
            row.date, row.symbol, row.currency, quantity, sell_price, new_forex, fees
        )


class CurrencyConversionEvent(ReportEvent):
    def __init__(
        self,
        date: datetime,
        foreign_amount: decimal,
        source_fees: Optional[Forex],
        source_currency: str,
        target_currency: str,
    ):
        super().__init__(date, 4)
        self.foreign_amount = foreign_amount
        self.source_fees = source_fees
        self.source_currency = source_currency
        self.target_currency = target_currency

    @staticmethod
    def from_df_row(row):
        row = CurrencyConversionRow.from_df_row(row)
        if row.source_fees < 0:
            msg = f"For Transaction on {row.date}, fee of {row.source_fees} {row.source_currency} is negative."
            raise ValueError(msg)

        if row.source_fees > 0:
            source_fees = Forex(
                currency=row.source_currency,
                date=row.date,
                amount=to_decimal(row.source_fees),
                comment=f"Fees for converting {row.source_currency} to {row.target_currency}",
            )
        else:
            source_fees = None

        return CurrencyConversionEvent(
            row.date,
            to_decimal(row.foreign_amount),
            source_fees,
            row.source_currency,
            row.target_currency,
        )


class StockSplitEvent(ReportEvent):
    def __init__(self, date: datetime, symbol: str, shares_after_split: Decimal):
        super().__init__(date, 3)
        self.symbol = symbol
        self.shares_after_split = shares_after_split

    @staticmethod
    def from_df_row(row):
        row = StockSplitRow.from_df_row(row)
        return StockSplitEvent(
            date=row.date,
            symbol=row.symbol,
            shares_after_split=to_decimal(row.shares_after_split),
        )
