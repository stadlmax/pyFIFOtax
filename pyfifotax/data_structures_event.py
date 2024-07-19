from __future__ import annotations

import decimal
import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

import pandas as pd
from pandas.core.series import Series

from pyfifotax.data_structures_fifo import FIFOForex, FIFOShare, Forex
from pyfifotax.data_structures_dataframe import (
    ESPPRow,
    RSURow,
    BuyOrderRow,
    SellOrderRow,
    DividendRow,
    CurrencyConversionRow,
    MoneyTransferRow,
    StockSplitRow,
)
from pyfifotax.utils import to_decimal, round_decimal, get_daily_rate


class EventPriority(Enum):
    EMPTY: int = 0
    ESPP: int = 0
    RSU: int = 0
    DIVIDEND: int = 1
    TAX: int = 2
    MONEY_DEPOSIT: int = 3
    CURRENCY_CONVERSION_FROM_EUR_TO_FOREX = 4
    SELL: int = 5
    CURRENCY_CONVERSION_FROM_FOREX_TO_FOREX = 6
    BUY: int = 7
    CURRENCY_CONVERSION_FROM_FOREX_TO_EUR = 8
    MONEY_WITHDRAWAL: int = 9
    STOCK_SPLIT: int = 10  # at the end as assumed after market-close


class ReportEvent:
    def __init__(self, date: datetime.date, priority: EventPriority):
        self.date = date
        self.priority = priority.value

    @staticmethod
    def from_df_row(df_row: Series, **kwargs) -> ReportEvent:
        raise NotImplementedError

    @classmethod
    def from_report(cls, df: pd.DataFrame, **kwargs) -> list[ReportEvent]:
        events = []
        for _, row in df.iterrows():
            events.append(cls.from_df_row(row, **kwargs))
        return events

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} on {self.date}"


class RSUEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        symbol: str,
        received_shares: FIFOShare,
        withheld_shares: Optional[FIFOShare],
    ):
        super().__init__(date, EventPriority.RSU)
        self.symbol = symbol
        self.received_shares = received_shares
        self.withheld_shares = withheld_shares

    @staticmethod
    def from_df_row(df_row: Series, **kwargs) -> RSUEvent:
        row = RSURow.from_df_row(df_row)
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

    def __repr__(self):
        shares = self.received_shares
        total = shares.quantity * shares.buy_price
        withheld = f" (+{self.withheld_shares.quantity:g} shares withheld)"
        return f"RSU vested on {self.date}: {shares.quantity:g} {self.symbol} for {total:.2f} {shares.currency}{withheld}"


class DividendEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        currency: str,
        received_dividend: Optional[Forex],
    ):
        super().__init__(date, EventPriority.DIVIDEND)
        self.currency = currency
        self.received_dividend = received_dividend

    @staticmethod
    def from_df_row(df_row: Series, **kwargs) -> DividendEvent:
        row = DividendRow.from_df_row(df_row)
        gross_amount = to_decimal(row.amount)

        if gross_amount != to_decimal(0):
            div = Forex(
                currency=row.currency,
                date=row.date,
                amount=gross_amount,
                comment=f"Dividend Payment ({row.symbol})",
            )
            return DividendEvent(row.date, row.currency, div)

        return DividendEvent(row.date, row.currency, None)

    def __repr__(self):
        amount = f": {self.received_dividend.amount:.2f} {self.received_dividend.currency}" if self.received_dividend else ""
        return f"Dividend received on {self.date}{amount}"


class TaxEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        currency: str,
        withheld_tax: Optional[Forex],
        reverted_tax: Optional[FIFOForex],
    ):
        super().__init__(date, EventPriority.TAX)
        self.currency = currency
        self.withheld_tax = withheld_tax
        self.reverted_tax = reverted_tax

    @staticmethod
    def from_df_row(df_row: Series, **kwargs) -> TaxEvent:
        row = DividendRow.from_df_row(df_row)
        withheld_tax = to_decimal(row.tax_withholding)
        if withheld_tax > 0:
            withheld_tax = Forex(
                currency=row.currency,
                date=row.date,
                amount=withheld_tax,
                comment=f"Tax Withholding ({row.symbol})",
            )

            return TaxEvent(
                row.date,
                row.currency,
                withheld_tax,
                None,
            )

        elif withheld_tax < 0:
            reverted_tax = FIFOForex(
                currency=row.currency,
                buy_date=row.date,
                quantity=-withheld_tax,
                source=f"Tax Reversal ({row.symbol})",
                tax_free_forex=True,
            )

            return TaxEvent(
                row.date,
                row.currency,
                None,
                reverted_tax,
            )

        return TaxEvent(
            row.date,
            row.currency,
            None,
            None,
        )

    def __repr__(self):
        if not self.withheld_tax and not self.reverted_tax:
            return ""

        if self.withheld_tax:
            tax_type = "withholding"
            amount = f"{self.withheld_tax.amount:.2f} {self.withheld_tax.currency} tax withheld"
        else:
            tax_type = "reversal"
            amount = f"{self.reverted_tax.quantity:.2f} {self.reverted_tax.currency} tax reverted"

        return f"Tax {tax_type} on {self.date}: {amount}"


class ESPPEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        symbol: str,
        currency: str,
        received_shares: FIFOShare,
        contribution: decimal.Decimal,
        bonus: decimal.Decimal,
    ):
        super().__init__(date, EventPriority.ESPP)
        self.symbol = symbol
        self.currency = currency

        self.contribution = contribution
        self.bonus = bonus
        self.received_shares = received_shares

    @staticmethod
    def from_df_row(df_row: Series, **kwargs) -> ESPPEvent:
        row = ESPPRow.from_df_row(df_row)
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

    def __repr__(self):
        shares = self.received_shares
        quantity = shares.quantity
        currency = shares.currency
        total = quantity * shares.buy_price
        bonus = f"(out of which the bonus is {self.bonus:.2f} {currency})"
        return f"ESPP received on {self.date}: {quantity:g} {self.symbol} for {total:.2f} {currency} {bonus}"


class BuyEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        symbol: str,
        received_shares: FIFOShare,
        cost_of_shares: decimal.Decimal,
        paid_fees: Optional[Forex],
        currency: str,
    ):
        super().__init__(date, EventPriority.BUY)
        self.symbol = symbol
        self.received_shares = received_shares
        self.cost_of_shares = cost_of_shares
        self.paid_fees = paid_fees
        self.currency = currency

    @staticmethod
    def from_df_row(df_row: Series, **kwargs) -> BuyEvent:
        row = BuyOrderRow.from_df_row(df_row)
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

        if fees < to_decimal(0.0):
            msg = f"For Transaction on {row.date}, fee of {fees} {row.currency} is negative."
            raise ValueError(msg)

        if fees > to_decimal(0.0):
            paid_fees = Forex(
                currency=row.fee_currency,
                date=row.date,
                amount=fees,
                comment=f"Fees for Buy Order ({quantity:.2f} x {row.symbol})",
            )
            recv_shares.buy_cost = paid_fees.amount / recv_shares.quantity
            recv_shares.buy_cost_currency = row.fee_currency
        else:
            paid_fees = None

        # schwab sometimes has sub-cent share prices
        # e.g. 100.7438, the deposited value, however,
        # is rounded to full cents
        # TODO: check if rounding mode matches schwab
        cost_of_shares = round_decimal(buy_price * quantity, precision="0.01")

        return BuyEvent(
            row.date,
            row.symbol,
            recv_shares,
            cost_of_shares,
            paid_fees,
            currency=row.currency,
        )

    def __repr__(self):
        shares = self.received_shares
        details = f"{shares.quantity:.2f} {shares.symbol} for {shares.buy_price:.2f} {shares.currency}"
        fees = f" for {self.paid_fees.amount:.2f} {self.paid_fees.currency} additional fee" if self.paid_fees else ""
        return f"Bought securities on {self.date}: {details} (Σ {self.cost_of_shares:.2f} {shares.currency}){fees}"


class SellEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        symbol: str,
        currency: str,
        quantity: decimal.Decimal,
        sell_price: decimal.Decimal,
        received_forex: FIFOForex,
        paid_fees: Optional[Forex],
    ):
        super().__init__(date, EventPriority.SELL)
        self.symbol = symbol
        self.currency = currency
        self.quantity = quantity
        self.sell_price = sell_price
        self.received_forex = received_forex
        self.paid_fees = paid_fees

    @staticmethod
    def from_df_row(df_row: Series, **kwargs) -> SellEvent:
        row = SellOrderRow.from_df_row(df_row)
        sell_price = to_decimal(row.sell_price)
        quantity = to_decimal(row.quantity)
        fees = to_decimal(row.fees)
        # schwab sometimes has sub-cent share prices
        # e.g. 100.7438, the deposited value, however,
        # is rounded to full cents
        # TODO: check if rounding mode matches schwab
        proceeds = sell_price * quantity

        new_forex = FIFOForex(
            currency=row.currency,
            quantity=proceeds,
            buy_date=row.date,
            source="Sales Proceeds",
        )
        if fees < to_decimal(0.0):
            msg = f"For Transaction on {row.date}, fee of {fees} {row.currency} is negative."
            raise ValueError(msg)

        if fees > to_decimal(0.0):
            fees = Forex(
                currency=row.fee_currency,
                date=row.date,
                amount=fees,
                comment=f"Fees for Sell Order ({quantity:.2f} x {row.symbol})",
            )
        else:
            fees = None

        return SellEvent(
            row.date, row.symbol, row.currency, quantity, sell_price, new_forex, fees
        )

    def __repr__(self):
        fees = f" for {self.paid_fees.amount:.2f} {self.paid_fees.currency} additional fee" if self.paid_fees else ""
        total = f" (Σ {self.sell_price * self.quantity:.2f} {self.currency})"
        return f"Sold securities on {self.date}: {self.quantity} {self.symbol}{total}{fees}"


class CurrencyConversionEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        source_amount: decimal.Decimal,
        source_currency: str,
        target_amount: decimal.Decimal,
        target_currency: str,
        fees: Optional[Forex],
        priority: EventPriority,
    ):
        super().__init__(date, priority)
        self.source_amount = source_amount
        self.source_currency = source_currency
        self.target_amount = target_amount
        self.target_currency = target_currency
        self.fees = fees

    @staticmethod
    def from_df_row(df_row: Series, **kwargs) -> CurrencyConversionEvent:
        daily_rates = kwargs["daily_rates"]
        row = CurrencyConversionRow.from_df_row(df_row)
        if row.fees < 0.0:
            msg = f"For Transaction on {row.date}, fee of {row.fees} {row.source_currency} is negative."
            raise ValueError(msg)

        if row.fees > 0.0:
            fees = Forex(
                currency=row.fee_currency,
                date=row.date,
                amount=to_decimal(row.fees),
                comment=f"Fees for converting {row.source_currency} to {row.target_currency}",
            )
        else:
            fees = None

        # conversion from EUR to FOREX
        if row.source_currency == "EUR" and row.target_currency != "EUR":
            if row.source_amount < 0.0:
                source_amount = to_decimal(row.target_amount) / get_daily_rate(
                    daily_rates, row.date, row.source_currency
                )
            else:
                source_amount = to_decimal(row.source_amount)

            return CurrencyConversionEvent(
                row.date,
                source_amount,
                "EUR",
                to_decimal(row.target_amount),
                row.target_currency,
                fees,
                EventPriority.CURRENCY_CONVERSION_FROM_EUR_TO_FOREX,
            )

        # conversion from FOREX to EUR
        if row.source_currency != "EUR" and row.target_currency == "EUR":
            if row.target_amount < 0.0:
                target_amount = to_decimal(row.source_amount) / get_daily_rate(
                    daily_rates, row.date, row.source_currency
                )
            else:
                target_amount = to_decimal(row.target_amount)

            return CurrencyConversionEvent(
                row.date,
                to_decimal(row.source_amount),
                row.source_currency,
                target_amount,
                "EUR",
                fees,
                EventPriority.CURRENCY_CONVERSION_FROM_FOREX_TO_EUR,
            )

        # conversion from FOREX to FOREX
        if row.source_currency != "EUR" and row.target_currency != "EUR":
            if row.source_amount < 0.0 or row.target_amount < 0.0:
                raise ValueError(
                    "Conversions between two different FOREX must include explicit source and target amounts."
                )

            return CurrencyConversionEvent(
                row.date,
                to_decimal(row.source_amount),
                row.source_currency,
                to_decimal(row.target_amount),
                row.target_currency,
                fees,
                EventPriority.CURRENCY_CONVERSION_FROM_FOREX_TO_FOREX,
            )

        msg = (
            "Detected Currency Conversion from EUR to EUR, aborting processing of data."
            " In case you want to specify a deposit or a withdrawal of EUR, check the tab for money transfers."
        )
        raise ValueError(msg)

    def __repr__(self):
        fees = f"for an additional {self.fees.amount:.2f} {self.fees.currency} fee" if self.fees else ""

        from_fx = f"from {self.source_amount:.2f} {self.source_currency}"
        to_fx = f"to {self.target_amount:.2f} {self.target_currency}"
        return f"Converted money on {self.date}: {from_fx} {to_fx} {fees}"


class MoneyTransferEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        buy_date: datetime.date,
        amount: decimal.Decimal,
        fees: Optional[Forex],
        currency: str,
        priority: EventPriority,
    ):
        super().__init__(date, priority)
        self.buy_date = buy_date
        self.amount = amount
        self.fees = fees
        self.currency = currency

    @staticmethod
    def from_df_row(df_row: Series, **kwargs) -> MoneyTransferEvent:
        row = MoneyTransferRow.from_df_row(df_row)
        if row.fees > 0.0:
            fee_comment = "Fees for Transfer"
            if row.comment != "":
                fee_comment += f" ({row.comment})"
            fees = Forex(
                currency=row.fee_currency,
                date=row.date,
                amount=to_decimal(row.fees),
                comment=fee_comment,
            )
        else:
            fees = None

        amount = to_decimal(row.amount)
        if amount > 0:
            return MoneyDepositEvent(row.date, row.buy_date, amount, fees, row.currency)

        return MoneyWithdrawalEvent(row.date, row.buy_date, -amount, fees, row.currency)


class MoneyDepositEvent(MoneyTransferEvent):
    def __init__(
        self,
        date: datetime.date,
        buy_date: datetime.date,
        amount: decimal.Decimal,
        fees: Optional[Forex],
        currency: str,
    ):
        super().__init__(
            date, buy_date, amount, fees, currency, EventPriority.MONEY_DEPOSIT
        )

    def __repr__(self):
        amount = abs(self.amount)
        acquired = f" which was acquired on {self.buy_date}" if self.buy_date.year >= 2009 else ""
        fees = f" for {self.fees} {self.fees.currency} additional fee" if self.fees else ""
        return f"Deposited money on {self.date}: {amount:.2f} {self.currency}{acquired}{fees}"


class MoneyWithdrawalEvent(MoneyTransferEvent):
    def __init__(
        self,
        date: datetime.date,
        buy_date: datetime.date,
        amount: decimal.Decimal,
        fees: Optional[Forex],
        currency: str,
    ):
        super().__init__(
            date, buy_date, amount, fees, currency, EventPriority.MONEY_WITHDRAWAL
        )

    def __repr__(self):
        amount = abs(self.amount)
        acquired = f" which was acquired on {self.buy_date}" if self.buy_date.year >= 2009 else ""
        fees = f" for {self.fees.amount:.2f} {self.fees.currency} additional fee" if self.fees else ""
        return f"Withdrew money on {self.date}: {amount:.2f} {self.currency}{acquired}{fees}"


class StockSplitEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        symbol: str,
        shares_after_split: Decimal,
    ):
        super().__init__(date, EventPriority.STOCK_SPLIT)
        self.symbol = symbol
        self.shares_after_split = shares_after_split

    @staticmethod
    def from_df_row(df_row: Series, **kwargs) -> StockSplitEvent:
        row = StockSplitRow.from_df_row(df_row)
        return StockSplitEvent(
            date=row.date,
            symbol=row.symbol,
            shares_after_split=to_decimal(row.shares_after_split),
        )

    def __repr__(self):
        return f"Stock split happened on {self.date}: 1 {self.symbol} became {self.shares_after_split:.2f}"
