"""
Event data structures for FIFO tax calculations - refactored with correct split adjustments
"""

import datetime
from decimal import Decimal
from enum import IntEnum
from typing import Optional, Dict, Any
from .historic_prices import historic_price_manager


def format_currency(
    amount: Optional[Decimal], currency: str = "USD", decimals: int = 2
) -> str:
    """Format decimal amount as currency string."""
    if amount is None:
        return f"0.00 {currency}"
    return f"{amount:.{decimals}f} {currency}"


def format_quantity(quantity: Optional[Decimal], decimals: int = 4) -> str:
    """Format decimal quantity as string."""
    if quantity is None:
        return "0.0000"
    return f"{quantity:.{decimals}f}"


def format_price(
    price: Optional[Decimal], currency: str = "USD", decimals: int = 2
) -> str:
    """Format decimal price as currency string."""
    if price is None:
        return f"0.00 {currency}"
    return f"{price:.{decimals}f} {currency}"


class EventPriority(IntEnum):
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
        self.priority = priority

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.date})"

    def get_event_type(self) -> str:
        """Get the event type for UI display - remove 'Event' suffix"""
        class_name = self.__class__.__name__
        if class_name.endswith("Event"):
            return class_name[:-5]  # Remove "Event" suffix
        return class_name

    def get_long_form_type(self) -> str:
        raise NotImplementedError

    def apply_split_adjustment(self, split_factor: Decimal):
        pass


class RSUEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        symbol: str,
        received_shares_quantity: Decimal,
        received_shares_price: Decimal,
        withheld_shares_quantity: Optional[Decimal],
        currency: str,
        grant_id: Optional[str] = None,
    ):
        super().__init__(date, EventPriority.RSU)
        self.symbol = symbol
        self.currency = currency
        self.grant_id = grant_id  # Track grant/award ID for duplicate detection

        self.imported_received_shares_quantity = received_shares_quantity
        self.imported_received_shares_price = received_shares_price
        self.imported_withheld_shares_quantity = withheld_shares_quantity

        is_historic, hist_price = historic_price_manager.is_price_historic(
            received_shares_price, symbol, date
        )

        if is_historic:
            self.historic_received_shares_quantity = received_shares_quantity
            self.historic_received_shares_price = received_shares_price
            self.historic_withheld_shares_quantity = withheld_shares_quantity
        else:
            split_factor = round(float(hist_price / received_shares_price))
            assert split_factor > Decimal("0"), "RSUEvent: split_factor is 0"
            self.historic_received_shares_price = received_shares_price * Decimal(
                str(split_factor)
            )
            self.historic_received_shares_quantity = received_shares_quantity / Decimal(
                str(split_factor)
            )
            self.historic_withheld_shares_quantity = (
                withheld_shares_quantity / Decimal(str(split_factor))
                if withheld_shares_quantity
                else None
            )

    def get_long_form_type(self) -> str:
        return "RSU Vesting"


class DividendEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        symbol: str,
        currency: str,
        dividend_amount: Optional[Decimal],
    ):
        super().__init__(date, EventPriority.DIVIDEND)
        self.symbol = symbol
        self.currency = currency
        self.dividend_amount = dividend_amount

    def get_long_form_type(self) -> str:
        return "Dividend Payment"


class TaxEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        symbol: str,
        currency: str,
        withheld_tax_amount: Optional[Decimal],
        reverted_tax_amount: Optional[Decimal],
    ):
        super().__init__(date, EventPriority.TAX)
        self.symbol = symbol
        self.currency = currency
        self.withheld_tax_amount = withheld_tax_amount
        self.reverted_tax_amount = reverted_tax_amount

    def get_long_form_type(self) -> str:
        return "Tax Withholding"


class ESPPEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        symbol: str,
        currency: str,
        shares_quantity: Decimal,
        shares_price: Decimal,
        fair_market_value: Decimal,
    ):
        super().__init__(date, EventPriority.ESPP)
        self.symbol = symbol
        self.currency = currency

        self.imported_shares_quantity = shares_quantity
        self.imported_shares_price = shares_price
        self.imported_fair_market_value = fair_market_value

        self.contribution = shares_quantity * shares_price
        self.bonus = shares_quantity * (fair_market_value - shares_price)

        is_historic, hist_price = historic_price_manager.is_price_historic(
            fair_market_value, symbol, date
        )

        if is_historic:
            self.historic_shares_quantity = shares_quantity
            self.historic_shares_price = shares_price
            self.historic_fair_market_value = fair_market_value
        else:
            split_factor = round(float(hist_price / fair_market_value))
            self.historic_shares_price = shares_price * Decimal(str(split_factor))
            self.historic_fair_market_value = fair_market_value * Decimal(
                str(split_factor)
            )
            self.historic_shares_quantity = shares_quantity / Decimal(str(split_factor))

    def get_long_form_type(self) -> str:
        return "ESPP Purchase"


class BuyEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        symbol: str,
        shares_quantity: Decimal,
        shares_price: Decimal,
        cost_of_shares: Decimal,
        fees: Optional[Decimal],
        currency: str,
    ):
        super().__init__(date, EventPriority.BUY)
        self.symbol = symbol
        self.currency = currency
        self.cost_of_shares = cost_of_shares
        self.fees = fees

        self.imported_shares_quantity = shares_quantity
        self.imported_shares_price = shares_price

        is_historic, hist_price = historic_price_manager.is_price_historic(
            shares_price, symbol, date
        )

        if is_historic:
            self.historic_shares_quantity = shares_quantity
            self.historic_shares_price = shares_price
        else:
            split_factor = round(float(hist_price / shares_price))
            self.historic_shares_price = shares_price * Decimal(str(split_factor))
            self.historic_shares_quantity = shares_quantity / Decimal(str(split_factor))

    def get_long_form_type(self) -> str:
        return "Stock Purchase"


class SellEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        symbol: str,
        currency: str,
        quantity: Decimal,
        sell_price: Decimal,
        proceeds: Decimal,
        fees: Optional[Decimal],
        transaction_id: Optional[str] = None,
    ):
        super().__init__(date, EventPriority.SELL)
        self.symbol = symbol
        self.currency = currency
        self.proceeds = proceeds
        self.fees = fees
        self.transaction_id = (
            transaction_id  # General transaction ID (e.g., grant_id for RSU sales)
        )

        self.imported_quantity = quantity
        self.imported_sell_price = sell_price

        is_historic, hist_price = historic_price_manager.is_price_historic(
            sell_price, symbol, date
        )

        if is_historic:
            self.historic_quantity = quantity
            self.historic_sell_price = sell_price
        else:
            split_factor = round(float(hist_price / sell_price))
            self.historic_sell_price = sell_price * Decimal(str(split_factor))
            self.historic_quantity = quantity / Decimal(str(split_factor))

    def get_long_form_type(self) -> str:
        return "Stock Sale"


class CurrencyConversionEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        source_amount: Decimal,
        source_currency: str,
        target_amount: Decimal,
        target_currency: str,
        fees: Optional[Decimal],
        priority: EventPriority,
    ):
        super().__init__(date, priority)
        self.source_amount = source_amount
        self.source_currency = source_currency
        self.target_amount = target_amount
        self.target_currency = target_currency
        self.fees = fees

    def get_long_form_type(self) -> str:
        return "Currency Conversion"


class MoneyTransferEvent(ReportEvent):
    def __init__(
        self,
        date: datetime.date,
        buy_date: datetime.date,
        amount: Decimal,
        fees: Optional[Decimal],
        currency: str,
        priority: EventPriority,
    ):
        super().__init__(date, priority)
        self.buy_date = buy_date
        self.amount = amount
        self.fees = fees
        self.currency = currency

    def get_long_form_type(self) -> str:
        return "Money Transfer"


class MoneyDepositEvent(MoneyTransferEvent):
    def __init__(
        self,
        date: datetime.date,
        buy_date: datetime.date,
        amount: Decimal,
        fees: Optional[Decimal],
        currency: str,
    ):
        super().__init__(
            date, buy_date, amount, fees, currency, EventPriority.MONEY_DEPOSIT
        )

    def get_long_form_type(self) -> str:
        return "Money Deposit"


class MoneyWithdrawalEvent(MoneyTransferEvent):
    def __init__(
        self,
        date: datetime.date,
        buy_date: datetime.date,
        amount: Decimal,
        fees: Optional[Decimal],
        currency: str,
    ):
        super().__init__(
            date, buy_date, amount, fees, currency, EventPriority.MONEY_WITHDRAWAL
        )

    def get_long_form_type(self) -> str:
        return "Money Withdrawal"


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

    def get_long_form_type(self) -> str:
        return "Stock Split"
