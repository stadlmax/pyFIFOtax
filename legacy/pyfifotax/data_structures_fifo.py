import math
import decimal
import datetime
import warnings
from typing import Optional

from pyfifotax.utils import to_decimal


# base class for handling currency-valued items
# e.g. for keeping track of tax payments or
# bookkeeping of other monetary items
class Forex:
    def __init__(
        self, currency: str, date: datetime.date, amount: decimal.Decimal, comment: str
    ):
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
        quantity: decimal.Decimal,
        buy_date: datetime.date,
        buy_price: decimal.Decimal,
        currency: str,
    ):
        self.symbol: str = symbol
        self.currency: str = currency
        self.quantity: decimal.Decimal = quantity
        self.buy_date: datetime.date = buy_date
        self.sell_date: Optional[datetime.date] = None
        self.buy_price: decimal.Decimal = buy_price
        self.buy_price_eur_daily: Optional[decimal.Decimal] = None
        self.buy_price_eur_monthly: Optional[decimal.Decimal] = None
        self.buy_cost_currency: Optional[str] = None
        self.buy_cost: Optional[decimal.Decimal] = None
        self.buy_cost_eur_daily: Optional[decimal.Decimal] = None
        self.buy_cost_eur_monthly: Optional[decimal.Decimal] = None
        self.sell_price: Optional[decimal.Decimal] = None
        self.sell_price_eur_daily: Optional[decimal.Decimal] = None
        self.sell_price_eur_monthly: Optional[decimal.Decimal] = None
        self.sell_cost_currency: Optional[str] = None
        self.sell_cost: Optional[decimal.Decimal] = None
        self.sell_cost_eur_daily: Optional[decimal.Decimal] = None
        self.sell_cost_eur_monthly: Optional[decimal.Decimal] = None
        self.gain_eur_daily: Optional[decimal.Decimal] = None
        self.gain_eur_monthly: Optional[decimal.Decimal] = None
        self.cost_eur_daily: Optional[decimal.Decimal] = None
        self.cost_eur_monthly: Optional[decimal.Decimal] = None
        self.tax_free_forex = False

    def total_buy_value(self):
        return self.quantity * self.buy_price

    def total_sell_value(self):
        return self.quantity * self.sell_price


# class representing a FOREX object subject to FIFO treatment
class FIFOForex(FIFOObject):
    def __init__(
        self,
        currency: str,
        quantity: decimal.Decimal,
        buy_date: datetime.date,
        source: str,
        tax_free_forex: bool = False,
    ):
        # "money" is always a single unit "money"
        super().__init__(currency, quantity, buy_date, to_decimal(1), currency)
        self.source = source  # e.g. "sale", "deposit", "dividend", etc.
        # some FOREX (e.g. received dividends might not need to be declared)
        self.tax_free_forex = tax_free_forex

    def __repr__(self):
        return f"{self.symbol}(Quantity: {self.quantity:.2f}, Buy-Date: {self.buy_date:%Y-%b-%d})"


# class representing a share subject to FIFO treatment
class FIFOShare(FIFOObject):
    def __init__(self, symbol, quantity, buy_date, buy_price, currency):
        super().__init__(symbol, quantity, buy_date, buy_price, currency)

    def __repr__(self):
        return f"{self.symbol}(Quantity: {self.quantity}, Buy-Date: {self.buy_date:%Y-%b-%d}, Buy-Price: {self.buy_price:.2f} {self.currency})"


class FIFOQueue:
    def __init__(self, is_eur_queue: bool = False):
        self.assets: list[FIFOObject] = []
        self.total_quantity: decimal.Decimal = to_decimal(0)
        self.is_eur_queue = is_eur_queue
        if self.is_eur_queue:
            self.assets.append(
                FIFOForex("EUR", to_decimal(0), datetime.date(1, 1, 1), "init")
            )

    def apply_split(self, shares_after_split: decimal.Decimal):
        if self.is_eur_queue:
            return

        self.total_quantity = to_decimal(0)
        for asset in self.assets:
            asset.quantity = asset.quantity * shares_after_split
            asset.buy_price = asset.buy_price / shares_after_split
            self.total_quantity = self.total_quantity + asset.quantity

    def push(self, asset: FIFOObject):
        if self.is_empty():
            self.assets = [asset]

        else:
            if self.is_eur_queue:
                self.assets[0].quantity += asset.quantity
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

    def pop(
        self,
        quantity: decimal.Decimal,
        sell_price: decimal.Decimal,
        sell_date: datetime.date,
        sell_cost: Optional[decimal.Decimal] = None,
        sell_cost_currency: Optional[str] = None,
    ):
        if self.is_eur_queue:
            pop_asset = from_asset(self.peek(), quantity)
            # not really relevant, but leave them anyways
            pop_asset.sell_price = sell_price
            pop_asset.sell_date = sell_date
            if sell_cost is not None:
                pop_asset.sell_cost = sell_cost
                pop_asset.sell_cost_currency = sell_cost_currency
            self.peek().quantity -= quantity
            self.total_quantity -= quantity
            return [pop_asset]

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
                # with FOREX, there might be some rounding errors, to avoid
                # erroring out for rather small differences, we'll allow for
                # rounding errors below 1 currency unit
                if quantity < self.total_quantity + to_decimal(1.0):
                    msg = f"Trying to convert {quantity:.2f} {symbol}"
                    msg += f" despite only owning {self.total_quantity} {symbol}."
                    msg += " Assuming that this minor difference comes from rounding errors, proceeding with rounding down."
                    warnings.warn(msg)
                    return self.pop(self.total_quantity, sell_price, sell_date)
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
            if sell_cost is not None:
                pop_asset.sell_cost = sell_cost
                pop_asset.sell_cost_currency = sell_cost_currency
            self.peek().quantity -= quantity
            self.total_quantity -= quantity
            return [pop_asset]
        elif math.isclose(quantity, front_quantity):
            self.total_quantity -= front_quantity
            pop_asset = self.assets.pop(0)
            pop_asset.sell_date = sell_date
            pop_asset.sell_price = sell_price
            if sell_cost is not None:
                pop_asset.sell_cost = sell_cost
                pop_asset.sell_cost_currency = sell_cost_currency
            return [pop_asset]
        else:
            # quantity is larger
            # pop first item, then call pop on remaining quantity
            pop_asset = self.assets.pop(0)
            pop_asset.sell_price = sell_price
            pop_asset.sell_date = sell_date
            if sell_cost is not None:
                pop_asset.sell_cost = sell_cost
                pop_asset.sell_cost_currency = sell_cost_currency
            self.total_quantity -= pop_asset.quantity
            remaining_quantity = quantity - pop_asset.quantity
            return [pop_asset] + self.pop(
                remaining_quantity,
                sell_price,
                sell_date,
                sell_cost=sell_cost,
                sell_cost_currency=sell_cost_currency,
            )

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
