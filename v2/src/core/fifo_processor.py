"""
FIFO processor for pyFIFOtax Modern UI
Implements First-In-First-Out logic for matching buy/sell transactions
Based on legacy FIFO implementation
"""

import math
import warnings
from decimal import Decimal
from datetime import date
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from collections import defaultdict

from src.core.historic_prices import HistoricPriceManager


@dataclass
class FIFODebugStep:
    """Represents a single step in FIFO processing for debugging"""

    step_number: int
    event_type: str
    event_date: date
    event_symbol: Optional[str]
    event_index: int  # Index of the event in the processing order
    operation: str  # "PUSH", "POP", "SPLIT", "STATE", "INITIAL", "NOTE", etc.
    description: str
    queue_type: str  # "SHARES", "FOREX"
    queue_symbol: str
    queue_state_before: Optional[List[Dict[str, Any]]]  # Queue state before operation
    queue_state_after: Optional[List[Dict[str, Any]]]  # Queue state after operation
    popped_objects: Optional[List[Dict[str, Any]]] = (
        None  # Objects that were popped during this operation
    )
    pushed_objects: Optional[List[Dict[str, Any]]] = (
        None  # Objects that were pushed during this operation
    )


@dataclass
class FIFOShare:
    """Represents a share holding subject to FIFO treatment"""

    symbol: str
    quantity: Decimal
    buy_date: date
    buy_price: Decimal  # Price per share in original currency (adjusted for splits)
    currency: str
    source: str = "Purchase"  # Description of how share was acquired
    sell_date: Optional[date] = None
    sell_price: Optional[Decimal] = None  # Price per share when sold
    sell_cost: Optional[Decimal] = None  # Transaction costs when selling
    sell_cost_currency: Optional[str] = None
    buy_cost: Optional[Decimal] = None  # Transaction costs when buying
    buy_cost_currency: Optional[str] = None

    # Split tracking fields
    original_buy_price: Optional[Decimal] = None  # Original price before any splits
    cumulative_split_factor: Decimal = Decimal(
        "1"
    )  # Cumulative split factor from all splits

    # EUR converted values (filled during processing)
    buy_price_eur_daily: Optional[Decimal] = None
    buy_price_eur_monthly: Optional[Decimal] = None
    sell_price_eur_daily: Optional[Decimal] = None
    sell_price_eur_monthly: Optional[Decimal] = None
    gain_eur_daily: Optional[Decimal] = None
    gain_eur_monthly: Optional[Decimal] = None
    cost_eur_daily: Optional[Decimal] = None
    cost_eur_monthly: Optional[Decimal] = None

    def total_buy_value(self) -> Decimal:
        """Total value when bought (quantity * price)"""
        return self.quantity * self.buy_price

    def total_sell_value(self) -> Decimal:
        """Total value when sold (quantity * price)"""
        if self.sell_price is None:
            return Decimal("0")
        return self.quantity * self.sell_price


@dataclass
class FIFOForex:
    """Represents a foreign currency holding subject to FIFO treatment"""

    currency: str
    quantity: Decimal
    buy_date: date
    source: str  # Description of how currency was acquired
    sell_date: Optional[date] = None
    sell_price: Decimal = Decimal("1")  # Always 1 for currency
    tax_free_forex: bool = False  # Some forex (dividends) may be tax-free

    # EUR converted values (filled during processing)
    buy_price_eur_daily: Optional[Decimal] = None
    buy_price_eur_monthly: Optional[Decimal] = None
    sell_price_eur_daily: Optional[Decimal] = None
    sell_price_eur_monthly: Optional[Decimal] = None
    gain_eur_daily: Optional[Decimal] = None
    gain_eur_monthly: Optional[Decimal] = None

    @property
    def symbol(self) -> str:
        """For compatibility with FIFOShare interface"""
        return self.currency

    @property
    def buy_price(self) -> Decimal:
        """For compatibility with FIFOShare interface"""
        return Decimal("1")

    def total_buy_value(self) -> Decimal:
        """Total value when bought"""
        return self.quantity

    def total_sell_value(self) -> Decimal:
        """Total value when sold"""
        return self.quantity


@dataclass
class SimpleForex:
    """Simple forex transaction record (not subject to FIFO)"""

    currency: str
    date: date
    amount: Decimal
    comment: str

    # EUR converted values (filled during processing)
    amount_eur_daily: Optional[Decimal] = None
    amount_eur_monthly: Optional[Decimal] = None


class FIFOQueue:
    """FIFO queue for managing assets (shares or forex)"""

    def __init__(self, is_eur_queue: bool = False):
        self.assets: List[Any] = []  # Can hold FIFOShare or FIFOForex
        self.total_quantity: Decimal = Decimal("0")
        self.is_eur_queue = is_eur_queue

        if self.is_eur_queue:
            # EUR queue starts with zero balance
            self.assets.append(FIFOForex("EUR", Decimal("0"), date(1, 1, 1), "init"))

    def is_empty(self) -> bool:
        """Check if queue is empty"""
        return len(self.assets) == 0

    def peek(self):
        """Look at first asset without removing it"""
        if self.is_empty():
            raise ValueError("Cannot peek first element from an empty queue.")
        return self.assets[0]

    def push(self, asset):
        """Add asset to queue in chronological order"""
        if self.is_empty():
            self.assets = [asset]
        else:
            if self.is_eur_queue:
                # EUR queue just accumulates
                self.assets[0].quantity += asset.quantity
            else:
                # Insert based on buy date (first in)
                idx = 0
                while (idx < len(self.assets)) and (
                    asset.buy_date > self.assets[idx].buy_date
                ):
                    idx += 1
                self.assets.insert(idx, asset)

        self.total_quantity += asset.quantity

    def pop(
        self,
        quantity: Decimal,
        sell_price: Decimal,
        sell_date: date,
        sell_cost: Optional[Decimal] = None,
        sell_cost_currency: Optional[str] = None,
    ) -> List[Any]:
        """Remove assets from queue following FIFO principle"""

        if self.is_eur_queue:
            pop_asset = self._create_asset_copy(self.peek(), quantity)
            pop_asset.sell_price = sell_price
            pop_asset.sell_date = sell_date
            if sell_cost is not None:
                pop_asset.sell_cost = sell_cost
                pop_asset.sell_cost_currency = sell_cost_currency
            self.peek().quantity -= quantity
            self.total_quantity -= quantity
            self._clear_dust_if_needed()
            return [pop_asset]

        if abs(quantity) < Decimal("1e-10"):
            return []

        if quantity < 0:
            raise ValueError("Cannot sell negative amount of equity")

        if quantity > 0 and self.is_empty():
            raise ValueError("Cannot sell equities because there isn't any owned")

        if quantity > self.total_quantity and not math.isclose(
            float(quantity), float(self.total_quantity)
        ):
            symbol = self.peek().symbol
            asset_type = self.peek().__class__.__name__
            if asset_type == "FIFOShare":
                raise ValueError(
                    f"Cannot sell more {symbol} shares ({quantity:.2f}) than owned overall ({self.total_quantity:.2f})."
                )
            elif asset_type == "FIFOForex":
                # Allow small rounding errors for forex
                if quantity < self.total_quantity + Decimal("1.0"):
                    # msg = f"Trying to convert {quantity:.2f} {symbol}"
                    # msg += f" despite only owning {self.total_quantity} {symbol}."
                    # msg += " Assuming minor difference from rounding errors, proceeding with available amount."
                    # warnings.warn(msg)
                    return self.pop(self.total_quantity, sell_price, sell_date)
                raise ValueError(
                    f"Cannot convert more {symbol} ({quantity:.2f}) than owned overall ({self.total_quantity:.2f})."
                )

        if self.peek().buy_date > sell_date:
            symbol = self.peek().symbol
            raise ValueError(
                f"Cannot sell {symbol} because on sell date ({sell_date}) the requested amount is not available"
            )

        front_quantity = self.peek().quantity
        if quantity < front_quantity:
            # Partial sale of first asset
            pop_asset = self._create_asset_copy(self.peek(), quantity)
            pop_asset.sell_price = sell_price
            pop_asset.sell_date = sell_date
            if sell_cost is not None:
                pop_asset.sell_cost = sell_cost
                pop_asset.sell_cost_currency = sell_cost_currency
            self.peek().quantity -= quantity
            self.total_quantity -= quantity
            self._clear_dust_if_needed()
            return [pop_asset]

        elif abs(quantity - front_quantity) < Decimal("1e-10"):
            # Exact match - sell entire first asset
            self.total_quantity -= front_quantity
            pop_asset = self.assets.pop(0)
            pop_asset.sell_date = sell_date
            pop_asset.sell_price = sell_price
            if sell_cost is not None:
                pop_asset.sell_cost = sell_cost
                pop_asset.sell_cost_currency = sell_cost_currency
            self._clear_dust_if_needed()
            return [pop_asset]

        else:
            # Need to sell across multiple assets
            pop_asset = self.assets.pop(0)
            pop_asset.sell_price = sell_price
            pop_asset.sell_date = sell_date
            if sell_cost is not None:
                pop_asset.sell_cost = sell_cost
                pop_asset.sell_cost_currency = sell_cost_currency
            self.total_quantity -= pop_asset.quantity
            remaining_quantity = quantity - pop_asset.quantity
            result = [pop_asset] + self.pop(
                remaining_quantity,
                sell_price,
                sell_date,
                sell_cost=sell_cost,
                sell_cost_currency=sell_cost_currency,
            )
            self._clear_dust_if_needed()
            return result

    def _clear_dust_if_needed(self):
        """Clear FOREX queue to 0 if less than 1 cent remains"""
        if (
            hasattr(self, "assets")
            and self.assets
            and hasattr(self.assets[0], "currency")  # Check if this is a FOREX queue
            and self.total_quantity > Decimal("0.0")
            and self.total_quantity < Decimal("0.01")
        ):
            self.total_quantity = Decimal("0")
            self.assets.clear()
            return True
        return False

    def apply_split(self, split_factor: Decimal):
        self.total_quantity = Decimal("0")
        for asset in self.assets:
            # Track original price on first split
            if isinstance(asset, FIFOShare) and asset.original_buy_price is None:
                asset.original_buy_price = asset.buy_price

            # Update quantities and prices
            asset.quantity = asset.quantity * split_factor
            asset.buy_price = asset.buy_price / split_factor

            # Update cumulative split factor for shares
            if isinstance(asset, FIFOShare):
                asset.cumulative_split_factor = (
                    asset.cumulative_split_factor * split_factor
                )

            self.total_quantity += asset.quantity

    def _create_asset_copy(self, original_asset, new_quantity):
        """Create a copy of an asset with new quantity"""
        if isinstance(original_asset, FIFOShare):
            return FIFOShare(
                symbol=original_asset.symbol,
                quantity=new_quantity,
                buy_date=original_asset.buy_date,
                buy_price=original_asset.buy_price,
                currency=original_asset.currency,
                source=original_asset.source,
                buy_cost=original_asset.buy_cost,
                buy_cost_currency=original_asset.buy_cost_currency,
                original_buy_price=original_asset.original_buy_price,
                cumulative_split_factor=original_asset.cumulative_split_factor,
            )
        elif isinstance(original_asset, FIFOForex):
            return FIFOForex(
                currency=original_asset.currency,
                quantity=new_quantity,
                buy_date=original_asset.buy_date,
                source=original_asset.source,
                tax_free_forex=original_asset.tax_free_forex,
            )
        else:
            raise ValueError(f"Unsupported asset type: {type(original_asset)}")


@dataclass
class AWVEntryZ4:
    """AWS Z4 entry for bonus payments"""

    date: date
    purpose: str
    value: Decimal
    currency: str
    is_incoming: bool
    symbol: str = ""
    value_eur: Optional[Decimal] = None
    awv_threshold_eur: Decimal = Decimal("12500")

    def set_threshold(self, threshold: int):
        """Set AWV reporting threshold"""
        self.awv_threshold_eur = Decimal(str(threshold))

    def as_dict(self) -> Optional[Dict[str, Any]]:
        """Convert to dictionary for DataFrame creation"""
        if self.value_eur is None:
            raise RuntimeError("Currency conversion not applied")

        if self.value_eur < self.awv_threshold_eur:
            return None

        value_eur_k = str(round(self.value_eur / Decimal("1000"), 1))
        is_nvidia = "NVDA" in self.symbol or "NVIDIA" in self.purpose

        return {
            "Meldezeitraum": f"{self.date.year}-{self.date.month}",
            "Zweck der Zahlung": self.purpose,
            "BA": 1,
            "Kennzahl": 521,
            "Land": "USA" if is_nvidia else "FILL OUT COUNTRY",
            "Land-Code": "US" if is_nvidia else "FILL OUT COUNTRY CODE",
            "Eingehende Zahlungen": value_eur_k if self.is_incoming else "",
            "Ausgehende Zahlungen": value_eur_k if not self.is_incoming else "",
        }


@dataclass
class AWVEntryZ10:
    """AWS Z10 entry for share transactions"""

    date: date
    comment: str
    quantity: Decimal
    value: Decimal
    currency: str
    is_incoming: bool
    symbol: str = ""
    value_eur: Optional[Decimal] = None
    awv_threshold_eur: Decimal = Decimal("12500")

    def set_threshold(self, threshold: int):
        """Set AWV reporting threshold"""
        self.awv_threshold_eur = Decimal(str(threshold))

    def as_dict(self) -> Optional[Dict[str, Any]]:
        """Convert to dictionary for DataFrame creation"""
        if self.value_eur is None:
            raise RuntimeError("Currency conversion not applied")

        if self.value_eur < self.awv_threshold_eur:
            return None

        value_eur_k = str(round(self.value_eur / Decimal("1000"), 1))
        is_nvidia = "NVDA" in self.symbol or "NVIDIA" in self.comment

        return {
            "Meldezeitraum": f"{self.date.year}-{self.date.month}",
            "Kennzahl": 104,
            "Stückzahl": int(self.quantity),
            "Bezeichnung der Wertpapiere": self.comment,
            "ISIN": "US67066G1040" if is_nvidia else "FILL OUT ISIN",
            "Land": "USA" if is_nvidia else "FILL OUT COUNTRY",
            "Land-Code": "US" if is_nvidia else "FILL OUT COUNTRY CODE",
            "Eingehende Zahlungen": value_eur_k if self.is_incoming else "",
            "Ausgehende Zahlungen": value_eur_k if not self.is_incoming else "",
            "Emissionswährung": (
                self.currency if is_nvidia else f"{self.currency} [VALIDATE CURRENCY]"
            ),
        }


class FIFOProcessor:
    """Main processor for FIFO calculations and report generation"""

    def __init__(self, price_manager: HistoricPriceManager):
        self.price_manager = price_manager

        # FIFO queues for different assets
        self.held_shares: Dict[str, FIFOQueue] = defaultdict(FIFOQueue)
        self.held_forex: Dict[str, FIFOQueue] = defaultdict(FIFOQueue)

        # Sold assets (results of FIFO matching)
        self.sold_shares: Dict[str, List[FIFOShare]] = defaultdict(list)
        self.sold_forex: Dict[str, List[FIFOForex]] = defaultdict(list)
        self.withdrawn_forex: Dict[str, List[FIFOForex]] = defaultdict(list)

        # Non-FIFO items (fees, dividends, taxes)
        self.misc: Dict[str, List[SimpleForex]] = {
            "Fees": [],
            "Dividend Payments": [],
            "Tax Withholding": [],
        }

        # AWV reporting entries
        self.awv_z4_events: List[AWVEntryZ4] = []
        self.awv_z10_events: List[AWVEntryZ10] = []

        # Debug functionality
        self.debug_steps: List[FIFODebugStep] = []
        self.step_counter: int = 0
        self.current_event_index: int = 0

        # Initialize EUR queue
        self.held_forex["EUR"] = FIFOQueue(is_eur_queue=True)

    def reset(self):
        """Reset all queues and results"""
        self.held_shares.clear()
        self.held_forex.clear()
        self.sold_shares.clear()
        self.sold_forex.clear()
        self.withdrawn_forex.clear()
        self.misc = {
            "Fees": [],
            "Dividend Payments": [],
            "Tax Withholding": [],
        }
        self.awv_z4_events.clear()
        self.awv_z10_events.clear()

        # Reset debug data
        self.debug_steps.clear()
        self.step_counter = 0
        self.current_event_index = 0

        # Reinitialize EUR queue
        self.held_forex["EUR"] = FIFOQueue(is_eur_queue=True)

    def _capture_queue_state(self, queue: FIFOQueue) -> List[Dict[str, Any]]:
        """Capture current state of a FIFO queue for debugging"""
        state = []
        for asset in queue.assets:
            state.append(self._asset_to_dict(asset))
        return state

    def _asset_to_dict(self, asset) -> Dict[str, Any]:
        """Convert an asset to dictionary format for debugging"""
        return {
            "Date": asset.buy_date,
            "Quantity": float(asset.quantity),
            "Price": float(getattr(asset, "buy_price", 1)),
            "Currency": getattr(asset, "currency", ""),
            "Source": getattr(asset, "source", ""),
            "Sell_Date": getattr(asset, "sell_date", None),
            "Sell_Price": (
                float(getattr(asset, "sell_price", 0))
                if getattr(asset, "sell_price", None)
                else None
            ),
        }

    def _capture_popped_objects(self, popped_assets: List) -> List[Dict[str, Any]]:
        """Capture popped objects for debugging"""
        if not popped_assets:
            return []

        return [self._asset_to_dict(asset) for asset in popped_assets]

    def _record_debug_step(
        self,
        event,
        operation: str,
        description: str,
        queue_type: str,
        queue_symbol: str,
        queue: Optional[FIFOQueue],
        queue_state_before: Optional[List[Dict[str, Any]]] = None,
        popped_objects: Optional[List] = None,
        pushed_objects: Optional[List] = None,
    ):
        """Record a debug step with before and after queue states"""
        self.step_counter += 1

        step = FIFODebugStep(
            step_number=self.step_counter,
            event_type=type(event).__name__,
            event_date=event.date,
            event_symbol=getattr(event, "symbol", None),
            event_index=self.current_event_index,
            operation=operation,
            description=description,
            queue_type=queue_type,
            queue_symbol=queue_symbol,
            queue_state_before=queue_state_before,
            queue_state_after=self._capture_queue_state(queue) if queue else None,
            popped_objects=(
                self._capture_popped_objects(popped_objects) if popped_objects else None
            ),
            pushed_objects=(
                self._capture_popped_objects(pushed_objects) if pushed_objects else None
            ),
        )
        self.debug_steps.append(step)

    def process_events(self, events: List[Any]):
        """Process a list of events through FIFO logic"""
        # Import here to avoid circular imports
        from src.core.events import (
            RSUEvent,
            ESPPEvent,
            BuyEvent,
            SellEvent,
            DividendEvent,
            TaxEvent,
            MoneyDepositEvent,
            MoneyWithdrawalEvent,
            CurrencyConversionEvent,
            StockSplitEvent,
        )

        self.reset()

        # Sort events by date and priority
        sorted_events = sorted(
            events, key=lambda e: (e.date, getattr(e, "priority", 0))
        )

        for event_index, event in enumerate(sorted_events):
            self.current_event_index = event_index
            if isinstance(event, RSUEvent):
                self._process_rsu_event(event)
            elif isinstance(event, ESPPEvent):
                self._process_espp_event(event)
            elif isinstance(event, BuyEvent):
                self._process_buy_event(event)
            elif isinstance(event, SellEvent):
                self._process_sell_event(event)
            elif isinstance(event, DividendEvent):
                self._process_dividend_event(event)
            elif isinstance(event, TaxEvent):
                self._process_tax_event(event)
            elif isinstance(event, MoneyDepositEvent):
                self._process_money_deposit_event(event)
            elif isinstance(event, MoneyWithdrawalEvent):
                self._process_money_withdrawal_event(event)
            elif isinstance(event, CurrencyConversionEvent):
                self._process_currency_conversion_event(event)
            elif isinstance(event, StockSplitEvent):
                self._process_stock_split_event(event)

    def _process_rsu_event(self, event):
        """Process RSU vesting event"""
        # Add received shares to holdings
        received_share = FIFOShare(
            symbol=event.symbol,
            quantity=event.historic_received_shares_quantity,
            buy_date=event.date,
            buy_price=event.historic_received_shares_price,
            currency=event.currency,
            source="RSU Vesting",
        )
        # Capture state before operation
        before_state = self._capture_queue_state(self.held_shares[event.symbol])
        self.held_shares[event.symbol].push(received_share)
        self._record_debug_step(
            event,
            "PUSH",
            f"Add {event.historic_received_shares_quantity} RSU shares at {event.historic_received_shares_price} {event.currency}",
            "SHARES",
            event.symbol,
            self.held_shares[event.symbol],
            queue_state_before=before_state,
            pushed_objects=[received_share],
        )

        # Create AWV entries
        total_value = (
            event.historic_received_shares_quantity
            + event.historic_withheld_shares_quantity
        ) * event.historic_received_shares_price

        # Z4: Bonus payment
        z4_entry = AWVEntryZ4(
            date=event.date,
            purpose=f"Bonuserhalt in Form von Aktien aus RSUs ({event.symbol})",
            value=total_value,
            currency=event.currency,
            is_incoming=True,
            symbol=event.symbol,
        )
        self.awv_z4_events.append(z4_entry)

        # Z10: Share deposit (net + withheld)
        z10_deposit = AWVEntryZ10(
            date=event.date,
            comment=f"{event.symbol} Corp. (Erhalt Aktien aus RSUs)",
            quantity=event.historic_received_shares_quantity
            + event.historic_withheld_shares_quantity,
            value=total_value,
            currency=event.currency,
            is_incoming=False,  # Buying shares = outgoing
            symbol=event.symbol,
        )
        self.awv_z10_events.append(z10_deposit)

        # Z10: Tax withholding if any
        if event.historic_withheld_shares_quantity > 0:
            z10_tax = AWVEntryZ10(
                date=event.date,
                comment=f"{event.symbol} Corp. (Verkauf zur Erzielung dt. EkSt.)",
                quantity=event.historic_withheld_shares_quantity,
                value=event.historic_withheld_shares_quantity
                * event.historic_received_shares_price,
                currency=event.currency,
                is_incoming=True,  # Receiving proceeds = incoming
                symbol=event.symbol,
            )
            self.awv_z10_events.append(z10_tax)

    def _process_espp_event(self, event):
        """Process ESPP purchase event"""
        # Add received shares to holdings
        received_share = FIFOShare(
            symbol=event.symbol,
            quantity=event.historic_shares_quantity,
            buy_date=event.date,
            buy_price=event.historic_fair_market_value,
            currency=event.currency,
            source="ESPP Purchase",
        )
        # Capture state before operation
        before_state = self._capture_queue_state(self.held_shares[event.symbol])
        self.held_shares[event.symbol].push(received_share)
        self._record_debug_step(
            event,
            "PUSH",
            f"Add {event.historic_shares_quantity} ESPP shares at {event.historic_fair_market_value} {event.currency}",
            "SHARES",
            event.symbol,
            self.held_shares[event.symbol],
            queue_state_before=before_state,
            pushed_objects=[received_share],
        )

        # Create AWV entries
        # Z4: Bonus (discount benefit)
        z4_entry = AWVEntryZ4(
            date=event.date,
            purpose=f"Bonuserhalt in Form von Aktien aus ESPP ({event.symbol})",
            value=event.bonus,
            currency=event.currency,
            is_incoming=True,
            symbol=event.symbol,
        )
        self.awv_z4_events.append(z4_entry)

        # Z10: Share purchase
        z10_entry = AWVEntryZ10(
            date=event.date,
            comment=f"{event.symbol} Corp. (ESPP Kauf)",
            quantity=event.historic_shares_quantity,
            value=event.historic_shares_quantity * event.historic_fair_market_value,
            currency=event.currency,
            is_incoming=False,  # Buying shares = outgoing
            symbol=event.symbol,
        )
        self.awv_z10_events.append(z10_entry)

    def _process_buy_event(self, event):
        """Process buy event"""
        # Remove currency from holdings
        before_state = self._capture_queue_state(self.held_forex[event.currency])
        popped_forex = self.held_forex[event.currency].pop(
            event.cost_of_shares, Decimal("1"), event.date
        )
        self._record_debug_step(
            event,
            "POP",
            f"Remove {event.cost_of_shares} {event.currency} for share purchase",
            "FOREX",
            event.currency,
            self.held_forex[event.currency],
            queue_state_before=before_state,
            popped_objects=popped_forex,
        )

        # Handle fees
        if event.fees and event.fees > 0:
            before_state_fees = self._capture_queue_state(
                self.held_forex[event.currency]
            )
            popped_fees = self.held_forex[event.currency].pop(
                event.fees, Decimal("1"), event.date
            )
            self._record_debug_step(
                event,
                "POP",
                f"Remove {event.fees} {event.currency} for transaction fees",
                "FOREX",
                event.currency,
                self.held_forex[event.currency],
                queue_state_before=before_state_fees,
                popped_objects=popped_fees,
            )
        # Add shares to holdings
        share = FIFOShare(
            symbol=event.symbol,
            quantity=event.historic_shares_quantity,
            buy_date=event.date,
            buy_price=event.historic_shares_price,
            currency=event.currency,
            source="Market Purchase",
            buy_cost=(
                event.fees / event.historic_shares_quantity if event.fees else None
            ),
            buy_cost_currency=event.currency if event.fees else None,
        )
        before_state_shares = self._capture_queue_state(self.held_shares[event.symbol])
        self.held_shares[event.symbol].push(share)
        self._record_debug_step(
            event,
            "PUSH",
            f"Add {event.historic_shares_quantity} shares at {event.historic_shares_price} {event.currency}",
            "SHARES",
            event.symbol,
            self.held_shares[event.symbol],
            queue_state_before=before_state_shares,
            pushed_objects=[share],
        )

        # Create AWV Z10 entry
        z10_entry = AWVEntryZ10(
            date=event.date,
            comment=f"{event.symbol} Corp. (Kauf)",
            quantity=event.historic_shares_quantity,
            value=event.historic_shares_quantity * event.historic_shares_price,
            currency=event.currency,
            is_incoming=False,  # Buying shares = outgoing
            symbol=event.symbol,
        )
        self.awv_z10_events.append(z10_entry)

    def _process_sell_event(self, event):
        """Process sell event"""
        # Calculate sell cost per share if fees exist
        sell_cost = (
            event.fees / event.historic_quantity
            if event.fees and event.fees > 0
            else None
        )
        sell_cost_currency = event.currency if sell_cost else None

        # Remove shares from holdings (FIFO matching)
        before_state_shares = self._capture_queue_state(self.held_shares[event.symbol])
        sold_shares = self.held_shares[event.symbol].pop(
            event.historic_quantity,
            event.historic_sell_price,
            event.date,
            sell_cost=sell_cost,
            sell_cost_currency=sell_cost_currency,
        )
        self.sold_shares[event.symbol].extend(sold_shares)
        self._record_debug_step(
            event,
            "POP",
            f"Remove {event.historic_quantity} shares at {event.historic_sell_price} {event.currency}",
            "SHARES",
            event.symbol,
            self.held_shares[event.symbol],
            queue_state_before=before_state_shares,
            popped_objects=sold_shares,
        )

        # Add proceeds to forex holdings
        proceeds = FIFOForex(
            currency=event.currency,
            quantity=event.proceeds,
            buy_date=event.date,
            source="Sales Proceeds",
        )
        before_state_forex = self._capture_queue_state(self.held_forex[event.currency])
        self.held_forex[event.currency].push(proceeds)
        self._record_debug_step(
            event,
            "PUSH",
            f"Add {event.proceeds} {event.currency} from sale proceeds",
            "FOREX",
            event.currency,
            self.held_forex[event.currency],
            queue_state_before=before_state_forex,
            pushed_objects=[proceeds],
        )

        # Handle fees
        if event.fees and event.fees > 0:
            before_state_fees = self._capture_queue_state(
                self.held_forex[event.currency]
            )
            popped_fees = self.held_forex[event.currency].pop(
                event.fees, Decimal("1"), event.date
            )
            self._record_debug_step(
                event,
                "POP",
                f"Remove {event.fees} {event.currency} for transaction fees",
                "FOREX",
                event.currency,
                self.held_forex[event.currency],
                queue_state_before=before_state_fees,
                popped_objects=popped_fees,
            )
        # Create AWV Z10 entry
        z10_entry = AWVEntryZ10(
            date=event.date,
            comment=f"{event.symbol} Corp. (Verkauf)",
            quantity=event.historic_quantity,
            value=event.historic_quantity * event.historic_sell_price,
            currency=event.currency,
            is_incoming=True,  # Selling shares = incoming
            symbol=event.symbol,
        )
        self.awv_z10_events.append(z10_entry)

    def _process_dividend_event(self, event):
        """Process dividend event"""
        if event.dividend_amount and event.dividend_amount > 0:
            # Add dividend to forex holdings
            dividend_forex = FIFOForex(
                currency=event.currency,
                quantity=event.dividend_amount,
                buy_date=event.date,
                source=f"Dividend ({event.symbol})",
                tax_free_forex=True,  # Dividends are typically tax-free for forex purposes
            )
            before_state = self._capture_queue_state(self.held_forex[event.currency])
            self.held_forex[event.currency].push(dividend_forex)
            self._record_debug_step(
                event,
                "PUSH",
                f"Add {event.dividend_amount} {event.currency} from dividend",
                "FOREX",
                event.currency,
                self.held_forex[event.currency],
                queue_state_before=before_state,
                pushed_objects=[dividend_forex],
            )

            # Add to misc tracking
            dividend_record = SimpleForex(
                currency=event.currency,
                date=event.date,
                amount=event.dividend_amount,
                comment=f"Dividend ({event.symbol})",
            )
            self.misc["Dividend Payments"].append(dividend_record)

    def _process_tax_event(self, event):
        """Process tax withholding event"""
        if event.withheld_tax_amount and event.withheld_tax_amount > 0:
            # Remove tax from forex holdings
            before_state_tax = self._capture_queue_state(
                self.held_forex[event.currency]
            )
            popped_tax = self.held_forex[event.currency].pop(
                event.withheld_tax_amount, Decimal("1"), event.date
            )
            self._record_debug_step(
                event,
                "POP",
                f"Remove {event.withheld_tax_amount} {event.currency} for tax withholding",
                "FOREX",
                event.currency,
                self.held_forex[event.currency],
                queue_state_before=before_state_tax,
                popped_objects=popped_tax,
            )
            # Add to misc tracking
            tax_record = SimpleForex(
                currency=event.currency,
                date=event.date,
                amount=event.withheld_tax_amount,
                comment=f"Tax Withholding ({event.symbol})",
            )
            self.misc["Tax Withholding"].append(tax_record)

        if event.reverted_tax_amount and event.reverted_tax_amount > 0:
            # Add reverted tax back to holdings
            reverted_forex = FIFOForex(
                currency=event.currency,
                quantity=event.reverted_tax_amount,
                buy_date=event.date,
                source=f"Tax Reversal ({event.symbol})",
                tax_free_forex=True,
            )
            before_state_reversal = self._capture_queue_state(
                self.held_forex[event.currency]
            )
            self.held_forex[event.currency].push(reverted_forex)
            self._record_debug_step(
                event,
                "PUSH",
                f"Add {event.reverted_tax_amount} {event.currency} from tax reversal",
                "FOREX",
                event.currency,
                self.held_forex[event.currency],
                queue_state_before=before_state_reversal,
                pushed_objects=[reverted_forex],
            )

            # Add negative entry to misc tracking
            tax_record = SimpleForex(
                currency=event.currency,
                date=event.date,
                amount=-event.reverted_tax_amount,
                comment=f"Tax Reversal ({event.symbol})",
            )
            self.misc["Tax Withholding"].append(tax_record)

    def _process_money_deposit_event(self, event):
        """Process money deposit event"""
        if event.amount > 0:
            deposit_forex = FIFOForex(
                currency=event.currency,
                quantity=event.amount,
                buy_date=event.date,
                source="Money Deposit",
            )
            before_state_deposit = self._capture_queue_state(
                self.held_forex[event.currency]
            )
            self.held_forex[event.currency].push(deposit_forex)
            self._record_debug_step(
                event,
                "PUSH",
                f"Add {event.amount} {event.currency} from deposit",
                "FOREX",
                event.currency,
                self.held_forex[event.currency],
                queue_state_before=before_state_deposit,
                pushed_objects=[deposit_forex],
            )

        # Handle fees
        if event.fees and event.fees > 0:
            before_state_fees = self._capture_queue_state(
                self.held_forex[event.currency]
            )
            popped_fees = self.held_forex[event.currency].pop(
                event.fees, Decimal("1"), event.date
            )
            self._record_debug_step(
                event,
                "POP",
                f"Remove {event.fees} {event.currency} for deposit fees",
                "FOREX",
                event.currency,
                self.held_forex[event.currency],
                queue_state_before=before_state_fees,
                popped_objects=popped_fees,
            )
            fee_record = SimpleForex(
                currency=event.currency,
                date=event.date,
                amount=event.fees,
                comment="Money Deposit Fee",
            )
            self.misc["Fees"].append(fee_record)

    def _process_money_withdrawal_event(self, event):
        """Process money withdrawal event"""
        if event.amount > 0:
            before_state_withdrawal = self._capture_queue_state(
                self.held_forex[event.currency]
            )
            withdrawn_forex = self.held_forex[event.currency].pop(
                event.amount, Decimal("1"), event.date
            )
            self.withdrawn_forex[event.currency].extend(withdrawn_forex)
            self._record_debug_step(
                event,
                "POP",
                f"Remove {event.amount} {event.currency} from withdrawal",
                "FOREX",
                event.currency,
                self.held_forex[event.currency],
                queue_state_before=before_state_withdrawal,
                popped_objects=withdrawn_forex,
            )
        # Handle fees
        if event.fees and event.fees > 0:
            before_state_fees = self._capture_queue_state(
                self.held_forex[event.currency]
            )
            popped_fees = self.held_forex[event.currency].pop(
                event.fees, Decimal("1"), event.date
            )
            self._record_debug_step(
                event,
                "POP",
                f"Remove {event.fees} {event.currency} for withdrawal fees",
                "FOREX",
                event.currency,
                self.held_forex[event.currency],
                queue_state_before=before_state_fees,
                popped_objects=popped_fees,
            )
            fee_record = SimpleForex(
                currency=event.currency,
                date=event.date,
                amount=event.fees,
                comment="Money Withdrawal Fee",
            )
            self.misc["Fees"].append(fee_record)

    def _process_currency_conversion_event(self, event):
        """Process currency conversion event"""
        # Remove source currency
        before_state_source = self._capture_queue_state(
            self.held_forex[event.source_currency]
        )
        sold_forex = self.held_forex[event.source_currency].pop(
            event.source_amount, Decimal("1"), event.date
        )
        self.sold_forex[event.source_currency].extend(sold_forex)
        self._record_debug_step(
            event,
            "POP",
            f"Remove {event.source_amount} {event.source_currency} for conversion",
            "FOREX",
            event.source_currency,
            self.held_forex[event.source_currency],
            queue_state_before=before_state_source,
            popped_objects=sold_forex,
        )

        # Add target currency only if target_amount is not -1 (EUR conversion via ECB rates)
        if event.target_amount != Decimal("-1"):
            new_forex = FIFOForex(
                currency=event.target_currency,
                quantity=event.target_amount,
                buy_date=event.date,
                source=f"Currency Conversion {event.source_currency} to {event.target_currency}",
            )
            before_state_target = self._capture_queue_state(
                self.held_forex[event.target_currency]
            )
            self.held_forex[event.target_currency].push(new_forex)
            self._record_debug_step(
                event,
                "PUSH",
                f"Add {event.target_amount} {event.target_currency} from conversion",
                "FOREX",
                event.target_currency,
                self.held_forex[event.target_currency],
                queue_state_before=before_state_target,
                pushed_objects=[new_forex],
            )
        else:
            # EUR conversion via ECB rates - no queue operation needed
            self._record_debug_step(
                event,
                "NOTE",
                f"Conversion to EUR via ECB rates - no queue operation required",
                "FOREX",
                "EUR",
                None,
                queue_state_before=None,
            )

        # Handle fees (typically deducted from source currency)
        if event.fees and event.fees > 0:
            before_state_fees = self._capture_queue_state(
                self.held_forex[event.source_currency]
            )
            popped_fees = self.held_forex[event.source_currency].pop(
                event.fees, Decimal("1"), event.date
            )
            self._record_debug_step(
                event,
                "POP",
                f"Remove {event.fees} {event.source_currency} for conversion fees",
                "FOREX",
                event.source_currency,
                self.held_forex[event.source_currency],
                queue_state_before=before_state_fees,
                popped_objects=popped_fees,
            )
            fee_record = SimpleForex(
                currency=event.source_currency,
                date=event.date,
                amount=event.fees,
                comment="Currency Conversion Fee",
            )
            self.misc["Fees"].append(fee_record)

    def _process_stock_split_event(self, event):
        """Process stock split event"""
        if event.symbol in self.held_shares:
            before_state_split = self._capture_queue_state(
                self.held_shares[event.symbol]
            )
            # Calculate split factor from shares_after_split
            # For a 2:1 split, shares_after_split would be 2
            split_factor = event.shares_after_split
            self.held_shares[event.symbol].apply_split(split_factor)
            self._record_debug_step(
                event,
                "SPLIT",
                f"Apply {split_factor}:1 stock split",
                "SHARES",
                event.symbol,
                self.held_shares[event.symbol],
                queue_state_before=before_state_split,
            )
