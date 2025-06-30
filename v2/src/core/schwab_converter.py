import json
import datetime
from typing import List, Dict, Any, Tuple, Optional
from decimal import Decimal
import pandas as pd
import numpy as np
import streamlit as st


from .events import (
    EventPriority,
    ReportEvent,
    RSUEvent,
    DividendEvent,
    TaxEvent,
    ESPPEvent,
    BuyEvent,
    SellEvent,
    CurrencyConversionEvent,
    MoneyDepositEvent,
    MoneyWithdrawalEvent,
)


def convert_schwab_json_to_events(json_data: Dict[str, Any]) -> List[ReportEvent]:
    """
    Convert Schwab JSON data to our event data structures.

    Args:
        json_data: The parsed JSON data from Schwab

    Returns:
        List of ReportEvent objects
    """
    events = []

    for transaction in json_data.get("Transactions", []):
        action = transaction.get("Action")
        description = transaction.get("Description")

        if action == "Deposit" and description == "ESPP":
            event = _create_espp_event(transaction)
            if event:
                events.append(event)

        elif action == "Lapse" and description == "Restricted Stock Lapse":
            # Create RSU event directly from Lapse (no matching needed)
            rsu_event = _create_rsu_event_from_lapse(transaction)
            if rsu_event:
                events.append(rsu_event)

        elif action == "Dividend" and description == "Credit":
            event = _create_dividend_event(transaction)
            if event:
                events.append(event)

        elif action == "Sale" and description == "Share Sale":
            sell_events = _create_sell_events(transaction)
            events.extend(sell_events)

        elif action == "Wire Transfer" and description == "Cash Disbursement":
            event = _create_wire_transfer_event(transaction)
            if event:
                events.append(event)

        elif action == "Tax Withholding" and description == "Debit":
            event = _create_tax_withholding_event(transaction)
            if event:
                events.append(event)

        elif action == "Tax Reversal" and description == "Credit":
            event = _create_tax_reversal_event(transaction)
            if event:
                events.append(event)

        # Skip RSU Deposit events - we get all needed data from Lapse events

    # Sort events by date
    events.sort(key=lambda e: e.date)

    return events


def _parse_date(date_str: str) -> datetime.date:
    """Parse date string in MM/DD/YYYY format."""
    return datetime.datetime.strptime(date_str, "%m/%d/%Y").date()


def _parse_amount(amount_str: str) -> Decimal:
    """Parse amount string, removing $ and , characters."""
    if not amount_str:
        return Decimal("0.0")
    return Decimal(
        amount_str.replace("$", "").replace(",", "").replace("-", "").strip()
    )


def _create_espp_event(transaction: Dict[str, Any]) -> Optional[ESPPEvent]:
    """Create ESPP event from transaction data."""
    try:
        symbol = transaction["Symbol"]
        quantity = Decimal(transaction["Quantity"])

        if not transaction.get("TransactionDetails"):
            return None

        details = transaction["TransactionDetails"][0]["Details"]
        date = _parse_date(details["PurchaseDate"])
        purchase_price = _parse_amount(details["PurchasePrice"])
        fair_market_value = _parse_amount(details["PurchaseFairMarketValue"])

        return ESPPEvent(
            date=date,
            symbol=symbol,
            currency="USD",
            shares_quantity=quantity,
            shares_price=purchase_price,
            fair_market_value=fair_market_value,
        )
    except (KeyError, ValueError) as e:
        print(f"Error creating ESPP event: {e}")
        return None


def _create_rsu_event_from_lapse(transaction: Dict[str, Any]) -> Optional[RSUEvent]:
    """Create RSU event directly from Lapse transaction (simplified approach)."""
    try:
        date = _parse_date(transaction["Date"])
        symbol = transaction["Symbol"]
        gross_quantity = Decimal(transaction["Quantity"])

        if not transaction.get("TransactionDetails"):
            return None

        details = transaction["TransactionDetails"][0]["Details"]
        fair_market_value = _parse_amount(details["FairMarketValuePrice"])
        net_quantity = Decimal(details["NetSharesDeposited"])

        # Extract AwardId for RSU events
        award_id = details.get("AwardId")

        # Calculate withheld shares
        withheld_shares_quantity = gross_quantity - net_quantity

        return RSUEvent(
            date=date,
            symbol=symbol,
            received_shares_quantity=net_quantity,
            received_shares_price=fair_market_value,
            withheld_shares_quantity=(
                withheld_shares_quantity if withheld_shares_quantity > 0 else None
            ),
            currency="USD",
            grant_id=award_id,
        )
    except (KeyError, ValueError) as e:
        print(f"Error creating RSU event from lapse: {e}")
        return None


def _create_dividend_event(transaction: Dict[str, Any]) -> Optional[DividendEvent]:
    """Create dividend event from transaction data."""
    try:
        date = _parse_date(transaction["Date"])
        symbol = transaction["Symbol"]
        amount = _parse_amount(transaction["Amount"])

        return DividendEvent(
            date=date,
            symbol=symbol,
            currency="USD",
            dividend_amount=amount,
        )
    except (KeyError, ValueError) as e:
        print(f"Error creating dividend event: {e}")
        return None


def _create_sell_events(transaction: Dict[str, Any]) -> List[SellEvent]:
    """Create sell events from transaction data (may be multiple if split across orders)."""
    events = []

    try:
        date = _parse_date(transaction["Date"])
        symbol = transaction["Symbol"]
        total_fees = _parse_amount(transaction.get("FeesAndCommissions", "0"))
        total_quantity = Decimal(transaction["Quantity"])

        for i, detail in enumerate(transaction.get("TransactionDetails", [])):
            details = detail["Details"]
            shares = Decimal(details["Shares"])
            sale_price = _parse_amount(details["SalePrice"])

            # Extract GrantId for sell events (use as transaction_id)
            transaction_id = details.get("GrantId")

            if i == 0:
                fees_per_order = total_fees
            else:
                fees_per_order = Decimal("0")

            sell_event = SellEvent(
                date=date,
                symbol=symbol,
                currency="USD",
                quantity=shares,
                sell_price=sale_price,
                proceeds=shares * sale_price,
                fees=fees_per_order,
                transaction_id=transaction_id,
            )
            events.append(sell_event)

    except (KeyError, ValueError) as e:
        print(f"Error creating sell events: {e}")

    return events


def _create_wire_transfer_event(
    transaction: Dict[str, Any],
) -> Optional[ReportEvent]:
    """Create wire transfer event - either currency conversion or money withdrawal based on settings."""
    try:
        date = _parse_date(transaction["Date"])
        amount = _parse_amount(transaction["Amount"])
        fees = _parse_amount(transaction.get("FeesAndCommissions", "0"))

        # Check global setting for forex transfer treatment
        treat_as_forex = False
        if hasattr(st.session_state, "settings"):
            treat_as_forex = st.session_state.settings.get(
                "forex_transfer_as_exchange", False
            )

        if treat_as_forex:
            # Create currency conversion event (USD to EUR)
            return CurrencyConversionEvent(
                date=date,
                source_amount=amount,
                source_currency="USD",
                target_amount=Decimal("-1"),  # To be determined/calculated
                target_currency="EUR",
                fees=fees if fees > 0 else None,
                priority=EventPriority.CURRENCY_CONVERSION_FROM_FOREX_TO_EUR,
            )
        else:
            # Create money withdrawal event
            return MoneyWithdrawalEvent(
                date=date,
                buy_date=date,  # Use same date as placeholder
                amount=amount,
                fees=fees if fees > 0 else None,
                currency="USD",
            )
    except (KeyError, ValueError) as e:
        print(f"Error creating wire transfer event: {e}")
        return None


def _create_tax_withholding_event(transaction: Dict[str, Any]) -> Optional[TaxEvent]:
    """Create tax event from tax withholding transaction."""
    try:
        date = _parse_date(transaction["Date"])
        symbol = transaction["Symbol"]
        amount = _parse_amount(transaction["Amount"])

        return TaxEvent(
            date=date,
            symbol=symbol,
            currency="USD",
            withheld_tax_amount=amount,
            reverted_tax_amount=None,
        )
    except (KeyError, ValueError) as e:
        print(f"Error creating tax withholding event: {e}")
        return None


def _create_tax_reversal_event(transaction: Dict[str, Any]) -> Optional[TaxEvent]:
    """Create tax event from tax reversal transaction."""
    try:
        date = _parse_date(transaction["Date"])
        symbol = transaction["Symbol"]
        amount = _parse_amount(transaction["Amount"])

        return TaxEvent(
            date=date,
            symbol=symbol,
            currency="USD",
            withheld_tax_amount=None,
            reverted_tax_amount=amount,
        )
    except (KeyError, ValueError) as e:
        print(f"Error creating tax reversal event: {e}")
        return None


def process_schwab_json_file(json_content: str) -> List[ReportEvent]:
    """
    Process Schwab JSON file content and return list of events.

    Args:
        json_content: String content of the JSON file

    Returns:
        List of ReportEvent objects
    """
    try:
        json_data = json.loads(json_content)
        return convert_schwab_json_to_events(json_data)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return []
    except Exception as e:
        print(f"Error processing Schwab JSON: {e}")
        return []
