"""
Data management page for pyFIFOtax Modern UI
Handles Schwab JSON import, data export, file management and deletion
"""

import streamlit as st
import pandas as pd
import json
from typing import List, Dict, Tuple
from ..core.schwab_converter import process_schwab_json_file

# Import report generation components
from src.core.fifo_processor import FIFOProcessor
from src.core.report_generator import ReportGenerator, ReportSettings
from src.core.historic_prices import HistoricPriceManager


def show():
    """Display the import data page"""

    st.title("📊 Data Management")
    st.markdown("---")

    # Import section
    st.header("📥 Import Data")

    st.subheader("📊 Schwab JSON Import")
    _handle_schwab_import()
    st.subheader("📋 pyFIFOtax JSON Import")
    _handle_json_import()
    st.markdown("---")
    _show_wire_transfer_settings()

    st.markdown("---")
    _show_imported_files()

    # Export section
    st.markdown("---")
    _show_export_options()

    # Duplicate detection sections (moved after imported files)
    if (
        hasattr(st.session_state, "deduplication_stats")
        and st.session_state.deduplication_stats
    ):
        st.markdown("---")
        _show_deduplication_stats()

        if st.session_state.deduplication_stats.get("duplicates_removed", 0) > 0:
            st.markdown("---")
            _show_duplicate_details()


def _handle_schwab_import():
    """Handle Schwab JSON file import"""

    # Instructions
    with st.expander("How to export from Schwab", expanded=False):
        st.markdown(
            """
        1. Log into your Schwab account
        2. Go to **History → Transactions → Export**
        3. Select the desired date range
        4. Choose **JSON format**
        5. Download and upload the file below
        """
        )

    # Initialize uploader key for clearing
    if "uploader_key" not in st.session_state:
        st.session_state.uploader_key = 0

    # File uploader with dynamic key
    uploaded_file = st.file_uploader(
        "Upload Schwab JSON file",
        type=["json"],
        help="Upload the JSON file exported from Schwab",
        key=f"schwab_uploader_{st.session_state.uploader_key}",
    )

    if uploaded_file is not None:
        # Initialize imported files tracking
        if "imported_files_data" not in st.session_state:
            st.session_state.imported_files_data = {}

        # Check for duplicates
        if uploaded_file.name in st.session_state.imported_files_data:
            st.error(f"File '{uploaded_file.name}' has already been imported")
            return

        try:
            # Read and parse JSON
            json_content = uploaded_file.read().decode("utf-8")
            json_data = json.loads(json_content)

            if "Transactions" in json_data:
                # Convert JSON to events using our converter
                events = process_schwab_json_file(json_content)

                transaction_count = len(json_data["Transactions"])
                event_count = len(events)

                # Store file data with metadata
                import_time = pd.Timestamp.now()
                st.session_state.imported_files_data[uploaded_file.name] = {
                    "json_content": json_content,
                    "json_data": json_data,
                    "events": events,
                    "transaction_count": transaction_count,
                    "event_count": event_count,
                    "import_time": import_time,
                }

                # Regenerate all events from all files with deduplication
                _regenerate_all_events()

                # Clear the file uploader by incrementing its key
                st.session_state.uploader_key += 1

                st.success(
                    f"✅ Imported {transaction_count} transactions, created {event_count} events from {uploaded_file.name}"
                )
                st.balloons()
                st.rerun()  # Refresh to clear the file uploader
            else:
                st.error("❌ Invalid Schwab JSON format - 'Transactions' key not found")

        except json.JSONDecodeError as e:
            st.error(f"❌ Error parsing JSON file: {str(e)}")
        except Exception as e:
            st.error(f"❌ Error processing file: {str(e)}")


def _handle_json_import():
    """Handle pyFIFOtax JSON file import"""

    # Instructions
    with st.expander("How to import pyFIFOtax JSON", expanded=False):
        st.markdown(
            """
        1. Use a previously exported pyFIFOtax JSON file
        2. Upload the file below
        3. Events will be imported and merged with existing data
        """
        )

    # Initialize uploader key for clearing
    if "json_uploader_key" not in st.session_state:
        st.session_state.json_uploader_key = 0

    # File uploader with dynamic key
    uploaded_file = st.file_uploader(
        "Upload pyFIFOtax JSON file",
        type=["json"],
        help="Upload a previously exported pyFIFOtax JSON file",
        key=f"json_uploader_{st.session_state.json_uploader_key}",
    )

    if uploaded_file is not None:
        # Check for duplicates across both file types
        schwab_files = getattr(st.session_state, "imported_files_data", {})
        json_files = getattr(st.session_state, "imported_json_files", {})

        if uploaded_file.name in schwab_files or uploaded_file.name in json_files:
            st.error(f"File '{uploaded_file.name}' has already been imported")
            return

        try:
            # Read and parse JSON
            json_content = uploaded_file.read().decode("utf-8")
            json_data = json.loads(json_content)

            # Validate the JSON structure
            if not _validate_json_import(json_data):
                st.error("❌ Invalid pyFIFOtax JSON format")
                return

                # Import events
            imported_events = _import_events_from_json(json_data)

            if imported_events:
                # Initialize tracking for imported JSON files
                if "imported_json_files" not in st.session_state:
                    st.session_state.imported_json_files = {}

                # Store JSON file data with metadata
                import_time = pd.Timestamp.now()
                st.session_state.imported_json_files[uploaded_file.name] = {
                    "json_data": json_data,
                    "events": imported_events,
                    "event_count": len(imported_events),
                    "import_time": import_time,
                }

                # Add to existing events
                if not hasattr(st.session_state, "imported_events"):
                    st.session_state.imported_events = []
                st.session_state.imported_events.extend(imported_events)

                # Initialize price manager and other components if not present
                if "price_manager" not in st.session_state:
                    from src.core.historic_prices import HistoricPriceManager

                    st.session_state.price_manager = HistoricPriceManager()

                if "fifo_processor" not in st.session_state:
                    from src.core.fifo_processor import FIFOProcessor

                    st.session_state.fifo_processor = FIFOProcessor(
                        st.session_state.price_manager
                    )

                if "report_generator" not in st.session_state:
                    from src.core.report_generator import ReportGenerator

                    st.session_state.report_generator = ReportGenerator(
                        st.session_state.fifo_processor, st.session_state.price_manager
                    )

                # Update timestamp to trigger report regeneration
                st.session_state.events_last_updated = pd.Timestamp.now()

                # Clear the file uploader by incrementing its key
                st.session_state.json_uploader_key += 1

                st.success(
                    f"✅ Imported {len(imported_events)} events from {uploaded_file.name}"
                )
                st.balloons()
                st.rerun()  # Refresh to clear the file uploader
            else:
                st.warning("⚠️ No events found in the JSON file")

        except json.JSONDecodeError as e:
            st.error(f"❌ Error parsing JSON file: {str(e)}")
        except Exception as e:
            st.error(f"❌ Error processing file: {str(e)}")


def _validate_json_import(json_data: dict) -> bool:
    """Validate that the JSON has the expected pyFIFOtax structure"""

    # Check for required top-level keys
    if "metadata" not in json_data or "events" not in json_data:
        return False

    # Check metadata structure
    metadata = json_data["metadata"]
    if not isinstance(metadata, dict):
        return False

    # Check events structure
    events = json_data["events"]
    if not isinstance(events, list):
        return False

    # Basic validation of event structure
    for event in events:
        if not isinstance(event, dict):
            return False
        if "event_type" not in event or "date" not in event:
            return False

    return True


def _import_events_from_json(json_data: dict) -> list:
    """Convert JSON data back to ReportEvent objects"""

    from src.core.events import (
        BuyEvent,
        SellEvent,
        DividendEvent,
        RSUEvent,
        ESPPEvent,
        TaxEvent,
        MoneyDepositEvent,
        MoneyWithdrawalEvent,
        CurrencyConversionEvent,
        StockSplitEvent,
        EventPriority,
    )
    from decimal import Decimal
    import datetime

    def safe_decimal(value, default="0"):
        """Convert value to Decimal, handling None and 'None' string cases"""
        if value is None or value == "None" or value == "":
            return None if default is None else Decimal(default)
        return Decimal(str(value))

    def safe_decimal_required(value, default="0"):
        """Convert value to Decimal for required fields, never returning None"""
        if value is None or value == "None" or value == "":
            return Decimal(default)
        return Decimal(str(value))

    events = []

    for event_data in json_data["events"]:
        event_type = event_data["event_type"]

        # Parse date
        date_str = event_data["date"]
        if isinstance(date_str, str):
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        else:
            date = date_str

            # Create event based on type
        try:
            if event_type == "Buy":
                event = BuyEvent(
                    date=date,
                    symbol=event_data.get("symbol", ""),
                    currency=event_data.get("currency", "USD"),
                    shares_quantity=Decimal(
                        str(
                            event_data.get(
                                "imported_shares_quantity",
                                event_data.get("historic_shares_quantity", "0"),
                            )
                        )
                    ),
                    shares_price=Decimal(
                        str(
                            event_data.get(
                                "imported_shares_price",
                                event_data.get("historic_shares_price", "0"),
                            )
                        )
                    ),
                    cost_of_shares=safe_decimal_required(
                        event_data.get("cost_of_shares", "0")
                    ),
                    fees=safe_decimal(event_data.get("fees"), None),
                )
            elif event_type == "Sell":
                event = SellEvent(
                    date=date,
                    symbol=event_data.get("symbol", ""),
                    currency=event_data.get("currency", "USD"),
                    quantity=Decimal(
                        str(
                            event_data.get(
                                "imported_quantity",
                                event_data.get("historic_quantity", "0"),
                            )
                        )
                    ),
                    sell_price=Decimal(
                        str(
                            event_data.get(
                                "imported_sell_price",
                                event_data.get("historic_sell_price", "0"),
                            )
                        )
                    ),
                    proceeds=safe_decimal_required(event_data.get("proceeds", "0")),
                    fees=safe_decimal(event_data.get("fees"), None),
                    transaction_id=event_data.get("transaction_id", "") or None,
                )
            elif event_type == "Dividend":
                event = DividendEvent(
                    date=date,
                    symbol=event_data.get("symbol", ""),
                    currency=event_data.get("currency", "USD"),
                    dividend_amount=safe_decimal(
                        event_data.get("dividend_amount"), None
                    ),
                )
            elif event_type == "RSU":
                event = RSUEvent(
                    date=date,
                    symbol=event_data.get("symbol", ""),
                    currency=event_data.get("currency", "USD"),
                    received_shares_quantity=Decimal(
                        str(
                            event_data.get(
                                "imported_received_shares_quantity",
                                event_data.get(
                                    "historic_received_shares_quantity", "0"
                                ),
                            )
                        )
                    ),
                    received_shares_price=Decimal(
                        str(
                            event_data.get(
                                "imported_received_shares_price",
                                event_data.get("historic_received_shares_price", "0"),
                            )
                        )
                    ),
                    withheld_shares_quantity=safe_decimal(
                        event_data.get(
                            "imported_withheld_shares_quantity",
                            event_data.get("historic_withheld_shares_quantity"),
                        ),
                        None,
                    ),
                    grant_id=event_data.get("grant_id", "") or None,
                )
            elif event_type == "ESPP":
                event = ESPPEvent(
                    date=date,
                    symbol=event_data.get("symbol", ""),
                    currency=event_data.get("currency", "USD"),
                    shares_quantity=Decimal(
                        str(
                            event_data.get(
                                "imported_shares_quantity",
                                event_data.get("historic_shares_quantity", "0"),
                            )
                        )
                    ),
                    shares_price=Decimal(
                        str(
                            event_data.get(
                                "imported_shares_price",
                                event_data.get("historic_shares_price", "0"),
                            )
                        )
                    ),
                    fair_market_value=Decimal(
                        str(
                            event_data.get(
                                "imported_fair_market_value",
                                event_data.get("historic_fair_market_value", "0"),
                            )
                        )
                    ),
                )
            elif event_type == "Tax":
                event = TaxEvent(
                    date=date,
                    symbol=event_data.get("symbol", ""),
                    currency=event_data.get("currency", "USD"),
                    withheld_tax_amount=safe_decimal(
                        event_data.get("withheld_tax_amount"), None
                    ),
                    reverted_tax_amount=safe_decimal(
                        event_data.get("reverted_tax_amount"), None
                    ),
                )
            elif event_type == "MoneyDeposit":
                event = MoneyDepositEvent(
                    date=date,
                    currency=event_data.get("currency", "USD"),
                    amount=safe_decimal_required(event_data.get("amount", "0")),
                    buy_date=(
                        datetime.datetime.strptime(
                            event_data.get("buy_date", str(date)), "%Y-%m-%d"
                        ).date()
                        if event_data.get("buy_date")
                        else date
                    ),
                    fees=safe_decimal(event_data.get("fees"), None),
                )
            elif event_type == "MoneyWithdrawal":
                event = MoneyWithdrawalEvent(
                    date=date,
                    currency=event_data.get("currency", "USD"),
                    amount=safe_decimal_required(event_data.get("amount", "0")),
                    buy_date=(
                        datetime.datetime.strptime(
                            event_data.get("buy_date", str(date)), "%Y-%m-%d"
                        ).date()
                        if event_data.get("buy_date")
                        else date
                    ),
                    fees=safe_decimal(event_data.get("fees"), None),
                )
            elif event_type == "CurrencyConversion":
                # Determine priority based on currencies
                source_curr = event_data.get("source_currency", "USD")
                target_curr = event_data.get("target_currency", "EUR")

                if source_curr == "EUR":
                    priority = EventPriority.CURRENCY_CONVERSION_FROM_EUR_TO_FOREX
                elif target_curr == "EUR":
                    priority = EventPriority.CURRENCY_CONVERSION_FROM_FOREX_TO_EUR
                else:
                    priority = EventPriority.CURRENCY_CONVERSION_FROM_FOREX_TO_FOREX

                event = CurrencyConversionEvent(
                    date=date,
                    source_currency=source_curr,
                    source_amount=safe_decimal_required(
                        event_data.get("source_amount", "0")
                    ),
                    target_currency=target_curr,
                    target_amount=safe_decimal_required(
                        event_data.get("target_amount", "0")
                    ),
                    fees=safe_decimal(event_data.get("fees"), None),
                    priority=priority,
                )
            elif event_type == "StockSplit":
                event = StockSplitEvent(
                    date=date,
                    symbol=event_data.get("symbol", ""),
                    shares_after_split=safe_decimal_required(
                        event_data.get("shares_after_split", "0")
                    ),
                )
            else:
                st.warning(f"⚠️ Unknown event type: {event_type}")
                continue

            events.append(event)

        except Exception as e:
            st.warning(f"⚠️ Error importing {event_type} event: {str(e)}")
            continue

    return events


def _generate_event_key(event) -> str:
    """
    Generate a unique key for an event based on metadata and historic values.

    Uses: event_type, date, symbol, historic_quantity, historic_price, currency, transaction_id/grant_id, and additional type-specific fields
    This ensures we identify duplicates based on standardized historic values and all relevant distinguishing factors.
    """
    # Get event type
    event_type = event.get_event_type()

    # Get basic metadata
    date_str = str(event.date)
    symbol = getattr(event, "symbol", "NO_SYMBOL")
    currency = getattr(event, "currency", "USD")

    # Get transaction/grant ID for events that have them
    transaction_id = None
    if event_type == "RSU":
        transaction_id = getattr(event, "grant_id", "NO_GRANT")  # RSU uses grant_id
    elif event_type == "Sell":
        transaction_id = getattr(
            event, "transaction_id", "NO_TRANSACTION_ID"
        )  # Sell uses transaction_id
    # ESPP no longer uses grant_id

    # Get historic quantity and price (use historic values for consistency)
    historic_quantity = None
    historic_price = None
    additional_fields = []  # For type-specific distinguishing fields

    # Extract historic values and additional fields based on event type
    if event_type == "Buy":
        historic_quantity = getattr(event, "historic_shares_quantity", None)
        historic_price = getattr(event, "historic_shares_price", None)
        additional_fields.append(str(getattr(event, "cost_of_shares", "None")))
        additional_fields.append(str(getattr(event, "fees", "None")))

    elif event_type == "Sell":
        historic_quantity = getattr(event, "historic_quantity", None)
        historic_price = getattr(event, "historic_sell_price", None)
        additional_fields.append(str(getattr(event, "proceeds", "None")))
        additional_fields.append(str(getattr(event, "fees", "None")))
        # Transaction ID is especially important for Sell events to distinguish sales from different sources

    elif event_type == "RSU":
        historic_quantity = getattr(event, "historic_received_shares_quantity", None)
        historic_price = getattr(event, "historic_received_shares_price", None)
        # Include withheld quantity as it's a distinguishing factor
        additional_fields.append(
            str(getattr(event, "historic_withheld_shares_quantity", "None"))
        )
        # Grant ID is crucial for RSU events to distinguish different grants

    elif event_type == "ESPP":
        historic_quantity = getattr(event, "historic_shares_quantity", None)
        historic_price = getattr(event, "historic_fair_market_value", None)
        additional_fields.append(str(getattr(event, "historic_shares_price", "None")))
        additional_fields.append(str(getattr(event, "contribution", "None")))
        additional_fields.append(str(getattr(event, "bonus", "None")))
        # ESPP events distinguished by their subscription dates and amounts

    elif event_type == "Dividend":
        historic_quantity = getattr(event, "dividend_amount", None)

    elif event_type == "Tax":
        historic_quantity = getattr(event, "withheld_tax_amount", None)
        additional_fields.append(str(getattr(event, "reverted_tax_amount", "None")))

    elif event_type in ["MoneyDeposit", "MoneyWithdrawal"]:
        historic_quantity = getattr(event, "amount", None)
        additional_fields.append(str(getattr(event, "buy_date", "None")))
        additional_fields.append(str(getattr(event, "fees", "None")))

    elif event_type == "CurrencyConversion":
        historic_quantity = getattr(event, "source_amount", None)
        additional_fields.append(str(getattr(event, "source_currency", "None")))
        additional_fields.append(str(getattr(event, "target_amount", "None")))
        additional_fields.append(str(getattr(event, "target_currency", "None")))
        additional_fields.append(str(getattr(event, "fees", "None")))

    elif event_type == "StockSplit":
        historic_quantity = getattr(event, "shares_after_split", None)

    # Convert to strings for key generation (handle None values)
    quantity_str = str(historic_quantity) if historic_quantity is not None else "None"
    price_str = str(historic_price) if historic_price is not None else "None"
    transaction_id_str = str(transaction_id) if transaction_id is not None else "NO_ID"
    additional_str = "|".join(additional_fields) if additional_fields else ""

    # Create unique key with transaction/grant ID as a primary distinguisher where applicable
    key_parts = [
        event_type,
        date_str,
        symbol,
        currency,
        transaction_id_str,
        quantity_str,
        price_str,
    ]
    if additional_str:
        key_parts.append(additional_str)

    key = "|".join(key_parts)
    return key


def _deduplicate_events(events: List) -> Tuple[List, Dict]:
    """
    Remove duplicate events based on metadata and historic values.

    Returns:
        - List of deduplicated events
        - Dictionary with deduplication statistics and detailed duplicate information grouped by key
    """
    if not events:
        return events, {}

    events_by_key = {}  # Track all events by key
    deduplicated_events = []
    duplicate_groups = {}  # Group duplicates by key
    duplicates_by_type = {}

    # First pass: group all events by their keys
    for i, event in enumerate(events):
        event_key = _generate_event_key(event)
        if event_key not in events_by_key:
            events_by_key[event_key] = []
        events_by_key[event_key].append((event, i))  # Include original index

    # Second pass: identify duplicates and keep first occurrence
    total_duplicates = 0
    for event_key, key_events in events_by_key.items():
        if len(key_events) > 1:
            # Multiple events with same key - duplicates found
            kept_event, kept_index = key_events[0]  # Keep the first one
            duplicate_events = [
                event for event, idx in key_events[1:]
            ]  # Rest are duplicates

            deduplicated_events.append(kept_event)

            # Track duplicate group
            duplicate_groups[event_key] = {
                "kept_event": kept_event,
                "kept_index": kept_index,
                "duplicate_events": duplicate_events,
                "duplicate_indices": [idx for event, idx in key_events[1:]],
                "event_type": kept_event.get_event_type(),
                "total_count": len(key_events),
                "duplicate_count": len(duplicate_events),
                "overridden": False,  # Track if user has overridden this group
            }

            # Count by type
            event_type = kept_event.get_event_type()
            if event_type not in duplicates_by_type:
                duplicates_by_type[event_type] = 0
            duplicates_by_type[event_type] += len(duplicate_events)

            total_duplicates += len(duplicate_events)
        else:
            # Unique event
            deduplicated_events.append(key_events[0][0])

    stats = {
        "original_count": len(events),
        "final_count": len(deduplicated_events),
        "duplicates_removed": total_duplicates,
        "duplicate_rate": (total_duplicates / len(events) * 100) if events else 0,
        "duplicates_by_type": duplicates_by_type,
        "duplicate_groups": duplicate_groups,
        "duplicate_keys_count": len(duplicate_groups),
        "original_events": events,  # Keep reference to original events for override functionality
    }

    return deduplicated_events, stats


def _apply_duplicate_overrides():
    """Apply user overrides to duplicate detection"""
    if (
        not hasattr(st.session_state, "deduplication_stats")
        or not st.session_state.deduplication_stats
    ):
        return

    stats = st.session_state.deduplication_stats
    original_events = stats.get("original_events", [])

    if not original_events:
        return

    # Rebuild events list considering overrides
    final_events = []
    duplicate_groups = stats["duplicate_groups"]

    # Track which events should be included
    included_indices = set()

    # Add all unique events (not in any duplicate group)
    events_by_key = {}
    for i, event in enumerate(original_events):
        event_key = _generate_event_key(event)
        if event_key not in events_by_key:
            events_by_key[event_key] = []
        events_by_key[event_key].append((event, i))

    for event_key, key_events in events_by_key.items():
        if len(key_events) == 1:
            # Unique event - always include
            included_indices.add(key_events[0][1])
        else:
            # Duplicate group - check override status
            if event_key in duplicate_groups:
                group = duplicate_groups[event_key]
                if group.get("overridden", False):
                    # User overrode - include all events from this group
                    for event, idx in key_events:
                        included_indices.add(idx)
                else:
                    # Normal duplicate handling - keep only first
                    included_indices.add(group["kept_index"])

    # Build final events list in original order
    final_events = [original_events[i] for i in sorted(included_indices)]

    # Update session state
    st.session_state.imported_events = final_events

    # Recalculate stats
    total_kept = len(final_events)
    total_removed = len(original_events) - total_kept

    stats["final_count"] = total_kept
    stats["duplicates_removed"] = total_removed
    stats["duplicate_rate"] = (
        (total_removed / len(original_events) * 100) if original_events else 0
    )

    # Update timestamp to trigger report regeneration
    st.session_state.events_last_updated = pd.Timestamp.now()


def _show_deduplication_stats():
    """Show deduplication statistics"""
    stats = st.session_state.deduplication_stats

    st.header("📊 Duplicate Detection Summary")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Original Events", stats["original_count"])

    with col2:
        st.metric("Final Events", stats["final_count"])

    with col3:
        st.metric("Duplicates Removed", stats["duplicates_removed"])

    with col4:
        st.metric("Duplicate Rate", f"{stats['duplicate_rate']:.1f}%")

    if stats["duplicates_removed"] > 0:
        st.success(
            f"✅ Successfully removed {stats['duplicates_removed']} duplicate events!"
        )

    else:
        st.info("ℹ️ No duplicate events detected across imported files.")


def _show_wire_transfer_settings():
    """Show wire transfer processing settings specific to Schwab imports"""

    st.header("⚙️ Wire Transfer Processing")

    # Initialize settings in session state
    if "settings" not in st.session_state:
        st.session_state.settings = {"forex_transfer_as_exchange": False}

    # Store previous setting value to detect changes
    if "previous_forex_setting" not in st.session_state:
        st.session_state.previous_forex_setting = st.session_state.settings.get(
            "forex_transfer_as_exchange", False
        )

    forex_transfer_as_exchange = st.checkbox(
        "Treat wire transfers as forex exchange",
        value=st.session_state.settings.get("forex_transfer_as_exchange", False),
        help="""
        When enabled, outgoing wire transfers are treated as currency exchanges to EUR 
        instead of money transfers. This simplifies reporting if wire transfers are your 
        primary method of currency conversion. 
        
        ⚠️ Please verify the actual conversion date and amounts for correctness!
        """,
    )

    # Check if setting changed and trigger reprocessing if needed
    if forex_transfer_as_exchange != st.session_state.previous_forex_setting:
        # Update session state
        st.session_state.settings["forex_transfer_as_exchange"] = (
            forex_transfer_as_exchange
        )
        st.session_state.previous_forex_setting = forex_transfer_as_exchange

        # Trigger reprocessing of all imported files if there are any
        if (
            hasattr(st.session_state, "imported_files_data")
            and st.session_state.imported_files_data
        ):
            _regenerate_all_events_from_raw_data()

            setting_status = "enabled" if forex_transfer_as_exchange else "disabled"
            st.success(
                f"✅ Setting updated and {len(st.session_state.imported_files_data)} files reprocessed with forex transfers {setting_status}"
            )
            st.rerun()
    else:
        # Update session state normally if no change
        st.session_state.settings["forex_transfer_as_exchange"] = (
            forex_transfer_as_exchange
        )


def _show_imported_files():
    """Show list of imported files with management options"""

    st.header("📁 Imported Files")

    # Check if any files are imported
    schwab_files = getattr(st.session_state, "imported_files_data", {})
    json_files = getattr(st.session_state, "imported_json_files", {})

    if not schwab_files and not json_files:
        st.info("No files imported yet. Upload files above to get started.")
        return

    # Create combined table of imported files
    files_data = []

    # Add Schwab files
    for filename, file_info in schwab_files.items():
        files_data.append(
            {
                "Filename": filename,
                "Type": "📊 Schwab",
                "Import Time": file_info["import_time"].strftime("%Y-%m-%d %H:%M:%S"),
                "Source Count": str(
                    file_info["transaction_count"]
                ),  # Convert to string
                "Events": file_info["event_count"],
            }
        )

    # Add JSON files
    for filename, file_info in json_files.items():
        files_data.append(
            {
                "Filename": filename,
                "Type": "📋 pyFIFOtax",
                "Import Time": file_info["import_time"].strftime("%Y-%m-%d %H:%M:%S"),
                "Source Count": "-",  # Keep as string
                "Events": file_info["event_count"],
            }
        )

    if files_data:
        df = pd.DataFrame(files_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Delete section
        st.subheader("🗑️ Delete Files")

        # Get all filenames for selection
        all_filenames = list(schwab_files.keys()) + list(json_files.keys())

        if len(all_filenames) == 1:
            filename = all_filenames[0]
            if st.button(f"🗑️ Delete {filename}", type="secondary"):
                _delete_file(filename)
        else:
            col1, col2 = st.columns(2)

            with col1:
                selected_file = st.selectbox(
                    "Select file to delete",
                    options=all_filenames,
                )

            with col2:
                if st.button("🗑️ Delete Selected File", type="secondary"):
                    _delete_file(selected_file)

            st.markdown("**Or delete all files:**")
            if st.button("🗑️ Delete All Files", type="secondary"):
                _delete_all_files()


def _delete_file(filename: str):
    """Delete a specific imported file and regenerate events"""

    deleted = False

    # Check if it's a Schwab file
    if (
        hasattr(st.session_state, "imported_files_data")
        and filename in st.session_state.imported_files_data
    ):
        del st.session_state.imported_files_data[filename]
        deleted = True

    # Check if it's a JSON file
    if (
        hasattr(st.session_state, "imported_json_files")
        and filename in st.session_state.imported_json_files
    ):
        del st.session_state.imported_json_files[filename]
        deleted = True

    if deleted:
        # Regenerate events from remaining files
        _regenerate_all_events()

        st.success(f"✅ Deleted {filename} and regenerated events")
        st.rerun()


def _delete_all_files():
    """Delete all imported files and clear events"""

    # Clear all imported files
    if hasattr(st.session_state, "imported_files_data"):
        st.session_state.imported_files_data = {}

    if hasattr(st.session_state, "imported_json_files"):
        st.session_state.imported_json_files = {}

    # Clear all events
    if "imported_events" in st.session_state:
        del st.session_state.imported_events

    # Clear deduplication stats
    if "deduplication_stats" in st.session_state:
        del st.session_state.deduplication_stats

    st.success("✅ Deleted all files and cleared events")
    st.rerun()


def _generate_reports_automatically():
    """Automatically generate reports after events are updated"""

    # Check if events exist
    if not (
        hasattr(st.session_state, "imported_events")
        and st.session_state.imported_events
    ):
        return

    # Get available years from events
    event_years = set()
    for event in st.session_state.imported_events:
        event_years.add(event.date.year)

    if not event_years:
        return

    # Use the most recent year as default, or existing report year if set
    default_year = max(event_years)
    report_year = getattr(st.session_state, "report_year", default_year)

    # Ensure the report year is valid
    if report_year not in event_years:
        report_year = default_year
        st.session_state.report_year = report_year

    # Use existing exchange rate mode or default to daily
    exchange_rate_mode = getattr(st.session_state, "exchange_rate_mode", "daily")

    # Initialize required components if not present
    if "price_manager" not in st.session_state:
        st.session_state.price_manager = HistoricPriceManager()

    if "fifo_processor" not in st.session_state:
        st.session_state.fifo_processor = FIFOProcessor(st.session_state.price_manager)

    if "report_generator" not in st.session_state:
        st.session_state.report_generator = ReportGenerator(
            st.session_state.fifo_processor, st.session_state.price_manager
        )

    # Show progress
    with st.spinner(f"🔄 Automatically generating reports for {report_year}..."):
        try:
            # Create settings
            current_settings = ReportSettings(
                report_year=report_year,
                exchange_rate_mode=exchange_rate_mode,
            )

            # Process events through FIFO
            st.session_state.fifo_processor.process_events(
                st.session_state.imported_events
            )

            # Generate reports
            st.session_state.tax_reports = (
                st.session_state.report_generator.generate_tax_report(current_settings)
            )
            st.session_state.awv_reports = (
                st.session_state.report_generator.generate_awv_report(current_settings)
            )
            st.session_state.report_settings = current_settings

            # Update timestamp to track when reports were last generated
            st.session_state.reports_last_generated = pd.Timestamp.now()

            # Show success message
            st.success(
                f"✅ Reports automatically generated for {report_year} using {exchange_rate_mode} exchange rates!"
            )
            st.info("📊 Your reports are now ready in the Reports section!")

        except Exception as e:
            st.error(f"❌ Error automatically generating reports: {str(e)}")
            st.error("You can still generate reports manually in the Reports section.")


def _regenerate_all_events():
    """Regenerate all events from all imported files with deduplication"""

    all_events = []

    # Combine events from Schwab files
    if hasattr(st.session_state, "imported_files_data"):
        for file_info in st.session_state.imported_files_data.values():
            all_events.extend(file_info["events"])

    # Combine events from JSON files
    if hasattr(st.session_state, "imported_json_files"):
        for file_info in st.session_state.imported_json_files.values():
            all_events.extend(file_info["events"])

    # Add stock split events for all symbols found in imported events
    if all_events:
        stock_split_events = _generate_stock_split_events(all_events)
        all_events.extend(stock_split_events)

    # Deduplicate events using historic values and metadata
    deduplicated_events, dedup_stats = _deduplicate_events(all_events)

    # Sort events by date
    deduplicated_events.sort(key=lambda event: event.date)

    # Store deduplicated events in session state
    st.session_state.imported_events = deduplicated_events
    st.session_state.deduplication_stats = dedup_stats

    # Update timestamp to trigger report regeneration
    st.session_state.events_last_updated = pd.Timestamp.now()

    # Automatically generate reports
    _generate_reports_automatically()


def _generate_stock_split_events(all_events):
    """Generate stock split events for all symbols found in imported events, optimized to only include splits after earliest transaction date"""
    from src.core.events import StockSplitEvent
    import datetime
    from decimal import Decimal

    # Get all unique symbols from imported events
    symbols = set()
    for event in all_events:
        if hasattr(event, "symbol") and event.symbol:
            symbols.add(event.symbol)

    if not symbols:
        return []

    # Find the earliest and most recent dates from all events
    earliest_date = min(event.date for event in all_events)
    most_recent_date = max(event.date for event in all_events)

    # Get historic price manager instance
    if not hasattr(st.session_state, "historic_price_manager"):
        st.session_state.historic_price_manager = HistoricPriceManager()

    price_manager = st.session_state.historic_price_manager

    stock_split_events = []

    # Generate split events for each symbol
    for symbol in symbols:
        try:
            # Get splits data for this symbol
            splits_df = price_manager.get_splits(symbol, most_recent_date)

            if splits_df is not None and not splits_df.empty:
                # Filter splits to only include those after earliest transaction date
                # Only splits after the earliest transaction can affect our calculations
                relevant_splits = splits_df[splits_df.index >= earliest_date]

                if not relevant_splits.empty:
                    # Convert relevant splits DataFrame to StockSplitEvent objects
                    for split_date, row in relevant_splits.iterrows():
                        split_event = StockSplitEvent(
                            date=split_date,
                            symbol=symbol,
                            shares_after_split=Decimal(str(row["shares_after_split"])),
                        )
                        stock_split_events.append(split_event)

        except Exception as e:
            # Log warning but continue processing other symbols
            st.warning(f"Could not fetch split data for {symbol}: {str(e)}")
            continue

    return stock_split_events


def _regenerate_all_events_from_raw_data():
    """Regenerate all events from raw JSON data using current settings (e.g., when global settings change)"""

    if (
        not hasattr(st.session_state, "imported_files_data")
        or not st.session_state.imported_files_data
    ):
        return

    all_events = []

    # Reprocess each file from its raw JSON data
    for _, file_info in st.session_state.imported_files_data.items():
        # Reprocess the raw JSON content with current settings
        new_events = process_schwab_json_file(file_info["json_content"])

        # Update the stored events for this file
        file_info["events"] = new_events
        file_info["event_count"] = len(new_events)

        # Add to combined events list
        all_events.extend(new_events)

    # Add stock split events for all symbols found in imported events
    if all_events:
        stock_split_events = _generate_stock_split_events(all_events)
        all_events.extend(stock_split_events)

    # Deduplicate events using historic values and metadata
    deduplicated_events, dedup_stats = _deduplicate_events(all_events)

    # Sort all events by date
    deduplicated_events.sort(key=lambda event: event.date)

    # Store combined deduplicated events in session state
    st.session_state.imported_events = deduplicated_events
    st.session_state.deduplication_stats = dedup_stats

    # Update timestamp to trigger report regeneration
    st.session_state.events_last_updated = pd.Timestamp.now()

    # Automatically generate reports
    _generate_reports_automatically()


def _show_duplicate_details():
    """Show detailed information about detected duplicates organized by detection key"""
    stats = st.session_state.deduplication_stats

    if stats["duplicates_removed"] == 0:
        return

    st.header("🔍 Detected Duplicates")

    # Summary by event type
    st.subheader("📊 Duplicates by Event Type")

    if stats["duplicates_by_type"]:
        # Create a DataFrame for better visualization
        type_data = []
        for event_type, count in stats["duplicates_by_type"].items():
            type_data.append({"Event Type": event_type, "Duplicates Found": count})

        df_types = pd.DataFrame(type_data)
        st.dataframe(df_types, use_container_width=True, hide_index=True)

    # Show duplicate groups with override options
    st.markdown(f"### Duplicate Groups ({stats['duplicate_keys_count']} groups)")

    # Organize duplicate groups by event type for display
    groups_by_type = {}
    for key, group_info in stats["duplicate_groups"].items():
        event_type = group_info["event_type"]
        if event_type not in groups_by_type:
            groups_by_type[event_type] = {}
        groups_by_type[event_type][key] = group_info

    # Show duplicate groups for each event type
    for event_type, type_groups in groups_by_type.items():
        total_duplicates_for_type = sum(
            group["duplicate_count"] for group in type_groups.values()
        )

        with st.expander(
            f"🔍 {event_type} Duplicates ({total_duplicates_for_type} duplicates in {len(type_groups)} groups)",
            expanded=False,
        ):

            for i, (detection_key, group_info) in enumerate(type_groups.items(), 1):
                st.markdown(
                    f"**Group {i}**: {group_info['duplicate_count']} duplicates"
                )

                # Override checkbox
                override_key = f"override_{detection_key}"
                is_overridden = st.checkbox(
                    f"Mark as NOT duplicates (keep all {group_info['total_count']} events)",
                    value=group_info.get("overridden", False),
                    key=override_key,
                    help="Check this box if these events should NOT be considered duplicates",
                )

                # Update override status if changed
                if is_overridden != group_info.get("overridden", False):
                    group_info["overridden"] = is_overridden
                    _apply_duplicate_overrides()
                    st.rerun()

                # Create a combined table showing kept event + duplicates
                all_events = [group_info["kept_event"]] + group_info["duplicate_events"]
                event_rows = []

                for j, event in enumerate(all_events):
                    if is_overridden:
                        status = f"✅ KEPT #{j+1}"
                    else:
                        status = "✅ KEPT" if j == 0 else f"❌ DUPLICATE #{j}"

                    # Extract key information based on event type
                    row_data = {
                        "Status": status,
                        "Date": str(event.date),
                        "Symbol": getattr(event, "symbol", "N/A"),
                        "Currency": getattr(event, "currency", "N/A"),
                    }

                    # Add grant/transaction ID if available
                    if (
                        event_type == "RSU"
                        and hasattr(event, "grant_id")
                        and getattr(event, "grant_id")
                    ):
                        row_data["Grant ID"] = str(getattr(event, "grant_id", "N/A"))
                    elif (
                        event_type == "Sell"
                        and hasattr(event, "transaction_id")
                        and getattr(event, "transaction_id")
                    ):
                        row_data["Transaction ID"] = str(
                            getattr(event, "transaction_id", "N/A")
                        )

                    # Add type-specific information
                    if event_type in ["Buy", "Sell"]:
                        if hasattr(event, "historic_shares_quantity"):
                            row_data["Historic Quantity"] = str(
                                getattr(event, "historic_shares_quantity", "N/A")
                            )
                        if hasattr(event, "historic_quantity"):
                            row_data["Historic Quantity"] = str(
                                getattr(event, "historic_quantity", "N/A")
                            )
                        if hasattr(event, "historic_shares_price"):
                            row_data["Historic Price"] = str(
                                getattr(event, "historic_shares_price", "N/A")
                            )
                        elif hasattr(event, "historic_sell_price"):
                            row_data["Historic Price"] = str(
                                getattr(event, "historic_sell_price", "N/A")
                            )
                        if hasattr(event, "fees"):
                            row_data["Fees"] = str(getattr(event, "fees", "N/A"))

                    elif event_type == "RSU":
                        if hasattr(event, "historic_received_shares_quantity"):
                            row_data["Received Qty"] = str(
                                getattr(
                                    event, "historic_received_shares_quantity", "N/A"
                                )
                            )
                        if hasattr(event, "historic_received_shares_price"):
                            row_data["Received Price"] = str(
                                getattr(event, "historic_received_shares_price", "N/A")
                            )
                        if hasattr(event, "historic_withheld_shares_quantity"):
                            row_data["Withheld Qty"] = str(
                                getattr(
                                    event, "historic_withheld_shares_quantity", "N/A"
                                )
                            )

                    elif event_type == "ESPP":
                        if hasattr(event, "historic_shares_quantity"):
                            row_data["Quantity"] = str(
                                getattr(event, "historic_shares_quantity", "N/A")
                            )
                        if hasattr(event, "historic_fair_market_value"):
                            row_data["FMV"] = str(
                                getattr(event, "historic_fair_market_value", "N/A")
                            )

                    elif event_type == "Dividend":
                        if hasattr(event, "dividend_amount"):
                            row_data["Amount"] = str(
                                getattr(event, "dividend_amount", "N/A")
                            )

                    elif event_type == "Tax":
                        if hasattr(event, "withheld_tax_amount"):
                            row_data["Withheld Tax"] = str(
                                getattr(event, "withheld_tax_amount", "N/A")
                            )
                        if hasattr(event, "reverted_tax_amount"):
                            row_data["Reverted Tax"] = str(
                                getattr(event, "reverted_tax_amount", "N/A")
                            )

                    elif event_type in ["MoneyDeposit", "MoneyWithdrawal"]:
                        if hasattr(event, "amount"):
                            row_data["Amount"] = str(getattr(event, "amount", "N/A"))
                        if hasattr(event, "fees"):
                            row_data["Fees"] = str(getattr(event, "fees", "N/A"))

                    elif event_type == "CurrencyConversion":
                        if hasattr(event, "source_amount"):
                            row_data["Source Amount"] = str(
                                getattr(event, "source_amount", "N/A")
                            )
                        if hasattr(event, "source_currency"):
                            row_data["Source Currency"] = str(
                                getattr(event, "source_currency", "N/A")
                            )
                        if hasattr(event, "target_amount"):
                            row_data["Target Amount"] = str(
                                getattr(event, "target_amount", "N/A")
                            )
                        if hasattr(event, "target_currency"):
                            row_data["Target Currency"] = str(
                                getattr(event, "target_currency", "N/A")
                            )

                    elif event_type == "StockSplit":
                        if hasattr(event, "shares_after_split"):
                            row_data["Shares After Split"] = str(
                                getattr(event, "shares_after_split", "N/A")
                            )

                    event_rows.append(row_data)

                if event_rows:
                    df_group = pd.DataFrame(event_rows)
                    st.dataframe(df_group, use_container_width=True, hide_index=True)


def _show_export_options():
    """Show export options for current events data"""

    st.header("📤 Export Data")

    # Check if we have any events to export
    if (
        not hasattr(st.session_state, "imported_events")
        or not st.session_state.imported_events
    ):
        st.info(
            "📝 No events to export. Import some data or manually add events first."
        )
        return

    events = st.session_state.imported_events

    # Single button export - direct download
    json_data = _prepare_json_export(events)
    json_str = json.dumps(json_data, indent=2, default=str)

    st.download_button(
        label="📥 Export JSON",
        data=json_str,
        file_name=f"pyFIFOtax_events_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json",
        mime="application/json",
        key="export_json",
        use_container_width=False,
    )


def _prepare_json_export(events) -> dict:
    """Prepare events data for JSON export"""

    export_data = {
        "metadata": {
            "export_timestamp": pd.Timestamp.now().isoformat(),
            "total_events": len(events),
            "pyFIFOtax_version": "2.0",
            "format_version": "1.0",
        },
        "events": [],
    }

    # Convert events to exportable format
    for event in events:
        event_data = {"event_type": event.get_event_type(), "date": str(event.date)}

        # Add all event attributes
        for attr_name in dir(event):
            # Skip internal attributes and methods
            if attr_name.startswith("_") or callable(getattr(event, attr_name)):
                continue

            # Skip attributes we've already added
            if attr_name in ["date", "priority"]:
                continue

            value = getattr(event, attr_name)

            # Handle None values and convert Decimals to strings for JSON serialization
            if value is None:
                event_data[attr_name] = None
            elif hasattr(value, "__str__") and str(type(value)).find("Decimal") != -1:
                event_data[attr_name] = str(value)
            else:
                event_data[attr_name] = value

        export_data["events"].append(event_data)

    return export_data
