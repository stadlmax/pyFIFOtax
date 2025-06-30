"""
Events page for pyFIFOtax Modern UI
Display transaction events as editable dataframes organized by event type
"""

import streamlit as st
import pandas as pd
from decimal import Decimal, InvalidOperation
from typing import List
from src.core.events import ReportEvent


def _get_event_info(event_type: str) -> dict:
    """Get emoji and description for each event type"""
    event_mapping = {
        "Buy": {"emoji": "🛒", "description": "stock purchase"},
        "Sell": {"emoji": "💰", "description": "stock sale"},
        "Dividend": {"emoji": "💸", "description": "dividend payment"},
        "RSU": {"emoji": "🎁", "description": "RSU vest"},
        "ESPP": {"emoji": "📈", "description": "ESPP purchase"},
        "Tax": {"emoji": "🏛️", "description": "tax withholding"},
        "MoneyDeposit": {"emoji": "💳", "description": "money deposit"},
        "MoneyWithdrawal": {"emoji": "🏧", "description": "money withdrawal"},
        "CurrencyConversion": {"emoji": "🔄", "description": "currency conversion"},
        "StockSplit": {"emoji": "📊", "description": "stock split"},
    }

    return event_mapping.get(event_type, {"emoji": "📋", "description": "transaction"})


def show_event_type(event_type: str):
    """Display events of a specific type in an editable dataframe"""

    # Get emoji and description for event type
    event_info = _get_event_info(event_type)

    st.title(f"{event_info['emoji']} {event_type} Events")
    st.markdown(f"View and edit your {event_info['description']} transactions")
    st.markdown("---")

    # Get imported events
    all_events = []
    if (
        hasattr(st.session_state, "imported_events")
        and st.session_state.imported_events
    ):
        all_events = st.session_state.imported_events

    # Filter events by type
    filtered_events = [
        event for event in all_events if event.get_event_type() == event_type
    ]

    if not filtered_events:
        st.info(
            f"📥 No {event_type} events found. You can add new {event_type.lower()} events using the table below or go to the Import Data page to upload files."
        )
        # Create empty dataframe with appropriate columns for this event type
        df = _create_empty_dataframe_for_event_type(event_type)
    else:
        # Convert events to dataframe
        df = _events_to_dataframe(filtered_events)

    # Section header for the editable data
    st.header(f"📝 Edit {event_type} Events")
    if filtered_events:
        st.markdown(
            f"**{len(filtered_events)}** {event_type.lower()} events found • Double-click cells to edit • Add/remove rows as needed"
        )
    else:
        st.markdown(
            f"**Add new {event_type.lower()} events** • Double-click cells to edit • Add/remove rows as needed"
        )

    # Display editable dataframe with raw values for accuracy
    edited_df = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic",  # Allow adding/removing rows
        key=f"{event_type}_data_editor",
    )

    # Check if data has been edited and validate changes before applying
    if not edited_df.equals(df):
        # Validate the edited data
        validation_result = _validate_edited_dataframe(edited_df, event_type)

        if validation_result["is_valid"]:
            try:
                updated_events = _update_events_from_dataframe(
                    edited_df, event_type, all_events
                )
                st.success(
                    f"✅ Changes saved! {len(updated_events)} {event_type.lower()} events updated successfully."
                )
                st.rerun()  # Refresh to show updated data
            except Exception as e:
                st.error(f"❌ Error updating events: {str(e)}")
        else:
            # Show validation errors
            st.error(
                "❌ **Validation Failed** - Please fix the following issues before saving:"
            )
            for i, error in enumerate(validation_result["errors"], 1):
                st.error(f"**{i}.** {error}")

            if validation_result["warnings"]:
                st.warning("⚠️ **Warnings** - Please review these potential issues:")
                for i, warning in enumerate(validation_result["warnings"], 1):
                    st.warning(f"**{i}.** {warning}")

            st.info(
                "💡 **Tips:** Double-check dates (YYYY-MM-DD), numbers (positive values), symbols (valid tickers), and required fields."
            )


def _events_to_dataframe(events: list[ReportEvent]) -> pd.DataFrame:
    """Convert a list of events to a pandas DataFrame using exact attribute names"""

    if not events:
        return pd.DataFrame()

    # Create data rows by introspecting event attributes
    rows = []
    for event in events:
        row = {}

        # Add basic fields with exact names
        row["date"] = str(event.date)
        row["event_type"] = event.get_event_type()

        # Add all attributes from the event object using exact attribute names
        for attr_name in dir(event):
            # Skip internal attributes and methods
            if attr_name.startswith("_") or callable(getattr(event, attr_name)):
                continue

            # Skip attributes we've already added
            if attr_name in ["date", "priority"]:
                continue

            value = getattr(event, attr_name)

            # Convert Decimals to strings for display
            row[attr_name] = format_value_for_display(value)

        rows.append(row)

    # Create DataFrame and convert for display
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # Define standard column order using exact attribute names
    standard_columns = ["date", "event_type", "symbol", "currency"]

    # Reorder columns: standard columns first, then alphabetically sorted others
    existing_standard_cols = [col for col in standard_columns if col in df.columns]
    other_cols = sorted([col for col in df.columns if col not in standard_columns])
    column_order = existing_standard_cols + other_cols

    df = df[column_order]

    return df


def _update_events_from_dataframe(
    edited_df: pd.DataFrame, event_type: str, all_events: list
) -> list[ReportEvent]:
    """Update the session state events list based on changes to the dataframe"""

    # Remove all events of this type from the list
    filtered_events = [
        event for event in all_events if event.get_event_type() != event_type
    ]

    # Convert edited dataframe back to events and add to the list
    new_events = _dataframe_to_events(edited_df, event_type)
    filtered_events.extend(new_events)

    # Update session state
    if hasattr(st.session_state, "imported_events"):
        st.session_state.imported_events = filtered_events
    else:
        # If using sample events, store the updated events in session state
        st.session_state.imported_events = filtered_events

    # Update timestamp to trigger report regeneration
    st.session_state.events_last_updated = pd.Timestamp.now()

    return new_events


def _validate_edited_dataframe(df: pd.DataFrame, event_type: str) -> dict:
    """
    Validate edited dataframe for data integrity, business logic, and consistency.

    Returns:
        dict: {
            "is_valid": bool,
            "errors": list[str],  # Critical issues that prevent saving
            "warnings": list[str]  # Potential issues that user should review
        }
    """
    errors: List[str] = []
    warnings: List[str] = []

    if df.empty:
        return {"is_valid": True, "errors": [], "warnings": []}

    # Validate each row
    for row_num, (idx, row) in enumerate(df.iterrows(), 1):
        # 1. Validate basic data types and formats
        _validate_basic_fields(row, row_num, event_type, errors, warnings)

        # 2. Validate event-specific business logic
        _validate_event_specific_logic(row, row_num, event_type, errors, warnings)

        # 3. Validate consistency within the row
        _validate_row_consistency(row, row_num, event_type, errors, warnings)

    # 4. Validate cross-row consistency
    _validate_cross_row_consistency(df, event_type, errors, warnings)

    # Return validation result
    is_valid = len(errors) == 0
    return {"is_valid": is_valid, "errors": errors, "warnings": warnings}


def _validate_basic_fields(
    row, row_num: int, event_type: str, errors: List[str], warnings: List[str]
):
    """Validate basic field formats and data types"""

    # Date validation
    date_str = str(row.get("date", "")).strip()
    if not date_str or date_str.lower() in ["", "none", "nan"]:
        errors.append(f"Row {row_num}: Date is required")
    else:
        try:
            if len(date_str) != 10:
                errors.append(
                    f"Row {row_num}: Date must be in YYYY-MM-DD format, got '{date_str}'"
                )
            else:
                import datetime

                datetime.datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            errors.append(
                f"Row {row_num}: Invalid date format '{date_str}' - use YYYY-MM-DD"
            )

    # Symbol validation (for events that have symbols)
    if event_type in ["Buy", "Sell", "RSU", "ESPP", "Dividend", "Tax", "StockSplit"]:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol or symbol.lower() in ["", "none", "nan", "unknown"]:
            errors.append(f"Row {row_num}: Symbol is required for {event_type} events")
        elif len(symbol) > 10:
            warnings.append(
                f"Row {row_num}: Symbol '{symbol}' is unusually long - verify it's correct"
            )
        elif not symbol.replace(".", "").replace("-", "").isalnum():
            warnings.append(
                f"Row {row_num}: Symbol '{symbol}' contains unusual characters"
            )

    # Currency validation
    currency = str(row.get("currency", "")).strip().upper()
    if not currency or currency.lower() in ["", "none", "nan"]:
        warnings.append(f"Row {row_num}: Currency is missing - will default to USD")
    elif len(currency) != 3:
        warnings.append(
            f"Row {row_num}: Currency '{currency}' should be 3 letters (e.g., USD, EUR)"
        )


def _validate_event_specific_logic(
    row, row_num: int, event_type: str, errors: List[str], warnings: List[str]
):
    """Validate business logic specific to each event type"""

    from decimal import Decimal, InvalidOperation

    def parse_decimal_safe(value):
        """Parse decimal safely, return None if invalid"""
        if pd.isna(value) or str(value).strip().lower() in ["", "none", "nan"]:
            return None
        try:
            return Decimal(str(value).strip())
        except (InvalidOperation, ValueError):
            return "INVALID"

    if event_type == "Buy":
        # Validate required fields
        quantity = parse_decimal_safe(row.get("imported_shares_quantity"))
        price = parse_decimal_safe(row.get("imported_shares_price"))
        cost = parse_decimal_safe(row.get("cost_of_shares"))

        if quantity is None:
            errors.append(f"Row {row_num}: Shares quantity is required for Buy events")
        elif quantity == "INVALID":
            errors.append(f"Row {row_num}: Invalid shares quantity format")
        elif quantity <= 0:
            errors.append(f"Row {row_num}: Shares quantity must be positive")

        if price is None:
            errors.append(f"Row {row_num}: Share price is required for Buy events")
        elif price == "INVALID":
            errors.append(f"Row {row_num}: Invalid share price format")
        elif price <= 0:
            errors.append(f"Row {row_num}: Share price must be positive")

        if cost is None:
            errors.append(f"Row {row_num}: Cost of shares is required for Buy events")
        elif cost == "INVALID":
            errors.append(f"Row {row_num}: Invalid cost of shares format")
        elif cost <= 0:
            errors.append(f"Row {row_num}: Cost of shares must be positive")

        # Cross-field validation
        if (
            quantity
            and price
            and cost
            and all(x != "INVALID" for x in [quantity, price, cost])
        ):
            expected_cost = quantity * price
            if abs(cost - expected_cost) > expected_cost * Decimal(
                "0.01"
            ):  # 1% tolerance
                warnings.append(
                    f"Row {row_num}: Cost of shares ({cost}) doesn't match quantity × price ({expected_cost:.2f})"
                )

    elif event_type == "Sell":
        # Similar validation for Sell events
        quantity = parse_decimal_safe(row.get("imported_quantity"))
        price = parse_decimal_safe(row.get("imported_sell_price"))
        proceeds = parse_decimal_safe(row.get("proceeds"))

        if quantity is None:
            errors.append(f"Row {row_num}: Quantity is required for Sell events")
        elif quantity == "INVALID":
            errors.append(f"Row {row_num}: Invalid quantity format")
        elif quantity <= 0:
            errors.append(f"Row {row_num}: Quantity must be positive")

        if price is None:
            errors.append(f"Row {row_num}: Sell price is required for Sell events")
        elif price == "INVALID":
            errors.append(f"Row {row_num}: Invalid sell price format")
        elif price <= 0:
            errors.append(f"Row {row_num}: Sell price must be positive")

        if proceeds is None:
            warnings.append(f"Row {row_num}: Proceeds is missing")
        elif proceeds == "INVALID":
            errors.append(f"Row {row_num}: Invalid proceeds format")
        elif proceeds < 0:
            warnings.append(
                f"Row {row_num}: Negative proceeds - verify this is correct"
            )

    elif event_type == "RSU":
        # RSU-specific validation
        received_qty = parse_decimal_safe(row.get("imported_received_shares_quantity"))
        received_price = parse_decimal_safe(row.get("imported_received_shares_price"))

        if received_qty is None:
            errors.append(
                f"Row {row_num}: Received shares quantity is required for RSU events"
            )
        elif received_qty == "INVALID":
            errors.append(f"Row {row_num}: Invalid received shares quantity format")
        elif received_qty <= 0:
            errors.append(f"Row {row_num}: Received shares quantity must be positive")

        if received_price is None:
            errors.append(
                f"Row {row_num}: Received shares price is required for RSU events"
            )
        elif received_price == "INVALID":
            errors.append(f"Row {row_num}: Invalid received shares price format")
        elif received_price <= 0:
            errors.append(f"Row {row_num}: Received shares price must be positive")

    elif event_type == "ESPP":
        # ESPP-specific validation
        quantity = parse_decimal_safe(row.get("imported_shares_quantity"))
        purchase_price = parse_decimal_safe(row.get("imported_shares_price"))
        fmv = parse_decimal_safe(row.get("imported_fair_market_value"))

        if quantity is None:
            errors.append(f"Row {row_num}: Shares quantity is required for ESPP events")
        elif quantity == "INVALID":
            errors.append(f"Row {row_num}: Invalid shares quantity format")
        elif quantity <= 0:
            errors.append(f"Row {row_num}: Shares quantity must be positive")

        if purchase_price is None:
            errors.append(f"Row {row_num}: Purchase price is required for ESPP events")
        elif purchase_price == "INVALID":
            errors.append(f"Row {row_num}: Invalid purchase price format")
        elif purchase_price <= 0:
            errors.append(f"Row {row_num}: Purchase price must be positive")

        if fmv is None:
            errors.append(
                f"Row {row_num}: Fair market value is required for ESPP events"
            )
        elif fmv == "INVALID":
            errors.append(f"Row {row_num}: Invalid fair market value format")
        elif fmv <= 0:
            errors.append(f"Row {row_num}: Fair market value must be positive")

        # ESPP business logic: purchase price should be <= FMV
        if (
            purchase_price
            and fmv
            and all(x != "INVALID" for x in [purchase_price, fmv])
        ):
            if purchase_price > fmv:
                warnings.append(
                    f"Row {row_num}: Purchase price ({purchase_price}) is higher than fair market value ({fmv}) - verify this is correct"
                )

    elif event_type in ["MoneyDeposit", "MoneyWithdrawal"]:
        # Money transfer validation
        amount = parse_decimal_safe(row.get("amount"))

        if amount is None:
            errors.append(f"Row {row_num}: Amount is required for {event_type} events")
        elif amount == "INVALID":
            errors.append(f"Row {row_num}: Invalid amount format")
        elif amount <= 0:
            errors.append(f"Row {row_num}: Amount must be positive")


def _validate_row_consistency(
    row, row_num: int, event_type: str, errors: List[str], warnings: List[str]
):
    """Validate consistency within a single row"""

    # Check for imported vs historic value consistency
    if event_type in ["Buy", "Sell", "RSU", "ESPP"]:
        # If both imported and historic values exist, they should be reasonable
        for field_base in [
            "shares_quantity",
            "shares_price",
            "quantity",
            "sell_price",
            "received_shares_quantity",
            "received_shares_price",
            "fair_market_value",
        ]:
            imported_key = f"imported_{field_base}"
            historic_key = f"historic_{field_base}"

            if imported_key in row and historic_key in row:
                imported_val = row.get(imported_key)
                historic_val = row.get(historic_key)

                if pd.notna(imported_val) and pd.notna(historic_val):
                    try:
                        imported_dec = Decimal(str(imported_val))
                        historic_dec = Decimal(str(historic_val))

                        # Historic values should generally be >= imported values (due to splits)
                        if historic_dec < imported_dec * Decimal(
                            "0.1"
                        ):  # Allow for large splits
                            warnings.append(
                                f"Row {row_num}: Historic {field_base} ({historic_dec}) seems unusually small compared to imported value ({imported_dec})"
                            )
                    except:
                        pass  # Already caught in other validations


def _validate_cross_row_consistency(
    df: pd.DataFrame, event_type: str, errors: List[str], warnings: List[str]
):
    """Validate consistency across multiple rows"""

    # Check for duplicate transactions (same date, symbol, amounts)
    if event_type in ["Buy", "Sell", "RSU", "ESPP"]:
        key_columns = ["date", "symbol"]

        # Add event-specific identifying columns
        if event_type == "Buy":
            key_columns.extend(["imported_shares_quantity", "imported_shares_price"])
        elif event_type == "Sell":
            key_columns.extend(["imported_quantity", "imported_sell_price"])
        elif event_type == "RSU":
            key_columns.extend(["imported_received_shares_quantity", "grant_id"])
        elif event_type == "ESPP":
            key_columns.extend(["imported_shares_quantity", "imported_shares_price"])

        # Check for exact duplicates
        available_columns = [col for col in key_columns if col in df.columns]
        if len(available_columns) >= 2:
            duplicates = df[df.duplicated(subset=available_columns, keep=False)]
            if not duplicates.empty:
                duplicate_groups = duplicates.groupby(available_columns)
                for group_values, group_df in duplicate_groups:
                    row_numbers = ", ".join(str(idx + 1) for idx in group_df.index)
                    warnings.append(
                        f"Potential duplicate {event_type} transactions in rows: {row_numbers}"
                    )

    # Date order validation
    if "date" in df.columns:
        dates = []
        for row_num, date_str in enumerate(df["date"], 1):
            try:
                import datetime

                date_obj = datetime.datetime.strptime(str(date_str), "%Y-%m-%d").date()
                dates.append((date_obj, row_num))
            except:
                pass  # Date format errors already caught

        if dates:
            dates.sort()
            # Check for dates far in the future
            import datetime

            today = datetime.date.today()
            future_dates = [(date, row_num) for date, row_num in dates if date > today]
            if future_dates:
                future_rows = ", ".join(str(row_num) for _, row_num in future_dates)
                warnings.append(
                    f"Future dates detected in rows: {future_rows} - verify these are correct"
                )


def _dataframe_to_events(df: pd.DataFrame, event_type: str) -> list[ReportEvent]:
    """Convert a dataframe back to a list of ReportEvent objects"""

    events = []

    for _, row in df.iterrows():
        try:
            event = _create_event_from_row(row, event_type)
            if event:
                events.append(event)
        except Exception as e:
            st.warning(f"⚠️ Skipping invalid row: {str(e)}")
            continue

    return events


def _create_event_from_row(row: pd.Series, event_type: str) -> ReportEvent:
    """Create a specific event object from a dataframe row using exact attribute names"""

    # Import here to avoid circular imports
    from src.core.events import (
        BuyEvent,
        SellEvent,
        DividendEvent,
        TaxEvent,
        RSUEvent,
        ESPPEvent,
        MoneyDepositEvent,
        MoneyWithdrawalEvent,
        CurrencyConversionEvent,
        StockSplitEvent,
        EventPriority,
    )
    import datetime
    from decimal import Decimal

    # Parse date using exact attribute name
    date_str = str(row.get("date", ""))
    try:
        if len(date_str) == 10:  # YYYY-MM-DD format
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        else:
            raise ValueError(f"Invalid date format: {date_str}")
    except ValueError as e:
        raise ValueError(f"Invalid date '{date_str}': {str(e)}")

    # Helper function to parse decimal values
    def parse_decimal(value, default=None):
        """Parse decimal values from dataframe, handling string representations"""
        if pd.isna(value) or value == "" or value is None or value == "None":
            return default
        try:
            # Handle string representations of decimals (from display conversion)
            str_val = str(value).strip()
            if str_val == "" or str_val.lower() == "none":
                return default
            return Decimal(str_val)
        except (ValueError, TypeError, InvalidOperation):
            # If conversion fails, return default
            return default

    # Helper function to get string value
    def get_str(key, default=""):
        value = row.get(key, default)
        return str(value) if value and not pd.isna(value) else default

    # Create event based on type using exact attribute names
    if event_type == "Buy":
        # Use imported values for constructor, let constructor handle historic conversion
        return BuyEvent(
            date=date,
            symbol=get_str("symbol", "UNKNOWN"),
            shares_quantity=parse_decimal(
                row.get("imported_shares_quantity"), Decimal("0")
            ),
            shares_price=parse_decimal(row.get("imported_shares_price"), Decimal("0")),
            cost_of_shares=parse_decimal(row.get("cost_of_shares"), Decimal("0")),
            fees=parse_decimal(row.get("fees")),
            currency=get_str("currency", "USD"),
        )

    elif event_type == "Sell":
        return SellEvent(
            date=date,
            symbol=get_str("symbol", "UNKNOWN"),
            currency=get_str("currency", "USD"),
            quantity=parse_decimal(row.get("imported_quantity"), Decimal("0")),
            sell_price=parse_decimal(row.get("imported_sell_price"), Decimal("0")),
            proceeds=parse_decimal(row.get("proceeds"), Decimal("0")),
            fees=parse_decimal(row.get("fees")),
            transaction_id=get_str("transaction_id"),
        )

    elif event_type == "Dividend":
        return DividendEvent(
            date=date,
            symbol=get_str("symbol", "UNKNOWN"),
            currency=get_str("currency", "USD"),
            dividend_amount=parse_decimal(row.get("dividend_amount")),
        )

    elif event_type == "Tax":
        return TaxEvent(
            date=date,
            symbol=get_str("symbol", "UNKNOWN"),
            currency=get_str("currency", "USD"),
            withheld_tax_amount=parse_decimal(row.get("withheld_tax_amount")),
            reverted_tax_amount=parse_decimal(row.get("reverted_tax_amount")),
        )

    elif event_type == "RSU":
        return RSUEvent(
            date=date,
            symbol=get_str("symbol", "UNKNOWN"),
            received_shares_quantity=parse_decimal(
                row.get("imported_received_shares_quantity"), Decimal("0")
            ),
            received_shares_price=parse_decimal(
                row.get("imported_received_shares_price"), Decimal("0")
            ),
            withheld_shares_quantity=parse_decimal(
                row.get("imported_withheld_shares_quantity")
            ),
            currency=get_str("currency", "USD"),
            grant_id=get_str("grant_id"),
        )

    elif event_type == "ESPP":
        return ESPPEvent(
            date=date,
            symbol=get_str("symbol", "UNKNOWN"),
            currency=get_str("currency", "USD"),
            shares_quantity=parse_decimal(
                row.get("imported_shares_quantity"), Decimal("0")
            ),
            shares_price=parse_decimal(row.get("imported_shares_price"), Decimal("0")),
            fair_market_value=parse_decimal(
                row.get("imported_fair_market_value"), Decimal("0")
            ),
        )

    elif event_type == "MoneyDeposit":
        return MoneyDepositEvent(
            date=date,
            buy_date=date,  # Use same date for buy_date if not specified
            amount=parse_decimal(row.get("amount"), Decimal("0")),
            fees=parse_decimal(row.get("fees")),
            currency=get_str("currency", "USD"),
        )

    elif event_type == "MoneyWithdrawal":
        return MoneyWithdrawalEvent(
            date=date,
            buy_date=date,  # Use same date for buy_date if not specified
            amount=parse_decimal(row.get("amount"), Decimal("0")),
            fees=parse_decimal(row.get("fees")),
            currency=get_str("currency", "USD"),
        )

    elif event_type == "CurrencyConversion":
        return CurrencyConversionEvent(
            date=date,
            source_amount=parse_decimal(row.get("source_amount"), Decimal("0")),
            source_currency=get_str("source_currency", "USD"),
            target_amount=parse_decimal(row.get("target_amount"), Decimal("0")),
            target_currency=get_str("target_currency", "USD"),
            fees=parse_decimal(row.get("fees")),
            priority=EventPriority.CURRENCY_CONVERSION_FROM_FOREX_TO_FOREX,
        )

    elif event_type == "StockSplit":
        return StockSplitEvent(
            date=date,
            symbol=get_str("symbol", "UNKNOWN"),
            shares_after_split=parse_decimal(
                row.get("shares_after_split"), Decimal("0")
            ),
        )

    else:
        raise ValueError(f"Unknown event type: {event_type}")


def _create_empty_dataframe_for_event_type(event_type: str) -> pd.DataFrame:
    """Create an empty dataframe with appropriate columns for the event type"""

    # Base columns that all events have
    base_columns = {"date": "", "event_type": event_type, "currency": "USD"}

    # Event-specific columns
    if event_type == "Buy":
        columns = {
            **base_columns,
            "symbol": "",
            "imported_shares_quantity": "",
            "imported_shares_price": "",
            "historic_shares_quantity": "",
            "historic_shares_price": "",
            "cost_of_shares": "",
            "fees": "",
        }
    elif event_type == "Sell":
        columns = {
            **base_columns,
            "symbol": "",
            "imported_quantity": "",
            "imported_sell_price": "",
            "historic_quantity": "",
            "historic_sell_price": "",
            "proceeds": "",
            "fees": "",
            "transaction_id": "",
        }
    elif event_type == "RSU":
        columns = {
            **base_columns,
            "symbol": "",
            "imported_received_shares_quantity": "",
            "imported_received_shares_price": "",
            "imported_withheld_shares_quantity": "",
            "historic_received_shares_quantity": "",
            "historic_received_shares_price": "",
            "historic_withheld_shares_quantity": "",
            "grant_id": "",
        }
    elif event_type == "ESPP":
        columns = {
            **base_columns,
            "symbol": "",
            "imported_shares_quantity": "",
            "imported_shares_price": "",
            "imported_fair_market_value": "",
            "historic_shares_quantity": "",
            "historic_shares_price": "",
            "historic_fair_market_value": "",
            "contribution": "",
            "bonus": "",
        }
    elif event_type == "Dividend":
        columns = {
            **base_columns,
            "symbol": "",
            "dividend_amount": "",
        }
    elif event_type == "Tax":
        columns = {
            **base_columns,
            "symbol": "",
            "withheld_tax_amount": "",
            "reverted_tax_amount": "",
        }
    elif event_type == "MoneyDeposit":
        columns = {
            **base_columns,
            "amount": "",
            "fees": "",
            "buy_date": "",
        }
    elif event_type == "MoneyWithdrawal":
        columns = {
            **base_columns,
            "amount": "",
            "fees": "",
            "buy_date": "",
        }
    elif event_type == "CurrencyConversion":
        columns = {
            **base_columns,
            "source_amount": "",
            "source_currency": "USD",
            "target_amount": "",
            "target_currency": "EUR",
            "fees": "",
        }
    elif event_type == "StockSplit":
        columns = {
            **base_columns,
            "symbol": "",
            "shares_after_split": "",
        }
    else:
        # Default columns for unknown event types
        columns = base_columns

    # Create DataFrame with one empty row
    df = pd.DataFrame([columns])

    # Define standard column order
    standard_columns = ["date", "event_type", "symbol", "currency"]
    existing_standard_cols = [col for col in standard_columns if col in df.columns]
    other_cols = sorted([col for col in df.columns if col not in standard_columns])
    column_order = existing_standard_cols + other_cols

    # Reorder columns
    df = df[column_order]

    return df


def format_value_for_display(value):
    """Convert individual values to appropriate display format"""
    from decimal import Decimal

    if isinstance(value, Decimal):
        return str(value)
    return value
