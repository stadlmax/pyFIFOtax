"""
FIFO Debugging Page
Shows step-by-step FIFO processing to help debug issues
"""

import streamlit as st
import pandas as pd
from src.core.fifo_processor import FIFOProcessor


def show():
    """Show FIFO debugging page"""
    st.header("🔍 FIFO Debugging")

    # Check if we have imported events
    if (
        not hasattr(st.session_state, "imported_events")
        or not st.session_state.imported_events
    ):
        st.warning("⚠️ No events imported. Please import transaction data first.")
        return

    # Check if we have a price manager
    if not hasattr(st.session_state, "price_manager"):
        st.warning(
            "⚠️ Price manager not initialized. Please check the import data page."
        )
        return

    # Process events with debug capture automatically
    with st.spinner("Processing events and capturing debug information..."):
        try:
            # Use the main FIFO processor (debug info always captured)
            debug_processor = FIFOProcessor(st.session_state.price_manager)

            # Process events - this uses the exact same logic as production
            debug_processor.process_events(st.session_state.imported_events)

            # Group debug steps by event
            events_with_steps = _group_steps_by_event(
                debug_processor.debug_steps,
                st.session_state.imported_events,
                debug_processor,
            )

            # Display processing results
            if debug_processor.has_errors:
                st.error(
                    f"❌ FIFO Queue Error: Processing stopped after {debug_processor.events_processed_before_error} events"
                )
                st.warning(
                    f"⚠️ Error in event {debug_processor.error_event_index + 1}: {debug_processor.error_message}"
                )
                st.info(
                    "📋 Tax reports cannot be generated when FIFO errors occur. Review the error details below."
                )

                # Display error event details
                if debug_processor.error_event:
                    st.subheader("🔍 Error Event Details")
                    _display_error_event_details(
                        debug_processor.error_event, debug_processor
                    )
            else:
                st.success(
                    f"✅ Processed {len(st.session_state.imported_events)} events with {len(debug_processor.debug_steps)} FIFO operations"
                )

            # Display events with their FIFO operations
            if events_with_steps:
                st.subheader(f"📋 Events and FIFO Operations")
                if debug_processor.has_errors:
                    st.info(
                        f"✅ {debug_processor.events_processed_before_error} events processed successfully, then ❌ error occurred at event {debug_processor.error_event_index + 1}"
                    )

                for i, event_info in enumerate(events_with_steps):
                    event = event_info["event"]
                    steps = event_info["steps"]

                    # Create event summary
                    event_summary = _get_event_summary(event)

                    # Mark error event with special styling
                    is_error_event = (
                        debug_processor.has_errors
                        and debug_processor.error_event
                        and event is debug_processor.error_event
                    )

                    if is_error_event:
                        event_summary = f"🔴 {event_summary} - **ERROR EVENT**"
                        expanded = True
                    else:
                        expanded = False

                    with st.expander(
                        f"Event {i+1}: {event_summary}", expanded=expanded
                    ):
                        if steps:
                            _display_interleaved_flow(steps)
                        else:
                            st.write("*No FIFO operations for this event*")
            else:
                st.info("No FIFO operations found.")

        except Exception as e:
            st.error(f"❌ Error during debug analysis: {str(e)}")
            st.exception(e)


def _group_steps_by_event(debug_steps, events, debug_processor):
    """Group debug steps by their originating event"""
    # Create a mapping using event index for unique identification
    event_map = {}
    for i, event in enumerate(events):
        key = i  # Use the event index as the unique key
        event_map[key] = event

    # Determine how many events to show based on error state
    if debug_processor.has_errors:
        # Show all successfully processed events plus the error event
        max_event_index = debug_processor.error_event_index
        show_error_event = True
    else:
        # Show all events
        max_event_index = len(events) - 1
        show_error_event = False

    # Group steps by event using event_index
    events_with_steps = []
    current_event_key = None
    current_steps = []

    for step in debug_steps:
        step_key = step.event_index

        # Only process steps for events we want to show
        if step_key > max_event_index:
            continue

        if step_key != current_event_key:
            # Save previous event if it exists
            if current_event_key is not None and current_event_key in event_map:
                events_with_steps.append(
                    {"event": event_map[current_event_key], "steps": current_steps}
                )

            # Start new event
            current_event_key = step_key
            current_steps = [step]
        else:
            # Add to current event
            current_steps.append(step)

    # Don't forget the last event
    if current_event_key is not None and current_event_key in event_map:
        events_with_steps.append(
            {"event": event_map[current_event_key], "steps": current_steps}
        )

    # Also add events that had no FIFO operations (but only up to the limit)
    processed_event_indices = {info["event"] for info in events_with_steps}
    processed_indices = set()
    for info in events_with_steps:
        # Find the index of this event in the original events list
        for i, event in enumerate(events):
            if event is info["event"]:
                processed_indices.add(i)
                break

    for i, event in enumerate(events):
        # Only add events up to our limit
        if i <= max_event_index and i not in processed_indices:
            events_with_steps.append({"event": event, "steps": []})

    # Sort by event index to maintain processing order
    events_with_steps.sort(key=lambda x: events.index(x["event"]))

    return events_with_steps


def _display_interleaved_flow(steps):
    """Display operations and queue states in an interleaved vertical flow with side-by-side queues"""

    if not steps:
        return

    # Track the current state of all relevant queues
    current_queue_states = {}

    # Get all relevant queues for this event
    all_relevant_queues = set()
    for step in steps:
        if step.queue_symbol != "EUR":  # Exclude EUR
            all_relevant_queues.add((step.queue_type, step.queue_symbol))

    # Convert to sorted list for consistent ordering
    relevant_queue_list = sorted(list(all_relevant_queues))

    # Initialize all queue states with the first available state for each queue
    for step in steps:
        # Skip EUR forex operations
        if step.queue_type == "FOREX" and step.queue_symbol == "EUR":
            continue

        queue_key = f"{step.queue_type}_{step.queue_symbol}"
        if (
            queue_key not in current_queue_states
            and step.queue_state_before is not None
        ):
            current_queue_states[queue_key] = step.queue_state_before

    for i, step in enumerate(steps):
        # Skip EUR forex operations completely
        if step.queue_type == "FOREX" and step.queue_symbol == "EUR":
            continue

        operation_icon = {
            "POP": "🔽",
            "PUSH": "🔼",
            "SPLIT": "⚡",
            "INITIAL": "🏁",
            "NOTE": "📝",
            "CLEAR": "🧹",
            "ERROR": "🔴",
        }.get(step.operation, "❓")

        queue_key = f"{step.queue_type}_{step.queue_symbol}"

        # Update current states with this step's before state
        if step.queue_state_before is not None:
            current_queue_states[queue_key] = step.queue_state_before

        # Show queue states before operation (if we have states to show)
        if step.operation in ["PUSH", "POP", "SPLIT"] and current_queue_states:
            _display_current_queue_states(relevant_queue_list, current_queue_states)
            st.write("    ↓")

        # Show the operation
        if step.operation == "NOTE":
            st.write(f"    **{operation_icon}** {step.description}")
        elif step.operation == "ERROR":
            st.error(f"**{operation_icon} {step.operation}** {step.description}")
        else:
            st.write(
                f"**{operation_icon} {step.operation}** {queue_key}: {step.description}"
            )

        # Show popped objects indented under the operation
        if step.operation == "POP" and step.popped_objects:
            st.write("        └── **Popped Objects:**")
            col1, col2 = st.columns([1, 10])
            with col1:
                st.write("")
            with col2:
                _display_objects_table(step.popped_objects, "Popped Objects")

        # Show pushed objects indented under the operation
        if step.operation == "PUSH" and step.pushed_objects:
            st.write("        └── **Pushed Objects:**")
            col1, col2 = st.columns([1, 10])
            with col1:
                st.write("")
            with col2:
                _display_objects_table(step.pushed_objects, "Pushed Objects")

        # Update current states with this step's after state
        if step.queue_state_after is not None:
            current_queue_states[queue_key] = step.queue_state_after

        # Add spacing between operations
        if i < len(steps) - 1:
            st.write("")

    # Show final state after all operations
    if current_queue_states:
        st.write("    ↓")
        st.write("**Final State:**")
        _display_current_queue_states(relevant_queue_list, current_queue_states)


def _queue_states_equal(state1, state2):
    """Check if two queue states are equal"""
    if state1 is None and state2 is None:
        return True
    if state1 is None or state2 is None:
        return False
    if len(state1) != len(state2):
        return False

    # Convert to comparable format and compare
    try:
        df1 = pd.DataFrame(state1) if state1 else pd.DataFrame()
        df2 = pd.DataFrame(state2) if state2 else pd.DataFrame()
        return df1.equals(df2)
    except:
        # Fallback to simple comparison
        return state1 == state2


def _display_queue_state(queue_state):
    """Helper to display a single queue state (legacy function)"""
    if queue_state:
        df_state = pd.DataFrame(queue_state)
        st.dataframe(
            df_state,
            use_container_width=True,
            hide_index=True,
            height=min(200, 35 + 35 * len(df_state)),
        )
    else:
        st.write("*Empty queue*")


def _display_queue_state_by_type(queue_state, queue_type):
    """Display queue state using appropriate formatter based on queue type"""
    if queue_type == "SHARES":
        _display_shares_queue(queue_state)
    elif queue_type == "FOREX":
        _display_forex_queue(queue_state)
    else:
        _display_queue_state(queue_state)  # Fallback to generic display


def _display_shares_queue(queue_state):
    """Display shares queue with appropriate columns"""
    if queue_state:
        df_state = pd.DataFrame(queue_state)
        # Reorder columns for shares display
        share_columns = ["Date", "Quantity", "Price", "Currency", "Source"]
        # Only include columns that exist
        available_cols = [col for col in share_columns if col in df_state.columns]
        if available_cols:
            df_state = df_state[available_cols]

        st.dataframe(
            df_state,
            use_container_width=True,
            hide_index=True,
            height=min(200, 35 + 35 * len(df_state)),
        )
    else:
        st.write("*Empty queue*")


def _display_forex_queue(queue_state):
    """Display forex queue with appropriate columns"""
    if queue_state:
        df_state = pd.DataFrame(queue_state)
        # Reorder columns for forex display
        forex_columns = ["Date", "Quantity", "Currency", "Source"]
        # Only include columns that exist
        available_cols = [col for col in forex_columns if col in df_state.columns]
        if available_cols:
            df_state = df_state[available_cols]

        st.dataframe(
            df_state,
            use_container_width=True,
            hide_index=True,
            height=min(200, 35 + 35 * len(df_state)),
        )
    else:
        st.write("*Empty queue*")


def _display_objects_table(objects_data, table_type="Objects"):
    """Display popped/pushed objects with appropriate formatting"""
    if objects_data:
        df_objects = pd.DataFrame(objects_data)

        # Format the display based on the type of objects - simplified and less verbose
        if any("Price" in str(col) for col in df_objects.columns):
            # This looks like shares data - only show essential columns
            share_columns = [
                "Date",
                "Quantity",
                "Price",
                "Currency",
                "Source",
            ]
            available_cols = [col for col in share_columns if col in df_objects.columns]
        else:
            # This looks like forex data - only show essential columns
            forex_columns = [
                "Date",
                "Quantity",
                "Currency",
                "Source",
            ]
            available_cols = [col for col in forex_columns if col in df_objects.columns]

        if available_cols:
            df_objects = df_objects[available_cols]

        st.dataframe(
            df_objects,
            use_container_width=True,
            hide_index=True,
            height=min(150, 35 + 35 * len(df_objects)),
        )
    else:
        st.write(f"*No {table_type.lower()}*")


def _display_current_queue_states(relevant_queue_list, current_queue_states):
    """Display current states of relevant queues side by side"""
    if not relevant_queue_list or not current_queue_states:
        return

    # Limit to 2 columns for readability
    queues_to_show = relevant_queue_list[:2]

    if len(queues_to_show) == 1:
        # Single queue - display normally
        queue_type, queue_symbol = queues_to_show[0]
        queue_key = f"{queue_type}_{queue_symbol}"
        if queue_key in current_queue_states:
            st.write(f"**{queue_key} State:**")
            _display_queue_state_by_type(current_queue_states[queue_key], queue_type)
    else:
        # Multiple queues - display side by side
        cols = st.columns(len(queues_to_show))

        for i, (queue_type, queue_symbol) in enumerate(queues_to_show):
            queue_key = f"{queue_type}_{queue_symbol}"
            with cols[i]:
                st.write(f"**{queue_key} State:**")
                if queue_key in current_queue_states:
                    _display_queue_state_by_type(
                        current_queue_states[queue_key], queue_type
                    )
                else:
                    st.write("*No state available*")


def _get_event_summary(event):
    """Create a summary string for an event"""
    event_type = event.__class__.__name__.replace("Event", "")
    date_str = event.date.strftime("%Y-%m-%d")

    if hasattr(event, "symbol") and event.symbol:
        symbol = event.symbol
        if event_type == "Buy":
            return f"📈 {date_str} | {event_type} {event.historic_shares_quantity} {symbol} @ {event.historic_shares_price} {event.currency}"
        elif event_type == "Sell":
            return f"📉 {date_str} | {event_type} {event.historic_quantity} {symbol} @ {event.historic_sell_price} {event.currency}"
        elif event_type == "RSU":
            return f"🎁 {date_str} | {event_type} {event.historic_received_shares_quantity} {symbol} @ {event.historic_received_shares_price} {event.currency}"
        elif event_type == "ESPP":
            return f"📈 {date_str} | {event_type} {event.historic_shares_quantity} {symbol} @ {event.historic_fair_market_value} {event.currency}"
        elif event_type == "Dividend":
            return f"💸 {date_str} | {event_type} {event.dividend_amount} {event.currency} from {symbol}"
        elif event_type == "Tax":
            return f"🏛️ {date_str} | {event_type} {event.withheld_tax_amount} {event.currency} from {symbol}"
        elif event_type == "StockSplit":
            return f"⚡ {date_str} | {event_type} {symbol} {event.shares_after_split}:1"
        else:
            return f"📊 {date_str} | {event_type} {symbol}"
    else:
        if event_type == "MoneyDeposit":
            return f"💰 {date_str} | {event_type} {event.amount} {event.currency}"
        elif event_type == "MoneyWithdrawal":
            return f"💸 {date_str} | {event_type} {event.amount} {event.currency}"
        elif event_type == "CurrencyConversion":
            return f"🔄 {date_str} | {event_type} {event.source_amount} {event.source_currency} → {event.target_amount} {event.target_currency}"
        else:
            return f"📊 {date_str} | {event_type}"


def _display_error_event_details(error_event, debug_processor):
    """Display detailed information about the event that caused the error"""

    # Show event summary
    event_summary = _get_event_summary(error_event)
    st.write(f"**Event:** {event_summary}")

    # Show error message
    st.error(f"**Error Message:** {debug_processor.error_message}")

    # Show event details
    st.write("**Event Details:**")
    event_details = {}
    for attr in dir(error_event):
        if not attr.startswith("_") and not callable(getattr(error_event, attr)):
            value = getattr(error_event, attr)
            if value is not None:
                event_details[attr] = value

    if event_details:
        import pandas as pd

        df_event = pd.DataFrame([event_details])
        st.dataframe(df_event, use_container_width=True, hide_index=True)

    # Show current queue states
    st.write("**Current Queue States at Time of Error:**")
    queue_states = debug_processor.get_current_queue_states()

    if queue_states:
        # Group by type
        shares_queues = {k: v for k, v in queue_states.items() if v["type"] == "SHARES"}
        forex_queues = {k: v for k, v in queue_states.items() if v["type"] == "FOREX"}

        col1, col2 = st.columns(2)

        with col1:
            st.write("**📈 Share Queues:**")
            if shares_queues:
                for queue_name, queue_info in shares_queues.items():
                    st.write(f"**{queue_name}** ({queue_info['count']} items)")
                    if queue_info["state"]:
                        df_shares = pd.DataFrame(queue_info["state"])
                        st.dataframe(
                            df_shares,
                            use_container_width=True,
                            hide_index=True,
                            height=min(200, 35 + 35 * len(df_shares)),
                        )
                    else:
                        st.write("*Empty queue*")
                    st.write("")  # Add spacing
            else:
                st.write("*No active share queues*")

        with col2:
            st.write("**💰 Forex Queues:**")
            if forex_queues:
                for queue_name, queue_info in forex_queues.items():
                    st.write(f"**{queue_name}** ({queue_info['count']} items)")
                    if queue_info["state"]:
                        df_forex = pd.DataFrame(queue_info["state"])
                        st.dataframe(
                            df_forex,
                            use_container_width=True,
                            hide_index=True,
                            height=min(200, 35 + 35 * len(df_forex)),
                        )
                    else:
                        st.write("*Empty queue*")
                    st.write("")  # Add spacing
            else:
                st.write("*No active forex queues*")
    else:
        st.write("*All queues are empty*")
