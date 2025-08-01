"""
Fun Facts Page - ESPP/RSU focused statistics and general trading insights
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
from ..core.events import (
    RSUEvent,
    ESPPEvent,
    SellEvent,
    MoneyWithdrawalEvent,
    CurrencyConversionEvent,
    BuyEvent,
    DividendEvent,
    TaxEvent,
)
from ..core.historic_prices import historic_price_manager


def show():
    """Display the fun facts page"""

    st.title("🎉 Fun Facts")
    st.markdown("---")

    # Get events or use empty list if none loaded
    events = []
    if (
        hasattr(st.session_state, "imported_events")
        and st.session_state.imported_events
    ):
        events = st.session_state.imported_events
    else:
        st.info("📊 Import some data to see your trading activities!")

    # Calculate statistics (will return 0 values if no events)
    stats = _calculate_statistics(events)

    # Display the fun facts (always show all metrics)
    _display_statistics(stats)


def _calculate_statistics(events: List[Any]) -> Dict[str, Any]:
    """Calculate various statistics from the events (all values converted to EUR using daily rates)"""

    stats = {
        "rsu_net_value": 0.0,
        "rsu_withheld_value": 0.0,
        "espp_value": 0.0,
        "espp_contributions": 0.0,
        "espp_gross_bonus": 0.0,
        "espp_net_bonus": 0.0,
        "withdrawals_value": 0.0,
        "currency_conversions_value": 0.0,
        "shares_sold_value": 0.0,
        "lost_opportunity_value": 0.0,
        "counts": {
            "rsu_events": 0,
            "espp_events": 0,
            "sell_events": 0,
            "withdrawal_events": 0,
            "currency_conversion_events": 0,
        },
        "trading_period": {"start": None, "end": None},
        "yearly_breakdown": {},
    }

    def _convert_to_eur(amount: float, currency: str, event_date) -> float:
        """Convert amount to EUR using daily exchange rate"""
        if currency == "EUR":
            return amount

        rate = historic_price_manager.get_exchange_rate(currency, event_date, "daily")
        if rate is None:
            return 0.0  # Skip if no exchange rate available

        return float(amount / float(rate))

    for event in events:
        # Track trading period
        if (
            stats["trading_period"]["start"] is None
            or event.date < stats["trading_period"]["start"]
        ):
            stats["trading_period"]["start"] = event.date
        if (
            stats["trading_period"]["end"] is None
            or event.date > stats["trading_period"]["end"]
        ):
            stats["trading_period"]["end"] = event.date

        # Initialize yearly breakdown for this year if not exists
        year = event.date.year
        if year not in stats["yearly_breakdown"]:
            stats["yearly_breakdown"][year] = {
                "rsu_net_income": 0.0,
                "rsu_taxes_withheld": 0.0,
                "espp_contribution": 0.0,
                "espp_gross_bonus": 0.0,
                "espp_net_bonus": 0.0,
                "espp_taxes_paid": 0.0,
                "total_taxes_paid": 0.0,
                "net_income": 0.0,  # rsu + espp contribution + espp net
                # General stats for the separate section
                "general_sell_proceeds": 0.0,
                "general_sell_costs": 0.0,
                "general_dividend_income": 0.0,
                "general_other_taxes": 0.0,
            }

        # RSU Events
        if isinstance(event, RSUEvent):
            stats["counts"]["rsu_events"] += 1
            event_currency = getattr(
                event, "currency", "USD"
            )  # Default to USD if no currency specified

            if hasattr(event, "historic_received_shares_quantity") and hasattr(
                event, "historic_received_shares_price"
            ):
                rsu_value_original = float(
                    event.historic_received_shares_quantity
                ) * float(event.historic_received_shares_price)
                rsu_value_eur = _convert_to_eur(
                    rsu_value_original, event_currency, event.date
                )
                stats["rsu_net_value"] += rsu_value_eur
                stats["yearly_breakdown"][year]["rsu_net_income"] += rsu_value_eur

            if hasattr(event, "historic_withheld_shares_quantity") and hasattr(
                event, "historic_received_shares_price"
            ):
                if event.historic_withheld_shares_quantity:
                    withheld_value_original = float(
                        event.historic_withheld_shares_quantity
                    ) * float(event.historic_received_shares_price)
                    withheld_value_eur = _convert_to_eur(
                        withheld_value_original, event_currency, event.date
                    )
                    stats["rsu_withheld_value"] += withheld_value_eur
                    stats["yearly_breakdown"][year][
                        "rsu_taxes_withheld"
                    ] += withheld_value_eur
                    stats["yearly_breakdown"][year][
                        "total_taxes_paid"
                    ] += withheld_value_eur

        # ESPP Events
        elif isinstance(event, ESPPEvent):
            stats["counts"]["espp_events"] += 1
            event_currency = getattr(
                event, "currency", "USD"
            )  # Default to USD if no currency specified

            if hasattr(event, "historic_shares_quantity") and hasattr(
                event, "historic_shares_price"
            ):
                espp_value_original = float(event.historic_shares_quantity) * float(
                    event.historic_shares_price
                )
                espp_value_eur = _convert_to_eur(
                    espp_value_original, event_currency, event.date
                )
                stats["espp_value"] += espp_value_eur

            # ESPP detailed breakdown
            if hasattr(event, "contribution"):
                contribution_value_original = float(event.contribution)
                contribution_value_eur = _convert_to_eur(
                    contribution_value_original, event_currency, event.date
                )
                stats["espp_contributions"] += contribution_value_eur
                stats["yearly_breakdown"][year][
                    "espp_contribution"
                ] += contribution_value_eur

            if hasattr(event, "bonus"):
                gross_bonus_original = float(event.bonus)
                gross_bonus_eur = _convert_to_eur(
                    gross_bonus_original, event_currency, event.date
                )
                net_bonus_eur = gross_bonus_eur * (1 - 0.4431)  # Net after 44.31% tax
                espp_taxes_eur = gross_bonus_eur * 0.4431  # ESPP taxes paid
                stats["espp_gross_bonus"] += gross_bonus_eur
                stats["espp_net_bonus"] += net_bonus_eur
                stats["yearly_breakdown"][year]["espp_gross_bonus"] += gross_bonus_eur
                stats["yearly_breakdown"][year]["espp_net_bonus"] += net_bonus_eur
                stats["yearly_breakdown"][year]["espp_taxes_paid"] += espp_taxes_eur
                stats["yearly_breakdown"][year]["total_taxes_paid"] += espp_taxes_eur
                stats["yearly_breakdown"][year]["net_income"] += net_bonus_eur

        # Sell Events
        elif isinstance(event, SellEvent):
            stats["counts"]["sell_events"] += 1
            event_currency = getattr(
                event, "currency", "USD"
            )  # Default to USD if no currency specified

            if hasattr(event, "historic_quantity") and hasattr(
                event, "historic_sell_price"
            ):
                sell_value_original = abs(float(event.historic_quantity)) * float(
                    event.historic_sell_price
                )
                sell_value_eur = _convert_to_eur(
                    sell_value_original, event_currency, event.date
                )
                stats["shares_sold_value"] += sell_value_eur

                # Add to yearly breakdown (general stats)
                stats["yearly_breakdown"][year][
                    "general_sell_proceeds"
                ] += sell_value_eur
                if hasattr(event, "fees") and event.fees:
                    fees_original = float(event.fees)
                    fees_eur = _convert_to_eur(
                        fees_original, event_currency, event.date
                    )
                    stats["yearly_breakdown"][year]["general_sell_costs"] += fees_eur

                # Calculate opportunity lost with split adjustments (in EUR)
                try:
                    # Get current/recent price for this symbol
                    current_price = historic_price_manager.get_latest_market_price(
                        getattr(event, "symbol", "Unknown")
                    )
                    if current_price is not None:
                        # Get cumulative split factor for splits that happened after the sell date
                        split_factor = historic_price_manager.get_cumulative_split_factor_after_date(
                            getattr(event, "symbol", "Unknown"), event.date
                        )

                        # Adjust the sell quantity and price for splits that happened after the sell
                        adjusted_quantity = (
                            abs(float(event.historic_quantity)) * split_factor
                        )
                        adjusted_sell_price = (
                            float(event.historic_sell_price) / split_factor
                        )

                        # Only calculate opportunity loss if current price is higher than adjusted sell price
                        if current_price > adjusted_sell_price:
                            # Calculate what the adjusted shares would be worth today vs what they sold for
                            current_value_original = adjusted_quantity * float(
                                current_price
                            )
                            adjusted_sold_value_original = (
                                adjusted_quantity * adjusted_sell_price
                            )

                            # Convert both to EUR (current value at today's rate, sold value at historical rate)
                            from datetime import date

                            current_value_eur = _convert_to_eur(
                                current_value_original, event_currency, date.today()
                            )
                            adjusted_sold_value_eur = _convert_to_eur(
                                adjusted_sold_value_original, event_currency, event.date
                            )

                            opportunity_lost_eur = (
                                current_value_eur - adjusted_sold_value_eur
                            )
                            if opportunity_lost_eur > 0:
                                stats["lost_opportunity_value"] += opportunity_lost_eur
                except Exception:
                    # If we can't get the price or split data, skip this calculation
                    pass

        # Dividend Events
        elif isinstance(event, DividendEvent):
            if hasattr(event, "dividend_amount") and event.dividend_amount:
                event_currency = getattr(
                    event, "currency", "USD"
                )  # Default to USD if no currency specified
                dividend_value_original = float(event.dividend_amount)
                dividend_value_eur = _convert_to_eur(
                    dividend_value_original, event_currency, event.date
                )
                stats["yearly_breakdown"][year][
                    "general_dividend_income"
                ] += dividend_value_eur
                stats["yearly_breakdown"][year]["net_income"] += dividend_value_eur

        # Tax Events
        elif isinstance(event, TaxEvent):
            if hasattr(event, "withheld_tax_amount") and event.withheld_tax_amount:
                event_currency = getattr(
                    event, "currency", "USD"
                )  # Default to USD if no currency specified
                tax_value_original = abs(float(event.withheld_tax_amount))
                tax_value_eur = _convert_to_eur(
                    tax_value_original, event_currency, event.date
                )
                stats["yearly_breakdown"][year]["general_other_taxes"] += tax_value_eur
                stats["yearly_breakdown"][year]["total_taxes_paid"] += tax_value_eur
                stats["yearly_breakdown"][year]["net_income"] -= tax_value_eur

        # Money Withdrawals
        elif isinstance(event, MoneyWithdrawalEvent):
            stats["counts"]["withdrawal_events"] += 1
            if hasattr(event, "amount"):
                event_currency = getattr(
                    event, "currency", "USD"
                )  # Default to USD if no currency specified
                withdrawal_value_original = abs(float(event.amount))
                withdrawal_value_eur = _convert_to_eur(
                    withdrawal_value_original, event_currency, event.date
                )
                stats["withdrawals_value"] += withdrawal_value_eur

        # Currency Conversions
        elif isinstance(event, CurrencyConversionEvent):
            stats["counts"]["currency_conversion_events"] += 1
            if hasattr(event, "source_amount"):
                source_currency = getattr(
                    event, "source_currency", "USD"
                )  # Default to USD if no currency specified
                conversion_value_original = abs(float(event.source_amount))
                conversion_value_eur = _convert_to_eur(
                    conversion_value_original, source_currency, event.date
                )
                stats["currency_conversions_value"] += conversion_value_eur

    # Calculate net income for each year: RSU + ESPP contribution + ESPP net bonus
    for year in stats["yearly_breakdown"]:
        year_data = stats["yearly_breakdown"][year]
        year_data["net_income"] = (
            year_data["rsu_net_income"]
            + year_data["espp_contribution"]
            + year_data["espp_net_bonus"]
        )
        # Calculate total gross income: RSU gross + ESPP gross
        year_data["total_gross_income"] = (
            year_data["rsu_net_income"]
            + year_data["rsu_taxes_withheld"]  # RSU gross
            + year_data["espp_contribution"]
            + year_data["espp_gross_bonus"]  # ESPP gross
        )

    return stats


def _display_statistics(stats: Dict[str, Any]):
    """Display the calculated statistics in a nice format (all values in EUR)"""

    # Header section
    st.markdown(
        "## 💰 Money Flows (All values converted to EUR using daily exchange rates)"
    )

    # Row 1: Shares sold, RSU net, RSU taxes
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "💸 Shares Sold Value",
            f"€{stats['shares_sold_value']:,.2f}",
            help="Total value of all shares sold (converted to EUR using daily rates)",
        )

    with col2:
        st.metric(
            "💎 RSU Net Value",
            f"€{stats['rsu_net_value']:,.2f}",
            help="Total value of RSU shares you received (net after withholding, converted to EUR)",
        )

    with col3:
        st.metric(
            "🏦 RSU Withheld Value",
            f"€{stats['rsu_withheld_value']:,.2f}",
            help="Total value of RSU shares withheld for taxes (converted to EUR)",
        )

    # Row 2: Everything ESPP
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "📈 ESPP Contributions",
            f"€{stats['espp_contributions']:,.2f}",
            help="Total amount you paid for ESPP shares (converted to EUR)",
        )

    with col2:
        st.metric(
            "🎁 ESPP Gross Bonus",
            f"€{stats['espp_gross_bonus']:,.2f}",
            help="Total gross benefit from ESPP discount (before taxes, converted to EUR)",
        )

    with col3:
        st.metric(
            "💰 ESPP Net Bonus",
            f"€{stats['espp_net_bonus']:,.2f}",
            help="ESPP bonus after 44.31% taxes (converted to EUR)",
        )

    # Row 3: Withdrawals, conversions, opportunity lost
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "💳 Withdrawals",
            f"€{stats['withdrawals_value']:,.2f}",
            help="Total value of money withdrawals (converted to EUR)",
        )

    with col2:
        st.metric(
            "🔄 Currency Conversions",
            f"€{stats['currency_conversions_value']:,.2f}",
            help="Total value of currency conversions (converted to EUR)",
        )

    with col3:
        st.metric(
            "😭 Opportunity Lost",
            f"€{stats['lost_opportunity_value']:,.2f}",
            help="What sold shares would be worth today vs. sale price (adjusted for stock splits and using latest market prices, converted to EUR)",
        )

    st.markdown("---")

    # ESPP/RSU Yearly Breakdown section
    if stats["yearly_breakdown"]:
        st.markdown("## 📅 ESPP/RSU Yearly Breakdown (EUR)")

        # Sort years in descending order
        sorted_years = sorted(stats["yearly_breakdown"].keys(), reverse=True)

        for year in sorted_years:
            year_data = stats["yearly_breakdown"][year]

            st.markdown(f"### {year}")

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    "💵 Net Income",
                    f"€{year_data['net_income']:,.2f}",
                    help="RSU + ESPP Contribution + ESPP Net Bonus (EUR)",
                )

            with col2:
                st.metric(
                    "💰 Total Gross Income",
                    f"€{year_data['total_gross_income']:,.2f}",
                    help="RSU Gross + ESPP Contribution + ESPP Gross Bonus (EUR)",
                )

            with col3:
                st.metric(
                    "💎 RSU Net Income",
                    f"€{year_data['rsu_net_income']:,.2f}",
                    help="Net RSU income received (EUR)",
                )

            with col4:
                st.metric(
                    "🏦 RSU Taxes Withheld",
                    f"€{year_data['rsu_taxes_withheld']:,.2f}",
                    help="RSU taxes withheld by employer (EUR)",
                )

            # Second row with ESPP details
            if year_data["espp_contribution"] > 0 or year_data["espp_net_bonus"] > 0:
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric(
                        "📈 ESPP Contribution",
                        f"€{year_data['espp_contribution']:,.2f}",
                        help="Amount paid for ESPP shares (EUR)",
                    )

                with col2:
                    st.metric(
                        "💰 ESPP Net Bonus",
                        f"€{year_data['espp_net_bonus']:,.2f}",
                        help="ESPP discount benefit after taxes (EUR)",
                    )

                with col3:
                    st.metric(
                        "🎁 ESPP Taxes Paid",
                        f"€{year_data['espp_taxes_paid']:,.2f}",
                        help="Estimated taxes on ESPP bonus (44.31%, EUR)",
                    )

                with col4:
                    st.metric(
                        "🏛️ Total Taxes Paid",
                        f"€{year_data['total_taxes_paid']:,.2f}",
                        help="RSU withholdings + ESPP taxes (EUR)",
                    )
            elif year_data["total_taxes_paid"] > 0:
                # Show total taxes paid even without ESPP activity
                col1, col2, col3, col4 = st.columns(4)
                with col4:
                    st.metric(
                        "🏛️ Total Taxes Paid",
                        f"€{year_data['total_taxes_paid']:,.2f}",
                        help="RSU withholdings + ESPP taxes (EUR)",
                    )

            st.markdown("")  # Add some spacing between years
