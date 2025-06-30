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
    """Calculate various statistics from the events"""

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
            if hasattr(event, "historic_received_shares_quantity") and hasattr(
                event, "historic_received_shares_price"
            ):
                rsu_value = float(event.historic_received_shares_quantity) * float(
                    event.historic_received_shares_price
                )
                stats["rsu_net_value"] += rsu_value
                stats["yearly_breakdown"][year]["rsu_net_income"] += rsu_value
            if hasattr(event, "historic_withheld_shares_quantity") and hasattr(
                event, "historic_received_shares_price"
            ):
                if event.historic_withheld_shares_quantity:
                    withheld_value = float(
                        event.historic_withheld_shares_quantity
                    ) * float(event.historic_received_shares_price)
                    stats["rsu_withheld_value"] += withheld_value
                    stats["yearly_breakdown"][year][
                        "rsu_taxes_withheld"
                    ] += withheld_value
                    stats["yearly_breakdown"][year][
                        "total_taxes_paid"
                    ] += withheld_value

        # ESPP Events
        elif isinstance(event, ESPPEvent):
            stats["counts"]["espp_events"] += 1
            if hasattr(event, "historic_shares_quantity") and hasattr(
                event, "historic_shares_price"
            ):
                stats["espp_value"] += float(event.historic_shares_quantity) * float(
                    event.historic_shares_price
                )

            # ESPP detailed breakdown
            if hasattr(event, "contribution"):
                contribution_value = float(event.contribution)
                stats["espp_contributions"] += contribution_value
                stats["yearly_breakdown"][year][
                    "espp_contribution"
                ] += contribution_value

            if hasattr(event, "bonus"):
                gross_bonus = float(event.bonus)
                stats["espp_gross_bonus"] += gross_bonus
                net_bonus = gross_bonus * (1 - 0.4431)  # Net after 44.31% tax
                espp_taxes = gross_bonus * 0.4431  # ESPP taxes paid
                stats["espp_net_bonus"] += net_bonus
                stats["yearly_breakdown"][year]["espp_gross_bonus"] += gross_bonus
                stats["yearly_breakdown"][year]["espp_net_bonus"] += net_bonus
                stats["yearly_breakdown"][year]["espp_taxes_paid"] += espp_taxes
                stats["yearly_breakdown"][year]["total_taxes_paid"] += espp_taxes
                stats["yearly_breakdown"][year]["net_income"] += net_bonus

        # Sell Events
        elif isinstance(event, SellEvent):
            stats["counts"]["sell_events"] += 1
            if hasattr(event, "historic_quantity") and hasattr(
                event, "historic_sell_price"
            ):
                sell_value = abs(float(event.historic_quantity)) * float(
                    event.historic_sell_price
                )
                stats["shares_sold_value"] += sell_value

                # Add to yearly breakdown (general stats)
                stats["yearly_breakdown"][year]["general_sell_proceeds"] += sell_value
                if hasattr(event, "fees") and event.fees:
                    fees = float(event.fees)
                    stats["yearly_breakdown"][year]["general_sell_costs"] += fees

                # Calculate opportunity lost with split adjustments
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
                            current_value = adjusted_quantity * float(current_price)
                            adjusted_sold_value = (
                                adjusted_quantity * adjusted_sell_price
                            )
                            opportunity_lost = current_value - adjusted_sold_value
                            if opportunity_lost > 0:
                                stats["lost_opportunity_value"] += opportunity_lost
                except Exception:
                    # If we can't get the price or split data, skip this calculation
                    pass

        # Dividend Events
        elif isinstance(event, DividendEvent):
            if hasattr(event, "dividend_amount") and event.dividend_amount:
                dividend_value = float(event.dividend_amount)
                stats["yearly_breakdown"][year][
                    "general_dividend_income"
                ] += dividend_value
                stats["yearly_breakdown"][year]["net_income"] += dividend_value

        # Tax Events
        elif isinstance(event, TaxEvent):
            if hasattr(event, "withheld_tax_amount") and event.withheld_tax_amount:
                tax_value = abs(float(event.withheld_tax_amount))
                stats["yearly_breakdown"][year]["general_other_taxes"] += tax_value
                stats["yearly_breakdown"][year]["total_taxes_paid"] += tax_value
                stats["yearly_breakdown"][year]["net_income"] -= tax_value

        # Money Withdrawals
        elif isinstance(event, MoneyWithdrawalEvent):
            stats["counts"]["withdrawal_events"] += 1
            if hasattr(event, "amount"):
                stats["withdrawals_value"] += abs(float(event.amount))

        # Currency Conversions
        elif isinstance(event, CurrencyConversionEvent):
            stats["counts"]["currency_conversion_events"] += 1
            if hasattr(event, "source_amount"):
                stats["currency_conversions_value"] += abs(float(event.source_amount))

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
    """Display the calculated statistics in a nice format"""

    # Header section
    st.markdown("## 💰 Money Flows")

    # Row 1: Shares sold, RSU net, RSU taxes
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "💸 Shares Sold Value",
            f"${stats['shares_sold_value']:,.2f}",
            help="Total value of all shares sold",
        )

    with col2:
        st.metric(
            "💎 RSU Net Value",
            f"${stats['rsu_net_value']:,.2f}",
            help="Total value of RSU shares you received (net after withholding)",
        )

    with col3:
        st.metric(
            "🏦 RSU Withheld Value",
            f"${stats['rsu_withheld_value']:,.2f}",
            help="Total value of RSU shares withheld for taxes",
        )

    # Row 2: Everything ESPP
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "📈 ESPP Contributions",
            f"${stats['espp_contributions']:,.2f}",
            help="Total amount you paid for ESPP shares",
        )

    with col2:
        st.metric(
            "🎁 ESPP Gross Bonus",
            f"${stats['espp_gross_bonus']:,.2f}",
            help="Total gross benefit from ESPP discount (before taxes)",
        )

    with col3:
        st.metric(
            "💰 ESPP Net Bonus",
            f"${stats['espp_net_bonus']:,.2f}",
            help="ESPP bonus after 44.31% taxes",
        )

    # Row 3: Withdrawals, conversions, opportunity lost
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "💳 Withdrawals",
            f"${stats['withdrawals_value']:,.2f}",
            help="Total value of money withdrawals",
        )

    with col2:
        st.metric(
            "🔄 Currency Conversions",
            f"${stats['currency_conversions_value']:,.2f}",
            help="Total value of currency conversions",
        )

    with col3:
        st.metric(
            "😭 Opportunity Lost",
            f"${stats['lost_opportunity_value']:,.2f}",
            help="What sold shares would be worth today vs. sale price (adjusted for stock splits and using latest market prices)",
        )

    st.markdown("---")

    # ESPP/RSU Yearly Breakdown section
    if stats["yearly_breakdown"]:
        st.markdown("## 📅 ESPP/RSU Yearly Breakdown")

        # Sort years in descending order
        sorted_years = sorted(stats["yearly_breakdown"].keys(), reverse=True)

        for year in sorted_years:
            year_data = stats["yearly_breakdown"][year]

            st.markdown(f"### {year}")

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    "💵 Net Income",
                    f"${year_data['net_income']:,.2f}",
                    help="RSU + ESPP Contribution + ESPP Net Bonus",
                )

            with col2:
                st.metric(
                    "💰 Total Gross Income",
                    f"${year_data['total_gross_income']:,.2f}",
                    help="RSU Gross + ESPP Contribution + ESPP Gross Bonus",
                )

            with col3:
                st.metric(
                    "💎 RSU Net Income",
                    f"${year_data['rsu_net_income']:,.2f}",
                    help="Net RSU income received",
                )

            with col4:
                st.metric(
                    "🏦 RSU Taxes Withheld",
                    f"${year_data['rsu_taxes_withheld']:,.2f}",
                    help="RSU taxes withheld by employer",
                )

            # Second row with ESPP details
            if year_data["espp_contribution"] > 0 or year_data["espp_net_bonus"] > 0:
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric(
                        "📈 ESPP Contribution",
                        f"${year_data['espp_contribution']:,.2f}",
                        help="Amount paid for ESPP shares",
                    )

                with col2:
                    st.metric(
                        "💰 ESPP Net Bonus",
                        f"${year_data['espp_net_bonus']:,.2f}",
                        help="ESPP discount benefit after taxes",
                    )

                with col3:
                    st.metric(
                        "🎁 ESPP Taxes Paid",
                        f"${year_data['espp_taxes_paid']:,.2f}",
                        help="Estimated taxes on ESPP bonus (44.31%)",
                    )

                with col4:
                    st.metric(
                        "🏛️ Total Taxes Paid",
                        f"${year_data['total_taxes_paid']:,.2f}",
                        help="RSU withholdings + ESPP taxes",
                    )
            elif year_data["total_taxes_paid"] > 0:
                # Show total taxes paid even without ESPP activity
                col1, col2, col3, col4 = st.columns(4)
                with col4:
                    st.metric(
                        "🏛️ Total Taxes Paid",
                        f"${year_data['total_taxes_paid']:,.2f}",
                        help="RSU withholdings + ESPP taxes",
                    )

            st.markdown("")  # Add some spacing between years
