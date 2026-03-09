"""
pyFIFOtax - Modern UI
Tax Reporting Tool for Stock Transactions on Foreign Exchanges

A Streamlit-based interface for the pyFIFOtax calculation engine.
"""

import streamlit as st
import sys
from pathlib import Path

# Add legacy modules to Python path
legacy_path = Path(__file__).parent / "legacy"
sys.path.insert(0, str(legacy_path))

# Page configuration
st.set_page_config(
    page_title="pyFIFOtax",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Import page modules
from src.pages import home, events, data_management, fun_facts, reports, fifo_debug


def home_page():
    """Home page wrapper"""
    home.show()


def data_management_page():
    """Data management page wrapper"""
    data_management.show()


def fun_facts_page():
    """Fun Facts page wrapper"""
    fun_facts.show()


def buy_events_page():
    """Buy events page wrapper"""
    events.show_event_type("Buy")


def sell_events_page():
    """Sell events page wrapper"""
    events.show_event_type("Sell")


def dividend_events_page():
    """Dividend events page wrapper"""
    events.show_event_type("Dividend")


def rsu_events_page():
    """RSU events page wrapper"""
    events.show_event_type("RSU")


def espp_events_page():
    """ESPP events page wrapper"""
    events.show_event_type("ESPP")


def tax_events_page():
    """Tax events page wrapper"""
    events.show_event_type("Tax")


def money_deposit_events_page():
    """Money Deposit events page wrapper"""
    events.show_event_type("MoneyDeposit")


def money_withdrawal_events_page():
    """Money Withdrawal events page wrapper"""
    events.show_event_type("MoneyWithdrawal")


def currency_conversion_events_page():
    """Currency Conversion events page wrapper"""
    events.show_event_type("CurrencyConversion")


def tax_reports_page():
    """Tax Reports page wrapper"""
    reports.show_tax_reports()


def awv_reports_page():
    """AWV Reports page wrapper"""
    reports.show_awv_reports()


def fifo_results_page():
    """FIFO Results page wrapper"""
    reports.show_fifo_results()


def fifo_debug_page():
    """FIFO Debug page wrapper"""
    fifo_debug.show()


def main():
    """Main application entry point"""

    # Create navigation with event type sub-pages
    pg = st.navigation(
        {
            "Home": [
                st.Page(home_page, title="Home"),
                st.Page(data_management_page, title="Data Management"),
                st.Page(fun_facts_page, title="Fun Facts"),
                st.Page(fifo_debug_page, title="FIFO Debug"),
            ],
            "Events": [
                st.Page(buy_events_page, title="Buy Events"),
                st.Page(sell_events_page, title="Sell Events"),
                st.Page(dividend_events_page, title="Dividend Events"),
                st.Page(rsu_events_page, title="RSU Events"),
                st.Page(espp_events_page, title="ESPP Events"),
                st.Page(tax_events_page, title="Tax Events"),
                st.Page(money_deposit_events_page, title="Money Deposits"),
                st.Page(money_withdrawal_events_page, title="Money Withdrawals"),
                st.Page(currency_conversion_events_page, title="Currency Conversions"),
            ],
            "Reports": [
                st.Page(tax_reports_page, title="Tax Reports"),
                st.Page(awv_reports_page, title="AWV Reports"),
            ],
        }
    )

    # Run the selected page
    pg.run()


if __name__ == "__main__":
    main()
