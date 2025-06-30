"""
Reports page for pyFIFOtax Modern UI
Main reports section with settings and automatic report generation
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from ..core.pdf_exporter import (
    GermanTaxHTMLExporter,
    UnifiedTableGenerator,
    ReportSettings as PDFReportSettings,
)

from src.core.fifo_processor import FIFOProcessor
from src.core.report_generator import ReportGenerator, ReportSettings
from src.core.historic_prices import HistoricPriceManager


def show_tax_reports():
    """Display tax reports section with integrated settings"""
    st.title("📈 Tax Reports (ELSTER)")
    st.markdown("German tax reports with integrated settings and PDF export")
    st.markdown("---")

    # Check if events are imported
    if not (
        hasattr(st.session_state, "imported_events")
        and st.session_state.imported_events
    ):
        st.info("No events imported yet. Go to the Import Data page to upload files.")
        return

    # Get available years from events
    event_years = set()
    for event in st.session_state.imported_events:
        event_years.add(event.date.year)

    if not event_years:
        st.warning("No events with valid dates found.")
        return

    available_years = sorted(list(event_years), reverse=True)

    # Report settings section
    st.header("📊 Report Configuration")

    col1, col2 = st.columns(2)

    with col1:
        # Report year selection - let Streamlit handle the state
        report_year = st.selectbox(
            "Report Year",
            options=available_years,
            help="Select the year for which to generate reports",
            key="report_year_select",
        )

    with col2:
        # Exchange rate mode - let Streamlit handle the state
        exchange_rate_options = ["daily", "monthly"]
        exchange_rate_mode = st.selectbox(
            "Exchange Rate Mode",
            options=exchange_rate_options,
            help="Choose whether to use daily exchange rates or monthly averages for tax reports",
            key="exchange_rate_mode_select",
        )

    st.markdown("---")

    current_settings = ReportSettings(
        report_year=report_year,
        exchange_rate_mode=exchange_rate_mode,
    )

    if "price_manager" not in st.session_state:
        st.session_state.price_manager = HistoricPriceManager()

    if "fifo_processor" not in st.session_state:
        st.session_state.fifo_processor = FIFOProcessor(st.session_state.price_manager)

    if "report_generator" not in st.session_state:
        st.session_state.report_generator = ReportGenerator(
            st.session_state.fifo_processor, st.session_state.price_manager
        )

    # Check if settings have changed by comparing current selections with stored settings
    settings_changed = (
        not hasattr(st.session_state, "report_settings")
        or st.session_state.report_settings.report_year != report_year
        or st.session_state.report_settings.exchange_rate_mode != exchange_rate_mode
    )

    events_updated = hasattr(st.session_state, "events_last_updated") and (
        not hasattr(st.session_state, "reports_last_generated")
        or st.session_state.events_last_updated
        > st.session_state.reports_last_generated
    )

    # Only regenerate if settings changed or we don't have reports for current settings
    if (
        settings_changed
        or events_updated
        or not hasattr(st.session_state, "tax_reports")
    ):
        with st.spinner("Updating reports with new settings..."):
            try:
                # Process events through FIFO
                st.session_state.fifo_processor.process_events(
                    st.session_state.imported_events
                )

                # Generate reports
                st.session_state.tax_reports = (
                    st.session_state.report_generator.generate_tax_report(
                        current_settings
                    )
                )
                st.session_state.awv_reports = (
                    st.session_state.report_generator.generate_awv_report(
                        current_settings
                    )
                )
                st.session_state.report_settings = current_settings

                # Update timestamp to track when reports were last generated
                st.session_state.reports_last_generated = pd.Timestamp.now()

            except Exception as e:
                st.error(f"❌ Error processing events: {str(e)}")
                return

    # Tax PDF Export Section
    if hasattr(st.session_state, "tax_reports"):
        st.subheader("📄 Tax PDF Export")

        try:
            # Generate PDF for tax reports using local exporter
            exporter = GermanTaxHTMLExporter()

            # Create PDF settings
            pdf_settings = ReportSettings(
                report_year=report_year,
                exchange_rate_mode=exchange_rate_mode,
            )

            tax_pdf_bytes = exporter.generate_pdf(
                tax_reports=st.session_state.tax_reports,
                settings=pdf_settings,
            )

            st.download_button(
                label="📥 Steueraufstellung herunterladen",
                data=tax_pdf_bytes,
                file_name=f"steueraufstellung_{report_year}.pdf",
                mime="application/pdf",
                help="Deutsche Steueraufstellung für das Finanzamt herunterladen",
                type="secondary",
                use_container_width=False,
            )

        except Exception as e:
            st.error(f"❌ Fehler beim Erstellen der PDF: {str(e)}")
            st.error(f"Details: {e}")

    st.caption(
        "⚠️ Diese Aufstellung ersetzt nicht die professionelle Steuerberatung. Prüfen Sie alle Angaben vor Verwendung in Ihrer Steuererklärung."
    )

    # ELSTER Summary Section
    if hasattr(st.session_state, "tax_reports"):
        st.header("📋 ELSTER Summary")

        # Display the ELSTER summary table
        tax_reports = st.session_state.tax_reports
        if "summary" in tax_reports and not tax_reports["summary"].empty:
            # Initialize HTML table generator
            table_generator = UnifiedTableGenerator()

            # Generate and display ELSTER summary table
            elster_html = table_generator.generate_elster_summary_table_html(
                tax_reports["summary"]
            )
            st.markdown(elster_html, unsafe_allow_html=True)
        else:
            st.warning("No ELSTER summary data available.")

    # Detailed Tax Reports Section
    if hasattr(st.session_state, "tax_reports"):
        reports = st.session_state.tax_reports
        st.markdown("---")
        st.header("📊 Detailed Tax Reports")

        # Initialize HTML table generator
        table_generator = UnifiedTableGenerator()

        # Display CSS styles
        st.markdown(table_generator.get_web_css(), unsafe_allow_html=True)

        # 1. Share Transactions
        st.subheader("📈 Wertpapiergeschäfte")
        shares_html = table_generator.generate_shares_table_html(reports["shares"])
        st.markdown(shares_html, unsafe_allow_html=True)

        # 2. FOREX transactions
        st.subheader("💱 Devisengeschäfte")
        forex_html = table_generator.generate_forex_table_html(reports["forex"])
        st.markdown(forex_html, unsafe_allow_html=True)

        # 3. Dividend Payments
        st.subheader("💰 Dividendenerträge")
        dividends_html = table_generator.generate_dividends_table_html(
            reports["dividends"]
        )
        st.markdown(dividends_html, unsafe_allow_html=True)

        # 4. Fees (Werbungskosten)
        st.subheader("📄 Gebühren (Werbungskosten)")
        fees_html = table_generator.generate_fees_table_html(reports["fees"])
        st.markdown(fees_html, unsafe_allow_html=True)

        # 5. Tax Withholding
        st.subheader("🏛️ Quellensteuerzahlungen")
        taxes_html = table_generator.generate_taxes_table_html(reports["taxes"])
        st.markdown(taxes_html, unsafe_allow_html=True)


def show_awv_reports():
    """Display AWV reports section with integrated settings"""
    st.title("🏦 AWV Reports (Bundesbank)")
    st.markdown("Z4 and Z10 reports for German Federal Bank with integrated settings")
    st.markdown("---")

    # Check if events are imported
    if not (
        hasattr(st.session_state, "imported_events")
        and st.session_state.imported_events
    ):
        st.info("No events imported yet. Go to the Import Data page to upload files.")
        return

    # Get available years from events
    event_years = set()
    for event in st.session_state.imported_events:
        event_years.add(event.date.year)

    if not event_years:
        st.warning("No events with valid dates found.")
        return

    available_years = sorted(list(event_years), reverse=True)

    # AWV settings section
    st.header("🏦 AWV Report Configuration")

    # AWV report year selection
    awv_report_year = st.selectbox(
        "AWV Report Year",
        options=available_years,
        help="Select the year for AWV reporting to Bundesbank",
        key="awv_report_year_select",
    )

    st.markdown("---")

    awv_settings = ReportSettings(
        report_year=awv_report_year,
        exchange_rate_mode="daily",  # AWV always uses daily rates
    )

    if "price_manager" not in st.session_state:
        st.session_state.price_manager = HistoricPriceManager()

    if "fifo_processor" not in st.session_state:
        st.session_state.fifo_processor = FIFOProcessor(st.session_state.price_manager)

    if "report_generator" not in st.session_state:
        st.session_state.report_generator = ReportGenerator(
            st.session_state.fifo_processor, st.session_state.price_manager
        )

    # Check if AWV settings have changed by comparing current selection with stored settings
    awv_settings_changed = (
        not hasattr(st.session_state, "awv_report_settings")
        or st.session_state.awv_report_settings.report_year != awv_report_year
    )

    events_updated = hasattr(st.session_state, "events_last_updated") and (
        not hasattr(st.session_state, "awv_reports_last_generated")
        or st.session_state.events_last_updated
        > st.session_state.awv_reports_last_generated
    )

    # Only regenerate if settings changed or we don't have AWV reports for current settings
    if (
        awv_settings_changed
        or events_updated
        or not hasattr(st.session_state, "awv_reports")
    ):
        with st.spinner("Updating AWV reports with new settings..."):
            try:
                # Process events through FIFO
                st.session_state.fifo_processor.process_events(
                    st.session_state.imported_events
                )

                # Generate AWV reports
                st.session_state.awv_reports = (
                    st.session_state.report_generator.generate_awv_report(awv_settings)
                )
                st.session_state.awv_report_settings = awv_settings

                # Update timestamp to track when AWV reports were last generated
                st.session_state.awv_reports_last_generated = pd.Timestamp.now()

            except Exception as e:
                st.error(f"❌ Error processing events for AWV: {str(e)}")

        # AWV PDF Export Section
        if hasattr(st.session_state, "awv_reports"):
            st.subheader("📄 AWV PDF Export")

            try:
                # Generate PDF for AWV reports using local exporter
                exporter = GermanTaxHTMLExporter()

                awv_pdf_bytes = exporter.generate_pdf(
                    awv_reports=st.session_state.awv_reports,
                    settings=awv_settings,
                )

                st.download_button(
                    label="📥 AWV Meldungen herunterladen",
                    data=awv_pdf_bytes,
                    file_name=f"awv_meldungen_{awv_report_year}.pdf",
                    mime="application/pdf",
                    help="AWV Meldungen für die Bundesbank herunterladen",
                    type="secondary",
                    use_container_width=False,
                )

            except Exception as e:
                st.error(f"❌ Error generating AWV PDF: {str(e)}")
                st.error(f"Details: {e}")

    # AWV Reports Display Section
    if hasattr(st.session_state, "awv_reports"):
        reports = st.session_state.awv_reports
        st.markdown("---")
        st.header("📊 AWV Reports Details")

        # Initialize HTML table generator
        table_generator = UnifiedTableGenerator()

        # Display CSS styles
        st.markdown(table_generator.get_web_css(), unsafe_allow_html=True)

        # 1. Z4 - Capital Transfers (Bonus Payments)
        st.subheader("📋 Z4 - Kapitalzuflüsse (Bonuszahlungen)")
        z4_html = table_generator.generate_awv_z4_table_html(reports["z4"])
        st.markdown(z4_html, unsafe_allow_html=True)

        # 2. Z10 - Security Transactions
        st.subheader("📈 Z10 - Wertpapiergeschäfte")
        z10_html = table_generator.generate_awv_z10_table_html(reports["z10"])
        st.markdown(z10_html, unsafe_allow_html=True)
