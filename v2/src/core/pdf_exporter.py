"""
PDF export functionality for German tax reports using HTML/CSS
"""

import pandas as pd
from decimal import Decimal
from dataclasses import dataclass
from weasyprint import HTML, CSS
from weasyprint.text.fonts import FontConfiguration
from typing import Optional, Dict, Any, List, Union


@dataclass
class ReportSettings:
    """Settings for report generation"""

    report_year: int
    currency: str = "EUR"
    include_forex: bool = True
    include_dividends: bool = True
    include_fees: bool = True
    include_taxes: bool = True


class UnifiedTableGenerator:
    """Generate HTML tables for both web display and PDF export"""

    def __init__(self, format_type: str = "web"):
        """
        Initialize table generator

        Args:
            format_type: "web" for web display, "pdf" for PDF export
        """
        self.format_type = format_type

    def _safe_float(self, value, default=0.0):
        """Safely convert value to float"""
        if pd.isna(value) or value == "" or value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _safe_str(self, value, default=""):
        """Safely convert value to string"""
        if pd.isna(value) or value is None:
            return default
        return str(value)

    def _get_container_start(self, section_title: Optional[str] = None) -> List[str]:
        """Get container start based on format type"""
        if self.format_type == "pdf":
            parts = ["<div class='section'>"]
            if section_title:
                parts.append(f"<h1>{section_title}</h1>")
            return parts
        else:
            return ["<div class='table-container'>"]

    def _get_container_end(self) -> str:
        """Get container end based on format type"""
        return "</div>"

    def _get_table_classes(self, table_type: str) -> str:
        """Get CSS classes for table based on format and type"""
        if self.format_type == "pdf":
            return f"{table_type}-table"
        else:
            return f"modern-table {table_type}-table"

    def _get_subtable_title_element(self, title: str) -> str:
        """Get subtable title element based on format"""
        if self.format_type == "pdf":
            return f"<h2>{title}</h2>"
        else:
            return f"<h4 class='subtable-title'>{title}</h4>"

    def generate_shares_table_html(
        self, shares_df: pd.DataFrame, section_title: Optional[str] = None
    ) -> str:
        """Generate HTML table for shares"""
        if shares_df.empty:
            return "<p class='no-data'>Keine Wertpapiergeschäfte für das gewählte Jahr.</p>"

        html_parts = self._get_container_start(section_title)
        html_parts.extend(
            [
                f"<table class='{self._get_table_classes('shares')}'>",
                "<thead>",
                "<tr>",
                "<th style='width: 7.14%'>Symbol</th>",
                "<th style='width: 7.14%'>Anzahl</th>",
                "<th style='width: 7.14%'>Datum<br>Kauf</th>",
                "<th style='width: 7.14%'>Datum<br>Verkauf</th>",
                "<th style='width: 7.14%'>Kurs<br>Kauf</th>",
                "<th style='width: 7.14%'>Kurs<br>Verkauf</th>",
                "<th style='width: 7.14%'>Original<br>Kaufkurs</th>",
                "<th style='width: 7.14%'>Split<br>Faktor</th>",
                "<th style='width: 7.14%'>Kurs<br>Kauf [€]</th>",
                "<th style='width: 7.14%'>Kurs<br>Verkauf [€]</th>",
                "<th style='width: 7.14%'>Wert<br>Kauf [€]</th>",
                "<th style='width: 7.14%'>Wert<br>Verkauf [€]</th>",
                "<th style='width: 7.14%'>Transaktions-<br>kosten [€]</th>",
                "<th style='width: 7.14%'>Gewinn/Verlust<br>[€]</th>",
                "</tr>",
                "</thead>",
                "<tbody>",
            ]
        )

        total_buy = 0
        total_sell = 0
        total_gain = 0
        total_costs = 0

        for _, row in shares_df.iterrows():
            buy_value = self._safe_float(row.get("Buy Value [EUR]", 0))
            sell_value = self._safe_float(row.get("Sell Value [EUR]", 0))
            gain = self._safe_float(row.get("Gain [EUR]", 0))
            transaction_costs = self._safe_float(row.get("Transaction Costs [EUR]", 0))

            total_buy += buy_value
            total_sell += sell_value
            total_gain += gain
            total_costs += transaction_costs

            html_parts.extend(
                [
                    "<tr>",
                    f"<td class='symbol-cell'>{self._safe_str(row.get('Symbol', ''))}</td>",
                    f"<td>{self._safe_float(row.get('Quantity', 0)):.2f}</td>",
                    f"<td>{self._safe_str(row.get('Buy Date', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Sell Date', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Buy Price', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Sell Price', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Original Buy Price', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Split Factor', ''))}</td>",
                    f"<td>{self._safe_float(row.get('Buy Price [EUR]', 0)):.4f} €</td>",
                    f"<td>{self._safe_float(row.get('Sell Price [EUR]', 0)):.4f} €</td>",
                    f"<td>{buy_value:,.2f} €</td>",
                    f"<td>{sell_value:,.2f} €</td>",
                    f"<td>{transaction_costs:,.2f} €</td>",
                    f"<td class='{'profit' if gain >= 0 else 'loss'}'>{gain:,.2f} €</td>",
                    "</tr>",
                ]
            )

        # Add totals row
        html_parts.extend(
            [
                "<tr class='total-row'>",
                "<td><strong>GESAMT</strong></td>",
                "<td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td>",
                f"<td><strong>{total_buy:,.2f} €</strong></td>",
                f"<td><strong>{total_sell:,.2f} €</strong></td>",
                f"<td><strong>{total_costs:,.2f} €</strong></td>",
                f"<td class='{'profit' if total_gain >= 0 else 'loss'}'><strong>{total_gain:,.2f} €</strong></td>",
                "</tr>",
                "</tbody>",
                "</table>",
                self._get_container_end(),
            ]
        )

        return "\n".join(html_parts)

    def generate_forex_table_html(
        self, forex_df: pd.DataFrame, section_title: Optional[str] = None
    ) -> str:
        """Generate HTML table for forex"""
        if forex_df.empty:
            return (
                "<p class='no-data'>Keine Devisengeschäfte für das gewählte Jahr.</p>"
            )

        html_parts = self._get_container_start(section_title)

        # Split by holding period
        speculative_df = pd.DataFrame()
        long_term_df = pd.DataFrame()

        for _, row in forex_df.iterrows():
            try:
                buy_date = pd.to_datetime(row.get("Buy Date", "")).date()
                sell_date = pd.to_datetime(row.get("Sell Date", "")).date()
                holding_days = (sell_date - buy_date).days

                if holding_days < 365:
                    speculative_df = pd.concat(
                        [speculative_df, row.to_frame().T], ignore_index=True
                    )
                else:
                    long_term_df = pd.concat(
                        [long_term_df, row.to_frame().T], ignore_index=True
                    )
            except:
                speculative_df = pd.concat(
                    [speculative_df, row.to_frame().T], ignore_index=True
                )

        # Create tables for each category with their own totals
        if not speculative_df.empty:
            html_parts.extend(
                self._generate_forex_subtable_html(
                    "Private Veräußerungsgeschäfte (< 1 Jahr, steuerpflichtig)",
                    speculative_df,
                )
            )

        if not long_term_df.empty:
            html_parts.extend(
                self._generate_forex_subtable_html(
                    "Private Veräußerungsgeschäfte (≥ 1 Jahr, steuerfrei)", long_term_df
                )
            )

        html_parts.append(self._get_container_end())
        return "\n".join(html_parts)

    def _generate_forex_subtable_html(self, title: str, df: pd.DataFrame) -> List[str]:
        """Generate forex subtable HTML"""
        html_parts = [
            self._get_subtable_title_element(title),
            f"<table class='{self._get_table_classes('forex')}'>",
            "<thead>",
            "<tr>",
            "<th style='width: 10%'>Währung</th>",
            "<th style='width: 10%'>Menge</th>",
            "<th style='width: 10%'>Datum<br>Kauf</th>",
            "<th style='width: 10%'>Datum<br>Verkauf</th>",
            "<th style='width: 10%'>Haltedauer<br>(Tage)</th>",
            "<th style='width: 10%'>Kurs<br>Kauf</th>",
            "<th style='width: 10%'>Kurs<br>Verkauf</th>",
            "<th style='width: 10%'>Wert<br>Kauf [€]</th>",
            "<th style='width: 10%'>Wert<br>Verkauf [€]</th>",
            "<th style='width: 10%'>Gewinn/Verlust<br>[€]</th>",
            "</tr>",
            "</thead>",
            "<tbody>",
        ]

        # Calculate totals for this subtable
        total_buy = 0
        total_sell = 0
        total_gain = 0

        for _, row in df.iterrows():
            gain = self._safe_float(row.get("Gain [EUR]", 0))
            buy_value = self._safe_float(row.get("Buy Value [EUR]", 0))
            sell_value = self._safe_float(row.get("Sell Value [EUR]", 0))

            total_buy += buy_value
            total_sell += sell_value
            total_gain += gain

            # Calculate holding period
            holding_days = 0
            try:
                buy_date = pd.to_datetime(row.get("Buy Date", "")).date()
                sell_date = pd.to_datetime(row.get("Sell Date", "")).date()
                holding_days = (sell_date - buy_date).days
            except:
                holding_days = 0

            html_parts.extend(
                [
                    "<tr>",
                    f"<td>{self._safe_str(row.get('Symbol', ''))}</td>",
                    f"<td>{self._safe_float(row.get('Quantity', 0)):.2f}</td>",
                    f"<td>{self._safe_str(row.get('Buy Date', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Sell Date', ''))}</td>",
                    f"<td>{holding_days}</td>",
                    f"<td>{self._safe_float(row.get('Buy Price [EUR]', 0)):.4f}</td>",
                    f"<td>{self._safe_float(row.get('Sell Price [EUR]', 0)):.4f}</td>",
                    f"<td>{buy_value:,.2f} €</td>",
                    f"<td>{sell_value:,.2f} €</td>",
                    f"<td class='{'profit' if gain >= 0 else 'loss'}'>{gain:,.2f} €</td>",
                    "</tr>",
                ]
            )

        # Add totals row to this subtable
        html_parts.extend(
            [
                "<tr class='total-row'>",
                "<td><strong>GESAMT</strong></td>",
                "<td></td><td></td><td></td><td></td><td></td><td></td>",
                f"<td><strong>{total_buy:,.2f} €</strong></td>",
                f"<td><strong>{total_sell:,.2f} €</strong></td>",
                f"<td class='{'profit' if total_gain >= 0 else 'loss'}'><strong>{total_gain:,.2f} €</strong></td>",
                "</tr>",
                "</tbody>",
                "</table>",
            ]
        )

        return html_parts

    def generate_dividends_table_html(
        self, dividends_df: pd.DataFrame, section_title: Optional[str] = None
    ) -> str:
        """Generate HTML table for dividends"""
        if dividends_df.empty:
            return "<p class='no-data'>Keine Dividenden für das gewählte Jahr.</p>"

        html_parts = self._get_container_start(section_title)
        html_parts.extend(
            [
                f"<table class='{self._get_table_classes('dividends')}'>",
                "<thead>",
                "<tr>",
                "<th style='width: 15%'>Datum</th>",
                "<th style='width: 35%'>Symbol</th>",
                "<th style='width: 25%'>Betrag (Original)</th>",
                "<th style='width: 25%'>Betrag [EUR]</th>",
                "</tr>",
                "</thead>",
                "<tbody>",
            ]
        )

        total_amount_eur = 0

        for _, row in dividends_df.iterrows():
            amount_eur = self._safe_float(row.get("Amount [EUR]", 0))
            amount_original = self._safe_str(row.get("Amount", ""))
            total_amount_eur += amount_eur

            html_parts.extend(
                [
                    "<tr>",
                    f"<td>{self._safe_str(row.get('Date', ''))}</td>",
                    f"<td class='symbol-cell'>{self._safe_str(row.get('Symbol', ''))}</td>",
                    f"<td>{amount_original}</td>",
                    f"<td>{amount_eur:,.2f} €</td>",
                    "</tr>",
                ]
            )

        # Add totals row
        html_parts.extend(
            [
                "<tr class='total-row'>",
                "<td><strong>GESAMT</strong></td>",
                "<td></td>",
                "<td></td>",
                f"<td><strong>{total_amount_eur:,.2f} €</strong></td>",
                "</tr>",
                "</tbody>",
                "</table>",
                self._get_container_end(),
            ]
        )

        return "\n".join(html_parts)

    def generate_fees_table_html(
        self, fees_df: pd.DataFrame, section_title: Optional[str] = None
    ) -> str:
        """Generate HTML table for fees"""
        if fees_df.empty:
            return "<p class='no-data'>Keine Gebühren für das gewählte Jahr.</p>"

        html_parts = self._get_container_start(section_title)
        html_parts.extend(
            [
                f"<table class='{self._get_table_classes('fees')}'>",
                "<thead>",
                "<tr>",
                "<th style='width: 15%'>Datum</th>",
                "<th style='width: 35%'>Beschreibung</th>",
                "<th style='width: 25%'>Betrag (Original)</th>",
                "<th style='width: 25%'>Betrag [EUR]</th>",
                "</tr>",
                "</thead>",
                "<tbody>",
            ]
        )

        total_fees = 0

        for _, row in fees_df.iterrows():
            amount_eur = self._safe_float(row.get("Amount [EUR]", 0))
            amount_original = self._safe_str(row.get("Amount", ""))
            total_fees += amount_eur

            html_parts.extend(
                [
                    "<tr>",
                    f"<td>{self._safe_str(row.get('Date', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Comment', ''))}</td>",
                    f"<td>{amount_original}</td>",
                    f"<td>{amount_eur:,.2f} €</td>",
                    "</tr>",
                ]
            )

        # Add totals row
        html_parts.extend(
            [
                "<tr class='total-row'>",
                "<td><strong>GESAMT</strong></td>",
                "<td></td>",
                "<td></td>",
                f"<td><strong>{total_fees:,.2f} €</strong></td>",
                "</tr>",
                "</tbody>",
                "</table>",
                self._get_container_end(),
            ]
        )

        return "\n".join(html_parts)

    def generate_taxes_table_html(
        self, tax_payments_df: pd.DataFrame, section_title: Optional[str] = None
    ) -> str:
        """Generate HTML table for tax payments"""
        if tax_payments_df.empty:
            return "<p class='no-data'>Keine Steuerzahlungen für das gewählte Jahr.</p>"

        html_parts = self._get_container_start(section_title)
        html_parts.extend(
            [
                f"<table class='{self._get_table_classes('taxes')}'>",
                "<thead>",
                "<tr>",
                "<th style='width: 15%'>Datum</th>",
                "<th style='width: 35%'>Beschreibung</th>",
                "<th style='width: 25%'>Betrag (Original)</th>",
                "<th style='width: 25%'>Betrag [EUR]</th>",
                "</tr>",
                "</thead>",
                "<tbody>",
            ]
        )

        total_amount = 0
        for _, row in tax_payments_df.iterrows():
            amount_eur = self._safe_float(row.get("Amount [EUR]", 0))
            amount_original = self._safe_str(row.get("Amount", ""))
            total_amount += amount_eur

            html_parts.extend(
                [
                    "<tr>",
                    f"<td>{self._safe_str(row.get('Date', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Comment', ''))}</td>",
                    f"<td>{amount_original}</td>",
                    f"<td>{amount_eur:,.2f} €</td>",
                    "</tr>",
                ]
            )

        # Add totals row
        html_parts.extend(
            [
                "<tr class='total-row'>",
                "<td><strong>GESAMT</strong></td>",
                "<td></td>",
                "<td></td>",
                f"<td><strong>{total_amount:,.2f} €</strong></td>",
                "</tr>",
                "</tbody>",
                "</table>",
                self._get_container_end(),
            ]
        )

        return "\n".join(html_parts)

    def generate_awv_z4_table_html(
        self, z4_df: pd.DataFrame, section_title: Optional[str] = None
    ) -> str:
        """Generate HTML table for AWV Z4 reports"""
        if z4_df.empty:
            return "<p class='no-data'>Keine Z4-Meldungen für das gewählte Jahr.</p>"

        html_parts = self._get_container_start(section_title)
        html_parts.extend(
            [
                f"<table class='{self._get_table_classes('awv')}'>",
                "<thead>",
                "<tr>",
                "<th>Meldezeitraum</th>",
                "<th>Zweck der Zahlung</th>",
                "<th>BA</th>",
                "<th>Kennzahl</th>",
                "<th>Land</th>",
                "<th>Eingehende/Ausgehende Zahlungen [T€]</th>",
                "</tr>",
                "</thead>",
                "<tbody>",
            ]
        )

        for _, row in z4_df.iterrows():
            incoming = self._safe_str(row.get("Eingehende Zahlungen", ""))
            outgoing = self._safe_str(row.get("Ausgehende Zahlungen", ""))
            payment_amount = incoming if incoming else outgoing

            html_parts.extend(
                [
                    "<tr>",
                    f"<td>{self._safe_str(row.get('Meldezeitraum', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Zweck der Zahlung', ''))}</td>",
                    f"<td>{self._safe_str(row.get('BA', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Kennzahl', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Land', ''))}</td>",
                    f"<td>{payment_amount}</td>",
                    "</tr>",
                ]
            )

        html_parts.extend(
            [
                "</tbody>",
                "</table>",
                self._get_container_end(),
            ]
        )

        return "\n".join(html_parts)

    def generate_awv_z10_table_html(
        self, z10_df: pd.DataFrame, section_title: Optional[str] = None
    ) -> str:
        """Generate HTML table for AWV Z10 reports"""
        if z10_df.empty:
            return "<p class='no-data'>Keine Z10-Meldungen für das gewählte Jahr.</p>"

        html_parts = self._get_container_start(section_title)
        html_parts.extend(
            [
                f"<table class='{self._get_table_classes('awv')}'>",
                "<thead>",
                "<tr>",
                "<th>Meldezeitraum</th>",
                "<th>Stückzahl</th>",
                "<th>Bezeichnung</th>",
                "<th>ISIN</th>",
                "<th>Land</th>",
                "<th>Eingehende/Ausgehende Zahlungen [T€]</th>",
                "</tr>",
                "</thead>",
                "<tbody>",
            ]
        )

        for _, row in z10_df.iterrows():
            incoming = self._safe_str(row.get("Eingehende Zahlungen", ""))
            outgoing = self._safe_str(row.get("Ausgehende Zahlungen", ""))
            payment_amount = incoming if incoming else outgoing

            html_parts.extend(
                [
                    "<tr>",
                    f"<td>{self._safe_str(row.get('Meldezeitraum', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Stückzahl', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Bezeichnung der Wertpapiere', ''))}</td>",
                    f"<td>{self._safe_str(row.get('ISIN', ''))}</td>",
                    f"<td>{self._safe_str(row.get('Land', ''))}</td>",
                    f"<td>{payment_amount}</td>",
                    "</tr>",
                ]
            )

        html_parts.extend(
            [
                "</tbody>",
                "</table>",
                self._get_container_end(),
            ]
        )

        return "\n".join(html_parts)

    def generate_elster_summary_table_html(
        self, summary_df: pd.DataFrame, section_title: Optional[str] = None
    ) -> str:
        """Generate HTML table for ELSTER summary"""
        if summary_df.empty:
            return "<p class='no-data'>Keine ELSTER Zusammenfassung verfügbar.</p>"

        html_parts = self._get_container_start(section_title)
        html_parts.extend(
            [
                f"<table class='{self._get_table_classes('elster-summary')}'>",
                "<thead>",
                "<tr>",
                "<th style='width: 17%'>ELSTER - Anlage</th>",
                "<th style='width: 16%'>Zeile</th>",
                "<th style='width: 50%'>Beschreibung</th>",
                "<th style='width: 17%'>Betrag [€]</th>",
                "</tr>",
                "</thead>",
                "<tbody>",
            ]
        )

        for _, row in summary_df.iterrows():
            anlage = self._safe_str(row.get("ELSTER - Anlage", ""))
            zeile = self._safe_str(row.get("ELSTER - Zeile", ""))
            beschreibung = self._safe_str(row.get("ELSTER - Beschreibung", ""))
            value = self._safe_float(row.get("Value", 0))

            # Apply color coding based on value
            value_class = "profit" if value > 0 else "loss" if value < 0 else "neutral"

            html_parts.extend(
                [
                    "<tr>",
                    f"<td class='anlage-cell'><strong>{anlage}</strong></td>",
                    f"<td class='zeile-cell'><strong>{zeile}</strong></td>",
                    f"<td class='beschreibung-cell'>{beschreibung}</td>",
                    f"<td class='{value_class}'><strong>{value:,.2f} €</strong></td>",
                    "</tr>",
                ]
            )

        html_parts.extend(
            [
                "</tbody>",
                "</table>",
                self._get_container_end(),
            ]
        )

        return "\n".join(html_parts)

    def get_web_css(self) -> str:
        """Get CSS styles for web display"""
        return """
        <style>
        .table-container {
            margin: 2rem 0;
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
        }
        
        .modern-table {
            width: 100%;
            min-width: 800px;
            border-collapse: collapse;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
            font-size: 0.85rem;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
            line-height: 1.2;
        }
        
        .modern-table.shares-table,
        .modern-table.forex-table {
            min-width: 1200px;
        }
        
        .modern-table.dividends-table,
        .modern-table.fees-table,
        .modern-table.taxes-table {
            min-width: 600px;
        }
        
        .modern-table th {
            background: #1f2937;
            color: white;
            font-weight: 500;
            padding: 0.5rem;
            text-align: left;
            border: none;
            font-size: 0.8rem;
            letter-spacing: 0.025em;
            line-height: 1.2;
        }
        
        .modern-table td {
            padding: 0.4rem 0.5rem;
            border-bottom: 1px solid #f3f4f6;
            vertical-align: middle;
            background: white;
            line-height: 1.2;
        }
        
        .modern-table tbody tr:nth-child(even) td {
            background: #fafafa;
        }
        
        .modern-table .total-row td {
            background: #374151 !important;
            color: white !important;
            font-weight: 600 !important;
            border-top: 1px solid #9ca3af;
            padding: 0.5rem;
        }
        
        .symbol-cell {
            font-weight: 600;
            color: #111827;
        }
        
        .profit-cell {
            color: #16a34a !important;
            font-weight: 600 !important;
        }
        
        .loss-cell {
            color: #dc2626 !important;
            font-weight: 600 !important;
        }
        
        .subtable-title {
            margin: 1.5rem 0 0.75rem 0;
            font-size: 1rem;
            font-weight: 600;
            color: #374151;
        }
        
        .no-data {
            text-align: center;
            padding: 2rem;
            color: #6b7280;
            background: #f9fafb;
            border-radius: 8px;
            margin: 1rem 0;
        }
        </style>
        """


# Alias for backward compatibility
HTMLTableGenerator = lambda: UnifiedTableGenerator("web")


class GermanTaxHTMLExporter:
    """HTML-based PDF exporter for German tax reports"""

    def __init__(self):
        self.font_config = FontConfiguration()
        self.table_generator = UnifiedTableGenerator("pdf")

    def generate_pdf(
        self,
        tax_reports: Optional[Dict[str, pd.DataFrame]] = None,
        awv_reports: Optional[Dict[str, pd.DataFrame]] = None,
        settings=None,
    ) -> bytes:
        """Generate complete PDF tax report"""
        sections = []

        # 1. Tax sections (if tax_reports provided)
        if tax_reports:
            # Share transactions
            if not tax_reports["shares"].empty:
                sections.append(self._generate_shares_section(tax_reports["shares"]))

            # FOREX transactions
            if not tax_reports["forex"].empty:
                sections.append(self._generate_forex_section(tax_reports["forex"]))

            # Dividends
            if not tax_reports["dividends"].empty:
                sections.append(
                    self._generate_dividends_section(tax_reports["dividends"])
                )

            # Fees
            if not tax_reports["fees"].empty:
                sections.append(self._generate_fees_section(tax_reports["fees"]))

            # Tax payments
            if not tax_reports["taxes"].empty:
                sections.append(self._generate_taxes_section(tax_reports["taxes"]))

        # 2. AWV sections (if awv_reports provided)
        if awv_reports:
            # AWV Z4 reports
            if not awv_reports["z4"].empty:
                sections.append(self._generate_awv_z4_section(awv_reports["z4"]))

            # AWV Z10 reports
            if not awv_reports["z10"].empty:
                sections.append(self._generate_awv_z10_section(awv_reports["z10"]))

        # Create complete HTML
        html_content = self._create_html_document(sections, settings)

        # Convert to PDF
        pdf_bytes = HTML(string=html_content).write_pdf()
        return pdf_bytes

    def _generate_html(
        self,
        shares_df: pd.DataFrame,
        forex_df: pd.DataFrame,
        dividends_df: pd.DataFrame,
        fees_df: pd.DataFrame,
        tax_payments_df: pd.DataFrame,
        settings: ReportSettings,
    ) -> str:
        """Generate complete HTML document"""

        body_parts = []

        # Add shares section
        if not shares_df.empty:
            body_parts.append(
                self.table_generator.generate_shares_table_html(
                    shares_df, "Wertpapiere"
                )
            )

        # Add forex section
        if not forex_df.empty:
            body_parts.append(
                self.table_generator.generate_forex_table_html(
                    forex_df, "Devisengeschäfte"
                )
            )

        # Add dividends section
        if not dividends_df.empty:
            body_parts.append(
                self.table_generator.generate_dividends_table_html(
                    dividends_df, "Dividenden"
                )
            )

        # Add fees section
        if not fees_df.empty:
            body_parts.append(
                self.table_generator.generate_fees_table_html(fees_df, "Gebühren")
            )

        # Add taxes section
        if not tax_payments_df.empty:
            body_parts.append(
                self.table_generator.generate_taxes_table_html(
                    tax_payments_df, "Steuerzahlungen"
                )
            )

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Steueraufstellung {settings.report_year}</title>
        </head>
        <body>
        {"".join(body_parts)}
        </body>
        </html>
        """

        return html_content

    def _generate_shares_section(self, shares_df: pd.DataFrame) -> str:
        """Generate shares section HTML"""
        return self.table_generator.generate_shares_table_html(shares_df, "Wertpapiere")

    def _generate_forex_section(self, forex_df: pd.DataFrame) -> str:
        """Generate forex section HTML"""
        return self.table_generator.generate_forex_table_html(
            forex_df, "Devisengeschäfte"
        )

    def _generate_dividends_section(self, dividends_df: pd.DataFrame) -> str:
        """Generate dividends section HTML"""
        return self.table_generator.generate_dividends_table_html(
            dividends_df, "Dividenden"
        )

    def _generate_fees_section(self, fees_df: pd.DataFrame) -> str:
        """Generate fees section HTML"""
        return self.table_generator.generate_fees_table_html(fees_df, "Gebühren")

    def _generate_taxes_section(self, taxes_df: pd.DataFrame) -> str:
        """Generate taxes section HTML"""
        return self.table_generator.generate_taxes_table_html(
            taxes_df, "Steuerzahlungen"
        )

    def _generate_awv_z4_section(self, z4_df: pd.DataFrame) -> str:
        """Generate AWV Z4 section HTML"""
        return self.table_generator.generate_awv_z4_table_html(
            z4_df, "AWV Meldungen Z4 (Kapitalzuflüsse)"
        )

    def _generate_awv_z10_section(self, z10_df: pd.DataFrame) -> str:
        """Generate AWV Z10 section HTML"""
        return self.table_generator.generate_awv_z10_table_html(
            z10_df, "AWV Meldungen Z10 (Wertpapiergeschäfte)"
        )

    def _create_html_document(self, sections: List[str], settings) -> str:
        """Create complete HTML document from sections"""
        body_content = "".join(sections)

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Steueraufstellung {settings.report_year if hasattr(settings, 'report_year') else 'N/A'}</title>
            <style>{self._get_css_styles()}</style>
        </head>
        <body>
        {body_content}
        </body>
        </html>
        """

        return html_content

    def _get_css_styles(self) -> str:
        """Get CSS styles for the PDF"""
        return """
        @page {
            size: A4 landscape;
            margin: 0.8cm 0.5cm;
        }
        
        body {
            font-family: system-ui, -apple-system, sans-serif;
            font-size: 8pt;
            line-height: 1.2;
            color: #111827;
            margin: 0;
            padding: 0;
            background: white;
        }
        
        .section {
            margin-bottom: 1cm;
            page-break-inside: avoid;
        }
        
        h1 {
            font-size: 14pt;
            font-weight: 600;
            color: #111827;
            margin: 0 0 0.4cm 0;
            padding-bottom: 0.1cm;
            border-bottom: 1px solid #d1d5db;
        }
        
        h2 {
            font-size: 10pt;
            font-weight: 500;
            color: #4b5563;
            margin: 0.4cm 0 0.2cm 0;
        }
        
        table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            margin-bottom: 0.4cm;
            font-size: 7pt;
            line-height: 1.1;
            background: white;
            border-radius: 6px;
            overflow: hidden;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            border: 1px solid #e5e7eb;
        }
        
        th {
            background: #1f2937;
            color: white;
            font-weight: 500;
            padding: 6px 4px;
            text-align: left;
            border: none;
            line-height: 1.1;
            font-size: 7pt;
        }
        
        th:first-child {
            border-top-left-radius: 6px;
        }
        
        th:last-child {
            border-top-right-radius: 6px;
        }
        
        td {
            padding: 4px;
            border-bottom: 1px solid #f3f4f6;
            text-align: left;
            vertical-align: middle;
            background: white;
            line-height: 1.1;
        }
        
        td:first-child {
            font-weight: 500;
        }
        
        tbody tr:last-child td {
            border-bottom: none;
        }
        
        tbody tr:last-child td:first-child {
            border-bottom-left-radius: 6px;
        }
        
        tbody tr:last-child td:last-child {
            border-bottom-right-radius: 6px;
        }
        
        tr:nth-child(even) td {
            background: #fafafa;
        }
        
        .total-row td {
            background: #374151 !important;
            color: white !important;
            font-weight: 600 !important;
            border-top: 1px solid #9ca3af !important;
            border-bottom: none !important;
            padding: 6px 4px;
        }
        
        .total-row td:first-child {
            border-bottom-left-radius: 6px;
        }
        
        .total-row td:last-child {
            border-bottom-right-radius: 6px;
        }
        
        .profit {
            color: #16a34a;
            font-weight: 600;
        }
        
        .loss {
            color: #dc2626;
            font-weight: 600;
        }
        
        .shares-table {
            font-size: 6pt;
        }
        
        .forex-table {
            font-size: 6pt;
        }
        
        .dividends-table th:nth-child(1) { width: 12%; }
        .dividends-table th:nth-child(2) { width: 58%; }
        .dividends-table th:nth-child(3) { width: 15%; }
        .dividends-table th:nth-child(4) { width: 15%; }
        
        .fees-table th:nth-child(1) { width: 15%; }
        .fees-table th:nth-child(2) { width: 65%; }
        .fees-table th:nth-child(3) { width: 20%; }
        
        .taxes-table th:nth-child(1) { width: 15%; }
        .taxes-table th:nth-child(2) { width: 65%; }
        .taxes-table th:nth-child(3) { width: 20%; }
        
        .awv-table th:nth-child(1) { width: 12%; }
        .awv-table th:nth-child(2) { width: 15%; }
        .awv-table th:nth-child(3) { width: 15%; }
        .awv-table th:nth-child(4) { width: 15%; }
        .awv-table th:nth-child(5) { width: 20%; }
        .awv-table th:nth-child(6) { width: 23%; }
        """
