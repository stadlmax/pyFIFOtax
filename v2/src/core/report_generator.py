"""
Report generator for pyFIFOtax Modern UI
Consolidates FIFO results into tax reports and AWV reports
"""

import pandas as pd
from decimal import Decimal
from datetime import date
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from src.core.fifo_processor import (
    FIFOProcessor,
    FIFOShare,
    FIFOForex,
    SimpleForex,
    AWVEntryZ4,
    AWVEntryZ10,
)
from src.core.historic_prices import HistoricPriceManager


@dataclass
class ReportSettings:
    """Configuration settings for report generation"""

    report_year: int
    exchange_rate_mode: str  # "daily" or "monthly"

    @property
    def awv_threshold_eur(self) -> int:
        """AWV threshold based on report year (12500€ pre-2025, 50000€ from 2025)"""
        return 50000 if self.report_year >= 2025 else 12500

    @property
    def consider_tax_free_forex(self) -> bool:
        """Always consider tax-free forex transactions"""
        return True


class ReportGenerator:
    """Generates tax reports and AWV reports from FIFO results"""

    def __init__(
        self, fifo_processor: FIFOProcessor, price_manager: HistoricPriceManager
    ):
        self.fifo_processor = fifo_processor
        self.price_manager = price_manager

    def generate_tax_report(self, settings: ReportSettings) -> Dict[str, pd.DataFrame]:
        """Generate tax report dataframes"""
        # Apply exchange rates to all results
        self._apply_exchange_rates(settings.exchange_rate_mode)

        # Filter results by report year
        filtered_sold_shares = self._filter_sold_assets_by_year(
            self.fifo_processor.sold_shares, settings.report_year
        )
        filtered_sold_forex = self._filter_sold_assets_by_year(
            self.fifo_processor.sold_forex, settings.report_year, exclude_eur=True
        )
        filtered_misc = self._filter_misc_by_year(
            self.fifo_processor.misc, settings.report_year
        )

        # Convert to dataframes
        df_shares = self._sold_shares_to_dataframe(
            filtered_sold_shares, settings.exchange_rate_mode
        )
        df_forex = self._sold_forex_to_dataframe(
            filtered_sold_forex,
            settings.exchange_rate_mode,
            settings.consider_tax_free_forex,
        )
        df_dividends = self._misc_to_dataframe(
            {"Dividend Payments": filtered_misc["Dividend Payments"]},
            settings.exchange_rate_mode,
        )
        df_fees = self._misc_to_dataframe(
            {"Fees": filtered_misc["Fees"]}, settings.exchange_rate_mode
        )
        df_taxes = self._misc_to_dataframe(
            {"Tax Withholding": filtered_misc["Tax Withholding"]},
            settings.exchange_rate_mode,
        )
        df_summary = self._create_elster_summary(
            df_shares, df_forex, df_dividends, df_fees, df_taxes
        )

        return {
            "shares": df_shares,
            "forex": df_forex,
            "dividends": df_dividends,
            "fees": df_fees,
            "taxes": df_taxes,
            "summary": df_summary,
        }

    def generate_awv_report(self, settings: ReportSettings) -> Dict[str, pd.DataFrame]:
        """Generate AWV Z4 and Z10 reports"""
        # Apply exchange rates to AWV entries
        self._apply_awv_exchange_rates()

        # Set threshold and filter by year
        for entry in self.fifo_processor.awv_z4_events:
            entry.set_threshold(settings.awv_threshold_eur)
        for entry in self.fifo_processor.awv_z10_events:
            entry.set_threshold(settings.awv_threshold_eur)

        # Filter by year and convert to dicts
        z4_dicts = [
            entry.as_dict()
            for entry in self.fifo_processor.awv_z4_events
            if entry.date.year == settings.report_year
        ]
        z4_dicts = [d for d in z4_dicts if d is not None]

        z10_dicts = [
            entry.as_dict()
            for entry in self.fifo_processor.awv_z10_events
            if entry.date.year == settings.report_year
        ]
        z10_dicts = [d for d in z10_dicts if d is not None]

        df_z4 = pd.DataFrame(z4_dicts) if z4_dicts else pd.DataFrame()
        df_z10 = pd.DataFrame(z10_dicts) if z10_dicts else pd.DataFrame()

        # Sort by reporting period
        if not df_z4.empty:
            df_z4 = df_z4.sort_values("Meldezeitraum")
        if not df_z10.empty:
            df_z10 = df_z10.sort_values("Meldezeitraum")

        return {"z4": df_z4, "z10": df_z10}

    def _apply_exchange_rates(self, mode: str):
        """Apply exchange rates to all FIFO results"""
        # Apply rates to sold shares
        for symbol_shares in self.fifo_processor.sold_shares.values():
            for share in symbol_shares:
                self._apply_rates_to_share(share, mode)

        # Apply rates to sold forex
        for currency_forex in self.fifo_processor.sold_forex.values():
            for forex in currency_forex:
                self._apply_rates_to_forex(forex, mode)

        # Apply rates to misc items
        for misc_list in self.fifo_processor.misc.values():
            for misc_item in misc_list:
                self._apply_rates_to_misc(misc_item, mode)

    def _apply_rates_to_share(self, share: FIFOShare, mode: str):
        """Apply exchange rates to a single share"""
        if share.currency == "EUR":
            share.buy_price_eur_daily = share.buy_price
            share.buy_price_eur_monthly = share.buy_price
            if share.sell_price:
                share.sell_price_eur_daily = share.sell_price
                share.sell_price_eur_monthly = share.sell_price
        else:
            # Get rates from price manager
            buy_rate_daily = self.price_manager.get_exchange_rate(
                share.currency, share.buy_date, "daily"
            )
            buy_rate_monthly = self.price_manager.get_exchange_rate(
                share.currency, share.buy_date, "monthly"
            )

            if buy_rate_daily:
                share.buy_price_eur_daily = share.buy_price / buy_rate_daily
            if buy_rate_monthly:
                share.buy_price_eur_monthly = share.buy_price / buy_rate_monthly

            if share.sell_date and share.sell_price:
                sell_rate_daily = self.price_manager.get_exchange_rate(
                    share.currency, share.sell_date, "daily"
                )
                sell_rate_monthly = self.price_manager.get_exchange_rate(
                    share.currency, share.sell_date, "monthly"
                )

                if sell_rate_daily:
                    share.sell_price_eur_daily = share.sell_price / sell_rate_daily
                if sell_rate_monthly:
                    share.sell_price_eur_monthly = share.sell_price / sell_rate_monthly

        # Convert transaction costs to EUR
        # Buy costs
        if share.buy_cost and share.buy_cost_currency:
            if share.buy_cost_currency == "EUR":
                buy_cost_eur_daily = share.buy_cost
                buy_cost_eur_monthly = share.buy_cost
            else:
                buy_rate_daily = self.price_manager.get_exchange_rate(
                    share.buy_cost_currency, share.buy_date, "daily"
                )
                buy_rate_monthly = self.price_manager.get_exchange_rate(
                    share.buy_cost_currency, share.buy_date, "monthly"
                )
                buy_cost_eur_daily = (
                    share.buy_cost / buy_rate_daily if buy_rate_daily else Decimal("0")
                )
                buy_cost_eur_monthly = (
                    share.buy_cost / buy_rate_monthly
                    if buy_rate_monthly
                    else Decimal("0")
                )
        else:
            buy_cost_eur_daily = Decimal("0")
            buy_cost_eur_monthly = Decimal("0")

        # Sell costs
        if share.sell_cost and share.sell_cost_currency and share.sell_date:
            if share.sell_cost_currency == "EUR":
                sell_cost_eur_daily = share.sell_cost
                sell_cost_eur_monthly = share.sell_cost
            else:
                sell_rate_daily = self.price_manager.get_exchange_rate(
                    share.sell_cost_currency, share.sell_date, "daily"
                )
                sell_rate_monthly = self.price_manager.get_exchange_rate(
                    share.sell_cost_currency, share.sell_date, "monthly"
                )
                sell_cost_eur_daily = (
                    share.sell_cost / sell_rate_daily
                    if sell_rate_daily
                    else Decimal("0")
                )
                sell_cost_eur_monthly = (
                    share.sell_cost / sell_rate_monthly
                    if sell_rate_monthly
                    else Decimal("0")
                )
        else:
            sell_cost_eur_daily = Decimal("0")
            sell_cost_eur_monthly = Decimal("0")

        # Calculate total transaction costs (following legacy logic)
        share.cost_eur_daily = share.quantity * (
            buy_cost_eur_daily + sell_cost_eur_daily
        )
        share.cost_eur_monthly = share.quantity * (
            buy_cost_eur_monthly + sell_cost_eur_monthly
        )

        # Calculate gains including transaction costs
        if mode == "daily" and share.buy_price_eur_daily and share.sell_price_eur_daily:
            buy_value = share.quantity * share.buy_price_eur_daily
            sell_value = share.quantity * share.sell_price_eur_daily
            share.gain_eur_daily = sell_value - buy_value - share.cost_eur_daily
        elif (
            mode == "monthly"
            and share.buy_price_eur_monthly
            and share.sell_price_eur_monthly
        ):
            buy_value = share.quantity * share.buy_price_eur_monthly
            sell_value = share.quantity * share.sell_price_eur_monthly
            share.gain_eur_monthly = sell_value - buy_value - share.cost_eur_monthly

    def _apply_rates_to_forex(self, forex: FIFOForex, mode: str):
        """Apply exchange rates to a single forex item"""
        if forex.currency == "EUR":
            forex.buy_price_eur_daily = Decimal("1")
            forex.buy_price_eur_monthly = Decimal("1")
            if forex.sell_date:
                forex.sell_price_eur_daily = Decimal("1")
                forex.sell_price_eur_monthly = Decimal("1")
                # For EUR, gain is always 0 (buy price = sell price = 1)
                forex.gain_eur_daily = Decimal("0")
                forex.gain_eur_monthly = Decimal("0")
        else:
            buy_rate_daily = self.price_manager.get_exchange_rate(
                forex.currency, forex.buy_date, "daily"
            )
            buy_rate_monthly = self.price_manager.get_exchange_rate(
                forex.currency, forex.buy_date, "monthly"
            )

            if buy_rate_daily:
                forex.buy_price_eur_daily = Decimal("1") / buy_rate_daily
            if buy_rate_monthly:
                forex.buy_price_eur_monthly = Decimal("1") / buy_rate_monthly

        # Calculate gains and sell prices in EUR
        if mode == "daily" and forex.buy_price_eur_daily and forex.sell_date:
            sell_rate = self.price_manager.get_exchange_rate(
                forex.currency, forex.sell_date, "daily"
            )
            if sell_rate:
                forex.sell_price_eur_daily = Decimal("1") / sell_rate
                buy_value = forex.quantity * forex.buy_price_eur_daily
                sell_value = forex.quantity * forex.sell_price_eur_daily
                forex.gain_eur_daily = sell_value - buy_value
        elif mode == "monthly" and forex.buy_price_eur_monthly and forex.sell_date:
            sell_rate = self.price_manager.get_exchange_rate(
                forex.currency, forex.sell_date, "monthly"
            )
            if sell_rate:
                forex.sell_price_eur_monthly = Decimal("1") / sell_rate
                buy_value = forex.quantity * forex.buy_price_eur_monthly
                sell_value = forex.quantity * forex.sell_price_eur_monthly
                forex.gain_eur_monthly = sell_value - buy_value

    def _apply_rates_to_misc(self, misc_item: SimpleForex, mode: str):
        """Apply exchange rates to a misc item"""
        if misc_item.currency == "EUR":
            misc_item.amount_eur_daily = misc_item.amount
            misc_item.amount_eur_monthly = misc_item.amount
        else:
            rate_daily = self.price_manager.get_exchange_rate(
                misc_item.currency, misc_item.date, "daily"
            )
            rate_monthly = self.price_manager.get_exchange_rate(
                misc_item.currency, misc_item.date, "monthly"
            )

            if rate_daily:
                misc_item.amount_eur_daily = misc_item.amount / rate_daily
            if rate_monthly:
                misc_item.amount_eur_monthly = misc_item.amount / rate_monthly

    def _apply_awv_exchange_rates(self):
        """Apply exchange rates to AWV entries"""
        for entry in self.fifo_processor.awv_z4_events:
            if entry.currency != "EUR":
                rate = self.price_manager.get_exchange_rate(
                    entry.currency, entry.date, "daily"
                )
                if rate:
                    entry.value_eur = entry.value / rate
                else:
                    entry.value_eur = Decimal("0")  # Skip if no rate available
            else:
                entry.value_eur = entry.value

        for entry in self.fifo_processor.awv_z10_events:
            if entry.currency != "EUR":
                rate = self.price_manager.get_exchange_rate(
                    entry.currency, entry.date, "daily"
                )
                if rate:
                    entry.value_eur = entry.value / rate
                else:
                    entry.value_eur = Decimal("0")  # Skip if no rate available
            else:
                entry.value_eur = entry.value

    def _filter_sold_assets_by_year(
        self, sold_assets: Dict[str, List], year: int, exclude_eur: bool = False
    ) -> Dict[str, List]:
        """Filter sold assets by sell year"""
        filtered = {}
        for symbol, assets in sold_assets.items():
            if exclude_eur and symbol == "EUR":
                continue
            filtered[symbol] = [
                asset
                for asset in assets
                if asset.sell_date and asset.sell_date.year == year
            ]
        return filtered

    def _filter_misc_by_year(
        self, misc: Dict[str, List[SimpleForex]], year: int
    ) -> Dict[str, List[SimpleForex]]:
        """Filter misc items by year"""
        filtered = {}
        for category, items in misc.items():
            filtered[category] = [item for item in items if item.date.year == year]
        return filtered

    def _sold_shares_to_dataframe(
        self, sold_shares: Dict[str, List[FIFOShare]], mode: str
    ) -> pd.DataFrame:
        """Convert sold shares to DataFrame"""
        rows = []
        for symbol, shares in sold_shares.items():
            for share in shares:
                if not share.sell_date:
                    continue

                # Format buy price with split information
                buy_price_display = f"{share.buy_price:.2f} {share.currency}"
                if (
                    share.original_buy_price
                    and share.cumulative_split_factor != Decimal("1")
                ):
                    buy_price_display += f" (orig. {share.original_buy_price:.2f}, {share.cumulative_split_factor}:1 split)"

                row = {
                    "Symbol": symbol,
                    "Quantity": float(share.quantity),
                    "Buy Date": share.buy_date.strftime("%Y-%m-%d"),
                    "Sell Date": share.sell_date.strftime("%Y-%m-%d"),
                    "Buy Price": buy_price_display,
                    "Sell Price": f"{share.sell_price:.2f} {share.currency}",
                    "Original Buy Price": (
                        f"{share.original_buy_price:.2f} {share.currency}"
                        if share.original_buy_price
                        else buy_price_display
                    ),
                    "Split Factor": (
                        f"{share.cumulative_split_factor}:1"
                        if share.cumulative_split_factor != Decimal("1")
                        else "None"
                    ),
                }

                if mode == "daily":
                    row["Buy Price [EUR]"] = float(share.buy_price_eur_daily or 0)
                    row["Sell Price [EUR]"] = float(share.sell_price_eur_daily or 0)
                    row["Buy Value [EUR]"] = float(
                        (share.quantity * (share.buy_price_eur_daily or 0))
                    )
                    row["Sell Value [EUR]"] = float(
                        (share.quantity * (share.sell_price_eur_daily or 0))
                    )
                    # Add transaction costs and gain before costs
                    row["Transaction Costs [EUR]"] = float(share.cost_eur_daily or 0)
                    sell_value = float(row["Sell Value [EUR]"])
                    buy_value = float(row["Buy Value [EUR]"])
                    row["Gain before Costs [EUR]"] = sell_value - buy_value
                    row["Gain [EUR]"] = float(share.gain_eur_daily or 0)
                else:
                    row["Buy Price [EUR]"] = float(share.buy_price_eur_monthly or 0)
                    row["Sell Price [EUR]"] = float(share.sell_price_eur_monthly or 0)
                    row["Buy Value [EUR]"] = float(
                        (share.quantity * (share.buy_price_eur_monthly or 0))
                    )
                    row["Sell Value [EUR]"] = float(
                        (share.quantity * (share.sell_price_eur_monthly or 0))
                    )
                    # Add transaction costs and gain before costs
                    row["Transaction Costs [EUR]"] = float(share.cost_eur_monthly or 0)
                    sell_value = float(row["Sell Value [EUR]"])
                    buy_value = float(row["Buy Value [EUR]"])
                    row["Gain before Costs [EUR]"] = sell_value - buy_value
                    row["Gain [EUR]"] = float(share.gain_eur_monthly or 0)

                rows.append(row)

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(["Sell Date", "Buy Date"])
        return df

    def _sold_forex_to_dataframe(
        self, sold_forex: Dict[str, List[FIFOForex]], mode: str, consider_tax_free: bool
    ) -> pd.DataFrame:
        """Convert sold forex to DataFrame"""
        rows = []
        for currency, forex_list in sold_forex.items():
            for forex in forex_list:
                if not forex.sell_date:
                    continue

                # Check if it's within the speculative period (1 year)
                holding_period_days = (forex.sell_date - forex.buy_date).days
                is_speculative = holding_period_days <= 365
                is_tax_free = consider_tax_free and forex.tax_free_forex

                # Calculate values based on mode
                if mode == "daily":
                    buy_price_eur = forex.buy_price_eur_daily or 0
                    sell_price_eur = forex.sell_price_eur_daily or 0
                    gain_eur = forex.gain_eur_daily or 0
                else:
                    buy_price_eur = forex.buy_price_eur_monthly or 0
                    sell_price_eur = forex.sell_price_eur_monthly or 0
                    gain_eur = forex.gain_eur_monthly or 0

                row = {
                    "Symbol": currency,
                    "Quantity": float(forex.quantity),
                    "Buy Date": forex.buy_date.strftime("%Y-%m-%d"),
                    "Sell Date": forex.sell_date.strftime("%Y-%m-%d"),
                    "Buy Price": f"{forex.buy_price:.4f} {currency}",
                    "Sell Price": f"{forex.sell_price:.4f} {currency}",
                    "Buy Price [EUR]": float(buy_price_eur),
                    "Sell Price [EUR]": float(sell_price_eur),
                    "Buy Value [EUR]": float(forex.quantity * buy_price_eur),
                    "Sell Value [EUR]": float(forex.quantity * sell_price_eur),
                    "Gain [EUR]": float(gain_eur),
                }

                if is_tax_free:
                    row["Comment"] = "Tax-free (dividend/bonus)"
                elif not is_speculative:
                    row["Comment"] = "Tax-free (>1 year holding)"
                else:
                    row["Comment"] = "Taxable"

                rows.append(row)

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(["Sell Date", "Buy Date"])
        return df

    def _misc_to_dataframe(
        self, misc_dict: Dict[str, List[SimpleForex]], mode: str
    ) -> pd.DataFrame:
        """Convert misc items to DataFrame"""
        from decimal import Decimal

        rows = []
        for category, items in misc_dict.items():
            for item in items:
                row = {
                    "Symbol": category,
                    "Comment": item.comment,
                    "Date": item.date.strftime("%Y-%m-%d"),
                    "Amount": f"{item.amount:.2f} {item.currency}",
                }

                if mode == "daily":
                    # Use proper decimal precision to avoid rounding to 0
                    amount_eur = item.amount_eur_daily or Decimal("0")
                    row["Amount [EUR]"] = float(amount_eur.quantize(Decimal("0.01")))
                else:
                    # Use proper decimal precision to avoid rounding to 0
                    amount_eur = item.amount_eur_monthly or Decimal("0")
                    row["Amount [EUR]"] = float(amount_eur.quantize(Decimal("0.01")))

                rows.append(row)

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("Date")
        return df

    def _create_elster_summary(
        self,
        df_shares: pd.DataFrame,
        df_forex: pd.DataFrame,
        df_dividends: pd.DataFrame,
        df_fees: pd.DataFrame,
        df_taxes: pd.DataFrame,
    ) -> pd.DataFrame:
        """Create ELSTER tax summary following legacy logic"""
        from decimal import Decimal

        def sum_decimal(series: pd.Series):
            """Sum a pandas Series of decimal values"""
            if series.empty:
                return Decimal("0")
            return Decimal(str(series.sum()))

        def round_decimal(number: Decimal, precision: str = "0.01"):
            """Round a decimal to specified precision"""
            return number.quantize(Decimal(precision))

        # Calculate share gains and losses
        if not df_shares.empty:
            share_gain_series = pd.Series(
                [Decimal(str(x)) for x in df_shares["Gain [EUR]"]]
            )
            share_losses = sum_decimal(share_gain_series[share_gain_series < 0])
            share_gains = sum_decimal(share_gain_series[share_gain_series > 0])
        else:
            share_losses = Decimal("0")
            share_gains = Decimal("0")

        # Calculate forex gains
        if not df_forex.empty:
            forex_gain_series = pd.Series(
                [Decimal(str(x)) for x in df_forex["Gain [EUR]"]]
            )
            total_gain_forex = sum_decimal(forex_gain_series)

            # For Anlage SO values, we need buy/sell values
            total_buy_value_forex = sum_decimal(
                pd.Series([Decimal(str(x)) for x in df_forex["Buy Value [EUR]"]])
            )
            total_sell_value_forex = sum_decimal(
                pd.Series([Decimal(str(x)) for x in df_forex["Sell Value [EUR]"]])
            )
        else:
            total_gain_forex = Decimal("0")
            total_buy_value_forex = Decimal("0")
            total_sell_value_forex = Decimal("0")

        # Calculate totals for dividends, fees, and taxes
        total_dividends = (
            sum_decimal(
                pd.Series([Decimal(str(x)) for x in df_dividends["Amount [EUR]"]])
            )
            if not df_dividends.empty
            else Decimal("0")
        )
        total_fees = (
            sum_decimal(pd.Series([Decimal(str(x)) for x in df_fees["Amount [EUR]"]]))
            if not df_fees.empty
            else Decimal("0")
        )
        total_taxes = (
            sum_decimal(pd.Series([Decimal(str(x)) for x in df_taxes["Amount [EUR]"]]))
            if not df_taxes.empty
            else Decimal("0")
        )

        # Calculate total foreign gains (legacy logic)
        total_foreign_gains = share_losses + share_gains + total_dividends
        gains_from_shares = share_gains
        losses_from_shares = -share_losses

        # Create ELSTER entries with separate Zeile numbers and descriptions
        anlagen = [
            (
                "Anlage KAP",
                "19",
                "Ausländische Kapitalerträge (ohne Betrag lt. Zeile 47)",
                round_decimal(total_foreign_gains, precision="0.01"),
            ),
            (
                "Anlage KAP",
                "20",
                "In den Zeilen 18 und 19 enthaltene Gewinne aus Aktienveräußerungen i. S. d. § 20 Abs. 2 Satz 1 Nr 1 EStG",
                round_decimal(gains_from_shares, precision="0.01"),
            ),
            (
                "Anlage KAP",
                "23",
                "In den Zeilen 18 und 19 enthaltene Verluste aus der Veräuerung von Aktien i. S. d. § 20 Abs. 2 Satz 1 Nr. 1 EStG",
                round_decimal(losses_from_shares, precision="0.01"),
            ),
            (
                "Anlage KAP",
                "41",
                "Anrechenbare noch nicht angerechnete ausländische Steuern",
                round_decimal(total_taxes, precision="0.01"),
            ),
            (
                "Anlage N",
                "65",
                "(Werbungskosten Sonstiges): Überweisungsgebühren auf deutsches Konto für Gehaltsbestandteil RSU/ESPP",
                round_decimal(total_fees, precision="0.01"),
            ),
            (
                "Anlage SO",
                "48-54",
                "Gewinn / Verlust aus Verkauf von Fremdwährungen",
                round_decimal(total_gain_forex, precision="0.01"),
            ),
            (
                "Anlage SO",
                "48-54",
                "Veräußerungswert Fremdwährungen",
                round_decimal(total_sell_value_forex, precision="0.01"),
            ),
            (
                "Anlage SO",
                "48-54",
                "Anschaffungskosten Fremdwährungen",
                round_decimal(total_buy_value_forex, precision="0.01"),
            ),
        ]

        summary = {
            "ELSTER - Anlage": [a[0] for a in anlagen],
            "ELSTER - Zeile": [a[1] for a in anlagen],
            "ELSTER - Beschreibung": [a[2] for a in anlagen],
            "Value": [
                float(a[3]) for a in anlagen
            ],  # Convert Decimal to float for display
        }

        return pd.DataFrame(summary)
