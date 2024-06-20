import csv
import os

import pandas as pd
from babel.numbers import parse_decimal


class CSVConverter:
    def __init__(self, args, input_files: dict):
        self._input_files = input_files
        self._xlsx_filename = args.xlsx_filename

        self._current_file = 0

        self._buy_events = []
        self._sell_events = []
        self._dividend_events = []
        self._forex_events = []

        self.df_rsu = pd.DataFrame(
            columns=[
                "date",
                "symbol",
                "gross_quantity",
                "net_quantity",
                "fair_market_value",
                "currency",
                "comment",
            ]
        )
        self.df_espp = pd.DataFrame(
            columns=[
                "date",
                "symbol",
                "buy_price",
                "fair_market_value",
                "quantity",
                "currency",
                "comment",
            ]
        )
        self.df_deposits = pd.DataFrame(
            columns=[
                "date",
                "symbol",
                "quantity",
                "buy_price",
                "currency",
                "fees",
                "fee_currency",
                "comment",
            ]
        )
        self.df_sales = pd.DataFrame(
            columns=[
                "date",
                "symbol",
                "quantity",
                "sell_price",
                "currency",
                "fees",
                "fee_currency",
                "comment",
            ]
        )
        self.df_dividends = pd.DataFrame(
            columns=[
                "date",
                "symbol",
                "amount",
                "tax_withholding",
                "currency",
                "comment",
            ]
        )
        self.df_forex = pd.DataFrame(
            columns=["date",
                     "source_amount",
                     "source_currency",
                     "target_amount",
                     "target_currency",
                     "fees",
                     "fee_currency",
                     "comment"]
        )
        self.df_money_transfers = pd.DataFrame(
            columns=["date",
                     "buy_date",
                     "amount",
                     "currency",
                     "fees",
                     "fee_currency",
                     "comment"]
        )

        self.row = ""
        self.processed_trades = 0
        self.processed_dividends = 0
        self.processed_forex = 0
        self.processed_transfers = 0
        self.processed_instrument_information = 0

    def process_csv(self):
        for filename, encoding in self._input_files.items():
            with open(filename, encoding=encoding) as csv_file:
                self._current_file += 1
                csv_reader = csv.reader(csv_file)
                for row in csv_reader:
                    self.row = row
                    self._process_trades()
                    self._process_forex()
                    self._process_deposits_withdrawals()
                    self._process_dividends()
                    self._process_withholding_tax()
                    self._process_interest()
                    self._process_instrument_information()

        for df in [
            self.df_rsu,
            self.df_espp,
            self.df_deposits,
            self.df_sales,
            self.df_dividends,
            self.df_forex,
            self.df_money_transfers,
        ]:
            df.sort_values("date", inplace=True, kind="stable")

        print(f"Total processed trades: {self.processed_trades}")
        print(f"Total processed dividends: {self.processed_dividends}")
        print(f"Total processed Forex trades: {self.processed_forex}")
        print(f"Total processed transfers: {self.processed_transfers}")
        print(f"Replaced symbols: {self.processed_instrument_information}")

    def write_to_xlsx(self):
        with pd.ExcelWriter(self._xlsx_filename, engine="xlsxwriter") as writer:
            self._write_sheet("rsu", self.df_rsu, writer)
            self._write_sheet("espp", self.df_espp, writer)
            self._write_sheet("buy_orders", self.df_deposits, writer)
            self._write_sheet("dividends", self.df_dividends, writer)
            self._write_sheet("sell_orders", self.df_sales, writer)
            self._write_sheet("currency_conversions", self.df_forex, writer)
            self._write_sheet("money_transfers", self.df_money_transfers, writer)

        print(f"Results were written to '{os.path.basename(self._xlsx_filename)}'")

    @staticmethod
    def _write_sheet(name: str, df: pd.DataFrame, writer: pd.ExcelWriter):
        df.to_excel(writer, sheet_name=name, index=False, float_format="%.3f")
        worksheet = writer.sheets[name]
        worksheet.autofit()  # Adjust column widths to their maximum lengths

    @staticmethod
    def _parse_number(string: str):
        return float(parse_decimal(string, locale="en_US", strict=True))

    @staticmethod
    def _wrong_header(header: str):
        raise ValueError(
            "Input CSV is not in the expected format. Either this script needs adaptation or "
            f"a wrong type of CSV was downloaded. {header.title()} header is incorrect"
        )

    def _process_trades(self):
        raise NotImplementedError()

    def _process_forex(self):
        raise NotImplementedError()

    def _process_deposits_withdrawals(self):
        raise NotImplementedError()

    def _process_dividends(self):
        raise NotImplementedError()

    def _process_withholding_tax(self):
        raise NotImplementedError()

    def _process_interest(self):
        raise NotImplementedError()

    def _process_instrument_information(self):
        raise NotImplementedError()
