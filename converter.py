import argparse
import csv
import os

import pandas as pd
from babel.numbers import parse_decimal

parser = argparse.ArgumentParser(
    description="Convert Interactive Brokers and Schwab CSV output to XLSX for later processing"
)
parser.add_argument(
    "type",
    type=str,
    choices=["ibkr", "schwab"],
    help="Type of the CSV format for input",
)
parser.add_argument(
    "-i", "--csv",
    dest="csv_filename",
    type=str,
    required=True,
    help="CSV file from Interactive Brokers or Schwab",
)
parser.add_argument(
    "-o", "--xlsx",
    dest="xlsx_filename",
    type=str,
    required=True,
    help="Output XLSX file",
)
parser.add_argument(
    "--ticker-to-isin",
    dest="isin_replace",
    type=bool,
    default=False,
    action=argparse.BooleanOptionalAction,
    help="Replace tickers in the 'symbol' column to ISIN (only for IBKR)",
)

class Converter:
    def __init__(self, args):
        self.csv_filename = args.csv_filename
        self.xlsx_filename = args.xlsx_filename
        self.isin_replace = args.isin_replace

        self.df_deposits = pd.DataFrame(columns=["date", "symbol", "net_quantity", "fmv_or_buy_price", "fees", "currency", "Product"])
        self.df_sales = pd.DataFrame(columns=["date", "symbol", "quantity", "sell_price", "fees", "currency", "Product"])
        self.df_dividends = pd.DataFrame(columns=["date", "symbol", "amount", "tax_withholding", "currency", "Product"])
        self.df_forex_to_eur = pd.DataFrame(columns=["date", "net_amount", "fees", "currency"])

        self.row = ''
        self.skip_dividend_section = False
        self.processed_trades = 0
        self.processed_dividends = 0
        self.processed_forex = 0
        self.processed_instrument_information = 0

    def process_csv(self):
        with open(self.csv_filename, encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                self.row = row
                self._process_trades()
                self._process_forex()
                self._process_dividends()
                self._process_instrument_information()

            for df in [self.df_deposits, self.df_sales, self.df_dividends, self.df_forex_to_eur]:
                df.sort_values("date", inplace=True)

            print(f"Total processed trades: {self.processed_trades}")
            print(f"Total processed dividends: {self.processed_dividends}")
            print(f"Total processed Forex trades: {self.processed_forex}")
            print(f"Replaced symbols: {self.processed_instrument_information}")

    def write_to_xlsx(self):
        with pd.ExcelWriter(self.xlsx_filename, engine="xlsxwriter") as writer:
            self._write_sheet("deposits", self.df_deposits, writer)
            self._write_sheet("dividends", self.df_dividends, writer)
            self._write_sheet("sales", self.df_sales, writer)
            self._write_sheet("currency conversion to EUR", self.df_forex_to_eur, writer)

        print(f"Results were written to '{os.path.basename(self.xlsx_filename)}'")

    @staticmethod
    def _write_sheet(name: str, df: pd.DataFrame, writer: pd.ExcelWriter):
        df.to_excel(writer, sheet_name=name, index=False, float_format="%.2f")
        worksheet = writer.sheets[name]
        worksheet.autofit()  # Adjust column widths to their maximum lengths

    @staticmethod
    def _parse_number(string):
        return float(parse_decimal(string, locale="en_US", strict=True))

    def _process_trades(self):
        raise NotImplementedError()

    def _process_forex(self):
        raise NotImplementedError()

    def _process_dividends(self):
        raise NotImplementedError()

    def _process_instrument_information(self):
        raise NotImplementedError()


def main(args):
    if args.type == "ibkr":
        from ibkr_converter import IbkrConverter
        converter = IbkrConverter(args)
    elif args.type == "schwab":
        from schwab_converter import SchwabConverter
        converter = SchwabConverter(args)
    else:
        raise ValueError("This type of converter is not recognised")

    converter.process_csv()
    converter.write_to_xlsx()


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
