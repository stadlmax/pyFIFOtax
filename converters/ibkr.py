import csv
import os
from datetime import date

import pandas as pd
from babel.numbers import parse_decimal


class CSVConverter:
    def __init__(self, arguments):
        self._csv_filename = arguments.input_filename
        self._xlsx_filename = arguments.xlsx_filename
        self._isin_replace = arguments.isin_replace

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
        self.skip_dividend_section = False
        self.processed_trades = 0
        self.processed_dividends = 0
        self.processed_forex = 0
        self.processed_instrument_information = 0

    def process_csv(self):
        with open(self._csv_filename, encoding="utf-8-sig") as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                self.row = row
                self._process_trades()
                self._process_forex()
                self._process_dividends()
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
                df.sort_values("date", inplace=True)

            print(f"Total processed trades: {self.processed_trades}")
            print(f"Total processed dividends: {self.processed_dividends}")
            print(f"Total processed Forex trades: {self.processed_forex}")
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
        df.to_excel(writer, sheet_name=name, index=False, float_format="%.2f")
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

    def _process_dividends(self):
        raise NotImplementedError()

    def _process_instrument_information(self):
        raise NotImplementedError()


class IbkrConverter(CSVConverter):
    def __init__(self, args):
        super().__init__(args)

    def _process_trades(self):
        if self.row[0] != "Trades" or self.row[3] == "Forex":
            return

        if self._check_trades_header():
            # Skip after header processing
            return

        if self._process_trade_row():
            self.processed_trades += 1

    def _check_trades_header(self):
        if self.row[1] == "Header" and self.row[10] == "C. Price":
            expected_headers = [
                "Trades",
                "Header",
                "DataDiscriminator",
                "Asset Category",
                "Currency",
                "Symbol",
                "Date/Time",
                "Exchange",
                "Quantity",
                "T. Price",
                "C. Price",
                "Proceeds",
                "Comm/Fee",
                "Basis",
                "Realized P/L",
                "MTM P/L",
                "Code",
            ]

            if self.row != expected_headers:
                self._wrong_header("trade")

            return True

        return False

    def _process_trade_row(self):
        if self.row[2] != "Order":
            return False

        quantity = self._parse_number(self.row[8])
        df = self.df_deposits if quantity > 0 else self.df_sales
        df.loc[len(df.index)] = [
            date.fromisoformat(self.row[6].split(",")[0]),  # Date
            self.row[5],  # Symbol
            abs(quantity),  # Quantity
            self._parse_number(self.row[9]),  # T. Price
            self.row[4],  # Currency
            abs(self._parse_number(self.row[12])),  # Comm/Fee
            self.row[4],  # Fee Currency
            self.row[5] + " [on IBKR]",  # Symbol, comment
        ]

        return True

    def _process_forex(self):
        if self.row[0] != "Trades" or self.row[3] != "Forex":
            return

        if self._check_forex_header():
            # Skip after header processing
            return

        if self._process_forex_row():
            self.processed_forex += 1

    def _check_forex_header(self):
        if self.row[1] == "Header" and self.row[10] != "C. Price":
            expected_headers = [
                "Trades",
                "Header",
                "DataDiscriminator",
                "Asset Category",
                "Currency",
                "Symbol",
                "Date/Time",
                "Exchange",
                "Quantity",
                "T. Price",
            ]

            if self.row != expected_headers:
                self._wrong_header("forex")

            return True

        return False

    def _process_forex_row(self):
        if (
            self.row[2] != "Order" or self.row[4] == "EUR"
        ):  # pyFIFOtax spreadsheet supports only conversion into EUR
            return False

        self.df_forex.loc[len(self.df_forex.index)] = [
            date.fromisoformat(self.row[6].split(",")[0]),  # Date
            abs(self._parse_number(self.row[11])),  # Proceeds
            self.row[4],  # Currency
            -1,
            self.row[4],  # Currency
            abs(
                self._parse_number(self.row[12]) * self._parse_number(self.row[9])
            ),  # Comm in EUR * T. Price
            self.row[5].replace(self.row[4], "").replace(".", ""),  # Symbol
            self.row[4],  # Currency
            "[on IBKR]",  # Comment
        ]

        return True

    def _process_dividends(self):
        if self.row[0] != "Dividends":
            return

        if self._check_dividends_header():
            # Skip after header processing
            return

        if self._process_dividend_row():
            self.processed_dividends += 1

    def _check_dividends_header(self):
        if self.row[1] == "Header":
            wrong_headers = expected_headers = [
                "Dividends",
                "Header",
                "Currency",
                "Date",
                "Description",
                "Amount",
            ]
            if self.row == wrong_headers:
                # There are two "Dividends" sections in the CSV file, duplicating the same information. Skip the first
                self.skip_dividend_section = True

                return True

            expected_headers += ["Code"]
            if not self.row == expected_headers:
                self._wrong_header("Dividends")

            self.skip_dividend_section = False
            return True

        return False

    def _process_dividend_row(self):
        if self.skip_dividend_section or self.row[2] == "Total":
            return False

        symbol = self.row[4].split(" ")[0]
        self.df_dividends.loc[len(self.df_dividends.index)] = [
            date.fromisoformat(self.row[3]),  # Date
            symbol,
            self._parse_number(self.row[5]),  # Amount
            0,  # Tax withholding
            self.row[2],  # Currency
            symbol + " [on IBKR]",  # Comment
        ]

        return True

    def _process_instrument_information(self):
        if self.row[0] != "Financial Instrument Information":
            return

        if self._check_instrument_information_header():
            # Skip after header processing
            return

        self._process_instrument_information_row()
        self.processed_instrument_information += 1

    def _check_instrument_information_header(self):
        if self.row[1] == "Header":
            expected_headers = [
                [
                    "Financial Instrument Information",
                    "Header",
                    "Asset Category",
                    "Symbol",
                    "Description",
                    "Conid",
                    "Security ID",
                    "Listing Exch",
                    "Multiplier",
                    "Code",
                ]
            ]
            expected_headers.append(expected_headers[0].copy())
            expected_headers[1].insert(-1, "Type")

            if self.row not in expected_headers:
                self._wrong_header("Financial Instrument Information")

            return True

        return False

    def _process_instrument_information_row(self):
        symbol = self.row[3]
        isin = self.row[6]
        product = self.row[4]

        for df in [self.df_deposits, self.df_sales, self.df_dividends]:
            if self._isin_replace:
                df.loc[df["symbol"] == symbol, "symbol"] = isin

            df.loc[df["comment"] == symbol + " [on IBKR]", "comment"] = product + " [on IBKR]"
