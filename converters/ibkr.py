import re
from datetime import date

from .csv_converter import CSVConverter


class IbkrConverter(CSVConverter):
    def __init__(self, args):
        super().__init__(args, {args.input_filename: "utf-8-sig"})
        self._isin_replace = args.ibkr_isin_replace

        self._skip_dividend_section = False
        self._skip_dw_section = False
        self._withholding_tax = {}

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
            abs(self._parse_number(self.row[12])),  # Comm/Fee
            self.row[4],  # Currency
            self.row[5],  # Symbol
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
        if self.row[2] != "Order":
            return False

        self.df_forex.loc[len(self.df_forex.index)] = [
            date.fromisoformat(self.row[6].split(",")[0]),  # Date
            abs(self._parse_number(self.row[11])),  # Proceeds
            abs(
                self._parse_number(self.row[12]) * self._parse_number(self.row[9])
            ),  # Comm in EUR * T. Price
            self.row[5].replace(self.row[4], "").replace(".", ""),  # Symbol - Source currency
            self.row[4],  # Currency - Target currency
        ]

        return True

    def _process_deposits_withdrawals(self):
        if self.row[0] != "Deposits & Withdrawals":
            return

        if self._check_dw_header():
            # Skip after header processing
            return

        if self._process_dw_row():
            self.processed_forex += 1

    def _check_dw_header(self):
        if self.row[1] == "Header":
            wrong_headers = expected_headers = [
                "Deposits & Withdrawals", "Header", "Currency", "Settle Date", "Description", "Amount"
            ]

            if self.row == wrong_headers:
                # There are two "Deposits & Withdrawals" sections in the CSV file, duplicating the same information. Skip the first
                self._skip_dw_section = True

                return True

            expected_headers += ["Code"]

            if self.row != expected_headers:
                self._wrong_header("Deposits & Withdrawals")

            return True

        return False

    def _process_dw_row(self):
        if self._skip_dw_section or self.row[2].startswith("Total"):
            return False

        amount = self._parse_number(self.row[5])  # Amount
        if amount <= 0:
            # Don't care about disbursements
            return False

        self.df_forex.loc[len(self.df_forex.index)] = [
            date.fromisoformat(self.row[3]),  # Settle Date
            amount,  # Proceeds
            0,  # Fees
            self.row[2],  # Symbol
            self.row[2],  # Symbol
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
                self._skip_dividend_section = True

                return True

            expected_headers += ["Code"]
            if not self.row == expected_headers:
                self._wrong_header("Dividends")

            self._skip_dividend_section = False
            return True

        return False

    def _process_dividend_row(self):
        if self._skip_dividend_section or self.row[2] == "Total":
            return False

        symbol = self.row[4].split(" ")[0]
        self.df_dividends.loc[len(self.df_dividends.index)] = [
            date.fromisoformat(self.row[3]),  # Date
            symbol,
            self._parse_number(self.row[5]),  # Amount
            0,  # Tax withholding
            self.row[2],  # Currency
            symbol,
        ]

        return True

    def _process_withholding_tax(self):
        if self.row[0] != "Withholding Tax":
            return

        if self._check_withholding_tax_header():
            # Skip after header processing
            return

        self._process_withholding_tax_row()

    def _check_withholding_tax_header(self):
        if self.row[1] == "Header":
            expected_headers = ["Withholding Tax", "Header", "Currency", "Date", "Description", "Amount", "Code"]
            if not self.row == expected_headers:
                self._wrong_header("Withholding Tax")

            return True

        return False

    def _process_withholding_tax_row(self):
        if self.row[2].startswith("Total"):
            return False

        # Withholding @ 20% on Credit Interest for May-2023 -> ZWD Credit Interest for May-2023
        description = re.sub(r"Withholding @ \d+% on", self.row[2], self.row[4])
        amount = -self._parse_number(self.row[5])  # Amount
        self._withholding_tax[description] = amount

    def _process_interest(self):
        if self.row[0] != "Interest":
            return

        if self._check_interest_header():
            # Skip after header processing
            return

        if self._process_interest_row():
            self.processed_dividends += 1

    def _check_interest_header(self):
        if self.row[1] == "Header":
            expected_headers = ["Interest", "Header", "Currency", "Date", "Description", "Amount"]
            if not self.row == expected_headers:
                self._wrong_header("Interest")

            return True

        return False

    def _process_interest_row(self):
        if self.row[2].startswith("Total"):
            return False

        self.df_dividends.loc[len(self.df_dividends.index)] = [
            date.fromisoformat(self.row[3]),  # Date
            self.row[2],  # Currency
            self._parse_number(self.row[5]),  # Amount
            self._withholding_tax[self.row[4]],  # Tax withholding
            self.row[2],  # Currency
            f"{self.row[2]} interest",  # Product
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

            df.loc[df["Product"] == symbol, "Product"] = product
