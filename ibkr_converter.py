from datetime import date

from converter import Converter


class IbkrConverter(Converter):
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
            expected_headers = ["Trades", "Header", "DataDiscriminator", "Asset Category", "Currency", "Symbol",
                                "Date/Time", "Exchange", "Quantity", "T. Price", "C. Price", "Proceeds", "Comm/Fee",
                                "Basis", "Realized P/L", "MTM P/L", "Code"]

            if self.row != expected_headers:
                raise ValueError("IBKR CSV is not in the expected format. Either this script needs adaption or "
                                 "a wrong type of CSV was downloaded. Trade header is incorrect")

            return True

        return False

    def _process_trade_row(self):
        if self.row[2] != "Order":
            return False

        quantity = self._parse_number(self.row[8])
        df = self.df_deposits if quantity > 0 else self.df_sales
        df.loc[len(df.index)] = [
            date.fromisoformat(self.row[6].split(',')[0]),  # Date
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
            expected_headers = ["Trades", "Header", "DataDiscriminator", "Asset Category", "Currency", "Symbol",
                                "Date/Time", "Exchange", "Quantity", "T. Price"]

            if self.row != expected_headers:
                raise ValueError("IBKR CSV is not in the expected format. Either this script needs adaption or "
                                 "a wrong type of CSV was downloaded. Forex header is incorrect")

            return True

        return False

    def _process_forex_row(self):
        if self.row[2] != "Order" or self.row[4] == "EUR":  # pyFIFOtax spreadsheet supports only conversion into EUR
            return False

        self.df_forex_to_eur.loc[len(self.df_forex_to_eur.index)] = [
            date.fromisoformat(self.row[6].split(',')[0]),  # Date
            abs(self._parse_number(self.row[11])),  # Proceeds
            abs(self._parse_number(self.row[12]) * self._parse_number(self.row[9])),  # Comm in EUR * T. Price
            self.row[4],  # Symbol
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
            wrong_headers = expected_headers = ["Dividends", "Header", "Currency", "Date", "Description", "Amount"]
            if self.row == wrong_headers:
                # There are two "Dividends" sections in the CSV file, duplicating the same information. Skip the first
                self.skip_dividend_section = True

                return True

            expected_headers += ["Code"]
            if not self.row == expected_headers:
                raise ValueError("IBKR CSV is not in the expected format. Either this script needs adaption or "
                                 "a wrong type of CSV was downloaded. Dividends header is incorrect")

            self.skip_dividend_section = False
            return True

        return False

    def _process_dividend_row(self):
        if self.skip_dividend_section or self.row[2] == "Total":
            return False

        symbol = self.row[4].split(' ')[0]
        self.df_dividends.loc[len(self.df_dividends.index)] = [
            date.fromisoformat(self.row[3]),  # Date
            symbol,
            self._parse_number(self.row[5]),  # Amount
            0,  # Tax withholding
            self.row[2],  # Currency
            symbol,
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
            expected_headers = [["Financial Instrument Information", "Header", "Asset Category", "Symbol",
                                 "Description", "Conid", "Security ID", "Listing Exch", "Multiplier", "Code"]]
            expected_headers.append(expected_headers[0].copy())
            expected_headers[1].insert(-1, "Type")

            if self.row not in expected_headers:
                raise ValueError("IBKR CSV is not in the expected format. Either this script needs adaption or "
                                 "a wrong type of CSV was downloaded. Financial instrument information header "
                                 "is incorrect")
            return True

        return False

    def _process_instrument_information_row(self):
        symbol = self.row[3]
        isin = self.row[6]
        product = self.row[4]

        for df in [self.df_deposits, self.df_sales, self.df_dividends]:
            if self.isin_replace:
                df.loc[df["symbol"] == symbol, "symbol"] = isin

            df.loc[df["Product"] == symbol, "Product"] = product
