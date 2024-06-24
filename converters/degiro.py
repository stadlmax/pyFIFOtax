import sys
from datetime import datetime

from .csv_converter import CSVConverter


class DeGiroConverter(CSVConverter):
    def __init__(self, args):
        if not args.degiro_account_csv:
            print("Specify the Account.csv file with --degiro-account-csv", file=sys.stderr)
            exit(1)

        super().__init__(args, {args.input_filename: "utf-8", args.degiro_account_csv: "utf-8"})

        self._forex_in_progress = None

    def _skip_account_row(self):
        return (
            self._current_file != 2 or  # It's not the Account.csv file
            self.row[0] not in ["Datum", "Date"] and (  # Permit header check
                not self.row[0] or  # Datum is empty
                not self.row[7] or  # Änderung is empty
                # Order-ID is filled (processed in the other spreadsheet), except for currency conversions
                (self.row[11] and not self.row[5].startswith("Währungswechsel"))
            )
        )

    def _get_row_date(self, column: int):
        return datetime.strptime(self.row[column], "%d-%m-%Y").date()

    def _process_trades(self):
        if self._current_file != 1:
            return

        if self._check_trades_header():
            # Skip after header processing
            return

        if self._process_trade_row():
            self.processed_trades += 1

    def _check_trades_header(self):
        condition = self.row[0] in ["Datum", "Date"]
        expected_headers = [
            "Datum",
            "Uhrzeit",
            "Produkt",
            "ISIN",
            "Referenzbörse",
            "Ausführungsort",
            "Anzahl", "Kurs",
            "",
            "Wert in Lokalwährung",
            "",
            "Wert",
            "",
            "Wechselkurs",
            "Transaktionsgebühren",
            "",
            "Gesamt",
            "",
            "Order-ID",
        ]
        return self._check_header(condition, expected_headers, "Transactions")

    def _process_trade_row(self):
        # Sometimes the "Product" continues in a second row, which doesn't contain a date
        if not self.row[0]:
            return False

        quantity = self._parse_number(self.row[6])  # Anzahl
        fee_eur = -self._parse_number(self.row[14]) if self.row[14] else 0  # Transaktionsgebühren
        exchange_rate = self._parse_number(self.row[13]) if self.row[13] else 0  # Wechselkurs
        fee = fee_eur if self.row[15] == "EUR" else fee_eur * exchange_rate

        df = self.df_deposits if quantity > 0 else self.df_sales
        df.loc[len(df.index)] = [
            self._get_row_date(0),  # Datum
            self.row[3],  # ISIN
            abs(quantity),  # Anzahl
            self._parse_number(self.row[7]),  # Kurs
            fee,  # Transaktionsgebühren
            self.row[8],  # (Currency)
            self.row[2],  # Produkt
        ]

        return True

    def _process_forex(self):
        if self._skip_account_row() or not self.row[5].startswith("Währungswechsel"):
            return

        if self._process_forex_row():
            self.processed_forex += 1

    def _process_forex_row(self):
        currency = self.row[7]
        amount = self._parse_number(self.row[8])

        if self._forex_in_progress is None:
            self._forex_in_progress = [
                self._get_row_date(2),  # Valutadatum
                abs(amount),  # Änderung
                0,  # Commission
                currency,  # Symbol - Source currency
                currency,  # Currency - Target currency
            ]

            return False
        else:
            if amount < 0:
                # Override the source amount to be correct if it wasn't already.
                # Only the negative (conversion from) amount should be reflected at the end
                self._forex_in_progress[1] = -amount
                # Similar to source currency
                self._forex_in_progress[3] = currency
            else:
                self._forex_in_progress[4] = currency

            self.df_forex.loc[len(self.df_forex.index)] = self._forex_in_progress
            self._forex_in_progress = None
            return True

    def _process_deposits_withdrawals(self):
        if self._skip_account_row() or self.row[5] not in ["flatex Einzahlung", "Einzahlung", "Interne Einzahlung"]:
            return

        if self._process_dw_row():
            self.processed_forex += 1

    def _process_dw_row(self):
        amount = self._parse_number(self.row[8])  # Änderung
        if amount <= 0:
            # Don't care about disbursements
            return False

        self.df_forex.loc[len(self.df_forex.index)] = [
            self._get_row_date(2),  # Valutadatum
            amount,  # Proceeds
            0,  # Fees
            self.row[7],  # Symbol
            self.row[7],  # Symbol
        ]

        return True

    def _process_dividends(self):
        if self._skip_account_row() or self.row[5] != "Dividende":
            return

        if self._process_dividend_row():
            self.processed_dividends += 1

    def _process_dividend_row(self):
        self.df_dividends.loc[len(self.df_dividends.index)] = [
            self._get_row_date(2),  # Valutadatum
            self.row[4],  # ISIN
            self._parse_number(self.row[8]),  # Änderung
            0.0,  # Tax withholding
            self.row[7],  # Currency
            self.row[3],  # Produkt
        ]

        return True

    def _process_withholding_tax(self):
        if self._skip_account_row() or self.row[5] != "Dividendensteuer":
            return

        self._process_withholding_tax_row()

    def _process_withholding_tax_row(self):
        self.df_dividends.iloc[-1, 3] = -self._parse_number(self.row[8])

    def _process_interest(self):
        if self._skip_account_row() or not self.row[5].startswith("Flatex Interest"):
            return

        if self._process_interest_row():
            self.processed_dividends += 1

    def _process_interest_row(self):
        amount = self._parse_number(self.row[8])  # Änderung
        if amount == 0.0:
            return False

        self.df_dividends.loc[len(self.df_dividends.index)] = [
            self._get_row_date(2),  # Valutadatum
            self.row[7],  # Currency
            amount,
            0,  # Tax withholding
            self.row[7],  # Currency
            f"{self.row[7]} interest",  # Product
        ]

        return True

    def _process_instrument_information(self):
        pass
