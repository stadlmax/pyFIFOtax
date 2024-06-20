import math
import re
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
        self._pending_gu = None

    def _skip_account_row(self):
        return (
            self._current_file != 2 or  # It's not the Account.csv file
            self.row[0] not in ["Datum", "Date"] and (  # Permit header check
                not self.row[0] or  # Datum is empty
                # Änderung is empty, except for erroneusly missing "Geldmarktfonds Umwandlung" data
                (not self.row[7] and not self.row[5].startswith("Geldmarktfonds")) or
                # Order-ID is filled (processed in the other spreadsheet), except for currency conversions
                (self.row[11] and not self.row[5].startswith("Währungswechsel"))
            )
        )

    def _get_row_date(self, column: int):
        return datetime.strptime(self.row[column], "%d-%m-%Y").date()

    def _process_trades(self):
        self._process_faulty_gu_row()

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

        day = self._get_row_date(0)
        quantity = self._parse_number(self.row[6])  # Anzahl
        isin = self.row[3]
        fee = -self._parse_number(self.row[14]) if self.row[14] else 0  # Transaktionsgebühren
        currency = self.row[8]
        fee_currency = self.row[15]

        df = self.df_deposits if quantity > 0 else self.df_sales
        df.loc[len(df.index)] = [
            day,  # Datum
            isin,  # ISIN
            abs(quantity),  # Anzahl
            self._parse_number(self.row[7]),  # Kurs
            currency,
            fee,  # Transaktionsgebühren
            fee_currency,
            f"{self.row[2]} [on DEGIRO]",  # Produkt
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
                abs(amount),  # Source amount
                currency,  # Source currency
                abs(amount),  # Target amount
                currency,  # Target currency
                0,
                "",
                "[on DEGIRO]",  # Comment
            ]

            return False
        else:
            if amount > 0:
                # Positive (converted to) amount is the target
                self._forex_in_progress[3] = amount
                self._forex_in_progress[4] = currency
            else:
                # Negative (converted from) amount is the source
                self._forex_in_progress[1] = -amount
                self._forex_in_progress[2] = currency

            self.df_forex.loc[len(self.df_forex.index)] = self._forex_in_progress
            self._forex_in_progress = None
            return True

    def _process_deposits_withdrawals(self):
        desc = self.row[5]
        if (self._skip_account_row() or
            (desc not in ["flatex Einzahlung", "Einzahlung", "Interne Einzahlung",
                          "flatex Auszahlung", "Auszahlung"]
             and not desc.startswith("Einrichtung") and not desc.startswith("Gebühr"))):
            return

        if self._process_dw_row():
            self.processed_transfers += 1

    def _process_dw_row(self):
        settle_date = self._get_row_date(2)  # Valutadatum
        amount = fees = self._parse_number(self.row[8])
        desc = self.row[5]
        currency = fee_currency = self.row[7]
        if (desc.startswith("Einrichtung") or desc.startswith("Gebühr")) and amount < 0:
            amount = 0
            fees = -fees
        else:
            fees = 0
            fee_currency = ""

        self.df_money_transfers.loc[len(self.df_money_transfers.index)] = [
            settle_date,
            settle_date,
            amount,  # Änderung
            currency,  # Symbol
            fees,  # Fees
            fee_currency,
            f"{desc} [on DEGIRO]",  # Comment
        ]

        return True

    def _process_dividends(self):
        if self._skip_account_row() or (self.row[5] != "Dividende" and not self.row[5].startswith("Geldmarktfonds")):
            return

        if self._process_dividend_row():
            self.processed_dividends += 1

    def _process_dividend_row(self):
        processing_pending_gu = False

        if self.row[5].startswith("Geldmarktfonds"):
            comment = re.match(r"\w+ \w+", self.row[5])[0]
        else:
            comment = self.row[3]

        if not self.row[8]:
            # "Geldmarktfonds Umwandlung" data is missing
            comment = f"{comment} (automatically calculated)"
            currency = "EUR"
            amount = self._parse_number(self.row[10])
            processing_pending_gu = True
        else:
            amount = self._parse_number(self.row[8])
            currency = self.row[7]

        dividend_row = [
            self._get_row_date(2),  # Valutadatum
            self.row[4],  # ISIN
            amount,  # Änderung
            0.0,  # Tax withholding
            currency,  # Currency
            f"{comment} [on DEGIRO]",  # Produkt
        ]

        if processing_pending_gu:
            self._pending_gu = dividend_row
            return False

        if math.isclose(amount, 0, abs_tol=1e-8):
            return False

        self.df_dividends.loc[len(self.df_dividends.index)] = dividend_row

        return True

    def _process_faulty_gu_row(self):
        if self._current_file != 2 or self._pending_gu is None or self.row[9] != "EUR":
            return

        current_balance = self._parse_number(self.row[10])
        amount = self._pending_gu[2] - current_balance  # The spreadsheet is ordered by descending date
        self._pending_gu[2] = amount

        if not math.isclose(amount, 0, abs_tol=1e-8):
            self.df_dividends.loc[len(self.df_dividends.index)] = self._pending_gu
            self.processed_dividends += 1

        self._pending_gu = None

    def _process_withholding_tax(self):
        if self._skip_account_row() or self.row[5] != "Dividendensteuer":
            return

        self._process_withholding_tax_row()

    def _process_withholding_tax_row(self):
        amount = self._parse_number(self.row[8])

        if math.isclose(amount, 0, abs_tol=1e-8):
            return

        self.df_dividends.iloc[-1, 3] = -amount

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
            f"{self.row[7]} interest [on DEGIRO]",  # Product
        ]

        return True

    def _process_instrument_information(self):
        pass
