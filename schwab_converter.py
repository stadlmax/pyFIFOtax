from datetime import datetime
from types import SimpleNamespace

from converter import Converter


class SchwabConverter(Converter):
    def __init__(self, args):
        super().__init__(args)

        self.header_checked = False
        self.trade_in_progress = SimpleNamespace(**{
            'row': [],
            'type': [],
            'commission_per_share': 0.0,
        })
        self.dividend_in_progress = SimpleNamespace(**{
             'row': [],
             'withholding': 0.0,
         })

    def _parse_usd(self, string: str):
        return self._parse_number(string.replace('$', ''))

    def _process_trades(self):
        self._check_header()

        if self.row[1] != "":
            self.trade_in_progress.row = []

        if self.row[1] not in ["Deposit", "Sale"] and not self.trade_in_progress.row:
            return

        if self._process_trade_row():
            self.processed_trades += 1

    def _check_header(self):
        if self.header_checked:
            return

        expected_headers = ["Date", "Action", "Symbol", "Description", "Quantity", "FeesAndCommissions",
                            "DisbursementElection", "Amount", "Type", "Shares", "PurchaseDate", "PurchasePrice",
                            "PurchaseFairMarketValue", "SubscriptionDate", "SubscriptionFairMarketValue",
                            "DispositionType", "VestDate", "VestFairMarketValue", "GrantId", "AwardDate", "AwardId",
                            "FairMarketValuePrice", "SalePrice", "SharesSoldWithheldForTaxes", "NetSharesDeposited",
                            "Taxes", "GrossProceeds"]

        if self.row != expected_headers:
            raise ValueError("Schwab CSV is not in the expected format. Either this script needs adaption or "
                             "a wrong type of CSV was downloaded.")

        self.header_checked = True

    def _process_trade_row(self):
        if self.row[1] in ["Deposit", "Sale"]:
            date = datetime.strptime(self.row[0], "%m/%d/%Y").date()  # Date
            symbol = self.row[2]  # Symbol
            commission = self._parse_usd('0' if not self.row[5] else self.row[5])  # FeesAndCommissions
            quantity = self._parse_number(self.row[4])  # Quantity

            self.trade_in_progress.type = [self.row[1], self.row[3]]
            self.trade_in_progress.row = [
                date,
                symbol,
                quantity,
                None,
                commission,
                'USD',
                symbol,
            ]

            if self.row[1] == "Sale":
                self.trade_in_progress.commission_per_share = commission / quantity

            return False
        else:
            if self.trade_in_progress.type[0] == "Deposit":
                df = self.df_deposits
                if self.trade_in_progress.type[1] == "RS":  # RSU and ESPP FMV columns are different
                    transaction_price = self._parse_usd(self.row[17])
                else:
                    transaction_price = self._parse_usd(self.row[12])
            else:
                df = self.df_sales
                transaction_price = self._parse_usd(self.row[22])  # SalePrice
                quantity = self._parse_number(self.row[9])
                self.trade_in_progress.row[2] = quantity  # Itemised share quantity
                self.trade_in_progress.row[4] = quantity * self.trade_in_progress.commission_per_share  # Commission

            self.trade_in_progress.row[3] = transaction_price
            df.loc[len(df.index)] = self.trade_in_progress.row

            return True

    def _process_forex(self):
        pass

    def _process_dividends(self):
        if self.row[1] not in ["Dividend", "Tax Withholding"]:
            self.dividend_in_progress.row = []
            self.dividend_in_progress.withholding = 0.0
            return

        if self._process_dividend_row():
            self.processed_dividends += 1

    def _process_dividend_row(self):
        if self.row[1] == "Dividend":
            self.dividend_in_progress.row = [
                datetime.strptime(self.row[0], "%m/%d/%Y").date(),  # Date
                self.row[2],  # Symbol
                self._parse_usd(self.row[7]),  # Amount
                None,
                'USD',
                self.row[2],  # Symbol
            ]
        else:
            self.dividend_in_progress.withholding = abs(self._parse_usd(self.row[7]))  # Amount

        # It's unpredictable whether "Dividend" or "Tax Withholding" comes first in the CSV
        if self.dividend_in_progress.row and self.dividend_in_progress.withholding > 0:
            self.dividend_in_progress.row[3] = self.dividend_in_progress.withholding
            self.df_dividends.loc[len(self.df_dividends.index)] = self.dividend_in_progress.row

            return True

        return False

    def _process_instrument_information(self):
        pass
