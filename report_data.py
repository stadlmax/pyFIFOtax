import os
import numpy as np
import pandas as pd
from collections import defaultdict

from data_structures import Forex, FIFOShare, FIFOForex, FIFOQueue, StockSplit
from utils import apply_rates_forex_dict, filter_forex_dict, forex_dict_to_df
from utils import apply_rates_transact_dict, filter_transact_dict, transact_dict_to_df
from utils import get_reference_rates, read_data, write_report
from utils import to_decimal


class ReportData:
    def __init__(
        self,
        sub_dir: str,
        file_name: str,
        apply_stock_splits: bool = True,
    ):
        self.apply_stock_splits = apply_stock_splits

        self.stock_split_file_path = os.path.join(sub_dir, "stock_splits.csv")

        # sub_dir and file_name for the raw data
        self.sub_dir = sub_dir
        self.file_name = file_name

        # list of unsold shares, a sell order will move a share from this list
        # to the list of sold shares based on FIFO requirements
        # (which is trivial as shares is assumed to be ordered)
        # correctly based on its construction, a sell order will also
        # update the underlying asset object with the corresponding "sell_price"
        self.held_shares = {}
        self.sold_shares = {}

        # for each share symbol, keep track of a queue of stocksplits
        # when processing transactions, "pop" splits when applicable
        # and modify quantity and price of stocks in queue of shares
        self.stock_splits = defaultdict(list)

        # list of foreign currencies: dividend payments and sell orders
        self.held_forex = {}
        self.sold_forex = {}
        # Besides maintaining a list of ingoing and outgoing streams
        # of "foreign" currencies, we keep separate lists
        # - fees (pot. Werbungskosten),
        # - taxes (i.e. "Quellensteuer" or withheld taxes on dividends),
        # - and dividend payments themselves.
        # Those are relevant for the report year and don't have to
        # follow a FIFO principle. Besides e.g. gains on dividends,
        # one - in addition - also might have to consider gains/losses from
        # holding foreign currencies, which is covered by the "forex" above.
        self.fees = {}
        self.taxes = {}
        self.dividends = {}

        # (ticker) symbols and currencies in the report
        self.symbols = None
        self.currencies = None

        # data-frames for raw data
        self.df_deposits = None
        self.df_sales = None
        self.df_dividends = None
        self.df_forex_to_eur = None
        self.daily_rates = None
        self.monthly_rates = None
        # list of supported currencies - as in we have available exchange rate data
        self.supported_currencies = None

        # finally: read in raw data and initialize buffers based on these
        self.read_raw_data()

    def read_raw_data(self):
        if self.apply_stock_splits:
            df_stock_splits = pd.read_csv(
                self.stock_split_file_path, parse_dates=["date"]
            )
            df_stock_splits = df_stock_splits.sort_values(by="date", ascending=True)

            for _, row in df_stock_splits.iterrows():
                sym = row.symbol
                split = StockSplit.from_split_csv_row(row)
                self.stock_splits[sym].append(split)

        (
            self.daily_rates,
            self.monthly_rates,
            self.supported_currencies,
        ) = get_reference_rates()

        (
            self.df_deposits,
            self.df_sales,
            self.df_dividends,
            self.df_forex_to_eur,
        ) = read_data(self.sub_dir, self.file_name)
        currencies = self.df_deposits.currency.unique()
        symbols = self.df_deposits.symbol.unique()

        extra_currencies = pd.concat(
            [
                self.df_sales.currency,
                self.df_dividends.currency,
                self.df_forex_to_eur.currency,
            ]
        ).unique()
        extra_currencies = np.setdiff1d(extra_currencies, currencies).tolist()
        currencies = currencies.tolist()
        if len(extra_currencies) > 0:
            raise ValueError(
                "Sales, dividends, or currency conversions contain additional currencies which are not present in buy transactions. "
                "Most likely this indicates an error.\n"
                f"Extra currencies: {extra_currencies}"
            )

        extra_symbols = pd.concat(
            [self.df_sales.symbol, self.df_dividends.symbol]
        ).unique()
        extra_symbols = np.setdiff1d(extra_symbols, symbols).tolist()
        symbols = symbols.tolist()
        if len(extra_symbols) > 0:
            raise ValueError(
                "Sales or dividends contain additional symbols which are not present in buy transactions. "
                "Most likely this indicates an error.\n"
                f"Extra symbols: {extra_symbols}"
            )

        unsupported_currencies = []
        for c in currencies:
            if c not in self.supported_currencies:
                unsupported_currencies.append(c)

        if unsupported_currencies:
            raise ValueError(
                f"Currencies {unsupported_currencies} are not supported as exchange rate data is missing.\n"
                f"Supported currencies are: {sorted(self.supported_currencies)}"
            )

        self._init_data_dicts(symbols, currencies)
        self.process_fifo_data()

    def _init_data_dicts(self, symbols, currencies):
        self.held_shares = {s: FIFOQueue() for s in symbols}
        self.sold_shares = {s: [] for s in symbols}
        self.held_forex = {c: FIFOQueue() for c in currencies}
        self.sold_forex = {c: [] for c in currencies}

        self.fees = {f: [] for f in symbols + currencies}
        self.taxes = {s: [] for s in symbols}
        self.dividends = {s: [] for s in symbols}

    def process_fifo_data(self):
        # process data in the sequence deposits - dividends - sales - currency conversion to EUR
        # this should ensure a valid FIFO sequence in both the shares and the foreign currencies
        self.process_deposits(self.df_deposits)
        self.process_dividends(self.df_dividends)
        self.process_sales(self.df_sales)
        self.process_forex_to_eur(self.df_forex_to_eur)

    def apply_exchange_rates(self):
        apply_rates_forex_dict(self.fees, self.daily_rates, self.monthly_rates)
        apply_rates_forex_dict(self.taxes, self.daily_rates, self.monthly_rates)
        apply_rates_forex_dict(self.dividends, self.daily_rates, self.monthly_rates)
        apply_rates_transact_dict(
            self.sold_shares, self.daily_rates, self.monthly_rates
        )
        apply_rates_transact_dict(self.sold_forex, self.daily_rates, self.monthly_rates)

    def consolidate_report(self, report_year, mode):
        assert mode.lower() in ["daily", "monthly_avg"]
        self.apply_exchange_rates()

        # for fees, taxes, dividends: only filter for date in report_year
        fees_filtered = filter_forex_dict(self.fees, report_year)
        taxes_filtered = filter_forex_dict(self.taxes, report_year)
        dividends_filtered = filter_forex_dict(self.dividends, report_year)

        # for sold_shares and sold_forex: filter for sell-date in report_year
        # for sold_forex: also filter out entries where duration between buy and sell date
        # is more than 1 year (Spekulationsfrist, Privates Veräußerungsgeschäft)
        filtered_sold_shares = filter_transact_dict(self.sold_shares, report_year)
        filtered_sold_forex = filter_transact_dict(
            self.sold_forex, report_year, speculative_period=1
        )

        df_fees = forex_dict_to_df(fees_filtered, mode)
        df_taxes = forex_dict_to_df(taxes_filtered, mode)
        df_dividends = forex_dict_to_df(dividends_filtered, mode)
        df_shares = transact_dict_to_df(filtered_sold_shares, mode)
        df_forex = transact_dict_to_df(filtered_sold_forex, mode)
        df_forex = df_forex.drop(["Buy Price", "Sell Price"], axis="columns")

        res = (df_shares, df_forex, df_dividends, df_fees, df_taxes)

        return res

    def create_excel_report(self, report_year, mode, report_file_name):
        df_shares, df_forex, df_dividends, df_fees, df_taxes = self.consolidate_report(
            report_year, mode
        )
        write_report(
            df_shares,
            df_forex,
            df_dividends,
            df_fees,
            df_taxes,
            self.sub_dir,
            report_file_name,
        )

    def add_fees(self, row: pd.Series, comment: str):
        if row.fees < 0:
            raise ValueError(
                f"On {row.date} the fee of {row.fees} {row.currency} is negative"
            )

        if hasattr(row, "fees") and row.fees > 0:
            new_fees = Forex(
                currency=row.currency,
                date=row.date,
                amount=to_decimal(row.fees),
                comment=comment,
            )

            symbol = row.symbol if "symbol" in row else row.currency
            self.fees[symbol].append(new_fees)

    def process_deposits(self, df_deposits):
        # deposits of shares are simple, as df_deposits is assumed to be sorted
        # just build list of stocks (unit of 1 as smallest unit)
        for _, row in df_deposits.iterrows():
            self.add_fees(row, f"Buying {row.symbol}")
            symbol, new_shares = FIFOShare.from_deposits_row(row)

            if symbol in self.held_shares and not self.held_shares[symbol].is_empty():
                if self.held_shares[symbol].assets[-1].currency != row.currency:
                    raise NotImplementedError(
                        f"It is not yet supported to buy the same symbol ('{row.symbol}') in different currencies"
                    )

            self.held_shares[symbol].push(new_shares)

    def process_dividends(self, df_dividends):
        for _, row in df_dividends.iterrows():
            currency, new_forex = FIFOForex.from_dividends_row(row)
            symbol, new_div, new_tax = Forex.from_dividends_row(row)
            self.dividends[symbol].append(new_div)
            if new_tax.amount > 0:
                self.taxes[symbol].append(new_tax)
            self.held_forex[currency].push(new_forex)

    def process_sales(self, df_sales):
        # sales of shares are more complicated
        # - move shares from "held_shares" to "sold_shares"
        # - track "fee of sale" in "fees"
        # - track net proceeds in held_forex
        for row_idx, row in df_sales.iterrows():
            sold_quantity = to_decimal(row.quantity)
            sold_symbol = row.symbol

            if sold_quantity < 0:
                raise ValueError(
                    f"In 'sales' tab, row number {row_idx + 2} for symbol '{sold_symbol}' the quantity is negative"
                )

            # continously check whether stock splits are applicable
            stock_splits_applicable = True
            while stock_splits_applicable:
                if len(self.stock_splits[sold_symbol]) == 0:
                    stock_splits_applicable = False
                elif row.date <= self.stock_splits[sold_symbol][0].date:
                    # sell orders before any split don't need to consider stock splits
                    stock_splits_applicable = False
                elif self.held_shares[sold_symbol].is_empty():
                    # will raise an error later
                    stock_splits_applicable = False
                elif (
                    self.stock_splits[sold_symbol][0].date
                    < self.held_shares[sold_symbol].peek().buy_date
                ):
                    # pop splits which are older than any held share anyways
                    self.stock_splits[sold_symbol].pop(0)
                else:
                    # apply splits for all held shares which have been bought
                    # after the split, until date of sold share is before
                    # next potential split
                    split = self.stock_splits[sold_symbol].pop(0)
                    self.held_shares[sold_symbol].apply_split(split)

            tmp = self.held_shares[sold_symbol].pop(sold_quantity, row.date)
            for t in tmp:
                t.sell_date = row.date
                t.sell_price = to_decimal(row.sell_price)
                assert row.currency == t.currency, (
                    f"Currencies for buying and selling a share are not the same. Got {t.currency} and {row.currency}, "
                    f"respectively.\nSymbol: {t.symbol}, Buy date: {t.buy_date}, Sell date: {t.sell_date}"
                )
            self.sold_shares[sold_symbol].extend(tmp)

            # technically: the fees for selling shares are small enough to neglect them
            # for completeness, we add them to "fees" which will mainly comprise fees for wire transfers in the currency conversion to EUR sheet
            # technically: one should also separate those as these fees here might just might be used
            # to compute the "Kapitalertrag"
            self.add_fees(row, f"Selling {row.symbol}")

            _, new_forex = FIFOForex.from_share_sale(row)
            self.held_forex[row.currency].push(new_forex)

    def process_forex_to_eur(self, df_forex_to_eur):
        # When doing a currency conversion, you convert the USD you possess into the equivalent amount of EUR.
        # This doesn't include the fee you pay for the transfer, that just vanishes in the original denomination
        for _, row in df_forex_to_eur.iterrows():
            sold_currency = row.currency
            fees = to_decimal(row.fees)
            net_amount = to_decimal(row.net_amount)
            self.held_forex[sold_currency].pop(fees, row.date)  # remove fees
            tmp = self.held_forex[sold_currency].pop(net_amount, row.date)
            for t in tmp:
                t.sell_date = row.date
                t.sell_price = to_decimal(1)  # currency unit
            self.sold_forex[sold_currency].extend(tmp)

            self.add_fees(row, "Currency conversion or wire transfer")
