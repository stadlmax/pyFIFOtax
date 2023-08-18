from data_structures import Forex, FIFOShare, FIFOForex, FIFOQueue
from utils import get_reference_rates, read_data, write_report
from utils import apply_rates_forex_dict, filter_forex_dict, forex_dict_to_df
from utils import apply_rates_transact_dict, filter_transact_dict, transact_dict_to_df


class ReportData:
    def __init__(self, sub_dir: str, file_name: str):
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

        # list of forgein currencies: dividend payments and sell orders
        self.held_forex = {}
        self.sold_forex = {}
        # Besides maintaining a list of ingoing and outgoing streams
        # of "foreign" currencies, we keep separate lists
        # - fees (pot. Werbungskosten),
        # - taxes (i.e. "Quellensteuer" or withheld taxes on dividends),
        # - and dividend payments themselvs.
        # Those are relevant for the report year and don't have to
        # follow a FIFO principle. Besides e.g. gains on dividends,
        # one - in addition - also might have to consider gains/losses from
        # holding forgein currencies, which is covered by the "forex" above.
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
        self.df_wire_transfers = None
        self.daily_rates = None
        self.monthly_rates = None
        # list of supported currencies - as in we have available exchange rate data
        self.supported_currencies = None

        # finally: read in raw data and initialize buffers based on these
        self.read_raw_data()

    def read_raw_data(self):
        (
            self.daily_rates,
            self.monthly_rates,
            self.supported_currencies,
        ) = get_reference_rates()

        (
            self.df_deposits,
            self.df_sales,
            self.df_dividends,
            self.df_wire_transfers,
        ) = read_data(self.sub_dir, self.file_name)
        currencies = self.df_deposits.currency.unique().tolist()
        symbols = self.df_deposits.symbol.unique().tolist()

        unsupported_currencies = []
        for c in currencies:
            if c not in self.supported_currencies:
                unsupported_currencies.append(c)

        if unsupported_currencies:
            raise ValueError(
                f"Currencies {unsupported_currencies} are not supported as exchange rate data is missing, check 'supported currencies' for automated reports."
            )

        self._init_data_dicts(symbols, currencies)
        self.process_fifo_data()

    def _init_data_dicts(self, symbols, currencies):
        self.held_shares = {s: FIFOQueue() for s in symbols}
        self.sold_shares = {s: [] for s in symbols}
        self.held_forex = {c: FIFOQueue() for c in currencies}
        self.sold_forex = {c: [] for c in currencies}

        self.fees = {c: [] for c in currencies}
        self.taxes = {s: [] for s in symbols}
        self.dividends = {s: [] for s in symbols}

    def process_fifo_data(self):
        # process data in the sequence deposits - dividends - sales - wire_transfers
        # this should ensure a valid FIFO sequence in both the shares and the foreign currencies
        self.process_deposits(self.df_deposits)
        self.process_dividends(self.df_dividends)
        self.process_sales(self.df_sales)
        self.process_wire_transfers(self.df_wire_transfers)

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
        # for sold_forex: also suppress rows with quantity less than 0.01
        # for sold_forex: also filter out entries where duration between buy and sell date
        # is more than 1 year (Spekulationsfrist, Privates Veräußerungsgeschäft)
        filtered_sold_shares = filter_transact_dict(self.sold_shares, report_year, 0)
        filtered_sold_forex = filter_transact_dict(
            self.sold_forex, report_year, 0.01, speculative_period=1
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

    def process_deposits(self, df_deposits):
        # deposits of shares are simple, as df is assumed to be sorted
        # just build list of stocks (unit of 1 as smallest unit)
        for row_idx, row in df_deposits.iterrows():
            symbol, new_shares = FIFOShare.from_deposits_row(row)
            self.held_shares[symbol].push(new_shares)

    def process_dividends(self, df_dividends):
        for row_idx, row in df_dividends.iterrows():
            currency, new_forex = FIFOForex.from_dividends_row(row)
            symbol, new_div, new_tax = Forex.from_dividends_row(row)
            self.dividends[symbol].append(new_div)
            self.taxes[symbol].append(new_tax)
            self.held_forex[currency].push(new_forex)

    def process_sales(self, df_sales):
        # sales of shares are more complicated
        # - move shares from "held_shares" to "sold_shares"
        # - track "fee of sale" in "fees"
        # - track net proceeds in held_forex
        for _, row in df_sales.iterrows():
            sold_quantity = row.quantity
            sold_symbol = row.symbol
            tmp = self.held_shares[sold_symbol].pop(sold_quantity)
            for t in tmp:
                t.sell_date = row.date
                t.sell_price = row.sell_price
                assert (
                    row.currency == t.currency
                ), f"Currency for buying share and selling share not the same, got {t.currency} and {row.currency} respectively"
            self.sold_shares[sold_symbol].extend(tmp)

            # technically: the fees for selling shares are small enough to neglect them
            # for completeness, we add them to "fees" which will mainly comprise fees for wire transfers
            # technically: one should also separate those as these fees here might just might be used
            # to compute the "Kapitalertrag"
            new_fees = Forex(
                currency=row.currency,
                date=row.date,
                amount=row.fees,
                comment="Fees on Sale of Shares",
            )
            self.fees[row.currency].append(new_fees)
            currency, new_forex = FIFOForex.from_share_sale(row)
            self.held_forex[row.currency].push(new_forex)

    def process_wire_transfers(self, df_wire_transfers):
        # when doing a wire transfer, you sell
        # the USD you possess in the equivalent amount of EUR
        # this includes the fee you pay for the transfer
        for row_idx, row in df_wire_transfers.iterrows():
            sold_quantity = row.net_amount + row.fees
            sold_currency = row.currency
            tmp = self.held_forex[sold_currency].pop(sold_quantity)
            for t in tmp:
                t.sell_date = row.date
                t.sell_price = 1  # currency unit
            self.sold_forex[sold_currency].extend(tmp)

            new_fees = Forex(
                currency=row.currency,
                date=row.date,
                amount=row.fees,
                comment="Fee on Wire Transfer",
            )
            self.fees[row.currency].append(new_fees)
