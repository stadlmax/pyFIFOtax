import os
import warnings
import pandas as pd

import pyfifotax.data_structures_awv as awv
from pyfifotax.data_structures_event import (
    ReportEvent,
    RSUEvent,
    ESPPEvent,
    BuyEvent,
    SellEvent,
    CurrencyConversionEvent,
    DividendEvent,
    StockSplitEvent,
)

from pyfifotax.data_structures_fifo import (
    FIFOForex,
    FIFOQueue,
)
from pyfifotax.utils import apply_rates_forex_dict, filter_forex_dict, forex_dict_to_df
from pyfifotax.utils import (
    apply_rates_transact_dict,
    filter_transact_dict,
    transact_dict_to_df,
)
from pyfifotax.utils import (
    get_reference_rates,
    read_data,
    read_data_legacy,
    write_report,
    write_report_awv,
)
from pyfifotax.utils import to_decimal


class ReportData:
    def __init__(
        self,
        sub_dir: str,
        file_name: str,
        apply_stock_splits: bool = True,
    ):
        self.legacy_mode = False

        # sub_dir and file_name for the raw data
        self.sub_dir = sub_dir
        self.file_name = file_name

        # save flag to apply stock splits and remember path to file defining splits
        self.apply_stock_splits = apply_stock_splits
        self.stock_split_file_path = os.path.join(sub_dir, "stock_splits.xlsx")
        if apply_stock_splits and not os.path.exists(self.stock_split_file_path):
            raise FileNotFoundError(
                f"{self.stock_split_file_path} not found but set apply_stock_splits to True, abort."
            )

        # list of unsold shares, a sell order will move a share from this list
        # to the list of sold shares based on FIFO requirements
        # (which is trivial as shares is assumed to be ordered)
        # correctly based on its construction, a sell order will also
        # update the underlying asset object with the corresponding "sell_price"
        self.held_shares = {}
        self.sold_shares = {}

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
        self.misc = {"Fees": [], "Dividend Payments": [], "Tax Withholding": []}

        # (ticker) symbols and currencies in the report
        self.symbols = None
        self.currencies = None

        self.daily_rates = None
        self.monthly_rates = None
        # list of supported currencies - as in we have available exchange rate data
        self.supported_currencies = None

        # keep a list of Z4 / Z10 reporting events (only supporting symbol AWV at the moment)
        self.awv_z4_events: list[awv.AWVEntryZ4] = []
        self.awv_z10_events: list[awv.AWVEntryZ10] = []

        # later fill list of all events and sort according to date
        # traversing the list from earliest date to latest then allows
        # for gradually building FIFO-Queues
        self.report_events: list[ReportEvent] = []

        # read in currency information
        (
            self.daily_rates,
            self.monthly_rates,
            self.supported_currencies,
        ) = get_reference_rates()

        # read in raw data and initialize buffers based on these
        self.read_raw_data()
        # sort all events in ascending order based on their date
        self.report_events.sort(key=lambda event: (event.date, event.priority))

        # process report events generated from loading raw data
        self.process_report_events()

        # apply rates to awv events
        for e in self.awv_z4_events:
            e.apply_daily_rate(self.daily_rates)
        for e in self.awv_z10_events:
            e.apply_daily_rate(self.daily_rates)

    def read_raw_data(self):
        try:
            raw_data = read_data(self.sub_dir, self.file_name)
        except ValueError:
            self.legacy_mode = True
            msg = "Loading ReportData from legacy data layouts, some functionality might not be supported."
            msg += " See example/transactions.xlsx for the new report layout."
            msg += " Changed behavior in particular around deposits of shares. The new format distinguishes "
            msg += " between ESPP, RSU, and Buy Orders and thus has a different notion of price and quantity "
            msg += " compared to the previous net_quantity and fmw_or_buy_price. For backward compatibility, "
            msg += " an old 'deposit' with 'fee != 0' is assumed to be a buy order and 'fee = 0' is seen as"
            msg += " a RSU deposit. This should make the behavior of the Tax Reporting identical. If this "
            msg += " does not fit your needs, please adopt the new format of transactions as shown in the example."
            warnings.warn(msg, DeprecationWarning)
            raw_data = read_data_legacy(self.sub_dir, self.file_name)

        if self.apply_stock_splits:
            with pd.ExcelFile(self.stock_split_file_path) as xls:
                df_stock_splits = pd.read_excel(xls, parse_dates=["date"])
            raw_data.stock_splits = df_stock_splits

        used_symbols = []
        used_symbols.extend(list(raw_data.rsu.symbol.unique()))
        if not self.legacy_mode:
            used_symbols.extend(list(raw_data.espp.symbol.unique()))
        used_symbols.extend(list(raw_data.dividends.symbol.unique()))
        used_symbols.extend(list(raw_data.buy_orders.symbol.unique()))
        used_symbols.extend(list(raw_data.sell_orders.symbol.unique()))
        if raw_data.stock_splits is not None:
            used_symbols.extend(list(raw_data.stock_splits.symbol.unique()))
        used_symbols = set(used_symbols)

        used_currencies = []
        used_currencies.extend(list(raw_data.rsu.currency.unique()))
        if not self.legacy_mode:
            used_currencies.extend(list(raw_data.espp.currency.unique()))
        used_currencies.extend(list(raw_data.dividends.currency.unique()))
        used_currencies.extend(list(raw_data.buy_orders.currency.unique()))
        used_currencies.extend(list(raw_data.sell_orders.currency.unique()))
        used_currencies.extend(
            list(raw_data.currency_conversions.source_currency.unique())
        )
        used_currencies.extend(
            list(raw_data.currency_conversions.target_currency.unique())
        )
        used_currencies.append("EUR")
        used_currencies = set(used_currencies)

        unsupported_currencies = []
        for c in used_currencies:
            if c not in self.supported_currencies:
                unsupported_currencies.append(c)

        if unsupported_currencies:
            raise ValueError(
                f"Currencies {unsupported_currencies} are not supported as exchange rate data is missing.\n"
                f"Supported currencies are: {sorted(self.supported_currencies)}"
            )

        self.held_shares = {s: FIFOQueue() for s in used_symbols}
        self.sold_shares = {s: [] for s in used_symbols}
        self.held_forex = {c: FIFOQueue() for c in used_currencies}
        self.sold_forex = {c: [] for c in used_currencies}

        # first, just create all events from raw data
        self.report_events.extend(RSUEvent.from_report(raw_data.rsu))
        self.report_events.extend(DividendEvent.from_report(raw_data.dividends))
        self.report_events.extend(BuyEvent.from_report(raw_data.buy_orders))
        self.report_events.extend(SellEvent.from_report(raw_data.sell_orders))
        self.report_events.extend(
            CurrencyConversionEvent.from_report(raw_data.currency_conversions)
        )
        if raw_data.stock_splits is not None:
            self.report_events.extend(
                StockSplitEvent.from_report(raw_data.stock_splits)
            )
        if not self.legacy_mode:
            self.report_events.extend(ESPPEvent.from_report(raw_data.espp))

    def process_report_events(self):
        for event in self.report_events:
            if isinstance(event, RSUEvent):
                self.held_shares[event.symbol].push(event.received_shares)

                bonus = awv.AWVEntryZ4RSUBonus(
                    date=event.date,
                    symbol=event.symbol,
                    currency=event.received_shares.currency,
                    value=event.received_shares.total_buy_value()
                    + event.withheld_shares.total_buy_value(),
                )
                bought_shares = awv.AWVEntryZ10RSUDeposit(
                    date=event.date,
                    symbol=event.symbol,
                    currency=event.received_shares.currency,
                    quantity=event.received_shares.quantity,
                    value=event.received_shares.total_buy_value()
                    + event.withheld_shares.total_buy_value(),
                )
                withheld_shares = awv.AWVEntryZ10RSUTaxWithholding(
                    date=event.date,
                    symbol=event.symbol,
                    currency=event.received_shares.currency,
                    quantity=event.withheld_shares.quantity,
                    value=event.received_shares.total_buy_value(),
                )
                self.awv_z4_events.append(bonus)
                self.awv_z10_events.append(bought_shares)
                if event.withheld_shares.quantity > 0:
                    self.awv_z10_events.append(withheld_shares)

            elif isinstance(event, ESPPEvent):
                self.held_shares[event.symbol].push(event.received_shares)

                bonus = awv.AWVEntryZ4ESPPBonus(
                    symbol=event.symbol,
                    currency=event.currency,
                    date=event.date,
                    value=event.bonus,
                )

                bought_shares = awv.AWVEntryZ10ESPPDeposit(
                    date=event.date,
                    symbol=event.symbol,
                    currency=event.currency,
                    quantity=event.received_shares.quantity,
                    value=event.received_shares.total_buy_value(),
                )

                self.awv_z4_events.append(bonus)
                self.awv_z10_events.append(bought_shares)

            elif isinstance(event, DividendEvent):
                self.held_forex[event.currency].push(event.received_net_dividend)

                if event.withheld_tax is not None:
                    self.misc["Tax Withholding"].append(event.withheld_tax)
                self.misc["Dividend Payments"].append(event.received_dividend)

                # dividends should be small enough to not trigger AWV reportings

            elif isinstance(event, BuyEvent):
                # if not enough money, pop on FOREX Queue will fail
                tmp = self.held_forex[event.currency].pop(
                    event.cost_of_shares,
                    to_decimal(1),
                    event.date,
                )
                self.sold_forex[event.currency].extend(tmp)

                self.held_shares[event.symbol].push(event.received_shares)
                if event.paid_fees is not None:
                    self.misc["Fees"].append(event.paid_fees)

                self.awv_z10_events.append(
                    awv.AWVEntryZ10Buy(
                        date=event.date,
                        symbol=event.received_shares.symbol,
                        currency=event.currency,
                        quantity=event.received_shares.quantity,
                        value=event.received_shares.total_buy_value(),
                    )
                )

            elif isinstance(event, SellEvent):
                # if not enough shares to sell, pop on SHARE Queue will fail
                tmp = self.held_shares[event.symbol].pop(
                    event.quantity, event.sell_price, event.date
                )
                self.sold_shares[event.symbol].extend(tmp)

                self.held_forex[event.currency].push(event.received_forex)

                if event.paid_fees is not None:
                    self.misc["Fees"].append(event.paid_fees)

                self.awv_z10_events.append(
                    awv.AWVEntryZ10Sale(
                        date=event.date,
                        symbol=event.symbol,
                        currency=event.currency,
                        quantity=event.quantity,
                        value=event.quantity * event.sell_price,
                    )
                )

            elif isinstance(event, CurrencyConversionEvent):
                if not (
                    event.source_currency == event.target_currency
                    or event.source_currency == "EUR"
                    or event.target_currency == "EUR"
                ):
                    msg = "Only support currency conversions between one foreign and EUR or"
                    msg += " deposits of foreign currencies (same source and target currency)!"
                    msg += f" But got {event.source_currency} and {event.target_currency} respectively."
                    raise ValueError(msg)

                if event.source_currency == event.target_currency:
                    # simply deposit forex
                    if event.target_currency != "EUR":
                        warnings.warn(
                            f"Depositing foreign currency {event.target_currency}, FIFO of future sales is based on deposit date."
                        )
                    new_forex = FIFOForex(
                        currency=event.target_currency,
                        quantity=event.foreign_amount,
                        buy_date=event.date,
                        source=f"Deposit of {event.target_currency}",
                    )
                    self.held_forex[event.target_currency].push(new_forex)

                elif event.source_currency == "EUR":
                    # "buy" forex
                    new_forex = FIFOForex(
                        currency=event.target_currency,
                        quantity=event.foreign_amount,
                        buy_date=event.date,
                        source=f"Currency Conversion EUR to {event.target_currency}",
                    )
                    self.held_forex[event.target_currency].push(new_forex)
                    if event.source_fees is not None:
                        self.misc["Fees"].append(event.source_fees)

                else:
                    # "sell" forex
                    tmp = self.held_forex[event.source_currency].pop(
                        event.foreign_amount,
                        to_decimal(1),
                        event.date,
                    )
                    self.sold_forex[event.source_currency].extend(tmp)
                    if event.source_fees is not None:
                        self.misc["Fees"].append(event.source_fees)

            elif isinstance(event, StockSplitEvent):
                if self.apply_stock_splits:
                    self.held_shares[event.symbol].apply_split(event.shares_after_split)

            else:
                raise RuntimeError("Unexpected Code Path reached.")

    def apply_exchange_rates(self):
        apply_rates_forex_dict(self.misc, self.daily_rates, self.monthly_rates)
        apply_rates_transact_dict(
            self.sold_shares,
            self.daily_rates,
            self.monthly_rates,
        )
        apply_rates_transact_dict(
            self.sold_forex,
            self.daily_rates,
            self.monthly_rates,
        )

    def consolidate_report(self, report_year, mode):
        if mode.lower() not in ["daily", "monthly_avg"]:
            raise ValueError(
                f"Expected exchange rate mode to be in (daily, monthly_avg), got {mode}."
            )
        self.apply_exchange_rates()

        # for fees, taxes, dividends: only filter for date in report_year
        misc_filtered = filter_forex_dict(self.misc, report_year)

        # for sold_shares and sold_forex: filter for sell-date in report_year
        # for sold_forex: also filter out entries where duration between buy and sell date
        # is more than 1 year (Spekulationsfrist, Privates Veräußerungsgeschäft)
        filtered_sold_shares = filter_transact_dict(self.sold_shares, report_year)
        filtered_sold_forex = filter_transact_dict(
            self.sold_forex, report_year, speculative_period=1
        )

        df_misc = forex_dict_to_df(misc_filtered, mode)
        df_shares = transact_dict_to_df(filtered_sold_shares, mode)
        df_forex = transact_dict_to_df(filtered_sold_forex, mode)
        df_forex = df_forex.drop(["Buy Price", "Sell Price"], axis="columns")

        res = (
            df_shares,
            df_forex,
            df_misc[df_misc["Symbol"] == "Dividend Payments"],
            df_misc[df_misc["Symbol"] == "Fees"],
            df_misc[df_misc["Symbol"] == "Tax Withholding"],
        )

        return res

    def consolidate_awv_events(self, report_year, awv_threshold_eur=12_500):
        if self.legacy_mode:
            msg = "Can't create valid AWV report in legacy mode."
            msg += " This is mainly due to the new format introducing more information, e.g."
            msg += " the difference between buy_price and fair_market_value for ESPP which allows"
            msg += " to automatically calculate your own contribution and the company bonus."
            msg += " For AWV reports, please switch to the new format."
            warnings.warn(msg)
            return None, None

        for e in self.awv_z4_events:
            e.set_threshold(awv_threshold_eur)
        for e in self.awv_z10_events:
            e.set_threshold(awv_threshold_eur)

        filtered_z4_events = [
            e.as_dict() for e in self.awv_z4_events if e.date.year == report_year
        ]
        filtered_z4_events = [e for e in filtered_z4_events if e is not None]

        filtered_z10_events = [
            e.as_dict() for e in self.awv_z10_events if e.date.year == report_year
        ]
        filtered_z10_events = [e for e in filtered_z10_events if e is not None]

        df_z4 = pd.DataFrame(filtered_z4_events)
        df_z10 = pd.DataFrame(filtered_z10_events)

        return df_z4, df_z10

    def create_excel_report_awv(
        self, report_year, report_file_name, awv_threshold_eur=12_500
    ):
        if self.legacy_mode:
            msg = "Can't create valid AWV report in legacy mode."
            msg += " This is mainly due to the new format introducing more information, e.g."
            msg += " the difference between buy_price and fair_market_value for ESPP which allows"
            msg += " to automatically calculate your own contribution and the company bonus."
            msg += " For AWV reports, please switch to the new format."
            warnings.warn(msg)
            return

        df_z4, df_z10 = self.consolidate_awv_events(report_year, awv_threshold_eur)
        write_report_awv(
            df_z4,
            df_z10,
            self.sub_dir,
            report_file_name,
        )

    def create_excel_report(self, report_year, mode, report_file_name):
        dfs = self.consolidate_report(report_year, mode)
        write_report(
            *dfs,
            self.sub_dir,
            report_file_name,
        )
