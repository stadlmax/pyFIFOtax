import os
import datetime
import pandas as pd
import yfinance as yf


def modify_history(hist):
    hist = hist.reset_index()
    hist = hist[["Date", "Low", "High", "Open", "Close"]]
    return hist


def modify_splits(splits, min_date):
    splits[min_date] = 1.0
    splits = splits.sort_index()
    return splits


def modify_history_and_splits(hist, splits):
    hist = modify_history(hist)
    min_date = hist.Date.min()
    splits = modify_splits(splits, min_date)
    return hist, splits


def get_history_and_splits_from_ticker(ticker):
    stock = yf.Ticker(ticker)
    hist = stock.history(period="max")
    splits = stock.splits
    return modify_history_and_splits(hist, splits)


def get_reverse_splits(splits):
    rev_splits = splits.prod() / splits.cumprod()
    map = {}
    max_date = pd.Timestamp(year=2100, month=12, day=31, tz="America/New_York")
    numel = len(rev_splits)
    for i in range(numel):
        start = rev_splits.index[i]
        val = rev_splits.iloc[i]
        end = rev_splits.index[i + 1] if i < numel - 1 else max_date
        map[(start, end)] = val

    return map


def get_rev_split_from_timestamp(timestamp, rev_splits):
    for start, end in rev_splits.keys():
        if start <= timestamp < end:
            return rev_splits[(start, end)]

    raise ValueError(f"Could not find mapping for {timestamp}")


def adjust_row(row, rev_splits):
    factor = get_rev_split_from_timestamp(row.Date, rev_splits)
    row.Low = row.Low * factor
    row.High = row.High * factor
    row.Open = row.Open * factor
    row.Close = row.Close * factor
    return row


def adjust_history_for_splits(hist, splits):
    rev_splits = get_reverse_splits(splits)
    return hist.apply(lambda row: adjust_row(row, rev_splits), axis=1)


def get_historic_daily_prices(ticker: str):
    mod_date = None
    today = datetime.date.today()

    file_name = f"{ticker.lower()}-historic-daily-prices.csv"
    if os.path.exists(file_name):
        mod_time = os.path.getmtime(file_name)
        mod_date = datetime.datetime.fromtimestamp(mod_time).date()

    if mod_date != today:
        print(f"Downloading more recent stock data for ticker-symbol {ticker} ...")
        hist, splits = get_history_and_splits_from_ticker(ticker)
        hist_adj = adjust_history_for_splits(hist, splits)
        hist_adj.Date = hist_adj.Date.apply(pd.Timestamp.date)
        hist_adj = hist_adj.set_index("Date")
        hist_adj.to_csv(file_name)

    prices = pd.read_csv(file_name)
    return prices


class _HistoricPrices:
    def __init__(self):
        self._prices = {}

    def __getitem__(self, key):
        if isinstance(key, str):
            # key is ticker, will error out if not exists
            if not key in self._prices:
                self._prices[key] = get_historic_daily_prices(key)

            # return whole history of values
            return self._prices[key]

        if isinstance(key, tuple) and len(key) == 2:
            ticker, date = key
            return self._prices[ticker][date]

        raise KeyError(
            f"Unsupported key ({key})!. Expected either 'str' or a tuple of 'str' and 'date'"
        )


# global pseudo-singleton for managing state
historic_prices = _HistoricPrices()


def is_price_historic(price, symbol, date, kind="Close"):
    hist_price = pd.to_numeric(historic_prices[(symbol, date)][kind])

    if (price - hist_price) / hist_price < pd.to_humeric(0.01):
        return True, hist_price
    return False, hist_price
