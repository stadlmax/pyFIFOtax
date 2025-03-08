import os
import datetime
import decimal
import pandas as pd
import yfinance as yf
import requests_cache
from pathlib import Path


def modify_history(hist: pd.DataFrame):
    hist = hist.reset_index()
    hist = hist[["Date", "Low", "High", "Open", "Close"]]
    return hist


def modify_splits(splits: pd.DataFrame, min_date: datetime.date):
    splits[min_date] = 1.0
    splits = splits.sort_index()
    return splits


def modify_history_and_splits(hist: pd.DataFrame, splits: pd.DataFrame):
    hist = modify_history(hist)
    min_date = hist.Date.min()
    splits = modify_splits(splits, min_date)
    return hist, splits


def get_splits_from_ticker(ticker: str):
    cache = requests_cache.CachedSession(
        cache_name=os.path.join(
            f"{Path.home()}", ".cache", "pyfifotax", "yfinance-cache"
        ),
        backend="sqlite",
    )
    stock = yf.Ticker(ticker, session=cache)
    splits = stock.splits
    return splits


def get_history_and_splits_from_ticker(ticker: str):
    cache = requests_cache.CachedSession(
        cache_name=os.path.join(
            f"{Path.home()}", ".cache", "pyfifotax", "yfinance-cache"
        ),
        backend="sqlite",
    )
    stock = yf.Ticker(ticker, session=cache)
    hist = stock.history(period="max")
    splits = stock.splits
    if hist.empty:
        raise ValueError(
            f"Could not download stock prices for {ticker}, update yfinance or try again later."
        )
    return modify_history_and_splits(hist, splits)


def get_reverse_splits(splits: pd.DataFrame):
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


def get_rev_split_from_timestamp(timestamp: pd.Timestamp, rev_splits: pd.DataFrame):
    for start, end in rev_splits.keys():
        if start <= timestamp < end:
            return rev_splits[(start, end)]

    raise ValueError(f"Could not find mapping for {timestamp}")


def adjust_row(row: pd.core.series.Series, rev_splits: pd.DataFrame):
    factor = get_rev_split_from_timestamp(row.Date, rev_splits)
    row.Low = row.Low * factor
    row.High = row.High * factor
    row.Open = row.Open * factor
    row.Close = row.Close * factor
    return row


def adjust_history_for_splits(hist: pd.DataFrame, splits: pd.DataFrame):
    rev_splits = get_reverse_splits(splits)
    return hist.apply(lambda row: adjust_row(row, rev_splits), axis=1)


def get_historic_daily_prices(ticker: str):
    hist, splits = get_history_and_splits_from_ticker(ticker)
    if not splits.empty:
        prices = adjust_history_for_splits(hist, splits)
    else:
        prices = hist
    prices.Date = prices.Date.apply(pd.Timestamp.date)
    prices = prices.set_index("Date")
    return prices


class _HistoricPrices:
    def __init__(self):
        self._prices = {}

    def get_price(self, key: str, date: datetime.date, kind: str = "Close"):
        if not key in self._prices:
            self._prices[key] = get_historic_daily_prices(key)

        found = False
        prices = self._prices[key][kind]

        while not found:
            try:
                price = prices[date]
                found = True
            except KeyError:
                # if exact date not found, find closest previous date
                # e.g. might happen with vestings on days with closed
                # markets or other similar events
                date = date - datetime.timedelta(days=1)

        return price


# global pseudo-singleton for managing state
historic_prices = _HistoricPrices()


def is_price_historic(
    price: decimal.Decimal, symbol: str, date: datetime.date, kind: str = "Close"
):
    hist_price = pd.to_numeric(historic_prices.get_price(symbol, date, kind=kind))

    if (price - hist_price) / hist_price < pd.to_numeric(0.05):
        return True, hist_price
    return False, hist_price
