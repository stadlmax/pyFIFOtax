import datetime
import decimal
import hashlib
import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests_cache
import yfinance as yf

logger = logging.getLogger("pyfifotax")

def get_reverse_splits(splits: pd.Series):
    rev_splits = splits.prod() / splits.cumprod()
    split_map = {}
    max_date = datetime.date(2100, 12, 31)
    numel = len(rev_splits)
    for i in range(numel):
        start = rev_splits.index[i]
        val = rev_splits.iloc[i]
        end = rev_splits.index[i + 1] if i < numel - 1 else max_date
        split_map[(start, end)] = val

    return split_map


def get_rev_split_from_timestamp(date: datetime.date, rev_splits: dict):
    for start, end in rev_splits.keys():
        if start <= date < end:
            return rev_splits[(start, end)]

    raise ValueError(f"Could not find split-mapping for {date}")


def adjust_row(row: pd.Series, rev_splits: dict):
    factor = get_rev_split_from_timestamp(row.name, rev_splits)
    return row["close_price"] * factor


def adjust_history_for_splits(hist_prices: pd.DataFrame, splits: pd.Series):
    rev_splits = get_reverse_splits(splits)
    hist_prices["close_price"] = hist_prices.apply(
        lambda row: adjust_row(row, rev_splits), axis=1
    )
    return hist_prices


class YFinanceCacheManager:
    """
    Custom Cache Manager for YFinance across two levels for encountered tickers and dates
    - first level: history and splits downloaded into machine-readable files
    - second level: keep dataframes around in memory
    """

    def __init__(self):
        self.cache_path = os.path.join(f"{Path.home()}", ".cache", "pyfifotax")
        self.meta_file = os.path.join(self.cache_path, "yfinance_meta.csv")
        self.file_cache = {}

        if os.path.exists(self.meta_file):
            self.cache_manager = pd.read_csv(self.meta_file, index_col=0)
            self.cache_manager.fillna("", inplace=True)
            self.cache_manager["last_update"] = pd.to_datetime(
                self.cache_manager["last_update"]
            ).dt.date
            self.cache_manager = self.cache_manager.to_dict(orient="index")
            for ticker in self.cache_manager:
                for k, v in self.cache_manager[ticker].items():
                    if v == "":
                        self.cache_manager[ticker][k] = None
        else:
            self.cache_manager = {}

    def update(self):
        df = pd.DataFrame(self.cache_manager).T
        df.to_csv(self.meta_file)

    def _get_ticker_hash(self, ticker: str):
        ticker_hash = hashlib.sha256(ticker.encode()).hexdigest()
        return ticker_hash

    def _get_hist_file_path(self, ticker: str):
        ticker_hash = self._get_ticker_hash(ticker)
        return os.path.join(self.cache_path, f"{ticker_hash}_adjusted_history.csv")

    def _get_splits_file_path(self, ticker: str):
        ticker_hash = self._get_ticker_hash(ticker)
        return os.path.join(self.cache_path, f"{ticker_hash}_splits.csv")

    def _get_true_hist_file_path(self, ticker: str):
        ticker_hash = self._get_ticker_hash(ticker)
        return os.path.join(self.cache_path, f"{ticker_hash}_historical_history.csv")

    def _download_ticker_max_period(self, ticker: str):
        # if ticker already there but download necessary nevertheless,
        # delete old files and clear cache entries
        if ticker in self.cache_manager:
            # clear file cache
            self.file_cache[ticker] = None
            # delete old files
            files_to_delete = [
                self._get_hist_file_path(ticker),
                self._get_splits_file_path(ticker),
                self._get_true_hist_file_path(ticker),
            ]
            for file in files_to_delete:
                if os.path.exists(file):
                    os.remove(file)
            # reset cache entry
            self.cache_manager[ticker] = {}
            self.update()

        try:
            logger.debug(f"Retrieving stock split and history information for {ticker}")
            ticker_hist_file = self._get_hist_file_path(ticker)
            ticker_splits_file = self._get_splits_file_path(ticker)
            ticker_true_hist_file = self._get_true_hist_file_path(ticker)

            stock = yf.Ticker(ticker)
            time.sleep(1)
            hist_prices = stock.history(period="max", auto_adjust=True)
            time.sleep(1)
            splits = stock.splits.to_frame()
            time.sleep(1)

            if hist_prices.empty:
                # should not happen, nevertheless seems to happen sometimes
                # without yf throwing a ValueError
                raise ValueError(f"No historical prices found for {ticker}")

            hist_prices = hist_prices[["Close"]]
            hist_prices.rename(
                columns={"Close": "close_price"},
                inplace=True,
            )
            hist_prices.index = pd.to_datetime(hist_prices.index).date
            min_date = hist_prices.index.min()
            hist_prices.to_csv(ticker_hist_file)

            if not splits.empty:
                splits.rename(
                    columns={"Stock Splits": "shares_after_split"}, inplace=True
                )
                splits.index = pd.to_datetime(splits.index).date
                splits.loc[min_date, "shares_after_split"] = 1.0
                splits.sort_index(inplace=True, ascending=True)
                splits.to_csv(ticker_splits_file)
                true_hist_prices = adjust_history_for_splits(hist_prices, splits)
                true_hist_prices.to_csv(ticker_true_hist_file)

            else:
                splits = None
                true_hist_prices = hist_prices

            self.file_cache[ticker] = (hist_prices, splits, true_hist_prices)
            self.cache_manager[ticker] = {
                "ticker_hash": self._get_ticker_hash(ticker),
                "has_hist": True,
                "has_splits": splits is not None,
                "last_update": datetime.datetime.now().date(),
            }
            self.update()

        except ValueError:
            logging.warning(
                f"ISIN not found: {ticker}. Stock splits might not be considered or treated incorrectly."
            )
            self.file_cache[ticker] = (None, None, None)
            self.cache_manager[ticker] = {
                "ticker_hash": self._get_ticker_hash(ticker),
                "has_hist": False,
                "has_splits": False,
                "last_update": datetime.datetime.now().date(),
            }
            self.update()

        except Exception as e:
            raise RuntimeError(f"Unexpected error while downloading {ticker}: {e}")

    def get_ticker_hist_and_split(self, ticker: str, date: datetime.date):
        if not ticker in self.cache_manager:
            # download if not in cache
            self._download_ticker_max_period(ticker)

        cached_ticker = self.cache_manager[ticker]

        if not cached_ticker["has_hist"]:
            hist_prices, splits, true_hist_prices = None, None, None

        else:
            if date < cached_ticker["last_update"]:
                if ticker in self.file_cache:
                    hist_prices, splits, true_hist_prices = self.file_cache[ticker]

                else:
                    hist_file = self._get_hist_file_path(ticker)
                    true_hist_file = self._get_true_hist_file_path(ticker)
                    splits_file = self._get_splits_file_path(ticker)
                    hist_prices = pd.read_csv(hist_file, index_col=0)
                    hist_prices.index = pd.to_datetime(hist_prices.index).date
                    true_hist_prices = pd.read_csv(true_hist_file, index_col=0)
                    true_hist_prices.index = pd.to_datetime(true_hist_prices.index).date
                    if cached_ticker["has_splits"]:
                        splits = pd.read_csv(splits_file, index_col=0)
                        splits.index = pd.to_datetime(splits.index).date
                    else:
                        splits = None

            else:
                # download if last update is before date
                # need to download whole range again as events like splits
                # might have happened since last update
                self._download_ticker_max_period(ticker)
                # after download, dataframes are already in cache
                hist_prices, splits, true_hist_prices = self.file_cache[ticker]

        return hist_prices, splits, true_hist_prices


def get_closest_price_from_date(prices: pd.Series, date: datetime.date):
    found = False
    price = prices.iloc[0]  # mostly for typing / linting
    while not found:
        try:
            price = prices[date]
            found = True
        except KeyError:
            date = date - datetime.timedelta(days=1)
    return price


class HistoricPrices:
    def __init__(self):
        self._yf_cache_manager = YFinanceCacheManager()

    def get_yf_splits(self, ticker: str, date: datetime.date):
        _, splits, _ = self._yf_cache_manager.get_ticker_hist_and_split(ticker, date)
        return splits

    def get_yf_close_price(self, ticker: str, date: datetime.date):
        hist_prices, _, _ = self._yf_cache_manager.get_ticker_hist_and_split(
            ticker, date
        )
        price = get_closest_price_from_date(hist_prices, date)
        return price

    def get_historic_close_price(self, ticker: str, date: datetime.date):
        _, _, true_hist_prices = self._yf_cache_manager.get_ticker_hist_and_split(
            ticker, date
        )
        price = get_closest_price_from_date(true_hist_prices, date)
        return price


# global pseudo-singletons for managing state
yf_cache_manager = YFinanceCacheManager()
historic_prices = HistoricPrices()


def get_splits_for_symbol(symbol: str, date: datetime.date):
    _, splits, _ = yf_cache_manager.get_ticker_hist_and_split(symbol, date)
    return splits


def is_price_historic(price: decimal.Decimal, symbol: str, date: datetime.date):
    hist_price = historic_prices.get_historic_close_price(symbol, date)
    if hist_price is None:
        # if no historic price is available, assume it is historic
        return True, None

    hist_price = pd.to_numeric(hist_price)

    # allow 5% deviation from historic price
    if (price - hist_price) / hist_price < pd.to_numeric(0.05):
        return True, hist_price

    return False, hist_price
