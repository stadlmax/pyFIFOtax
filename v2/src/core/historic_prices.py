"""
Historic price management for pyFIFOtax
Handles downloading, caching, and adjusting stock prices and splits from Yahoo Finance
Also handles ECB exchange rates for currency conversion
"""

import os
import datetime
import hashlib
import logging
import time
import requests
import zipfile
import io
import numpy as np
from pathlib import Path
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any, Union
import pandas as pd
import yfinance as yf
import streamlit as st


def get_reverse_splits(
    splits: pd.Series,
) -> Dict[Tuple[datetime.date, datetime.date], float]:
    """Calculate reverse split factors for historical price adjustment"""
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


def get_rev_split_from_timestamp(date: datetime.date, rev_splits: Dict) -> float:
    """Get the reverse split factor for a specific date"""
    for (start, end), factor in rev_splits.items():
        if start <= date < end:
            return factor

    raise ValueError(f"Could not find split-mapping for {date}")


def adjust_history_for_splits(
    hist_prices: pd.DataFrame, splits: pd.Series
) -> pd.DataFrame:
    """Adjust historical prices for stock splits"""
    rev_splits = get_reverse_splits(splits)
    adjusted_prices = hist_prices.copy()

    def adjust_row(row):
        factor = get_rev_split_from_timestamp(row.name, rev_splits)
        return row["close_price"] * factor

    adjusted_prices["close_price"] = adjusted_prices.apply(adjust_row, axis=1)
    return adjusted_prices


def get_closest_price_from_date(
    prices: pd.Series, date: datetime.date
) -> Optional[float]:
    """Get the closest available price for a given date (going backwards if needed)"""
    if prices is None or prices.empty:
        return None

    found = False
    price = None
    search_date = date
    max_lookback = 30  # Limit lookback to 30 days
    days_searched = 0

    while not found and days_searched < max_lookback:
        try:
            price = prices[search_date]
            found = True
        except KeyError:
            search_date = search_date - datetime.timedelta(days=1)
            days_searched += 1

    return price if found else None


def to_decimal(number: Union[float, np.float64, None, Decimal]) -> Decimal:
    """Convert number to Decimal safely"""
    if number is None:
        return Decimal("0")
    if isinstance(number, Decimal):
        return number
    return Decimal(str(number))


class HistoricPriceManager:
    """
    Modern cache manager for Yahoo Finance data and ECB exchange rates with Streamlit integration
    Handles downloading, caching, and providing historic prices, splits, and exchange rates
    """

    def __init__(self, cache_dir: Optional[str] = None):
        self.cache_path = cache_dir or os.path.join(Path.home(), ".cache", "pyfifotax")
        self.meta_file = os.path.join(self.cache_path, "yfinance_meta.csv")
        self.ecb_cache_file = os.path.join(self.cache_path, "eurofxref-hist.csv")
        self.file_cache = {}

        # Exchange rate data
        self.daily_rates = None
        self.monthly_rates = None
        self.supported_currencies = None

        # Ensure cache directory exists
        os.makedirs(self.cache_path, exist_ok=True)

        # Load existing cache metadata
        self._load_cache_metadata()

        # Load exchange rates
        self._load_exchange_rates()

    def _load_cache_metadata(self):
        """Load cache metadata from CSV file"""
        if os.path.exists(self.meta_file):
            try:
                df = pd.read_csv(self.meta_file, index_col=0)
                df.fillna("", inplace=True)
                df["last_update"] = pd.to_datetime(df["last_update"]).dt.date
                self.cache_manager = df.to_dict(orient="index")

                # Clean up empty values
                for ticker in self.cache_manager:
                    for k, v in self.cache_manager[ticker].items():
                        if v == "":
                            self.cache_manager[ticker][k] = None
            except Exception as e:
                logging.warning(f"Error loading cache metadata: {e}")
                self.cache_manager = {}
        else:
            self.cache_manager = {}

    def _save_cache_metadata(self):
        """Save cache metadata to CSV file"""
        try:
            df = pd.DataFrame(self.cache_manager).T
            df.to_csv(self.meta_file)
        except Exception as e:
            logging.warning(f"Error saving cache metadata: {e}")

    def _load_exchange_rates(self):
        """Load ECB exchange rates, downloading if necessary"""
        try:
            # Check if we need to download fresh rates
            mod_date = None
            today = datetime.date.today()

            if os.path.exists(self.ecb_cache_file):
                mod_time = os.path.getmtime(self.ecb_cache_file)
                mod_date = datetime.datetime.fromtimestamp(mod_time).date()

            # Download fresh rates if cache is old
            if mod_date != today:
                self._download_ecb_rates()

            # Load rates from cache
            daily_ex_rates = pd.read_csv(self.ecb_cache_file, parse_dates=["Date"])
            daily_ex_rates = daily_ex_rates.loc[
                :, ~daily_ex_rates.columns.str.contains("^Unnamed")
            ]

            daily_ex_rates.index = daily_ex_rates["Date"]
            # Drop years earlier than 2009 for simplicity
            daily_ex_rates = daily_ex_rates.loc[daily_ex_rates.index.year >= 2009]
            # Drop columns with NaN values
            daily_ex_rates = daily_ex_rates.dropna(axis="columns")

            daily_ex_rates = daily_ex_rates.drop("Date", axis="columns")
            monthly_ex_rates = daily_ex_rates.groupby(
                by=[daily_ex_rates.index.year, daily_ex_rates.index.month]
            ).mean()

            supported_currencies = set(daily_ex_rates.columns)
            supported_currencies.add("EUR")

            # Convert to lookup dictionaries
            self.daily_rates = {
                cur: {
                    date.date(): daily_ex_rates[cur][date]
                    for date in daily_ex_rates[cur].index
                }
                for cur in supported_currencies
                if cur != "EUR"
            }

            self.monthly_rates = {
                cur: {
                    (year, month): monthly_ex_rates[cur][(year, month)]
                    for year, month in monthly_ex_rates[cur].index
                }
                for cur in supported_currencies
                if cur != "EUR"
            }

            self.supported_currencies = supported_currencies

        except Exception as e:
            logging.warning(f"Error loading exchange rates: {e}")
            self.daily_rates = {}
            self.monthly_rates = {}
            self.supported_currencies = {"EUR"}

    def _download_ecb_rates(self):
        """Download fresh ECB exchange rates"""
        try:
            print("Downloading more recent exchange rate data...")

            response = requests.get(
                "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip?69474b1034fa15ae36103fbf3554d272"
            )

            if response.status_code != 200:
                raise RuntimeError(
                    f"Failed to download exchange rate data (status code {response.status_code})"
                )

            zip_file = zipfile.ZipFile(io.BytesIO(response.content))
            zip_file.extractall(self.cache_path)

        except Exception as e:
            logging.warning(f"Failed to download ECB rates: {e}")
            # Continue with potentially stale data if available

    def get_exchange_rate(
        self, currency: str, date: datetime.date, mode: str = "daily"
    ) -> Optional[Decimal]:
        """Get exchange rate for currency on given date"""
        if currency == "EUR":
            return Decimal("1")

        if self.daily_rates is None or self.monthly_rates is None:
            return None

        try:
            if mode == "daily":
                return self._get_daily_rate(currency, date)
            elif mode == "monthly":
                return self._get_monthly_rate(currency, date)
            else:
                raise ValueError(f"Invalid mode: {mode}. Must be 'daily' or 'monthly'")
        except (KeyError, ValueError) as e:
            logging.warning(
                f"Could not get {mode} exchange rate for {currency} on {date}: {e}"
            )
            return None

    def _get_daily_rate(self, currency: str, date: datetime.date) -> Decimal:
        """Get daily exchange rate with fallback logic"""
        if currency not in self.daily_rates:
            raise ValueError(f"Currency {currency} not supported")

        currency_rates = self.daily_rates[currency]

        # Try exact date first
        if date in currency_rates:
            return to_decimal(currency_rates[date])

        # On currency settlement holidays, look forward up to 7 days
        for day_increase in range(1, 8):
            lookup_date = date + datetime.timedelta(days=day_increase)
            if lookup_date in currency_rates:
                return to_decimal(currency_rates[lookup_date])

        raise ValueError(
            f"{currency} exchange rate not found for {date} or following 7 days"
        )

    def _get_monthly_rate(self, currency: str, date: datetime.date) -> Decimal:
        """Get monthly average exchange rate"""
        if currency not in self.monthly_rates:
            raise ValueError(f"Currency {currency} not supported")

        month_key = (date.year, date.month)
        if month_key not in self.monthly_rates[currency]:
            raise ValueError(
                f"No monthly rate for {currency} in {date.year}-{date.month}"
            )

        return to_decimal(self.monthly_rates[currency][month_key])

    def _get_ticker_hash(self, ticker: str) -> str:
        """Generate hash for ticker symbol for file naming"""
        return hashlib.sha256(ticker.encode()).hexdigest()

    def _get_file_paths(self, ticker: str) -> Tuple[str, str, str]:
        """Get file paths for ticker data files"""
        ticker_hash = self._get_ticker_hash(ticker)
        base_path = self.cache_path

        hist_file = os.path.join(base_path, f"{ticker_hash}_adjusted_history.csv")
        splits_file = os.path.join(base_path, f"{ticker_hash}_splits.csv")
        true_hist_file = os.path.join(
            base_path, f"{ticker_hash}_historical_history.csv"
        )

        return hist_file, splits_file, true_hist_file

    def _download_ticker_data(
        self, ticker: str
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """Download historical data and splits for a ticker"""
        print(f"Downloading price data for {ticker}...")

        # Clear existing cache for this ticker
        if ticker in self.cache_manager:
            self._clear_ticker_cache(ticker)

        try:
            hist_file, splits_file, true_hist_file = self._get_file_paths(ticker)

            # Download data from Yahoo Finance
            stock = yf.Ticker(ticker)
            time.sleep(1)  # Rate limiting

            hist_prices = stock.history(period="max", auto_adjust=True)
            time.sleep(1)

            splits = stock.splits.to_frame()
            time.sleep(1)

            if hist_prices.empty:
                raise ValueError(f"No historical prices found for {ticker}")

            # Process historical prices
            hist_prices = hist_prices[["Close"]].copy()
            hist_prices.rename(columns={"Close": "close_price"}, inplace=True)
            hist_prices.index = pd.to_datetime(hist_prices.index).date
            min_date = hist_prices.index.min()
            hist_prices.to_csv(hist_file)

            # Process splits
            if not splits.empty:
                splits.rename(
                    columns={"Stock Splits": "shares_after_split"}, inplace=True
                )
                splits.index = pd.to_datetime(splits.index).date
                splits.loc[min_date, "shares_after_split"] = 1.0
                splits.sort_index(inplace=True, ascending=True)
                splits.to_csv(splits_file)

                # Calculate true historical prices (split-adjusted)
                true_hist_prices = adjust_history_for_splits(
                    hist_prices, splits["shares_after_split"]
                )
                true_hist_prices.to_csv(true_hist_file)
            else:
                splits = None
                true_hist_prices = hist_prices.copy()
                true_hist_prices.to_csv(true_hist_file)

            # Cache in memory
            self.file_cache[ticker] = (hist_prices, splits, true_hist_prices)

            # Update metadata
            self.cache_manager[ticker] = {
                "ticker_hash": self._get_ticker_hash(ticker),
                "has_hist": True,
                "has_splits": splits is not None,
                "last_update": datetime.datetime.now().date(),
            }
            self._save_cache_metadata()

            return hist_prices, splits, true_hist_prices

        except ValueError as e:
            logging.warning(f"ISIN not found: {ticker}. {e}")
            self.file_cache[ticker] = (None, None, None)
            self.cache_manager[ticker] = {
                "ticker_hash": self._get_ticker_hash(ticker),
                "has_hist": False,
                "has_splits": False,
                "last_update": datetime.datetime.now().date(),
            }
            self._save_cache_metadata()
            return None, None, None

        except Exception as e:
            raise RuntimeError(f"Unexpected error while downloading {ticker}: {e}")

    def _clear_ticker_cache(self, ticker: str):
        """Clear cached data for a ticker"""
        # Clear memory cache
        if ticker in self.file_cache:
            del self.file_cache[ticker]

        # Delete cached files
        hist_file, splits_file, true_hist_file = self._get_file_paths(ticker)
        for file_path in [hist_file, splits_file, true_hist_file]:
            if os.path.exists(file_path):
                os.remove(file_path)

    def get_ticker_data(
        self, ticker: str, date: datetime.date
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """Get ticker data from cache or download if needed"""
        if ticker not in self.cache_manager:
            return self._download_ticker_data(ticker)

        cached_ticker = self.cache_manager[ticker]

        if not cached_ticker["has_hist"]:
            return None, None, None

        # Check if we need fresh data
        if date > cached_ticker["last_update"]:
            return self._download_ticker_data(ticker)

        # Load from cache
        if ticker in self.file_cache:
            return self.file_cache[ticker]

        # Load from disk
        try:
            hist_file, splits_file, true_hist_file = self._get_file_paths(ticker)

            hist_prices = pd.read_csv(hist_file, index_col=0)
            hist_prices.index = pd.to_datetime(hist_prices.index).date

            true_hist_prices = pd.read_csv(true_hist_file, index_col=0)
            true_hist_prices.index = pd.to_datetime(true_hist_prices.index).date

            splits = None
            if cached_ticker["has_splits"] and os.path.exists(splits_file):
                splits = pd.read_csv(splits_file, index_col=0)
                splits.index = pd.to_datetime(splits.index).date

            # Cache in memory
            self.file_cache[ticker] = (hist_prices, splits, true_hist_prices)
            return hist_prices, splits, true_hist_prices

        except Exception as e:
            logging.warning(f"Error loading cached data for {ticker}: {e}")
            return self._download_ticker_data(ticker)

    def get_historic_price(self, ticker: str, date: datetime.date) -> Optional[Decimal]:
        """Get historic price for ticker on specific date"""
        _, _, true_hist_prices = self.get_ticker_data(ticker, date)
        if true_hist_prices is None or true_hist_prices.empty:
            return None

        price = get_closest_price_from_date(true_hist_prices["close_price"], date)
        return Decimal(str(price)) if price is not None else None

    def get_splits(self, ticker: str, date: datetime.date) -> Optional[pd.DataFrame]:
        """Get splits data for ticker"""
        _, splits, _ = self.get_ticker_data(ticker, date)
        return splits

    def is_price_historic(
        self, price: Decimal, ticker: str, date: datetime.date
    ) -> Tuple[bool, Optional[Decimal]]:
        """
        Check if a price matches the historic (split-adjusted) price for a ticker on a given date.

        Returns:
            (True, hist_price) if price is already historic/split-adjusted (within 5% tolerance)
            (False, hist_price) if price doesn't match historic price
            (True, None) if no historic price is available
        """
        # Get the actual historic price for this ticker on this date
        hist_price = self.get_historic_price(ticker, date)

        if hist_price is None:
            # If no historic price is available, assume it is historic
            return True, None

        # Allow 5% deviation from historic price
        tolerance = Decimal("0.05")
        if abs(price - hist_price) / hist_price < tolerance:
            return True, hist_price

        return False, hist_price

    def get_latest_market_price(self, ticker: str) -> Optional[Decimal]:
        """Get the latest available market price for ticker"""
        _, _, true_hist_prices = self.get_ticker_data(ticker, datetime.date.today())
        if true_hist_prices is None or true_hist_prices.empty:
            return None

        # Get the most recent price from cached data
        latest_date = max(true_hist_prices.index)
        latest_price = true_hist_prices.loc[latest_date, "close_price"]
        return Decimal(str(latest_price))

    def get_cumulative_split_factor_after_date(
        self, ticker: str, after_date: datetime.date
    ) -> float:
        """Get cumulative split factor for all splits after given date"""
        _, splits, _ = self.get_ticker_data(ticker, datetime.date.today())
        if splits is None or splits.empty:
            return 1.0

        # Get splits after the specified date
        future_splits = splits[splits.index > after_date]
        if future_splits.empty:
            return 1.0

        # Calculate cumulative factor
        return float(future_splits["shares_after_split"].prod())


# Global instance for the application
historic_price_manager = HistoricPriceManager()
