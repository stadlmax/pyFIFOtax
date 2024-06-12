from datetime import datetime
from dataclasses import dataclass, asdict
import pandas as pd

from typing import Optional


@dataclass
class DataFrameRow:
    def to_dict(self):
        return asdict(self)

    @staticmethod
    def from_schwab_json(json_dict):
        raise NotImplementedError

    @staticmethod
    def empty():
        raise NotImplementedError

    @staticmethod
    def from_df_row(row):
        raise NotImplementedError


@dataclass
class ESPPRow(DataFrameRow):
    date: datetime
    symbol: str
    buy_price: pd.Float64Dtype
    fair_market_value: pd.Float64Dtype
    quantity: pd.Float64Dtype
    currency: str

    @staticmethod
    def from_schwab_json(json_dict):
        symbol = json_dict["Symbol"]
        quantity = pd.to_numeric(json_dict["Quantity"])
        if not len(json_dict["TransactionDetails"]) == 1:
            raise RuntimeError(
                "Could not convert ESPP information from Schwab JSON, expected TransactionDetails to be of length 1."
            )
        details = json_dict["TransactionDetails"][0]["Details"]
        date = datetime.strptime(details["PurchaseDate"], "%m/%d/%Y")
        buy_price = pd.to_numeric(details["PurchasePrice"].strip("$").replace(",", ""))
        fair_market_value = pd.to_numeric(
            details["PurchaseFairMarketValue"].strip("$").replace(",", "")
        )
        return ESPPRow(
            date,
            symbol,
            buy_price,
            fair_market_value,
            quantity,
            "USD",
        )

    @staticmethod
    def empty():
        return ESPPRow(None, None, None, None, None, None)

    @staticmethod
    def from_df_row(row):
        return ESPPRow(
            row.date,
            row.symbol,
            row.buy_price,
            row.fair_market_value,
            row.quantity,
            row.currency,
        )


@dataclass
class RSURow(DataFrameRow):
    date: datetime
    symbol: str
    gross_quantity: Optional[pd.Float64Dtype]
    net_quantity: pd.Float64Dtype
    fair_market_value: pd.Float64Dtype
    currency: str

    @staticmethod
    def from_schwab_lapse_json(json_dict):
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y")
        symbol = json_dict["Symbol"]
        gross_quantity = pd.to_numeric(json_dict["Quantity"])

        if not len(json_dict["TransactionDetails"]) == 1:
            raise RuntimeError(
                "Could not convert RSU information from Schwab JSON, expected TransactionDetails to be of length 1."
            )
        details = json_dict["TransactionDetails"][0]["Details"]
        fair_market_value = pd.to_numeric(
            details["FairMarketValuePrice"].strip("$").replace(",", "")
        )

        net_quantity = pd.to_numeric(details["NetSharesDeposited"])

        award_id = details["AwardId"]

        return (
            RSURow(
                date,
                symbol,
                gross_quantity,
                net_quantity,
                fair_market_value,
                "USD",
            ),
            award_id,
        )

    @staticmethod
    def from_schwab_deposit_json(json_dict):
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y")
        symbol = json_dict["Symbol"]
        net_quantity = pd.to_numeric(json_dict["Quantity"])
        # to be set later
        gross_quantity = None

        if not len(json_dict["TransactionDetails"]) == 1:
            raise RuntimeError(
                "Could not convert RSU information from Schwab JSON, expected TransactionDetails to be of length 1."
            )
        details = json_dict["TransactionDetails"][0]["Details"]
        fair_market_value = pd.to_numeric(
            details["VestFairMarketValue"].strip("$").replace(",", "")
        )

        award_id = details["AwardId"]

        return (
            RSURow(
                date,
                symbol,
                gross_quantity,
                net_quantity,
                fair_market_value,
                "USD",
            ),
            award_id,
        )

    @staticmethod
    def empty():
        return ESPPRow(None, None, None, None, None, None)

    @staticmethod
    def from_df_row(row):
        return RSURow(
            row.date,
            row.symbol,
            row.gross_quantity,
            row.net_quantity,
            row.fair_market_value,
            row.currency,
        )


@dataclass
class DividendRow(DataFrameRow):
    date: datetime
    symbol: str
    amount: pd.Float64Dtype
    tax_withholding: pd.Float64Dtype
    currency: str

    @staticmethod
    def from_schwab_json(json_dict):
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y")
        symbol = json_dict["Symbol"]
        amount = pd.to_numeric(json_dict["Amount"].strip("$").replace(",", ""))

        return DividendRow(
            date,
            symbol,
            amount,
            pd.to_numeric(0),
            "USD",
        )

    @staticmethod
    def empty():
        return DividendRow(None, None, None, None, None)

    @staticmethod
    def from_df_row(row):
        return DividendRow(
            row.date, row.symbol, row.amount, row.tax_withholding, row.currency
        )


@dataclass
class TaxWithholdingRow(DataFrameRow):
    date: datetime
    symbol: str
    amount: pd.Float64Dtype
    currency: str

    @staticmethod
    def from_schwab_json(json_dict):
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y")
        symbol = json_dict["Symbol"]
        amount = pd.to_numeric(json_dict["Amount"].strip("-$").replace(",", ""))

        return TaxWithholdingRow(
            date,
            symbol,
            amount,
            "USD",
        )


@dataclass
class SellOrderRow(DataFrameRow):
    date: datetime
    symbol: str
    quantity: pd.Float64Dtype
    sell_price: pd.Float64Dtype
    fees: pd.Float64Dtype
    currency: str

    @staticmethod
    def from_schwab_json(json_dict):
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y")
        symbol = json_dict["Symbol"]
        fees = pd.to_numeric(
            json_dict["FeesAndCommissions"].strip("-$").replace(",", "")
        )
        quantity = pd.to_numeric(json_dict["Quantity"])
        details = json_dict["TransactionDetails"]

        sale_price = details[0]["Details"]["SalePrice"]
        check_quantity = pd.to_numeric(0)
        for det in details:
            if not det["Details"]["SalePrice"] == sale_price:
                raise ValueError(
                    "Unexpected behavior when converting SellOrder data from Schwab JSON, check [TransactionDetails][i][Details][SalePrices] for inconsistent values"
                )
            check_quantity += pd.to_numeric(det["Details"]["Shares"])
        if not check_quantity == quantity:
            raise ValueError(
                "Unexpected behavior when converting SellOrder data from Schwab JSON, check [TransactionDetails][i][Details][Shares] for inconsistent values"
            )

        sale_price = pd.to_numeric(sale_price.strip("$").replace(",", ""))

        return SellOrderRow(
            date,
            symbol,
            quantity,
            sale_price,
            fees,
            "USD",
        )

    @staticmethod
    def empty():
        return SellOrderRow(None, None, None, None, None, None)

    @staticmethod
    def from_df_row(row):
        return SellOrderRow(
            row.date, row.symbol, row.quantity, row.sell_price, row.fees, row.currency
        )


@dataclass
class BuyOrderRow(DataFrameRow):
    date: str
    symbol: str
    quantity: pd.Float64Dtype
    buy_price: pd.Float64Dtype
    fees: pd.Float64Dtype
    currency: str

    @staticmethod
    def empty():
        return BuyOrderRow(None, None, None, None, None, None)

    @staticmethod
    def from_df_row(row):
        return BuyOrderRow(
            row.date, row.symbol, row.quantity, row.buy_price, row.fees, row.currency
        )


@dataclass
class CurrencyConversionRow(DataFrameRow):
    date: datetime
    foreign_amount: pd.Float64Dtype
    source_fees: pd.Float64Dtype
    source_currency: str
    target_currency: str

    @staticmethod
    def from_schwab_json(json_dict):
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y")
        fees = pd.to_numeric(
            json_dict["FeesAndCommissions"].strip("-$").replace(",", "")
        )
        foreign_amount = pd.to_numeric(json_dict["Amount"].strip("-$").replace(",", ""))
        return CurrencyConversionRow(
            date,
            foreign_amount,
            fees,
            "USD",
            "EUR",
        )

    @staticmethod
    def empty():
        return CurrencyConversionRow(None, None, None, None, None)

    @staticmethod
    def from_df_row(row):
        return CurrencyConversionRow(
            row.date,
            row.foreign_amount,
            row.source_fees,
            row.source_currency,
            row.target_currency,
        )


@dataclass
class StockSplitRow(DataFrameRow):
    date: datetime
    symbol: str
    shares_after_split: pd.Float64Dtype

    @staticmethod
    def from_df_row(row):
        return StockSplitRow(
            date=row.date,
            symbol=row.symbol,
            shares_after_split=row.shares_after_split,
        )
