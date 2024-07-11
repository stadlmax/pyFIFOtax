from __future__ import annotations

from datetime import datetime
from dataclasses import dataclass, asdict
import pandas as pd
import numpy as np
from pandas.core.series import Series

from typing import Optional


@dataclass
class DataFrameRow:
    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_schwab_json(json_dict: dict) -> DataFrameRow:
        raise NotImplementedError

    @staticmethod
    def from_df_row(row: Series) -> DataFrameRow:
        raise NotImplementedError

    @staticmethod
    def default_dict() -> dict:
        raise NotImplementedError

    @classmethod
    def type_dict(cls) -> dict:
        return {k: type(v) for k, v in cls.default_dict().items()}

    @classmethod
    def empty_dict(cls) -> dict:
        return {k: None for k in cls.default_dict().keys()}


@dataclass
class ESPPRow(DataFrameRow):
    date: datetime
    symbol: str
    buy_price: np.float64
    fair_market_value: np.float64
    quantity: np.float64
    currency: str
    comment: str

    @staticmethod
    def from_schwab_json(json_dict: dict) -> ESPPRow:
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
            "Automated Schwab Import (JSON)",
        )

    @staticmethod
    def default_dict() -> dict:
        tmp = ESPPRow(
            datetime(1, 1, 1), "", np.float64(0), np.float64(0), np.float64(0), "", ""
        ).to_dict()
        return {k: None for k in tmp.keys()}

    @staticmethod
    def from_df_row(row: Series) -> ESPPRow:
        return ESPPRow(
            row.date,
            row.symbol,
            row.buy_price,
            row.fair_market_value,
            row.quantity,
            row.currency,
            row.comment,
        )


@dataclass
class RSURow(DataFrameRow):
    date: datetime
    symbol: str
    gross_quantity: Optional[np.float64]
    net_quantity: np.float64
    fair_market_value: np.float64
    currency: str
    comment: str

    @staticmethod
    def from_schwab_lapse_json(json_dict: dict) -> tuple[RSURow, int]:
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
                f"Automated Schwab Import (JSON, Award ID {award_id})",
            ),
            award_id,
        )

    @staticmethod
    def from_schwab_deposit_json(json_dict: dict) -> tuple[RSURow, int]:
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
                f"Automated Schwab Import (JSON, Award ID {award_id})",
            ),
            award_id,
        )

    @staticmethod
    def default_dict() -> dict:
        return RSURow(
            datetime(1, 1, 1), "", np.float64(0), np.float64(0), np.float64(0), "", ""
        ).to_dict()

    @staticmethod
    def from_df_row(row: Series) -> RSURow:
        return RSURow(
            row.date,
            row.symbol,
            row.gross_quantity,
            row.net_quantity,
            row.fair_market_value,
            row.currency,
            row.comment,
        )


@dataclass
class DividendRow(DataFrameRow):
    date: datetime
    symbol: str
    amount: np.float64
    tax_withholding: np.float64
    currency: str
    comment: str

    @staticmethod
    def from_schwab_json(json_dict: dict) -> DividendRow:
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y")
        symbol = json_dict["Symbol"]
        amount = pd.to_numeric(json_dict["Amount"].strip("$").replace(",", ""))

        return DividendRow(
            date,
            symbol,
            amount,
            np.float64(0),
            "USD",
            "Automated Schwab Import (JSON)",
        )

    @staticmethod
    def default_dict() -> dict:
        return DividendRow(
            datetime(1, 1, 1), "", np.float64(0), np.float64(0), "", ""
        ).to_dict()

    @staticmethod
    def from_df_row(row: Series) -> DividendRow:
        return DividendRow(
            row.date,
            row.symbol,
            row.amount,
            row.tax_withholding,
            row.currency,
            row.comment,
        )


@dataclass
class TaxWithholdingRow(DataFrameRow):
    date: datetime
    symbol: str
    amount: np.float64
    currency: str

    @staticmethod
    def from_schwab_json(json_dict: dict) -> TaxWithholdingRow:
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y")
        symbol = json_dict["Symbol"]
        amount = pd.to_numeric(json_dict["Amount"].strip("-$").replace(",", ""))

        return TaxWithholdingRow(
            date,
            symbol,
            amount,
            "USD",
        )

    @staticmethod
    def default_dict() -> dict:
        return TaxWithholdingRow(datetime(1, 1, 1), "", np.float64(0), "").to_dict()


@dataclass
class SellOrderRow(DataFrameRow):
    date: datetime
    symbol: str
    quantity: np.float64
    sell_price: np.float64
    fees: np.float64
    currency: str
    comment: str

    @staticmethod
    def from_schwab_json(json_dict: dict) -> SellOrderRow:
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
            "Automated Schwab Import (JSON)",
        )

    @staticmethod
    def default_dict() -> dict:
        return SellOrderRow(
            datetime(1, 1, 1), "", np.float64(0), np.float64(0), np.float64(0), "", ""
        ).to_dict()

    @staticmethod
    def from_df_row(row: Series) -> SellOrderRow:
        return SellOrderRow(
            row.date,
            row.symbol,
            row.quantity,
            row.sell_price,
            row.fees,
            row.currency,
            row.comment,
        )


@dataclass
class BuyOrderRow(DataFrameRow):
    date: datetime
    symbol: str
    quantity: np.float64
    buy_price: np.float64
    fees: np.float64
    currency: str
    comment: str

    @staticmethod
    def default_dict() -> dict:
        return BuyOrderRow(
            datetime(1, 1, 1), "", np.float64(0), np.float64(0), np.float64(0), "", ""
        ).to_dict()

    @staticmethod
    def from_df_row(row: Series) -> BuyOrderRow:
        return BuyOrderRow(
            row.date,
            row.symbol,
            row.quantity,
            row.buy_price,
            row.fees,
            row.currency,
            row.comment,
        )


@dataclass
class CurrencyConversionRow(DataFrameRow):
    date: datetime
    foreign_amount: np.float64
    source_fees: np.float64
    source_currency: str
    target_currency: str
    comment: str

    @staticmethod
    def from_schwab_json(json_dict: dict) -> CurrencyConversionRow:
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
            "Automated Schwab Import (JSON, Currency Conversion from Wire Transfer, check correctness!)",
        )

    @staticmethod
    def default_dict() -> dict:
        return CurrencyConversionRow(
            datetime(1, 1, 1), np.float64(0), np.float64(0), "", "", ""
        ).to_dict()

    @staticmethod
    def from_df_row(row: Series) -> CurrencyConversionRow:
        return CurrencyConversionRow(
            row.date,
            row.foreign_amount,
            row.source_fees,
            row.source_currency,
            row.target_currency,
            row.comment,
        )


@dataclass
class CurrencyMovementRow(DataFrameRow):
    date: datetime
    buy_date: datetime
    amount: np.float64
    fees: np.float64
    currency: str
    comment: str

    @staticmethod
    def default_dict() -> dict:
        return CurrencyMovementRow(
            datetime(1, 1, 1), datetime(1, 1, 1), np.float64(0), np.float64(0), "", ""
        ).to_dict()

    @staticmethod
    def from_df_row(row: Series) -> CurrencyMovementRow:
        # dummy datetime for EUR movements and withdrawals
        buy_date = (
            datetime(1, 1, 1)
            if row.currency == "EUR" or row.amount < 0
            else row.buy_date
        )
        return CurrencyMovementRow(
            row.date,
            buy_date,
            row.amount,
            row.fees,
            row.currency,
            row.comment,
        )

    @staticmethod
    def from_schwab_json(json_dict: dict) -> CurrencyMovementRow:
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y")
        fees = pd.to_numeric(
            json_dict["FeesAndCommissions"].strip("-$").replace(",", "")
        )
        foreign_amount = pd.to_numeric(json_dict["Amount"].strip("-$").replace(",", ""))
        return CurrencyMovementRow(
            date,
            date,  # not relevant
            -foreign_amount,
            fees,
            "USD",
            "Automated Schwab Import (JSON, Wire Transfer)",
        )


@dataclass
class StockSplitRow(DataFrameRow):
    date: datetime
    symbol: str
    shares_after_split: np.float64

    @staticmethod
    def from_df_row(row: Series) -> StockSplitRow:
        return StockSplitRow(
            date=row.date,
            symbol=row.symbol,
            shares_after_split=row.shares_after_split,
        )

    @staticmethod
    def default_dict() -> dict:
        return StockSplitRow(datetime(1, 1, 1), "", np.float64(0.0)).to_dict()
