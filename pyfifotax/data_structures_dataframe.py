from __future__ import annotations

import datetime
from dataclasses import dataclass, asdict
import pandas as pd
import numpy as np
from pandas.core.series import Series

from typing import Optional

from pyfifotax.historic_price_utils import is_price_historic


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
    date: datetime.date
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
        date = datetime.datetime.strptime(details["PurchaseDate"], "%m/%d/%Y").date()
        buy_price = pd.to_numeric(details["PurchasePrice"].strip("$").replace(",", ""))
        fair_market_value = pd.to_numeric(
            details["PurchaseFairMarketValue"].strip("$").replace(",", "")
        )

        # ESPP typically evaluated at close
        is_historic, hist_price = is_price_historic(
            buy_price, symbol, date, kind="Close"
        )
        if is_historic:
            split_msg = ""

        else:
            # TODO: look into supporting arbitary splits
            # assumptions for now: if adjusted: price < hist_price and integer
            split_factor = int(hist_price / buy_price)
            buy_price = buy_price * split_factor
            fair_market_value = fair_market_value * split_factor
            quantity = quantity * split_factor
            split_msg = f": adjusted values for stock splits with an assumed split-factor of {split_factor}"

        return ESPPRow(
            date,
            symbol,
            buy_price,
            fair_market_value,
            quantity,
            "USD",
            "Automated Schwab Import (JSON)" + split_msg,
        )

    @staticmethod
    def default_dict() -> dict:
        return ESPPRow(
            datetime.date(1, 1, 1),
            "",
            np.float64(0),
            np.float64(0),
            np.float64(0),
            "",
            "",
        ).to_dict()

    @staticmethod
    def from_df_row(row: Series) -> ESPPRow:
        return ESPPRow(
            row.date.date(),
            row.symbol,
            row.buy_price,
            row.fair_market_value,
            row.quantity,
            row.currency,
            row.comment,
        )


@dataclass
class RSURow(DataFrameRow):
    date: datetime.date
    symbol: str
    gross_quantity: Optional[np.float64]
    net_quantity: np.float64
    fair_market_value: np.float64
    currency: str
    comment: str

    @staticmethod
    def from_schwab_lapse_json(json_dict: dict) -> tuple[RSURow, int]:
        date = datetime.datetime.strptime(json_dict["Date"], "%m/%d/%Y").date()
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

        # RSU typically evaluated at close
        is_historic, hist_price = is_price_historic(
            fair_market_value, symbol, date, kind="Close"
        )
        if is_historic:
            split_msg = ""

        else:
            # TODO: look into supporting arbitary splits
            # assumptions for now: if adjusted: price < hist_price and integer
            split_factor = int(hist_price / fair_market_value)
            fair_market_value = fair_market_value * split_factor
            net_quantity = net_quantity * split_factor
            gross_quantity = gross_quantity * split_factor
            split_msg = f": adjusted values for stock splits with an assumed split-factor of {split_factor}"

        return (
            RSURow(
                date,
                symbol,
                gross_quantity,
                net_quantity,
                fair_market_value,
                "USD",
                f"Automated Schwab Import (JSON, Award ID {award_id})" + split_msg,
            ),
            award_id,
        )

    @staticmethod
    def from_schwab_deposit_json(json_dict: dict) -> tuple[RSURow, int]:
        date = datetime.datetime.strptime(json_dict["Date"], "%m/%d/%Y").date()
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

        # RSU typically evaluated at close
        is_historic, hist_price = is_price_historic(
            fair_market_value, symbol, date, kind="Close"
        )
        if is_historic:
            split_msg = ""

        else:
            # TODO: look into supporting arbitary splits
            # assumptions for now: if adjusted: price < hist_price and integer
            split_factor = int(hist_price / fair_market_value)
            fair_market_value = fair_market_value * split_factor
            net_quantity = net_quantity * split_factor
            split_msg = f": adjusted values for stock splits with an assumed split-factor of {split_factor}"

        return (
            RSURow(
                date,
                symbol,
                gross_quantity,
                net_quantity,
                fair_market_value,
                "USD",
                f"Automated Schwab Import (JSON, Award ID {award_id})" + split_msg,
            ),
            award_id,
        )

    @staticmethod
    def default_dict() -> dict:
        return RSURow(
            datetime.date(1, 1, 1),
            "",
            np.float64(0),
            np.float64(0),
            np.float64(0),
            "",
            "",
        ).to_dict()

    @staticmethod
    def from_df_row(row: Series) -> RSURow:
        return RSURow(
            row.date.date(),
            row.symbol,
            row.gross_quantity,
            row.net_quantity,
            row.fair_market_value,
            row.currency,
            row.comment,
        )


@dataclass
class DividendRow(DataFrameRow):
    date: datetime.date
    symbol: str
    amount: np.float64
    tax_withholding: np.float64
    currency: str
    comment: str

    @staticmethod
    def from_schwab_json(json_dict: dict) -> DividendRow:
        date = datetime.datetime.strptime(json_dict["Date"], "%m/%d/%Y").date()
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
            datetime.date(1, 1, 1), "", np.float64(0), np.float64(0), "", ""
        ).to_dict()

    @staticmethod
    def from_df_row(row: Series) -> DividendRow:
        return DividendRow(
            row.date.date(),
            row.symbol,
            row.amount,
            row.tax_withholding,
            row.currency,
            row.comment,
        )


@dataclass
class TaxWithholdingRow(DataFrameRow):
    date: datetime.date
    symbol: str
    amount: np.float64
    currency: str

    @staticmethod
    def from_schwab_json(json_dict: dict) -> TaxWithholdingRow:
        date = datetime.datetime.strptime(json_dict["Date"], "%m/%d/%Y").date()
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
        return TaxWithholdingRow(
            datetime.date(1, 1, 1), "", np.float64(0), ""
        ).to_dict()

    def to_dividend_row(self) -> DividendRow:
        return DividendRow(
            self.date,
            self.symbol,
            np.float64(0),
            self.amount,
            self.currency,
            f"Tax Withholding ({self.symbol})",
        )


@dataclass
class TaxReversalRow(DataFrameRow):
    date: datetime.date
    symbol: str
    amount: np.float64
    currency: str

    @staticmethod
    def from_schwab_json(json_dict: dict) -> TaxReversalRow:
        date = datetime.datetime.strptime(json_dict["Date"], "%m/%d/%Y").date()
        symbol = json_dict["Symbol"]
        amount = pd.to_numeric(json_dict["Amount"].strip("-$").replace(",", ""))

        return TaxReversalRow(
            date,
            symbol,
            amount,
            "USD",
        )

    @staticmethod
    def default_dict() -> dict:
        return TaxReversalRow(datetime.date(1, 1, 1), "", np.float64(0), "").to_dict()

    def to_dividend_row(self) -> DividendRow:
        return DividendRow(
            self.date,
            self.symbol,
            np.float64(0),
            -self.amount,
            self.currency,
            f"Tax Reversal ({self.symbol})",
        )


@dataclass
class SellOrderRow(DataFrameRow):
    date: datetime.date
    symbol: str
    quantity: np.float64
    sell_price: np.float64
    currency: str
    fees: np.float64
    fee_currency: str
    comment: str

    @staticmethod
    def from_schwab_json(json_dict: dict) -> SellOrderRow:
        date = datetime.datetime.strptime(json_dict["Date"], "%m/%d/%Y").date()
        symbol = json_dict["Symbol"]
        fees = pd.to_numeric(
            json_dict["FeesAndCommissions"].strip("-$").replace(",", "")
        )
        if np.isnan(fees):
            fees = pd.to_numeric("0.0")
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

        # sell-orders typically should always be denoted in historical values
        # TODO: check if this is the case and potentially fix

        return SellOrderRow(
            date,
            symbol,
            quantity,
            sale_price,
            "USD",
            fees,
            "USD",
            "Automated Schwab Import (JSON)",
        )

    @staticmethod
    def default_dict() -> dict:
        return SellOrderRow(
            datetime.date(1, 1, 1),
            "",
            np.float64(0),
            np.float64(0),
            "",
            np.float64(0),
            "",
            "",
        ).to_dict()

    @staticmethod
    def from_df_row(row: Series) -> SellOrderRow:
        return SellOrderRow(
            row.date.date(),
            row.symbol,
            row.quantity,
            row.sell_price,
            row.currency,
            row.fees,
            row.fee_currency,
            row.comment,
        )


@dataclass
class BuyOrderRow(DataFrameRow):
    date: datetime.date
    symbol: str
    quantity: np.float64
    buy_price: np.float64
    currency: str
    fees: np.float64
    fee_currency: str
    comment: str

    @staticmethod
    def default_dict() -> dict:
        return BuyOrderRow(
            datetime.date(1, 1, 1),
            "",
            np.float64(0),
            np.float64(0),
            "",
            np.float64(0),
            "",
            "",
        ).to_dict()

    @staticmethod
    def from_df_row(row: Series) -> BuyOrderRow:
        return BuyOrderRow(
            row.date.date(),
            row.symbol,
            row.quantity,
            row.buy_price,
            row.currency,
            row.fees,
            row.fee_currency,
            row.comment,
        )


@dataclass
class CurrencyConversionRow(DataFrameRow):
    date: datetime.date
    source_amount: np.float64
    source_currency: str
    target_amount: np.float64
    target_currency: str
    fees: np.float64
    fee_currency: str
    comment: str

    @staticmethod
    def from_schwab_json(json_dict: dict) -> CurrencyConversionRow:
        date = datetime.datetime.strptime(json_dict["Date"], "%m/%d/%Y").date()
        fees = pd.to_numeric(
            json_dict["FeesAndCommissions"].strip("-$").replace(",", "")
        )
        if np.isnan(fees):
            fees = np.float64(0)
        target_amount = np.float64(-1)
        source_amount = pd.to_numeric(json_dict["Amount"].strip("-$").replace(",", ""))
        return CurrencyConversionRow(
            date,
            source_amount,
            "USD",
            target_amount,
            "EUR",
            fees,
            "USD",
            "Automated Schwab Import (JSON, Currency Conversion from Wire Transfer, check correctness!)",
        )

    @staticmethod
    def default_dict() -> dict:
        return CurrencyConversionRow(
            datetime.date(1, 1, 1),
            np.float64(0),
            "",
            np.float64(0),
            "",
            np.float64(0),
            "",
            "",
        ).to_dict()

    @staticmethod
    def from_df_row(row: Series) -> CurrencyConversionRow:
        return CurrencyConversionRow(
            row.date.date(),
            row.source_amount,
            row.source_currency,
            row.target_amount,
            row.target_currency,
            row.fees,
            row.fee_currency,
            row.comment,
        )


@dataclass
class MoneyTransferRow(DataFrameRow):
    date: datetime.date
    buy_date: datetime.date
    amount: np.float64
    currency: str
    fees: np.float64
    fee_currency: str
    comment: str

    @staticmethod
    def default_dict() -> dict:
        return MoneyTransferRow(
            datetime.date(1, 1, 1),
            datetime.date(1, 1, 1),
            np.float64(0),
            "",
            np.float64(0),
            "",
            "",
        ).to_dict()

    @staticmethod
    def from_df_row(row: Series) -> MoneyTransferRow:
        # dummy datetime for EUR transfers and withdrawals
        buy_date = (
            datetime.date(1, 1, 1)
            if row.currency == "EUR" or row.amount < 0
            else row.buy_date.date()
        )
        return MoneyTransferRow(
            row.date.date(),
            buy_date,
            row.amount,
            row.currency,
            row.fees,
            row.fee_currency,
            row.comment,
        )

    @staticmethod
    def from_schwab_json(json_dict: dict) -> MoneyTransferRow:
        date = datetime.datetime.strptime(json_dict["Date"], "%m/%d/%Y").date()
        fees = pd.to_numeric(
            json_dict["FeesAndCommissions"].strip("-$").replace(",", "")
        )
        if np.isnan(fees):
            fees = pd.to_numeric("0.0")
        foreign_amount = pd.to_numeric(json_dict["Amount"].strip("-$").replace(",", ""))
        return MoneyTransferRow(
            date,
            date,  # not relevant
            -foreign_amount,
            "USD",
            fees,
            "USD",
            "Automated Schwab Import (JSON, Wire Transfer)",
        )


@dataclass
class StockSplitRow(DataFrameRow):
    date: datetime.date
    symbol: str
    shares_after_split: np.float64

    @staticmethod
    def from_df_row(row: Series) -> StockSplitRow:
        return StockSplitRow(
            date=row.date.date(),
            symbol=row.symbol,
            shares_after_split=row.shares_after_split,
        )

    @staticmethod
    def default_dict() -> dict:
        return StockSplitRow(datetime.date(1, 1, 1), "", np.float64(0.0)).to_dict()
