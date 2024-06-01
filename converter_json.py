from datetime import datetime
from dataclasses import dataclass, asdict
import pandas as pd
import os
import warnings
import json
import argparse

from utils import create_report_sheet

parser = argparse.ArgumentParser(
    description="Convert Schwab JSON output to XLSX for later processing. Please review converted transactions before creating Tax/AWV reports!"
)
parser.add_argument(
    "-i",
    "--json",
    dest="json_filename",
    type=str,
    required=True,
    help="Schwab JSON History",
)
parser.add_argument(
    "-o",
    "--xlsx",
    dest="xlsx_filename",
    type=str,
    required=True,
    help="Output XLSX file",
)


@dataclass
class SchwabJSONElement:
    def to_dict(self):
        return asdict(self)

    @staticmethod
    def from_json(json_dict):
        raise NotImplementedError


@dataclass
class SchwabESPP(SchwabJSONElement):
    date: str
    symbol: str
    buy_price: pd.Float64Dtype
    fair_market_value: pd.Float64Dtype
    quantity: pd.Float64Dtype
    currency: str

    @staticmethod
    def from_json(json_dict):
        symbol = json_dict["Symbol"]
        quantity = pd.to_numeric(json_dict["Quantity"])
        assert len(json_dict["TransactionDetails"]) == 1
        details = json_dict["TransactionDetails"][0]["Details"]
        date = datetime.strptime(details["PurchaseDate"], "%m/%d/%Y").strftime(
            "%Y-%m-%d"
        )
        buy_price = pd.to_numeric(details["PurchasePrice"].strip("$").replace(",", ""))
        fair_market_value = pd.to_numeric(
            details["PurchaseFairMarketValue"].strip("$").replace(",", "")
        )
        return SchwabESPP(
            date,
            symbol,
            buy_price,
            fair_market_value,
            quantity,
            "USD",
        )


@dataclass
class SchwabRSU(SchwabJSONElement):
    date: str
    symbol: str
    gross_quantity: pd.Float64Dtype
    net_quantity: pd.Float64Dtype
    fair_market_value: pd.Float64Dtype
    currency: str

    @staticmethod
    def from_json(json_dict):
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y").strftime("%Y-%m-%d")
        symbol = json_dict["Symbol"]
        gross_quantity = pd.to_numeric(json_dict["Quantity"])
        assert len(json_dict["TransactionDetails"]) == 1
        details = json_dict["TransactionDetails"][0]["Details"]
        net_quantity = pd.to_numeric(details["NetSharesDeposited"])
        fair_market_value = pd.to_numeric(
            details["FairMarketValuePrice"].strip("$").replace(",", "")
        )

        return SchwabRSU(
            date,
            symbol,
            gross_quantity,
            net_quantity,
            fair_market_value,
            "USD",
        )


@dataclass
class SchwabDividend(SchwabJSONElement):
    date: str
    symbol: str
    amount: pd.Float64Dtype
    tax_withholding: pd.Float64Dtype
    currency: str

    @staticmethod
    def from_json(json_dict):
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y").strftime("%Y-%m-%d")
        symbol = json_dict["Symbol"]
        amount = pd.to_numeric(json_dict["Amount"].strip("$").replace(",", ""))

        return SchwabDividend(
            date,
            symbol,
            amount,
            pd.to_numeric(0),
            "USD",
        )


@dataclass
class SchwabTaxWitholding(SchwabJSONElement):
    date: datetime
    symbol: str
    amount: pd.Float64Dtype
    currency: str

    @staticmethod
    def from_json(json_dict):
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y").strftime("%Y-%m-%d")
        symbol = json_dict["Symbol"]
        amount = pd.to_numeric(json_dict["Amount"].strip("-$").replace(",", ""))

        return SchwabTaxWitholding(
            date,
            symbol,
            amount,
            "USD",
        )


@dataclass
class SchwabSale(SchwabJSONElement):
    date: str
    symbol: str
    quantity: pd.Float64Dtype
    sell_price: pd.Float64Dtype
    fees: pd.Float64Dtype
    currency: str

    @staticmethod
    def from_json(json_dict):
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y").strftime("%Y-%m-%d")
        symbol = json_dict["Symbol"]
        fees = pd.to_numeric(
            json_dict["FeesAndCommissions"].strip("-$").replace(",", "")
        )
        quantity = pd.to_numeric(json_dict["Quantity"])
        details = json_dict["TransactionDetails"]

        sale_price = details[0]["Details"]["SalePrice"]
        check_quantity = pd.to_numeric(0)
        for det in details:
            assert det["Details"]["SalePrice"] == sale_price
            check_quantity += pd.to_numeric(det["Details"]["Shares"])
        assert check_quantity == quantity

        sale_price = pd.to_numeric(sale_price.strip("$").replace(",", ""))

        return SchwabSale(
            date,
            symbol,
            quantity,
            sale_price,
            fees,
            "USD",
        )


@dataclass
class SchwabWireTransfer(SchwabJSONElement):
    date: str
    foreign_amount: pd.Float64Dtype
    source_fees: pd.Float64Dtype
    source_currency: str
    target_currency: str

    @staticmethod
    def from_json(json_dict):
        date = datetime.strptime(json_dict["Date"], "%m/%d/%Y").strftime("%Y-%m-%d")
        fees = pd.to_numeric(
            json_dict["FeesAndCommissions"].strip("-$").replace(",", "")
        )
        foreign_amount = pd.to_numeric(json_dict["Amount"].strip("-$").replace(",", ""))
        return SchwabWireTransfer(
            date,
            foreign_amount,
            fees,
            "USD",
            "EUR",
        )


def process_schwab_json(json_file_name, xlsx_file_name):
    schwab_rsu_events = []
    schwab_espp_events = []
    schwab_dividend_events = []
    schwab_buy_events = [
        {
            "date": None,
            "symbol": None,
            "quantity": None,
            "buy_price": None,
            "fees": None,
            "currency": None,
        }
    ]
    schwab_sell_events = []
    schwab_wire_events = []
    schwab_split_events = [{"date": None, "symbol": None, "shares_after_split": None}]

    with open(json_file_name) as f:
        d = json.load(f)
        for e in d["Transactions"]:
            if e["Action"] == "Deposit" and e["Description"] == "ESPP":
                schwab_espp_events.append(SchwabESPP.from_json(e).to_dict())
            elif (
                e["Action"] == "Lapse" and e["Description"] == "Restricted Stock Lapse"
            ):
                schwab_rsu_events.append(SchwabRSU.from_json(e).to_dict())

            elif e["Action"] == "Deposit" and e["Description"] == "RS":
                pass  # deposit of RSU shares covered in Laps

            elif e["Action"] == "Dividend" and e["Description"] == "Credit":
                tmp = SchwabDividend.from_json(e)
                if len(schwab_dividend_events) > 0 and isinstance(
                    schwab_dividend_events[-1], SchwabTaxWitholding
                ):
                    tax = schwab_dividend_events.pop(-1)
                    tmp.tax_withholding = tax.amount
                    schwab_dividend_events.append(tmp.to_dict())
                else:
                    schwab_dividend_events.append(tmp)

            elif e["Action"] == "Sale" and e["Description"] == "Share Sale":
                schwab_sell_events.append(SchwabSale.from_json(e).to_dict())

            elif (
                e["Action"] == "Wire Transfer"
                and e["Description"] == "Cash Disbursement"
            ):
                schwab_wire_events.append(SchwabWireTransfer.from_json(e).to_dict())

            elif e["Action"] == "Tax Withholding" and e["Description"] == "Debit":
                tmp = SchwabTaxWitholding.from_json(e)
                if len(schwab_dividend_events) > 0 and isinstance(
                    schwab_dividend_events[-1], SchwabDividend
                ):
                    div = schwab_dividend_events.pop(-1)
                    div.tax_withholding = tmp.amount
                    schwab_dividend_events.append(div.to_dict())
                else:
                    schwab_dividend_events.append(tmp)

            else:
                act = e["Action"]
                des = e["Description"]
                sym = e["Symbol"]
                date = e["Date"]
                warnings.warn(f"skipping {act} on {date} ({sym}: {des})")

    if len(schwab_rsu_events) == 0:
        schwab_rsu_events.append(
            {k: [] for k in SchwabRSU(None, None, None, None, None).to_dict()}
        )
    if len(schwab_espp_events) == 0:
        schwab_espp_events.append(
            {k: [] for k in SchwabESPP(None, None, None, None, None, None).to_dict()}
        )
    if len(schwab_dividend_events) == 0:
        schwab_dividend_events.append(
            {k: [] for k in SchwabDividend(None, None, None, None, None).to_dict()}
        )
    if len(schwab_sell_events) == 0:
        schwab_sell_events.append(
            {k: [] for k in SchwabSale(None, None, None, None, None, None).to_dict()}
        )
    if len(schwab_wire_events) == 0:
        schwab_wire_events.append(
            {k: [] for k in SchwabWireTransfer(None, None, None, None, None).to_dict()}
        )

    dfs = {
        "rsu": pd.DataFrame(schwab_rsu_events),
        "espp": pd.DataFrame(schwab_espp_events),
        "dividends": pd.DataFrame(schwab_dividend_events),
        "buy_orders": pd.DataFrame(schwab_buy_events),
        "sell_orders": pd.DataFrame(schwab_sell_events),
        "currency_conversions": pd.DataFrame(schwab_wire_events),
        "stock_splits": pd.DataFrame(schwab_split_events),
    }

    with pd.ExcelWriter(xlsx_file_name, engine="xlsxwriter") as writer:
        for k, v in dfs.items():
            create_report_sheet(k, v, writer)
            # overwrite column width somewhat inline with manual examples
            writer.sheets[k].set_column(1, 20, 16)


def main(args):
    process_schwab_json(args.json_filename, args.xlsx_filename)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
