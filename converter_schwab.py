import os
import pandas as pd
import warnings
import json
import argparse

from pyfifotax.utils import create_report_sheet
from pyfifotax.data_structures_dataframe import (
    BuyOrderRow,
    SellOrderRow,
    RSURow,
    ESPPRow,
    DividendRow,
    TaxWithholdingRow,
    CurrencyConversionRow,
)


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


def process_schwab_json(json_file_name, xlsx_file_name):
    schwab_rsu_events = []
    schwab_rsu_deposit_events = {}
    schwab_rsu_lapse_events = {}
    schwab_espp_events = []
    schwab_dividend_events = []
    schwab_buy_events = [BuyOrderRow.empty().to_dict()]
    schwab_sell_events = []
    schwab_wire_events = []

    with open(json_file_name) as f:
        d = json.load(f)
        for e in d["Transactions"]:
            if e["Action"] == "Deposit" and e["Description"] == "ESPP":
                schwab_espp_events.append(ESPPRow.from_schwab_json(e).to_dict())

            # assumption behind RSU: each grant has its own vest/deposit event
            # assumption behind RSU: award-id, year, and month are unique to each
            # deposit/lapse event (day of deposit and lapse might differ)
            elif (
                e["Action"] == "Lapse" and e["Description"] == "Restricted Stock Lapse"
            ):
                tmp, award_id = RSURow.from_schwab_lapse_json(e)
                key = (tmp.date.year, tmp.date.month, award_id)
                if key in schwab_rsu_lapse_events:
                    raise RuntimeError("Found duplicated RSU Lapse event: {tmp}")
                schwab_rsu_lapse_events[key] = tmp

            elif e["Action"] == "Deposit" and e["Description"] == "RS":
                tmp, award_id = RSURow.from_schwab_deposit_json(e)
                key = (tmp.date.year, tmp.date.month, award_id)
                if key in schwab_rsu_deposit_events:
                    raise RuntimeError(f"Found duplicated RSU deposit event: {tmp}")
                schwab_rsu_deposit_events[key] = tmp

            elif e["Action"] == "Dividend" and e["Description"] == "Credit":
                tmp = DividendRow.from_schwab_json(e)
                if len(schwab_dividend_events) > 0 and isinstance(
                    schwab_dividend_events[-1], TaxWithholdingRow
                ):
                    tax = schwab_dividend_events.pop(-1)
                    tmp.tax_withholding = tax.amount
                    schwab_dividend_events.append(tmp.to_dict())
                else:
                    schwab_dividend_events.append(tmp)

            elif e["Action"] == "Sale" and e["Description"] == "Share Sale":
                schwab_sell_events.append(SellOrderRow.from_schwab_json(e).to_dict())

            elif (
                e["Action"] == "Wire Transfer"
                and e["Description"] == "Cash Disbursement"
            ):
                schwab_wire_events.append(
                    CurrencyConversionRow.from_schwab_json(e).to_dict()
                )

            elif e["Action"] == "Tax Withholding" and e["Description"] == "Debit":
                tmp = TaxWithholdingRow.from_schwab_json(e)
                if len(schwab_dividend_events) > 0 and isinstance(
                    schwab_dividend_events[-1], DividendRow
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

    if len(schwab_rsu_lapse_events) != len(schwab_rsu_deposit_events):
        raise RuntimeError(
            f"Number of RSU Lapses {len(schwab_rsu_lapse_events)} does not match number of RSU deposits {len(schwab_rsu_deposit_events)}"
        )
    elif len(schwab_rsu_lapse_events) > 0:
        for key, rsu in schwab_rsu_deposit_events.items():
            if key in schwab_rsu_lapse_events:
                rsu_lapse = schwab_rsu_lapse_events[key]
            else:
                raise ValueError(
                    f"RSU Deposit {key} does not have a matching Lapse Event"
                )
            # schwab applies splits on historical lapse data but not on deposits
            # thus, use this difference to determine split factor on-the-fly
            # based on the split factor, we then can rely on the gross quantity
            # in the lapse event while the prices for the deposit event are already
            # correct
            split_factor = rsu_lapse.net_quantity / rsu.net_quantity
            rsu.gross_quantity = rsu_lapse.gross_quantity / split_factor
            schwab_rsu_events.append(rsu)

    if len(schwab_espp_events) == 0:
        schwab_espp_events.append(ESPPRow.empty().to_dict())
    if len(schwab_dividend_events) == 0:
        schwab_dividend_events.append(DividendRow.empty().to_dict())
    if len(schwab_sell_events) == 0:
        schwab_sell_events.append(SellOrderRow.empty().to_dict())
    if len(schwab_wire_events) == 0:
        schwab_wire_events.append(CurrencyConversionRow().empty().to_dict())

    dfs = {
        "rsu": pd.DataFrame(schwab_rsu_events),
        "espp": pd.DataFrame(schwab_espp_events),
        "dividends": pd.DataFrame(schwab_dividend_events),
        "buy_orders": pd.DataFrame(schwab_buy_events),
        "sell_orders": pd.DataFrame(schwab_sell_events),
        "currency_conversions": pd.DataFrame(schwab_wire_events),
    }

    with pd.ExcelWriter(
        xlsx_file_name, engine="xlsxwriter", datetime_format="yyyy-mm-dd"
    ) as writer:
        for k, v in dfs.items():
            create_report_sheet(k, v, writer)
            # overwrite column width somewhat inline with manual examples
            writer.sheets[k].set_column(1, 20, 16)


def main(args):
    process_schwab_json(args.json_filename, args.xlsx_filename)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
