import pandas as pd
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
    MoneyTransferRow,
    TaxReversalRow,
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
parser.add_argument(
    "--forex_transfer_as_exchange",
    action="store_true",
    help=(
        "If set, treats outgoing wire transfers as currency exchange to EUR."
        " This can be helpful to simplify the reporting of currency conversions"
        " if this is the only style of transfer. Please check the actual date"
        " of conversion and for correctness in general!"
    ),
)


def process_schwab_json(json_file_name, xlsx_file_name, forex_transfer_as_exchange):
    schwab_rsu_events = []
    schwab_rsu_deposit_events = {}
    schwab_rsu_lapse_events = {}
    schwab_espp_events = []
    schwab_dividend_events = []
    schwab_buy_events = [BuyOrderRow.empty_dict()]
    schwab_sell_events = []
    schwab_wire_events = []
    schwab_money_transfer_events = []

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
                    raise RuntimeError(f"Found duplicated RSU Lapse event: {tmp}")
                schwab_rsu_lapse_events[key] = tmp

            elif e["Action"] == "Deposit" and e["Description"] == "RS":
                tmp, award_id = RSURow.from_schwab_deposit_json(e)
                key = (tmp.date.year, tmp.date.month, award_id)
                if key in schwab_rsu_deposit_events:
                    raise RuntimeError(f"Found duplicated RSU deposit event: {tmp}")
                schwab_rsu_deposit_events[key] = tmp

            elif e["Action"] == "Dividend" and e["Description"] == "Credit":
                tmp = DividendRow.from_schwab_json(e)
                schwab_dividend_events.append(tmp)

            elif e["Action"] == "Sale" and e["Description"] == "Share Sale":
                # some sell orders might be split into different logical orders
                # at different prices, hence divide them up
                total_quantity = pd.to_numeric(e["Quantity"])
                total_fees = pd.to_numeric(e["FeesAndCommissions"].strip("$"))
                for det in e["TransactionDetails"]:
                    e_det = {**e}
                    shares = det["Details"]["Shares"]
                    e_det["Quantity"] = det["Details"]["Shares"]
                    e_det["TransactionDetails"] = [det]
                    e_det["Amount"] = None
                    fees_per_order = total_fees * pd.to_numeric(shares) / total_quantity
                    e_det["FeesAndCommissions"] = f"${fees_per_order:.3}"
                    schwab_sell_events.append(
                        SellOrderRow.from_schwab_json(e_det).to_dict()
                    )

            elif (
                e["Action"] == "Wire Transfer"
                and e["Description"] == "Cash Disbursement"
            ):
                if forex_transfer_as_exchange:
                    schwab_wire_events.append(
                        CurrencyConversionRow.from_schwab_json(e).to_dict()
                    )

                else:
                    schwab_money_transfer_events.append(
                        MoneyTransferRow.from_schwab_json(e).to_dict()
                    )

            elif e["Action"] == "Tax Withholding" and e["Description"] == "Debit":
                tmp = TaxWithholdingRow.from_schwab_json(e)
                schwab_dividend_events.append(tmp.to_dividend_row())

            elif e["Action"] == "Tax Reversal" and e["Description"] == "Credit":
                tmp = TaxReversalRow.from_schwab_json(e)
                schwab_dividend_events.append(tmp.to_dividend_row())

            else:
                # do nothing on unused fields
                pass

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
            rsu.gross_quantity = rsu_lapse.gross_quantity
            schwab_rsu_events.append(rsu)

    if len(schwab_espp_events) == 0:
        schwab_espp_events.append(ESPPRow.empty_dict())
    if len(schwab_dividend_events) == 0:
        schwab_dividend_events.append(DividendRow.empty_dict())
    if len(schwab_sell_events) == 0:
        schwab_sell_events.append(SellOrderRow.empty_dict())
    if len(schwab_wire_events) == 0:
        schwab_wire_events.append(CurrencyConversionRow.empty_dict())
    if len(schwab_money_transfer_events) == 0:
        schwab_money_transfer_events.append(MoneyTransferRow.empty_dict())

    dfs = {
        "rsu": pd.DataFrame(schwab_rsu_events),
        "espp": pd.DataFrame(schwab_espp_events),
        "dividends": pd.DataFrame(schwab_dividend_events),
        "buy_orders": pd.DataFrame(schwab_buy_events),
        "sell_orders": pd.DataFrame(schwab_sell_events),
        "currency_conversions": pd.DataFrame(schwab_wire_events),
        "money_transfers": pd.DataFrame(schwab_money_transfer_events),
    }

    with pd.ExcelWriter(
        xlsx_file_name, engine="xlsxwriter", datetime_format="yyyy-mm-dd"
    ) as writer:
        for k, v in dfs.items():
            v.sort_values("date", inplace=True)
            create_report_sheet(k, v, writer)
            # overwrite column width somewhat inline with manual examples
            writer.sheets[k].set_column(1, 20, 16)


def main(args):
    process_schwab_json(
        args.json_filename, args.xlsx_filename, args.forex_transfer_as_exchange
    )


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
