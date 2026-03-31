import datetime
import json
import logging

import pandas as pd

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
from pyfifotax.historic_price_utils import detect_split_adjustment
from pyfifotax.utils import create_report_sheet

logger = logging.getLogger("pyfifotax")


def _prescan_fmv_entries(transactions: list[dict]) -> list[tuple[float, str, datetime.date]]:
    """Extract (fmv, symbol, date) from RSU and ESPP entries for split detection."""
    entries = []
    for e in transactions:
        symbol = e.get("Symbol")
        if not symbol:
            continue
        details_list = e.get("TransactionDetails", [])
        if len(details_list) != 1:
            continue
        details = details_list[0].get("Details", {})

        if e["Action"] == "Lapse" and e["Description"] == "Restricted Stock Lapse":
            fmv_str = details.get("FairMarketValuePrice", "")
            if fmv_str:
                fmv = pd.to_numeric(fmv_str.strip("$").replace(",", ""))
                date = datetime.datetime.strptime(e["Date"], "%m/%d/%Y").date()
                entries.append((fmv, symbol, date))

        elif e["Action"] == "Deposit" and e["Description"] == "RS":
            fmv_str = details.get("VestFairMarketValue", "")
            if fmv_str:
                fmv = pd.to_numeric(fmv_str.strip("$").replace(",", ""))
                date = datetime.datetime.strptime(e["Date"], "%m/%d/%Y").date()
                entries.append((fmv, symbol, date))

        elif e["Action"] == "Deposit" and e["Description"] == "ESPP":
            fmv_str = details.get("PurchaseFairMarketValue", "")
            if fmv_str:
                fmv = pd.to_numeric(fmv_str.strip("$").replace(",", ""))
                date_str = details.get("PurchaseDate", e["Date"])
                date = datetime.datetime.strptime(date_str, "%m/%d/%Y").date()
                entries.append((fmv, symbol, date))

    return entries


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

        # Pre-scan: detect whether Schwab data is already split-adjusted
        fmv_entries = _prescan_fmv_entries(d["Transactions"])
        detection = detect_split_adjustment(fmv_entries)
        skip_adj = detection is True

        if detection is True:
            logger.info(
                "Auto-detected split-adjusted data (e.g. Schwab Equity Awards Center). "
                "Skipping per-entry split adjustment in converter."
            )
        elif detection is False:
            logger.info(
                "Auto-detected raw (non-split-adjusted) data. "
                "Applying per-entry split adjustment in converter."
            )
        else:
            logger.info(
                "Could not auto-detect split adjustment status "
                "(no entries before a known split). Using default behavior."
            )

        for e in d["Transactions"]:
            if e["Action"] == "Deposit" and e["Description"] == "ESPP":
                schwab_espp_events.append(ESPPRow.from_schwab_json(e, skip_split_adjustment=skip_adj).to_dict())

            # assumption behind RSU: each grant has its own vest/deposit event
            # assumption behind RSU: award-id, year, and month are unique to each
            # deposit/lapse event (day of deposit and lapse might differ)
            elif (
                e["Action"] == "Lapse" and e["Description"] == "Restricted Stock Lapse"
            ):
                tmp, award_id = RSURow.from_schwab_lapse_json(e, skip_split_adjustment=skip_adj)
                key = (tmp.date.year, tmp.date.month, award_id)
                if key in schwab_rsu_lapse_events:
                    raise RuntimeError(f"Found duplicated RSU Lapse event: {tmp}")
                schwab_rsu_lapse_events[key] = tmp

            elif e["Action"] == "Deposit" and e["Description"] == "RS":
                tmp, award_id = RSURow.from_schwab_deposit_json(e, skip_split_adjustment=skip_adj)
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
                        SellOrderRow.from_schwab_json(e_det, skip_split_adjustment=skip_adj).to_dict()
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
            writer.sheets[k].set_column(1, 20, 16)


def convert(args):
    process_schwab_json(
        args.input_filename, args.xlsx_filename, args.forex_transfer_as_exchange
    )
