import os
import io
import datetime
import requests
import zipfile
import decimal
import warnings
from dataclasses import dataclass
from datetime import timedelta

import pandas as pd


def get_date(forex):
    return forex.date


def round_decimal(number, precision: str = "0.01"):
    return number.quantize(
        decimal.Decimal(precision),
        rounding=decimal.ROUND_HALF_UP,
    )


def to_decimal(number):
    return decimal.Decimal(number)


def sum_decimal(series: pd.Series):
    if series.shape[0] == 0:
        return to_decimal(0)
    return series.sum()


def get_reference_rates():
    # check whether to download more recent exchange-rate data
    mod_date = None
    today = datetime.date.today()
    if os.path.exists("eurofxref-hist.csv"):
        mod_time = os.path.getmtime("eurofxref-hist.csv")
        mod_date = datetime.datetime.fromtimestamp(mod_time).date()

    if mod_date != today:
        print("Downloading more recent exchange rate data ...")
        response = requests.get(
            "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip?69474b1034fa15ae36103fbf3554d272"
        )
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        zip_file.extractall(".")

    daily_ex_rates = pd.read_csv("eurofxref-hist.csv", parse_dates=["Date"])
    daily_ex_rates = daily_ex_rates.loc[
        :, ~daily_ex_rates.columns.str.contains("^Unnamed")
    ]

    daily_ex_rates.index = daily_ex_rates["Date"]
    # drop years earlier as 2009 as this would make reporting tax earning
    # way more complicated anyway
    daily_ex_rates = daily_ex_rates.loc[daily_ex_rates.index.year >= 2009]
    # drop columns with nan values
    daily_ex_rates = daily_ex_rates.dropna(axis="columns")

    daily_ex_rates = daily_ex_rates.drop("Date", axis="columns")
    daily_ex_rates = daily_ex_rates
    monthly_ex_rates = daily_ex_rates.groupby(
        by=[daily_ex_rates.index.year, daily_ex_rates.index.month]
    ).mean()

    supported_currencies = set(daily_ex_rates.columns)
    supported_currencies.add("EUR")

    return daily_ex_rates, monthly_ex_rates, supported_currencies


def get_monthly_rate(monthly_rates: pd.DataFrame, date: datetime, currency: str):
    return to_decimal(monthly_rates[currency][date.year, date.month])


def get_daily_rate(daily_rates: pd.DataFrame, date: datetime, currency: str):
    if date in daily_rates[currency]:
        return to_decimal(daily_rates[currency][date])
    else:
        # On currency settlement holidays exchanges don't operate. Go ahead and find the next valid settlement date
        for day_increase in range(1, 8):
            day = date + timedelta(days=day_increase)

            if day in daily_rates[currency]:
                return to_decimal(daily_rates[currency][day])

    raise ValueError(
        f"{currency} currency exchange rate cannot be found for {date.date()} or for the following seven days"
    )


@dataclass
class RawData:
    rsu: pd.DataFrame
    espp: pd.DataFrame
    dividends: pd.DataFrame
    buy_orders: pd.DataFrame
    sell_orders: pd.DataFrame
    currency_conversions: pd.DataFrame
    stock_splits: pd.DataFrame


def read_data_legacy(sub_dir, file_name):
    # read list of deposits, sales, dividend payments and currency conversions
    # sort them to ensure that they are in chronological order
    file_path = os.path.join(sub_dir, file_name)
    with pd.ExcelFile(file_path) as xls:
        forex_sheet = (
            "wire_transfers"
            if "wire_transfers" in xls.sheet_names
            else "currency conversion to EUR"
        )
        if forex_sheet == "wire_transfers":
            warnings.warn(
                '"wire_transfers" as a sheet name is deprecated and discouraged; '
                'use "currency conversion to EUR" instead',
                DeprecationWarning,
            )

        df_deposits = pd.read_excel(xls, sheet_name="deposits", parse_dates=["date"])
        df_buy_orders = df_deposits.copy()

        df_deposits["gross_quantity"] = df_deposits["net_quantity"]
        df_deposits.rename(
            mapper={"fmv_or_buy_price": "fair_market_value"},
            axis="columns",
            inplace=True,
        )
        df_deposits = df_deposits[df_deposits.fees == 0]
        df_deposits.drop(labels=["fees"], axis="columns", inplace=True)

        df_buy_orders = df_buy_orders[df_buy_orders.fees != 0]
        df_buy_orders.rename(
            mapper={"fmv_or_buy_price": "buy_price", "net_quantity": "quantity"},
            axis="columns",
            inplace=True,
        )

        df_dividends = pd.read_excel(xls, sheet_name="dividends", parse_dates=["date"])
        df_sell_orders = pd.read_excel(xls, sheet_name="sales", parse_dates=["date"])

        df_currency_conversions = pd.read_excel(
            xls, sheet_name=forex_sheet, parse_dates=["date"]
        )
        df_currency_conversions["target_currency"] = "EUR"
        df_currency_conversions["foreign_amount"] = (
            df_currency_conversions.net_amount + df_currency_conversions.fees
        )
        df_currency_conversions.rename(
            mapper={"fees": "source_fees", "currency": "source_currency"},
            axis="columns",
            inplace=True,
        )
        df_currency_conversions.drop(
            labels=["net_amount"], axis="columns", inplace=True
        )

        df_stock_splits = None
        df_espp = None

    return RawData(
        df_deposits,
        df_espp,
        df_dividends,
        df_buy_orders,
        df_sell_orders,
        df_currency_conversions,
        df_stock_splits,
    )


def read_data(sub_dir, file_name):
    # read list of deposits, sales, dividend payments and currency conversions
    # sort them to ensure that they are in chronological order
    file_path = os.path.join(sub_dir, file_name)
    with pd.ExcelFile(file_path) as xls:
        df_rsu = pd.read_excel(xls, sheet_name="rsu", parse_dates=["date"])
        df_dividends = pd.read_excel(xls, sheet_name="dividends", parse_dates=["date"])
        df_buy_orders = pd.read_excel(
            xls, sheet_name="buy_orders", parse_dates=["date"]
        )
        df_sell_orders = pd.read_excel(
            xls, sheet_name="sell_orders", parse_dates=["date"]
        )
        df_currency_conversions = pd.read_excel(
            xls, sheet_name="currency_conversions", parse_dates=["date"]
        )
        df_stock_splits = None  # set later
        df_espp = pd.read_excel(
            xls,
            sheet_name="espp",
            parse_dates=["date"],
        )

    return RawData(
        df_rsu,
        df_espp,
        df_dividends,
        df_buy_orders,
        df_sell_orders,
        df_currency_conversions,
        df_stock_splits,
    )


def summarize_report(df_shares, df_forex, df_dividends, df_fees, df_taxes):
    # this is simply the sum of all gains and losses
    # for tax reasons, we usually also want the sum of gains
    # and the sum of losses
    share_gain_series = df_shares["Gain [EUR]"]
    share_losses = sum_decimal(share_gain_series[share_gain_series < 0])
    share_gains = sum_decimal(share_gain_series[share_gain_series > 0])

    forex_gain_series = df_forex["Gain [EUR]"]

    total_dividends = sum_decimal(df_dividends["Amount [EUR]"])
    total_fees = sum_decimal(df_fees["Amount [EUR]"])
    total_taxes = sum_decimal(df_taxes["Amount [EUR]"])

    # unlike a previous version, we have to split things here
    # losses from share can only be compared to gains from shares
    # hence, there is a "total" gain/loss and then separate gains and
    # losses from shares, e.g. one could have had a gain of 100
    # and a loss of 150 over a year, i.e. a total loss of 50
    total_foreign_gains = share_losses + share_gains + total_dividends
    gains_from_shares = share_gains
    losses_from_shares = -share_losses
    total_gain_forex = sum_decimal(forex_gain_series)

    anlagen = [
        (
            "Anlage KAP",
            "Zeile 19: Ausländische Kapitalerträge (ohne Betrag lt. Zeile 47)",
            round_decimal(total_foreign_gains),
        ),
        (
            "Anlage KAP",
            "Zeile 20: In den Zeilen 18 und 19 enthaltene Gewinne aus Aktienveräußerungen i. S. d. § 20 Abs. 2 Satz 1 Nr 1 EStG",
            round_decimal(gains_from_shares),
        ),
        (
            "Anlage KAP",
            "Zeile 23: In den Zeilen 18 und 19 enthaltene Verluste aus der Veräuerung von Aktien i. S. d. § 20 Abs. 2 Satz 1 Nr. 1 EStG",
            round_decimal(losses_from_shares),
        ),
        (
            "Anlage KAP",
            "Zeile 41: Anrechenbare noch nicht angerechnete ausländische Steuern",
            round_decimal(total_taxes),
        ),
        (
            "Anlage N",
            "Zeile 48: (Werbungskosten Sonstiges): Überweisungsgebühren auf deutsches Konto für Gehaltsbestandteil RSU/ESPP",
            round_decimal(total_fees),
        ),
        (
            "Anlage SO",
            "Zeilen 42 - 48: Gewinn / Verlust aus Verkauf von Fremdwährungen",
            round_decimal(total_gain_forex),  # here: the sum should be fine
        ),
    ]
    summary = {
        "ELSTER - Anlage": [a[0] for a in anlagen],
        "ELSTER - Zeile (Suggestion!)": [a[1] for a in anlagen],
        "Value": [a[2] for a in anlagen],
    }
    df_summary = pd.DataFrame(summary)
    return df_summary


def create_report_sheet(name: str, df: pd.DataFrame, writer: pd.ExcelWriter):
    if df.empty:
        return

    if "Date" in df:
        df.sort_values("Date", inplace=True)
    elif "date" in df:
        df.sort_values("date", inplace=True)
    elif "Sell Date" in df:
        df.sort_values(["Sell Date", "Buy Date"], inplace=True)
    elif "Meldezeitraum" in df:
        df.sort_values("Meldezeitraum", inplace=True)
    elif "ELSTER" in df.columns[0]:
        pass  # don't sort ELSTER frames
    else:
        raise RuntimeError(
            "Couldn't sort data, expected either 'Date', 'date', 'Sell Date', or 'Meldezeitraum' column to exist"
        )

    df.to_excel(writer, sheet_name=name, index=False)
    worksheet = writer.sheets[name]
    worksheet.autofit()  # Adjust column widths to their maximum lengths
    worksheet.set_landscape()
    worksheet.set_paper(9)  # A4
    worksheet.set_header(f"&C{name}")  # Put sheet name into the header
    worksheet.hide_gridlines(0)  # Do not hide gridlines
    worksheet.center_horizontally()


def add_total_amount_row(df):
    cols = df.columns
    amount = df["Amount [EUR]"].sum()

    total_dfs = []
    # add empty row first
    new_row = {c: None for c in cols}
    new_row = pd.DataFrame([new_row])
    total_dfs.append(new_row)

    new_row = {c: None for c in cols}
    new_row["Symbol"] = "Total Amount"
    new_row["Amount [EUR]"] = amount
    new_row = pd.DataFrame([new_row])
    total_dfs.append(new_row)

    df = pd.concat([df, *total_dfs], ignore_index=True)

    return df


def add_total_gain_rows(df):
    cols = df.columns
    all_gains = df["Gain [EUR]"]
    net_gains = all_gains.sum()
    gains = all_gains[all_gains > 0].sum()
    losses = all_gains[all_gains < 0].sum()

    total_dfs = []
    # add empty row first
    new_row = {c: None for c in cols}
    total_dfs.append(pd.DataFrame([new_row]))

    new_row = {c: None for c in cols}
    new_row["Symbol"] = "Gains (incl. losses)"
    new_row["Gain [EUR]"] = net_gains
    total_dfs.append(pd.DataFrame([new_row]))

    new_row = {c: None for c in cols}
    new_row["Symbol"] = "Gains (excl. losses)"
    new_row["Gain [EUR]"] = gains
    total_dfs.append(pd.DataFrame([new_row]))

    new_row = {c: None for c in cols}
    new_row["Symbol"] = "Losses"
    new_row["Gain [EUR]"] = losses
    total_dfs.append(pd.DataFrame([new_row]))

    df = pd.concat([df, *total_dfs], ignore_index=True)

    return df


def write_report(
    df_shares, df_forex, df_dividends, df_fees, df_taxes, sub_dir, file_name
):
    df_shares = df_shares.copy()
    df_forex = df_forex.copy()
    df_dividends = df_dividends.copy()
    df_fees = df_fees.copy()
    df_taxes = df_taxes.copy()

    df_summary = summarize_report(df_shares, df_forex, df_dividends, df_fees, df_taxes)

    df_shares = add_total_gain_rows(df_shares)
    df_forex = add_total_gain_rows(df_forex)
    df_dividends = add_total_amount_row(df_dividends)
    df_fees = add_total_amount_row(df_fees)
    df_taxes = add_total_amount_row(df_taxes)

    report_path = os.path.join(sub_dir, file_name)
    with pd.ExcelWriter(report_path, engine="xlsxwriter") as writer:
        create_report_sheet("Shares", df_shares, writer)
        create_report_sheet("Foreign Currencies", df_forex, writer)
        create_report_sheet("Dividend Payments", df_dividends, writer)
        create_report_sheet("Fees", df_fees, writer)
        create_report_sheet("Tax Withholding", df_taxes, writer)
        create_report_sheet("ELSTER - Summary", df_summary, writer)


def write_report_awv(df_z4, df_z10, sub_dir, file_name):
    report_path = os.path.join(sub_dir, file_name)
    with pd.ExcelWriter(report_path, engine="xlsxwriter") as writer:
        create_report_sheet("Z4", df_z4, writer)
        create_report_sheet("Z10", df_z10, writer)


def apply_rates_forex_dict(forex_dict, daily_rates, monthly_rates):
    for v in forex_dict.values():
        for f in v:
            if f.currency == "EUR":
                f.amount_eur_daily = f.amount
                f.amount_eur_monthly = f.amount
            else:
                # exchange rates are in 1 EUR : X FOREX
                f.amount_eur_daily = f.amount / get_daily_rate(
                    daily_rates, f.date, f.currency
                )
                f.amount_eur_monthly = f.amount / get_monthly_rate(
                    monthly_rates,
                    f.date,
                    f.currency,
                )


def filter_forex_dict(forex_dict, report_year):
    filtered_dict = {k: [] for k in forex_dict.keys()}
    for k, v in forex_dict.items():
        for f in v:
            # filter based on date of fee / taxation /etc. event
            if f.date.year == report_year:
                filtered_dict[k].append(f)
    for _, v in filtered_dict.items():
        v.sort(key=get_date)
    return filtered_dict


def forex_dict_to_df(forex_dict, mode):
    assert mode.lower() in ["daily", "monthly_avg"]
    tmp = {
        "Symbol": [],
        "Comment": [],
        "Date": [],
        "Amount": [],
        "Amount [EUR]": [],
    }
    for k, v in forex_dict.items():
        for f in v:
            tmp["Symbol"].append(k)
            tmp["Comment"].append(f.comment)
            date = f"{f.date.year}-{f.date.month:02}-{f.date.day:02}"
            tmp["Date"].append(date)
            amount = f"{f.amount:.2f} {f.currency}"
            tmp["Amount"].append(amount)
            if mode == "daily":
                tmp["Amount [EUR]"].append(round_decimal(f.amount_eur_daily))
            else:
                tmp["Amount [EUR]"].append(round_decimal(f.amount_eur_monthly))

    df = pd.DataFrame(
        tmp, columns=["Symbol", "Comment", "Date", "Amount", "Amount [EUR]"]
    )
    return df


def apply_rates_transact_dict(trans_dict, daily_rates, monthly_rates):
    for v in trans_dict.values():
        for f in v:
            buy_price, sell_price = to_decimal(f.buy_price), to_decimal(f.sell_price)

            if f.currency == "EUR":
                buy_rate_daily = to_decimal(1)
                buy_rate_monthly = to_decimal(1)
                sell_rate_daily = to_decimal(1)
                sell_rate_monthly = to_decimal(1)
            else:
                # exchange rates are in 1 EUR : X FOREX
                buy_rate_daily = get_daily_rate(daily_rates, f.buy_date, f.currency)
                buy_rate_monthly = get_monthly_rate(
                    monthly_rates, f.buy_date, f.currency
                )
                sell_rate_daily = get_daily_rate(daily_rates, f.sell_date, f.currency)
                sell_rate_monthly = get_monthly_rate(
                    monthly_rates, f.sell_date, f.currency
                )

            f.buy_price_eur_daily = buy_price / buy_rate_daily
            f.buy_price_eur_monthly = buy_price / buy_rate_monthly
            f.sell_price_eur_daily = sell_price / sell_rate_daily
            f.sell_price_eur_monthly = sell_price / sell_rate_monthly
            f.gain_eur_daily = f.quantity * (
                f.sell_price_eur_daily - f.buy_price_eur_daily
            )
            f.gain_eur_monthly = f.quantity * (
                f.sell_price_eur_monthly - f.buy_price_eur_monthly
            )


def filter_transact_dict(trans_dict, report_year, speculative_period=None):
    filtered_dict = {k: [] for k in trans_dict.keys()}
    for k, v in trans_dict.items():
        for f in v:
            # filter based on sell date
            if f.sell_date.year == report_year:
                if speculative_period is None:
                    filtered_dict[k].append(f)
                elif (f.sell_date - f.buy_date).days < speculative_period * 365:
                    filtered_dict[k].append(f)

    return filtered_dict


def transact_dict_to_df(transact_dict, mode):
    assert mode.lower() in ["daily", "monthly_avg"]
    tmp = {
        "Symbol": [],
        "Quantity": [],
        "Buy Date": [],
        "Sell Date": [],
        "Buy Price": [],
        "Sell Price": [],
        "Buy Price [EUR]": [],
        "Sell Price [EUR]": [],
        "Gain [EUR]": [],
    }

    for k, v in transact_dict.items():
        for f in v:
            tmp["Symbol"].append(k)
            if f.__class__.__name__ == "FIFOShare":
                tmp["Quantity"].append(round_decimal(f.quantity))
            else:
                tmp["Quantity"].append(round_decimal(f.quantity))

            buy_date = f"{f.buy_date.year}-{f.buy_date.month:02}-{f.buy_date.day:02}"
            sell_date = (
                f"{f.sell_date.year}-{f.sell_date.month:02}-{f.sell_date.day:02}"
            )
            tmp["Buy Date"].append(buy_date)
            tmp["Sell Date"].append(sell_date)
            tmp["Buy Price"].append(f"{f.buy_price:.2f} {f.currency}")
            tmp["Sell Price"].append(f"{f.sell_price:.2f} {f.currency}")
            if mode.lower() == "daily":
                tmp["Buy Price [EUR]"].append(round_decimal(f.buy_price_eur_daily))
                tmp["Sell Price [EUR]"].append(round_decimal(f.sell_price_eur_daily))
                tmp["Gain [EUR]"].append(round_decimal(f.gain_eur_daily))
            else:
                tmp["Buy Price [EUR]"].append(round_decimal(f.buy_price_eur_monthly))
                tmp["Sell Price [EUR]"].append(round_decimal(f.sell_price_eur_monthly))
                tmp["Gain [EUR]"].append(round_decimal(f.gain_eur_monthly))

    df = pd.DataFrame(
        tmp,
        columns=[
            "Symbol",
            "Quantity",
            "Buy Date",
            "Sell Date",
            "Buy Price",
            "Sell Price",
            "Buy Price [EUR]",
            "Sell Price [EUR]",
            "Gain [EUR]",
        ],
    )
    return df