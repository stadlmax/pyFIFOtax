import os
import pandas as pd


def get_date(forex):
    return forex.date


def get_reference_rates():
    daily_ex_rates = pd.read_csv("eurofxref-hist.csv")
    daily_ex_rates = daily_ex_rates.loc[
        :, ~daily_ex_rates.columns.str.contains("^Unnamed")
    ]
    daily_ex_rates.index = pd.to_datetime(daily_ex_rates["Date"], format="%Y-%m-%d")
    # drop years earlier as 2009 as this would make reporting tax earning
    # way more complicated anyways
    daily_ex_rates = daily_ex_rates.loc[daily_ex_rates.index.year >= 2009]
    # drop columns with nan values
    daily_ex_rates = daily_ex_rates.dropna(axis="columns")

    daily_ex_rates = daily_ex_rates.drop("Date", axis="columns")
    daily_ex_rates = daily_ex_rates
    monthly_ex_rates = daily_ex_rates.groupby(
        by=[daily_ex_rates.index.year, daily_ex_rates.index.month]
    ).mean()

    supported_currencies = set(daily_ex_rates.columns)
    print(f"INFO: supported currencies are: {supported_currencies}\n")
    return daily_ex_rates, monthly_ex_rates, supported_currencies


def read_data(sub_dir, file_name):
    # read list of deposits, sales, dividend payments and wire transfers
    # sort them to ensure that they are in chronological order
    file_path = os.path.join(sub_dir, file_name)
    df_deposits = pd.read_excel(file_path, sheet_name="deposits").sort_index(
        ascending=True
    )
    df_sales = pd.read_excel(file_path, sheet_name="sales").sort_index(ascending=True)
    df_dividends = pd.read_excel(file_path, sheet_name="dividends").sort_index(
        ascending=True
    )
    df_wire_transfers = pd.read_excel(
        file_path, sheet_name="wire_transfers"
    ).sort_index(ascending=True)

    return df_deposits, df_sales, df_dividends, df_wire_transfers

def summarize_report(df_shares, df_forex, df_dividends, df_fees, df_taxes):
    # this is simply the sum of all gains and losses
    # for tax reasons, we usually also want the sum of gains
    # and the sum of losses
    share_gain_series = df_shares["Gain [EUR]"]
    share_losses = share_gain_series[share_gain_series < 0].sum()
    share_gains = share_gain_series[share_gain_series > 0].sum()

    forex_gain_series = df_forex["Gain [EUR]"]

    total_dividends = df_dividends["Amount [EUR]"].sum()
    total_fees = df_fees["Amount [EUR]"].sum()
    total_taxes = df_taxes["Amount [EUR]"].sum()

    # unlike a previous version, we have to split things here
    # losses from share can only be compared to gains from shares
    # hence, there is a "total" gain/loss and then separate gains and 
    # losses from shares, e.g. one could have had a gain of 100
    # and a loss of 150 over a year, i.e. a total loss of 50
    total_foreign_gains = share_losses + share_gains + total_dividends
    gains_from_shares = share_gains
    losses_from_shares = -share_losses
    total_gain_forex = forex_gain_series.sum()

    anlagen = [
        (
            "Anlage KAP",
            "Zeile 19: Ausländische Kapitalerträge (ohne Betrag lt. Zeile 47)",
            round(total_foreign_gains, 2),
        ),
        (
            "Anlage KAP",
            "Zeile 20: In den Zeilen 18 und 19 enthaltene Gewinne aus Aktienveräußerungen i. S. d. § 20 Abs. 2 Satz 1 Nr 1 EStG",
            round(gains_from_shares, 2),
        ),
        (
            "Anlage KAP",
            "Zeile 23: In den Zeilen 18 und 19 enthaltene Verluste aus der Veräuerung von Aktien i. S. d. § 20 Abs. 2 Satz 1 Nr. 1 EStG",
            round(losses_from_shares, 2),
        ),
        (
            "Anlage KAP",
            "Zeile 41: Anrechenbare noch nicht angerechnete ausländische Steuern",
            round(total_taxes, 2),
        ),
        (
            "Anlage N",
            "Zeile 48: (Werbungskosten Sonstiges): Überweisungsgebühren auf deutsches Konto für Gehaltsbestandteil RSU/ESPP",
            round(total_fees, 2),
        ),
        (
            "Anlage SO",
            "Zeilen 42 - 48: Gewinn / Verlust aus Verkauf von Fremdwährungen",
            round(total_gain_forex, 2), # here: the sum should be fine
        ),
    ]
    summary = {
        "ELSTER - Anlage": [a[0] for a in anlagen],
        "ELSTER - Zeile (Suggestion!)": [a[1] for a in anlagen],
        "Value": [a[2] for a in anlagen],
    }
    df_summary = pd.DataFrame(summary)
    return df_summary

def write_report(
    df_shares, df_forex, df_dividends, df_fees, df_taxes, sub_dir, file_name
):
    df_summary = summarize_report(df_shares, df_forex, df_dividends, df_fees, df_taxes)
    report_path = os.path.join(sub_dir, file_name)
    writer = pd.ExcelWriter(report_path)
    df_shares.to_excel(writer, sheet_name="Shares", index=False)
    df_forex.to_excel(writer, sheet_name="Foreign Currencies", index=False)
    df_dividends.to_excel(writer, sheet_name="Dividend Payments", index=False)
    df_fees.to_excel(writer, sheet_name="Fees", index=False)
    df_taxes.to_excel(writer, sheet_name="Tax Withholdings", index=False)   
    df_summary.to_excel(writer, sheet_name="ELSTER - Summary", index=False)
    writer.close()


def apply_rates_forex_dict(forex_dict, daily_rates, monthly_rates):
    for k, v in forex_dict.items():
        for f in v:
            # exchange rates are in 1 EUR : X FOREX
            f.amount_eur_daily = f.amount / daily_rates[f.currency][f.date]
            f.amount_eur_monthly = (
                f.amount / monthly_rates[f.currency][f.date.year, f.date.month]
            )


def filter_forex_dict(forex_dict, report_year):
    filtered_dict = {k: [] for k in forex_dict.keys()}
    for k, v in forex_dict.items():
        for f in v:
            # filter based on date of fee / taxaction /etc. event
            if f.date.year == report_year:
                filtered_dict[k].append(f)
    for k, v in filtered_dict.items():
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
    total_amount = 0
    for k, v in forex_dict.items():
        for f in v:
            tmp["Symbol"].append(k)
            tmp["Comment"].append(f.comment)
            date = f"{f.date.year}-{f.date.month:02}-{f.date.day:02}"
            tmp["Date"].append(date)
            amount = f"{f.amount:.2f} {f.currency}"
            tmp["Amount"].append(amount)
            if mode == "daily":
                tmp["Amount [EUR]"].append(round(f.amount_eur_daily, 2))
            else:
                tmp["Amount [EUR]"].append(round(f.amount_eur_monthly, 2))

    df = pd.DataFrame(
        tmp, columns=["Symbol", "Comment", "Date", "Amount", "Amount [EUR]"]
    )
    return df


def apply_rates_transact_dict(trans_dict, daily_rates, monthly_rates):
    for k, v in trans_dict.items():
        for f in v:
            # exchange rates are in 1 EUR : X FOREX
            buy_price, sell_price = f.buy_price, f.sell_price
            buy_rate_daily = daily_rates[f.currency][f.buy_date]
            buy_rate_monthly = monthly_rates[f.currency][
                f.buy_date.year, f.buy_date.month
            ]
            sell_rate_daily = daily_rates[f.currency][f.sell_date]
            sell_rate_monthly = monthly_rates[f.currency][
                f.sell_date.year, f.sell_date.month
            ]
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


def filter_transact_dict(
    trans_dict, report_year, min_quantity, speculative_period=None
):
    filtered_dict = {k: [] for k in trans_dict.keys()}
    for k, v in trans_dict.items():
        for f in v:
            # filter based on sell date and quantity is larger min_quantity
            # (the latter is to filter out Forex transactions due to rounding errors)
            if (f.sell_date.year == report_year) and (f.quantity > min_quantity):
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

    total_gain = 0
    for k, v in transact_dict.items():

        for f in v:
            tmp["Symbol"].append(k)
            if f.__class__.__name__ == "FIFOShare":
                tmp["Quantity"].append(int(f.quantity))
            else:
                tmp["Quantity"].append(round(f.quantity, 2))
            buy_date = f"{f.buy_date.year}-{f.buy_date.month:02}-{f.buy_date.day:02}"
            sell_date = (
                f"{f.sell_date.year}-{f.sell_date.month:02}-{f.sell_date.day:02}"
            )
            tmp["Buy Date"].append(buy_date)
            tmp["Sell Date"].append(sell_date)
            tmp["Buy Price"].append(f"{f.buy_price:.2f} {f.currency}")
            tmp["Sell Price"].append(f"{f.sell_price:.2f} {f.currency}")
            if mode.lower() == "daily":
                tmp["Buy Price [EUR]"].append(round(f.buy_price_eur_daily, 2))
                tmp["Sell Price [EUR]"].append(round(f.sell_price_eur_daily, 2))
                tmp["Gain [EUR]"].append(round(f.gain_eur_daily, 2))
            else:
                tmp["Buy Price [EUR]"].append(round(f.buy_price_eur_monthly, 2))
                tmp["Sell Price [EUR]"].append(round(f.sell_price_eur_monthly, 2))
                tmp["Gain [EUR]"].append(round(f.gain_eur_monthly, 2))

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
