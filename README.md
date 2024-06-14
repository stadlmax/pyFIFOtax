# pyFIFOtax
Simple Tax Reporting Tool for share transactions on foreign exchanges

# Data Sources
A tax report is generated in an automated fashion based on two sources of inputs
- a history of transactions including deposits and sales of shares, dividend payments, currency conversions to EUR, and all relevant fees in the process (see `example/transactions.xlsx` for details)
- a history of all relevant exchange rates (`eurofxref-hist.csv`, based on official reference rates from [EZB](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.de.html)), in an updated version of this code, the latest rates are automatically downloaded

For the history of transactions, you don't have to bother about converting to EUR or matching the sell prices to the buy prices for instance. This is part of the automated reporting. Simply list the raw data. 

For the reference rates, you might have to download a newer version depending on your report year, depending on the version of your code. While it would be possible to have a variety of different foreign currencies based on this data, it is currently mainly intended in combination with USD.

NOTE: an update extends the functionality of the reporting scripts (including support for stock splits and automated suggestions for AWV reporting). As this breaks the way input data is defined, these things are only available once you have created a new list of transactions following this new format, see e.g. `examples/transactions.xlsx` as an example for this new format. The old format is still available. However, it won't produce AWV reports and it can't handle stock splits.

# Stock Splits
Over time, many companies will announce stock splits to reduce the price of their shares. Typical splits mean that for one existing share, you will receive a certain number of additional shares, e.g. 1, 3 ,or 9 in 2:1, 4:1, or 10:1 splits. NVDA, however, itself also had "odd" splits in the past, e.g. a 3:2 split, i.e. you received one additional share for two existing ones or half a share for one existing share. Technically, also reverse splits are possible, e.g. for two existing shares you will have one share after the split.

`ReportData` has an additional boolean flag (`apply_stock_splits`) to toggle whether stock splits should be automatically applied or not. If applied, historical split data is queried from Yahoo Finance. If not applied, it is up to you to keep historical data correct and up-to-date.

A stock split is assumed to take place after hours. I.e. if a stock split takes place on the 4th of August, any shares bought on that day will be assumed to split the next day. Any shares sold on this day, are seen as not having undergone the split.

NOTE: If you manually kept track of stock splits in the past in your list of transactions, please aware that these splits would be applied on top of your manual bookkeeping. Please check whether these calculations are done correctly.
NOTE: The Schwab import somewhat tries to account for values pre-/post-split to a certain extent but only can handle splits with integer-based ratios for now. For NVDA, this only is the case for shares held prior to 2007. Since the taxation of capital gains changed anyways around 2009, you are on your own anyways, good luck.

# ESPP and RSUs
Shares coming from ESPPs or RSU lapses can be treated differently. With ESPP often being sold directly after they are bought, it could actually reduce the reporting burden as one wouldn't expect capital gains from these transactions. If users want to do so, they should indicate the separate treatment by noting down the shares with a different symbol in all relevant transactions, e.g. by using `NVDA-ESPP` and `NVDA-RSU` instead of only `NVDA`. The reporting then will separate them in different calculations. 

# Currencies, FOREX, and Deposits
The reporting tool will create FIFO queues of Foreign Currencies (i.e. not EUR) and a balance of EUR. These are also used e.g. for stock transactions and implicit currency events. To ensure that e.g. buy transactions are properly tracked based on initial balances and that currency related events are properly tracked after withdrawals, the tool can work on a provided list of money transfers in EUR or a foreign currency.

## Money Transfers
The tab for money transfers allows to track withdrawals (negative deposits) and deposits
- for withdrawals, set the amount as negative value indicating the (net) outflow of money and indicate the date of withdrawal under `date` while being able to ignore the column `buy_date` (e.g. by setting it to a dummy date or to the same value as `date`). The withdrawn amount corresponds to the amount after applying fees and corresponds to what you would receive on the other end of this transaction.
- for deposits, indicate a positive (gross) amount as inflow to your account and the date of deposit under `date`; since taxation of FOREX transactions follows a FIFO principle, too, you also have to know the initial `buy_date` (or acquisition date) of the foreign currency such that later sell transactions can be correctly valued. The fee is assumed to be applied separately, i.e. the fee reduces the deposited amount.
- for deposits in EUR, `buy_date` isn't relevant and you can treat it similarly to withdrawals of any other currency (e.g. just setting it to the same value as the transaction)
- note that in general, only the EUR balance can become negative and errors will be thrown if FOREX balances are to become negative
- if you want to denote fees which are not tracked anywhere else, just include a row for a withdrawal of 0 and set the fee to the amount of the fee.

You can generate a list of withdrawals including the relevant `buy_date` information for use in other accounts. Note that these values are the net values after applying potential withdrawal fees.

## Currency Conversions
The tab for currency conversions allows to track conversions between different currencies by specifying the source and target amount incl. potential fees. If you exchange from EUR to FOREX or from FOREX to EUR, you can leave the corresponding EUR amount as "-1" and indicate that you don't care about the exact exchange rate. The resulting EUR balance then can be slightly incorrect, but as this balance is allowed to become negative, you shouldn't experience any resulting issues from doing so. In terms of taxation, currency exchanges are handled by the official ECB rates anyways, so the exact rate doesn't matter. This mode can be handy if you only want to receive useful inputs for your tax declaration but don't care about an exact EUR balance. When exchanging between two different FOREX, you will have to specify both amount explicitly.


# Known Limitations
Same day events are currently ordered based on certain priorities to cover typical transaction patterns. However, this usually leads to sell orders being processed before buy orders, i.e. buying and immediately selling shares without owning any others before will error out. 


# Report Generation
`create_report.py` will generate the report for you. Usage:
```
python3 create_report.py -dir <sub_dir of inputs/outputs> -f <file name of the transaction history> -y <report year> -m <kind of exchange rates> [--all]
```
Note that the input file name should include the file extension, e.g. `transactions.xlsx`. 

For the reporting, you can choose conversion based on daily exchange rates (`-m daily`) or monthly averages (`-m monthly`). Both should (no guarantee of correctness) be fine in terms of tax reporting. However, one should (no guarantee of correctness) be consistent across years. It might be just wise to choose it once and go for it in the following years. But note that it can actually make quite a difference. 

The generated report will contain several sheets with details of the transactions matching sell and buy orders based on the FIFO principle and including the right conversion rates to EUR. The last sheet will contain a summary intended as guidance for ELSTER.

If you include the optional flag `--all`, reports for all calendar years and both exchange rate modes will be generated.

## AWV Reports
In an updated version of this code, Z4 and Z10 entries intended for reporting transactions exceeding 12_500 EUR to the Bunsdesbank are also created automatically. You will find the corresponding sheets "Z4" and "Z10" in `awv_report_<report_year>.xlsx`. Note: for now this is only supported for any transaction involving NVDA shares denoted in USD.

# Conversion from broker export

Various conversion utilities are available to convert the output of the broker into a separate XLSX sheet.

Always inspect the results manually and copy only those values into the final spreadsheet which are verified.

Parameters:

```
convert.py [-h] -i INPUT_FILENAME -o XLSX_FILENAME [--ticker-to-isin | --no-ticker-to-isin] [--forex_transfer_as_exchange] {ibkr,schwab}

Convert Interactive Brokers CSV and Schwab JSON output to XLSX for later processing

positional arguments:
  {ibkr,schwab}         Used broker

options:
  -h, --help            show this help message and exit
  -i INPUT_FILENAME, --input INPUT_FILENAME
                        Input file (CSV file from Interactive Brokers or JSON from Schwab)
  -o XLSX_FILENAME, --output XLSX_FILENAME
                        Output XLSX file
  --ticker-to-isin, --no-ticker-to-isin
                        Replace tickers in the 'symbol' column to ISIN (only for IBKR)
  --forex_transfer_as_exchange
                        If set, treats outgoing wire transfers as currency exchange to EUR. This can be helpful to simplify the reporting of currency conversions if this is the only style
                        of transfer. Please check the actual date of conversion and for correctness in general! (Only for Schwab)
```

## Interactive Brokers

The following has to be done once on the IBKR web interface:

1. Go to _Performance & Reports_ → _Statements_
2. Create a new _Custom Statement_
3. Specify "pyFIFOtax CSV export" as _Statement Name_
4. In Sections select _All_
5. In _Section Configurations_ set all to _No_
6. Set _Format_ to _CSV_, _Period_ to _Daily_, and the language to _English_
7. Save it

Export the CSV file by running this newly created custom statement in the desired date range, then start the `converter.py` script with argument `ibkr`.

Always thoroughly examine the result.

By default, the "symbol" column contains the ticker name. If you'd like to see the ISIN there instead, use the `--ticker-to-isin` flag.

Note the following limitations:

* IBKR export format is very extensive, it's possible that some rows are not processed. Always verify the results
* Tax withholding calculation is not supported for dividends. Withheld tax will always be zero.

## Schwab

Export the JSON in the desired date range from History > Transactions > Export and select the JSON format, then start the `converter.py` script with argument `schwab`.

Always inspect the results manually and copy only those values into the final spreadsheet which are verified!

After inspection and curation of your actual list of transactions, you can run the usual report creation scripts.

Note the following limitations:
* To simplify things, any wire transfer is assumed to be an outgoing transfer to an account denoted in EUR and thus implicitly is assumed to be a currency exchange. If this is not the case or if the date of the transfer does not match the settlement date, please delete or correct the corresponding entries.
* Schwab CSV converter was not tested with a fully upgraded account
* Buy orders are currently not supported.

# Further Use
Since all the reporting is done in a very simple and quite naive Python implementation, one could easily use it to augment the data in other ways. `notebook_example.ipynb` for instance shows you how to retrieve certain results as `pd.DataFrame`.

# Requirements
- pandas
- XlsxWriter
- yfinance[nospam]
- requests-cache

Please make sure to update yfinance to the most revent version (0.2.54 as of now).

# Testing

If you develop a new feature or change an existing piece of code, test your changes with the following command:

```sh
PYTHONPATH=. pytest
```

# Disclaimer
I am neither a lawyer nor a tax advisor. This is no tax advice. For me, this is a helpful tool. Feel free to use it but note that you should know what you are doing, and you are responsible for reporting your taxes correctly.
