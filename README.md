# pyFIFOtax
Simple Tax Reporting Tool for share transactions on foreign exchanges

# Data Sources
A tax report is generated in an automated fashion based on two sources of inputs
- a history of transactions including deposits and sales of shares, dividend payments, currency conversions to EUR, and all relevant fees in the process (see `example/transactions.xlsx` for details)
- a history of all relevant exchange rates (`eurofxref-hist.csv`, based on official reference rates from [EZB](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.de.html))

For the history of transactions, you don't have to bother about converting to EUR or matching the sell prices to the buy prices for instance. This is part of the automated reporting. Simply list the raw data. 

For the reference rates, you might have to download a newer version depending on your report year. While it would be possible to have a variety of different foreign currencies based on this data, it is currently mainly intended in combination with USD.

# Stock Splits
Over time, many company will announce stock splits to reduce the price of their shares. Typical splits mean that for one existing share, you will receive a certain number of additional shares, e.g. 1, 3 ,or 9 in 2:1, 4:1, or 10:1 splits. NVDA, however, itself also had a "odd" splits in the past, e.g. a 3:2 split, i.e. you received one additional share for two existing ones or half a share for one existing share. Technically, also reverse splits are possible, e.g. for two existing shares you will have one share after the split.

To keep track of these splits, fill out the file `stock_splits.csv` by adding the symbol of asset for which the split should be tracked, the date, and the number of shares after the split. For instance, a 4:1 split is denoted as 4.0, a 3:2 split would be denoted as 1.5. Leaving this file empty (but keeping the header) will lead to no splits applied. `ReportData` also has has now an additional boolen flag to toggle this behavior (`apply_stock_splits`) even if the file is filled with some content. 

A stock split is assumed to take place after hours. I.e. if a stock split takes place on the 4th of August, any shares bought on that day will be assumed to split the next day. Any shares sold on this day, are seen as not having undergone the split.

NOTE: If you manually kept track of stock splits in the past in your list of transactions, please aware that these splits would be applied on top of your manual bookkeeping. Please check whether these calcualtions are done correctly.

# ESPP and RSUs
Shares coming from ESPPs or RSU lapses can be treated differently. With ESPP often being sold directly after they are bought, it could actually reduce the reporting burden as one wouldn't expect capital gains from these transactions. If users want to do so, they should indicate the separate treatment by noting down the shares with a different symbol in all relevant transactions, e.g. by using `NVDA-ESPP` and `NVDA-RSU` instead of only `NVDA`. The reporting then will separate them in different calculations. 

# Report Generation
`create_report.py` will generate the report for you. Usage:
```
python3 create_report.py -dir <sub_dir of inputs/outputs> -f <file name of the transaction history> -y <report year> -o <output file name> -m <kind of exchange rates>
```
Note that the filenames should be valid Excel files, e.g. `transactions.xlsx` as input and `report.xlsx` as output. 

For the reporting, you can choose conversion based on daily exchange rates (`-m daily`) or monthly averages (`-m monthly_avg`). Both should (no guarantee of correctness) be fine in terms of tax reporting. However, one should (no guarantee of correctness) be consistent across years. It might be just wise to choose it once and go for it in the following years. But note that it can actually make quite a difference.

The generated report will contain several sheets with details of the transactions matching sell and buy orders based on the FIFO principle and including the right conversion rates to EUR. The last sheet will contain a summary intended as guidance for ELSTER.

# Conversion from CSV export

Various conversion utilities are available to convert the CSV output of the broker into a separate XLSX sheet.

Always inspect the results manually and copy only those values into the final spreadsheet which are verified.

Parameters:

```
positional arguments:
  {ibkr,schwab}         Type of the CSV format for input

options:
  -h, --help            show this help message and exit
  -i CSV_FILENAME, --csv CSV_FILENAME
                        CSV file from Interactive Brokers or Schwab
  -o XLSX_FILENAME, --xlsx XLSX_FILENAME
                        Output XLSX file
  --ticker-to-isin, --no-ticker-to-isin
                        Replace tickers in the 'symbol' column to ISIN (only for IBKR)
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

Export the CSV in the desired date range from History > Transactions > Export.

Start the `converter.py` script with argument `schwab`.

Note the following limitations:

* Schwab CSV converter was not tested with a fully upgraded account
* The converter was not tested with more than one dividend and tax withholding row
* Currency conversions (if Schwab does it at all) are not supported

# Further Use
Since all the reporting is done in a very simple and quite naive Python implementation, one could easily use it to augment the data in other ways. `notebook_example.ipynb` for instance shows you how to retrieve certain results as `pd.DataFrame`.

# Requirements
- pandas
- XlsxWriter

# Testing

If you develop a new feature or change an existing piece of code, test your changes with the following command:

```sh
PYTHONPATH=. pytest
```

# Disclaimer
I am neither a lawyer nor a tax advisor. This is no tax advice. For me, this is a helpful tool. Feel free to use it but note that you should know what you are doing, and you are responsible for reporting your taxes correctly.
