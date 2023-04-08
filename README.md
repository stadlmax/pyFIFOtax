# pyFIFOtax
Simple Tax Reporting Tool for share transactions on foreign exchanges

# Data Sources
A tax report is generated in an automated fashion based on two sources of inputs
- a history of transactions including deposits and sales of shares, dividend payments, wire transfers, and all relevant fees in the process (see `example/transactions.xlsx` for details)
- a history of all relevant exchange rates (`eurofxref-hist.csv`, based on official reference rates from [EZB](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.de.html))

For the history of transactions, you don't have to bother about converting to EUR or matching the sell prices to the buy prices for instance. This is part of the automated reporting. Simply list the raw data. 

For the reference rates, you might have to download a newer version depending on your report year. Currently, all years until 2022 are supported. And while it would be possible to have a variety of different foreign currencies based on this data, it is currently mainly intened in combination with USD.

# ESPP and RSUs
Shares coming from ESPPs or RSU lapses can be treated differently. With ESPP often being sold directly after they are bought, it could actually reduce the reporting burden as one wouldn't expect capital gains from these transactions. If users want to do so, they should indicate the separate treatment by noting down the shares with a different symbol in all relevant transactions, e.g. by using `NVDA-ESPP` and `NVDA-RSU` instead of only `NVDA`. The reporting then will separate them in different calculations. 

# Report Generation
`create_report.py` will generate the report for you. Usage:
```
python3 create_report.py -dir <sub_dir of inputs/outputs> -f <file name of the transaction history> -y <report year> -o <output file name> -m <kind of exchange rates>
```
Note that the filenames should be valid Excel files, e.g. `transactions.xlsx` as input and `report.xlsx` as output. 

For the reporting, you can choose conversion based on daily exchange rates (`-m daily`) or monthly averages (`-m monthly_avg`). Both should (no gurantuee of correctness) be fine in terms of tax reporting. However, one should (no gurantuee of correctness) be consistent across years. It might be just wise to choose it once and go for it in the following years. But note that it can actually make quite a difference.

The generated report will contain several sheets with details of the transactions matching sell and buy orders based on the FIFO principle and including the right conversion rates to EUR. The last sheet will contain a summary intended as guidance for ELSTER.

# Further Use
Since all of the reportingn is done in a very simple and quite naive Python implementation, one could easily use it to augment the data in other ways. `notebook_example.ipynb` for instance shows you how to retrieve certain results as `pd.DataFrame`.

# Requirements
- pandas
- openpyxl

# Disclaimer
I am neither a lawyer nor a tax advisor. This is no tax advice. For me, this is a helpful tool. Feel free to use it but note that you should know what you are doing and you are responsible for reporting your taxes correctly.
