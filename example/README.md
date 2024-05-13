# example
This folder contains an example.
- `transactions.xlsx` is the input of the report, i.e. all relevant transactions (sales / deposits), currency conversions to EUR, dividend payments, and fees
- `report_2022_daily_rates.xlsx` is the generated report for this example for the report year 2022 and using daily exchange rates
- `report_2022_monthly_rates.xlsx` is the generated report for this example for the report year 2022 and using monthly averages of exchange rates

## commands to generate the example
This assumes to running the script in the root directory.
```
python3 create_report.py -dir example -f transactions.xlsx -y 2022 -m daily -o report_2022_daily_rates.xlsx
```
for the daily exchange rates, and
```
python3 create_report.py -dir example -f transactions.xlsx -y 2022 -m monthly_avg -o report_2022_monthly_rates.xlsx
```
