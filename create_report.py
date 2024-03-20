import argparse

from report_data import ReportData

parser = argparse.ArgumentParser(
    description="Simple tool for calculating gains/losses from \
        transactions with shares on foreign exchanges based on FIFO methods, \
        intended for income tax reports in Germany. Note that the output of \
        this tool is just a suggestions and not a recommendations. \n \
        There is no guarantee for correctness. \n \
        To be sure, please contact your tax advisor for reliable information."
)
parser.add_argument(
    "-dir",
    dest="sub_dir",
    type=str,
    help="sub_dir which contains the list of transactions (input)",
)
parser.add_argument(
    "-f",
    dest="file_name",
    type=str,
    default="transactions.xlsx",
    help="file for list of transactions (input), default is 'transactions.xlsx'",
)
parser.add_argument(
    "-y",
    dest="report_year",
    type=int,
    default=2022,
    help="year for which report should be generated, default is 2022",
)
parser.add_argument(
    "-m",
    dest="rate_mode",
    choices=["daily", "monthly_avg"],
    help="which exchange rates to apply, either daily exchange rates or monthly averages",
)
parser.add_argument(
    "-o",
    dest="report_file_name",
    type=str,
    help="file name of generated report file (output), e.g. report_tax_2022.xlsx",
)


def main(sub_dir, file_name, report_year, rate_mode, report_file_name):
    report = ReportData(sub_dir=sub_dir, file_name=file_name)
    report.create_excel_report(report_year, rate_mode, report_file_name)


if __name__ == "__main__":
    args = parser.parse_args()
    main(
        args.sub_dir,
        args.file_name,
        args.report_year,
        args.rate_mode,
        args.report_file_name,
    )
