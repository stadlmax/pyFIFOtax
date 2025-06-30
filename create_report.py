import argparse

from pyfifotax.report_data import ReportData


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
    help="file name of to file containing list of transactions",
)
parser.add_argument(
    "-y",
    dest="report_year",
    type=int,
    help="year for which report should be generated",
)
parser.add_argument(
    "-m",
    dest="rate_mode",
    choices=["daily", "monthly"],
    help="which exchange rates to apply, either daily exchange rates or monthly averages",
)
parser.add_argument(
    "--all",
    action="store_true",
    help=(
        "If set, generate reports for all years found in the data,"
        " and both daily and monthly exchange rates."
        " Overrides both 'rate_mode' and 'report_year'."
    ),
)


def main(sub_dir, file_name, report_year, rate_mode, create_all_reports):
    report = ReportData(sub_dir=sub_dir, file_name=file_name)
    if create_all_reports:
        report.create_all_reports()

    else:
        report.create_excel_report(
            report_year, rate_mode, f"tax_report_{report_year}_{rate_mode}_rates"
        )
        report.create_excel_report_awv(report_year, f"awv_report_{report_year}.xlsx")


if __name__ == "__main__":
    args = parser.parse_args()
    main(
        args.sub_dir,
        args.file_name,
        args.report_year,
        args.rate_mode,
        args.all,
    )
