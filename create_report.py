import argparse
import logging
import sys

from pyfifotax.report_data import ReportData

logger = logging.getLogger("pyfifotax")


parser = argparse.ArgumentParser(
    description="Tool for calculating gains/losses from \
        transactions with shares on foreign exchanges based on FIFO methods, \
        intended for income tax reports in Germany. Note, that the output of \
        this tool is just a suggestion and not a recommendation.\n \
        There is no guarantee for correctness. \n \
        To be sure, please contact your tax advisor for reliable information."
)
parser.add_argument(
    "-d", "--dir",
    dest="sub_dir",
    type=str,
    help="directory which contains the transactions and the output",
)
parser.add_argument(
    "-f", "--file",
    dest="file_name",
    type=str,
    help="filename which contains the transactions",
)
parser.add_argument(
    "-y", "--year",
    dest="report_year",
    type=int,
    help="year for which report should be generated",
)
parser.add_argument(
    "-m", "--mode",
    dest="rate_mode",
    choices=["daily", "monthly"],
    default="daily",
    help="which exchange rates to apply, either daily exchange rates (default) or monthly averages",
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
parser.add_argument(
    "--log-level",
    choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    default="WARNING",
    dest="log_level",
    help="sets the logging level. Default: warning",
)

def setup_logging(log_level: str):
    logging.basicConfig(stream=sys.stdout, format="[%(levelname)-8s] %(message)s")
    logger.setLevel(log_level)

def exception_hook(type, value, traceback):
    logger.critical(value, exc_info=(type, value, traceback))

def main(sub_dir, file_name, report_year, rate_mode, create_all_reports, log_level):
    setup_logging(log_level)
    sys.excepthook = exception_hook

    report = ReportData(sub_dir=sub_dir, file_name=file_name)
    if create_all_reports:
        report.create_all_reports()

    else:
        report.create_excel_report(
            report_year, rate_mode, f"tax_report_{report_year}_{rate_mode}_rates.xlsx"
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
        args.log_level,
    )
