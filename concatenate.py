import argparse
import logging
from os.path import exists

import pandas as pd

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description="Concatenate multiple transactions spreadsheets into one"
)
parser.add_argument(
    "spreadsheets",
    type=str,
    nargs="+",
    help="Spreadsheets to concatenate",
)
parser.add_argument(
    "-o",
    "--output",
    dest="output",
    type=str,
    required=True,
    help="Concatenated .xlsx file",
)
parser.add_argument(
    "--log-level",
    choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    default="INFO",
    dest="log_level",
    help="Sets the logging level. Default: info",
)


def read_files(spreadsheets):
    merged = {}
    transaction_num = {}

    logger.info(f"Reading files")
    for file in spreadsheets:
        logger.debug(f"Processing '{file}'")
        if not exists(file):
            logger.critical(f"Spreadsheet '{file}' doesn't exist.")
            exit(1)

        spreadsheets = pd.read_excel(file, sheet_name=None)
        for name, sheet in spreadsheets.items():
            if name not in merged:
                merged[name] = [sheet]
                transaction_num[name] = len(sheet)
            elif not sheet.empty:
                transaction_num[name] += len(sheet)
                # Do not let to have empty values - pd.concat cannot handle it anymore
                if merged[name][-1].empty:
                    merged[name][-1] = sheet
                else:
                    merged[name].append(sheet)

    return merged, transaction_num


def write_files(output, merged, transaction_num):
    logger.info("Concatenating files and writing out the result")
    stat_str = "Number of transactions processed:\n"

    with pd.ExcelWriter(
        output, engine="xlsxwriter", datetime_format="YYYY-MM-DD"
    ) as writer:
        transaction_sum = 0
        for name, sheets in merged.items():
            stat_str += f"• {name.replace('_', ' ').title()}: {transaction_num[name]}\n"
            transaction_sum += transaction_num[name]

            concat = (
                sheets[0] if len(sheets) == 1 else pd.concat(sheets, ignore_index=True)
            )

            duplicates = concat[concat.duplicated()]
            if not duplicates.empty:
                warn = f"The following duplicate entries were found in sheet \"{name}\":\n"
                warn += duplicates.to_string(index=False, justify="center")
                logger.warning(warn)

            concat.to_excel(writer, sheet_name=name, index=False)
            worksheet = writer.sheets[name]
            worksheet.autofit()  # Adjust column widths to their maximum lengths

        stat_str += f"• Σ: {transaction_sum}"
        logger.info(stat_str)


def main(arguments):
    logging.basicConfig(level=arguments.log_level, format="[%(levelname)-8s] %(message)s")

    merged, transaction_num = read_files(arguments.spreadsheets)
    write_files(arguments.output, merged, transaction_num)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
