import argparse

parser = argparse.ArgumentParser(
    description="Convert Interactive Brokers CSV and Schwab JSON output to XLSX for later processing"
)
parser.add_argument(
    "type",
    type=str,
    choices=["ibkr", "schwab"],
    help="Used broker",
)
parser.add_argument(
    "-i",
    "--input",
    dest="input_filename",
    type=str,
    required=True,
    help="Input file (CSV file from Interactive Brokers or JSON from Schwab)",
)
parser.add_argument(
    "-o",
    "--output",
    dest="xlsx_filename",
    type=str,
    required=True,
    help="Output XLSX file",
)
parser.add_argument(
    "--ticker-to-isin",
    dest="ticker_to_isin",
    type=bool,
    default=False,
    action=argparse.BooleanOptionalAction,
    help="Replace tickers in the 'symbol' column to ISIN (only for IBKR)",
)
parser.add_argument(
    "--forex_transfer_as_exchange",
    action="store_true",
    help=(
        "If set, treats outgoing wire transfers as currency exchange to EUR."
        " This can be helpful to simplify the reporting of currency conversions"
        " if this is the only style of transfer. Please check the actual date"
        " of conversion and for correctness in general!"
        "\n(Only for Schwab)"
    ),
)


def main(arguments):
    if arguments.type == "ibkr":
        from converters.ibkr import IbkrConverter
        converter = IbkrConverter(arguments)
        converter.process_csv()
        converter.write_to_xlsx()
    elif arguments.type == "schwab":
        from converters.schwab import convert
        convert(arguments)
    else:
        raise ValueError("This type of converter is not recognised")


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
