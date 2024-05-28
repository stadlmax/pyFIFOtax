import pytest
import numpy as np
from numpy.testing import assert_allclose

from report_data import ReportData
from utils import summarize_report


def get_elster_summary(file_name, year, mode, apply_stock_splits=False):
    report = ReportData(
        sub_dir="test/files",
        file_name=file_name,
        stock_split_file_path="test/files/stock_splits.csv",
        apply_stock_splits=apply_stock_splits,
    )

    dfs = report.consolidate_report(year, mode)
    summary = summarize_report(*dfs)
    return summary[summary.columns[2]].values.astype(np.float64)


order_file_names = [
    "order_random.xlsx",
    "order_ascending.xlsx",
    "order_descending.xlsx",
]


@pytest.mark.parametrize("file_name", order_file_names)
def test_summarize_report_order(file_name):
    summary = get_elster_summary(file_name, 2022, "daily")
    assert_allclose(summary, [-297.27, 48.35, 347.51, 0.29, 6.01, 0])


example_outputs = [
    ("daily", [914.25, 974.86, 247.01, 27.96, 35.04, 67.67]),
    ("monthly_avg", [829.32, 932.75, 294.1, 28.6, 34.63, 16.86]),
]


example_stock_split_outputs = [
    ("daily", [6728.57, 6691.20, 149.03, 27.96, 35.04, 67.67]),
    ("monthly_avg", [6650.94, 6640.13, 179.86, 28.6, 34.63, 16.86]),
]


@pytest.mark.parametrize("mode, desired", example_outputs)
def test_summarize_report_example(mode, desired: list):
    summary = get_elster_summary("example.xlsx", 2022, mode)
    assert_allclose(summary, desired)


@pytest.mark.parametrize("mode, desired", example_stock_split_outputs)
def test_summarize_report_example_stock_splits(mode, desired: list):
    summary = get_elster_summary(
        "example_stock_splits.xlsx", 2022, mode, apply_stock_splits=True
    )
    assert_allclose(summary, desired)


def test_summarize_forex_simple():
    summary = get_elster_summary("forex_simple.xlsx", 2022, "daily")
    assert_allclose(summary, [0, 0, 0, 0, 19, 0])


exception_outputs = [
    (
        "forex_not_enough_currency_in_the_end.xlsx",
        r"Cannot convert more USD \(5000\) than owned overall.+",
    ),
    (
        "forex_not_enough_currency_inbetween.xlsx",
        r"Cannot sell the requested USD equity.+amount is not available",
    ),
]


@pytest.mark.parametrize("file_name, error_msg", exception_outputs)
def test_summarize_exception(file_name, error_msg):
    with pytest.raises(ValueError, match=error_msg):
        get_elster_summary(file_name, 2022, "daily")
