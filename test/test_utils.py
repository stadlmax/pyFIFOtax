import pytest
import numpy as np
from numpy.testing import assert_allclose

from pyfifotax.report_data import ReportData
from pyfifotax.utils import summarize_report


def get_elster_summary(
    file_name,
    year,
    mode,
    apply_stock_splits=True,
):
    # "legacy" transactions don't consider stock splits
    legacy_mode = "legacy" in file_name
    apply_stock_splits = apply_stock_splits and not legacy_mode

    report = ReportData(
        sub_dir="test/files",
        file_name=file_name,
        apply_stock_splits=apply_stock_splits,
    )

    dfs = report.consolidate_report(year, mode)
    summary = summarize_report(*dfs)
    return summary[summary.columns[2]].values.astype(np.float64)


order_file_names_legacy = [
    "order_random_legacy.xlsx",
    "order_ascending_legacy.xlsx",
    "order_descending_legacy.xlsx",
]


@pytest.mark.parametrize("file_name", order_file_names_legacy)
def test_summarize_report_order(file_name):
    with pytest.deprecated_call():
        summary = get_elster_summary(file_name, 2022, "daily")
        assert_allclose(summary, [-297.27, 48.35, 347.51, 0.29, 0, 0])


example_outputs = [
    ("daily", [914.25, 974.86, 247.01, 27.96, 29.9, 66.64]),
    ("monthly_avg", [829.32, 932.75, 294.1, 28.6, 29.54, 15.89]),
]


@pytest.mark.parametrize("mode, desired", example_outputs)
def test_summarize_report_example_legacy(mode, desired: list):
    with pytest.deprecated_call():
        summary = get_elster_summary("example_legacy.xlsx", 2022, mode)
        assert_allclose(summary, desired)


@pytest.mark.parametrize("mode, desired", example_outputs)
def test_summarize_report_example(mode, desired: list):
    summary = get_elster_summary("example.xlsx", 2022, mode, apply_stock_splits=False)
    assert_allclose(summary, desired)


example_stock_split_outputs = [
    ("daily", [6728.57, 6691.20, 149.03, 27.96, 29.9, 66.64]),
    ("monthly_avg", [6650.94, 6640.13, 179.86, 28.6, 29.54, 15.89]),
]


@pytest.mark.parametrize("mode, desired", example_stock_split_outputs)
def test_summarize_report_example_stock_splits(mode, desired: list):
    summary = get_elster_summary("example.xlsx", 2022, mode, apply_stock_splits=True)
    assert_allclose(summary, desired)


def test_summarize_forex_simple_legacy():
    with pytest.deprecated_call():
        summary = get_elster_summary("forex_simple_legacy.xlsx", 2022, "daily")
        assert_allclose(summary, [0, 0, 0, 0, 16, 0])


def test_summarize_forex_next_exchange_date_legacy():
    # Earliest imported exchange date: 2009-01-02
    with pytest.deprecated_call():
        summary = get_elster_summary(
            "forex_next_exchange_date_legacy.xlsx", 2022, "daily"
        )
        assert_allclose(summary, [0, 0, 0, 0, 0, 0])


exception_outputs_legacy = [
    (
        "forex_not_enough_currency_in_the_end_legacy.xlsx",
        r"Cannot convert more USD \(.+\) than owned overall \(.+\).",
    ),
    (
        "forex_not_enough_currency_inbetween_legacy.xlsx",
        r"Cannot convert more USD \(.+\) than owned overall \(.+\).",
    ),
    (
        "forex_nonexistent_exchange_date_legacy.xlsx",
        r".+exchange rate cannot be found.+or for the following seven days",
    ),
]


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize("file_name, error_msg", exception_outputs_legacy)
def test_summarize_exception_legacy(file_name, error_msg):
    with pytest.raises(ValueError, match=error_msg):
        get_elster_summary(file_name, 2022, "daily")
