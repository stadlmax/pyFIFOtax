import logging

import numpy as np
import pytest
from numpy.testing import assert_allclose

from pyfifotax.report_data import ReportData
from pyfifotax.utils import summarize_report


def get_elster_summary(
    file_name,
    year,
    mode,
    apply_stock_splits=True,
    consider_tax_free_forex=True,
):
    # "legacy" transactions don't consider stock splits
    legacy_mode = "legacy" in file_name
    apply_stock_splits = apply_stock_splits and not legacy_mode

    report = ReportData(
        sub_dir="test/files",
        file_name=file_name,
        apply_stock_splits=apply_stock_splits,
    )

    dfs = report.consolidate_report(
        year, mode, consider_tax_free_forex=consider_tax_free_forex
    )
    summary = summarize_report(*dfs)
    return summary[summary.columns[2]].values.astype(np.float64)


order_file_names_legacy = [
    "order_random_legacy.xlsx",
    "order_ascending_legacy.xlsx",
    "order_descending_legacy.xlsx",
]


@pytest.mark.parametrize("file_name", order_file_names_legacy)
def test_summarize_report_order(file_name, caplog):
    with caplog.at_level(logging.WARNING):
        summary = get_elster_summary(file_name, 2022, "daily")
        assert "legacy" in caplog.text
        assert_allclose(summary, [-297.27, 48.35, 347.51, 0.29, 0, 0, 0, 0])


example_outputs = [
    # slight change of test values as fees for buy/sell events
    # are now considered as part of capital gains
    # >>> ("daily", [914.25, 974.86, 247.01, 27.96, 29.9, 66.64]),
    # >>> ("monthly", [829.32, 932.75, 294.1, 28.6, 29.54, 15.89]),
    # slight change again as popping fees changes forex gains
    # >>> ("daily", [914.14, 974.81, 247.07, 27.96, 29.80, 66.64]),
    # >>> ("monthly", [829.22, 932.71, 294.16, 28.60, 29.44, 15.89]),
    (
        "daily",
        False,
        [914.14, 974.81, 247.07, 27.96, 29.80, 69.70, 10296.5, 10226.78],
    ),
    (
        "monthly",
        False,
        [829.22, 932.71, 294.16, 28.60, 29.44, 18.24, 10170.24, 10152.0],
    ),
    (
        "daily",
        True,
        [914.14, 974.81, 247.07, 27.96, 29.80, 50.04, 10090.44, 10040.38],
    ),
    (
        "monthly",
        True,
        [829.22, 932.71, 294.16, 28.60, 29.44, 5.37, 9966.70, 9961.33],
    ),
]


@pytest.mark.parametrize("mode, consider_tax_free_forex, desired", example_outputs)
def test_summarize_report_example_legacy(mode, consider_tax_free_forex, desired, caplog):
    with caplog.at_level(logging.WARNING):
        summary = get_elster_summary(
            "example_legacy.xlsx",
            2022,
            mode,
            consider_tax_free_forex=consider_tax_free_forex,
        )
        assert "legacy" in caplog.text
        assert_allclose(summary, desired)


@pytest.mark.parametrize("mode, consider_tax_free_forex, desired", example_outputs)
def test_summarize_report_example(mode, consider_tax_free_forex, desired):
    summary = get_elster_summary(
        "example.xlsx",
        2022,
        mode,
        apply_stock_splits=False,
        consider_tax_free_forex=consider_tax_free_forex,
    )
    assert_allclose(summary, desired)


example_stock_split_outputs = [
    # slight change of test values as fees for buy/sell events
    # are now considered as part of capital gains
    # >>> >>> ("daily", [6728.57, 6691.20, 149.03, 27.96, 29.9, 66.64]),
    # >>> >>> ("monthly", [6650.94, 6640.13, 179.86, 28.6, 29.54, 15.89]),
    # >>> ("daily", [6728.47, 6691.13, 149.06, 27.96, 29.8, 66.64]),
    # >>> ("monthly", [6650.83, 6640.05, 179.89, 28.6, 29.44, 15.89]),
    # previous values did not consider stock splits for AAPL properly
    # ("daily", [7200.49, 7112.91, 98.82, 27.96, 29.8, 69.7, 10296.5, 10226.78])
    # ("monthly", [7122.44, 7041.58, 109.81, 28.6, 29.44, 18.24, 10170.24, 10152.0])
    (
        "daily",
        False,
        # [6728.47, 6691.13, 149.06, 27.96, 29.8, 69.70, 10296.5, 10226.78],
        [7200.49, 7112.91, 98.82, 27.96, 29.8, 69.7, 10296.5, 10226.78],
    ),
    (
        "monthly",
        False,
        # [6650.83, 6640.05, 179.89, 28.6, 29.44, 18.24, 10170.24, 10152.0],
        [7122.44, 7041.58, 109.81, 28.6, 29.44, 18.24, 10170.24, 10152.0],
    ),
    (
        "daily",
        True,
        # [6728.47, 6691.13, 149.06, 27.96, 29.8, 50.04, 10090.44, 10040.38],
        [7200.49, 7112.91, 98.82, 27.96, 29.8, 50.04, 10090.44, 10040.38],
    ),
    (
        "monthly",
        True,
        # [6650.83, 6640.05, 179.89, 28.6, 29.44, 5.37, 9966.70, 9961.33],
        [7122.44, 7041.58, 109.81, 28.6, 29.44, 5.37, 9966.70, 9961.33],
    ),
]


@pytest.mark.parametrize(
    "mode, consider_tax_free_forex, desired", example_stock_split_outputs
)
def test_summarize_report_example_stock_splits(mode, consider_tax_free_forex, desired):
    summary = get_elster_summary(
        "example.xlsx",
        2022,
        mode,
        apply_stock_splits=True,
        consider_tax_free_forex=consider_tax_free_forex,
    )
    assert_allclose(summary, desired)


file_names_partial_tests = [
    "example_rsu.xlsx",
    "example_espp.xlsx",
]


@pytest.mark.parametrize("file_name", file_names_partial_tests)
def test_example_partial(file_name):
    for year in range(2019, 2024 + 1):
        for mode in ("daily", "monthly"):
            summary = get_elster_summary(file_name, year, mode)
            assert_allclose(summary, [0, 0, 0, 0, 0, 0, 0, 0])


def test_summarize_forex_simple_legacy(caplog):
    with caplog.at_level(logging.WARNING):
        summary = get_elster_summary("forex_simple_legacy.xlsx", 2022, "daily")
        assert "legacy" in caplog.text
        assert_allclose(summary, [-1, 0, 1, 0, 15, 0, 3002.8, 3002.8])


def test_summarize_forex_next_exchange_date_legacy(caplog):
    # Earliest imported exchange date: 2009-01-02
    with caplog.at_level(logging.WARNING):
        summary = get_elster_summary(
            "forex_next_exchange_date_legacy.xlsx", 2022, "daily"
        )
        assert "legacy" in caplog.text
        assert_allclose(summary, [0, 0, 0, 0, 0, 0, 97.69, 97.69])


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


@pytest.mark.parametrize("file_name, error_msg", exception_outputs_legacy)
def test_summarize_exception_legacy(file_name, error_msg):
    with pytest.raises(ValueError, match=error_msg):
        get_elster_summary(file_name, 2022, "daily")


def test_negative_dividend():
    summary = get_elster_summary("negative_dividend.xlsx", 2018, "monthly")
    assert_allclose(summary, [43.54, 0, 0, 43.54, 0, 0, 0, 0])


def test_empty():
    try:
        get_elster_summary("example_empty.xlsx", 2022, "daily")
    except Exception as e:
        raise pytest.fail(f"EMPTY TEST FAILED, DID RAISE {e}")


def test_empty_legacy(caplog):
    try:
        with caplog.at_level(logging.WARNING):
            get_elster_summary("example_legacy_empty.xlsx", 2022, "daily")
            assert "legacy" in caplog.text
    except Exception as e:
        raise pytest.fail(f"EMPTY TEST FAILED, DID RAISE {e}")


just_test_success_files = [
    ("example_legacy_empty.xlsx", True),
    ("example_empty.xlsx", False),
    ("forex_deposit_convert_withdraw_same_day.xlsx", False),
    ("forex_first.xlsx", False),
    ("sell_others_first.xlsx", False),
]


@pytest.mark.parametrize("file_name, legacy_mode", just_test_success_files)
def test_success(file_name, legacy_mode, caplog):
    try:
        if legacy_mode:
            with caplog.at_level(logging.WARNING):
                get_elster_summary("example_legacy_empty.xlsx", 2022, "daily")
                assert "legacy" in caplog.text
        else:
            get_elster_summary(file_name, 2022, "daily")
    except Exception as e:
        raise pytest.fail(f"SUCCESS TEST FOR {file_name} FAILED, DID RAISE {e}")
