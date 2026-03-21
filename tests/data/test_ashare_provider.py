"""test_ashare_provider 测试用例。"""
from __future__ import annotations

import pandas as pd
import pytest

from quant_system.data import AshareDataProvider
from quant_system.data.exceptions import DataConfigError, DataLoadError, DataValidationError


def _mock_fetcher_ok(
    symbol: str,
    timeframe: str,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
) -> pd.DataFrame:
    del timeframe, start, end
    if symbol == "000001.SZ":
        return pd.DataFrame(
            {
                "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
                "open": [10.0, 10.1, 10.2],
                "high": [10.2, 10.3, 10.4],
                "low": [9.9, 10.0, 10.1],
                "close": [10.1, 10.2, 10.3],
                "volume": [100000, 120000, 110000],
                "amount": [1010000, 1224000, 1133000],
            }
        )
    return pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "open": [20.0, 20.1, 20.2],
            "high": [20.3, 20.4, 20.5],
            "low": [19.8, 19.9, 20.0],
            "close": [20.1, 20.2, 20.3],
            "volume": [80000, 82000, 90000],
            "amount": [1608000, 1656400, 1827000],
            "停牌": ["正常", "停牌", "正常"],
        }
    )


def test_load_bars_normalizes_schema_timezone_and_symbols() -> None:
    provider = AshareDataProvider(fetcher=_mock_fetcher_ok)

    bars = provider.load_bars(
        symbols=["000001.SZ", "sh600000"],
        start="2024-01-02",
        end="2024-01-04 23:59:59",
        timeframe="1d",
    )

    assert len(bars) == 6
    assert list(bars.columns) == [
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "adj_factor",
        "is_suspended",
    ]
    assert set(bars["symbol"].unique().tolist()) == {"000001.SZ", "600000.SH"}

    assert str(bars.iloc[0]["timestamp"].tz) == "UTC"
    first_date = bars.iloc[0]["timestamp"]
    assert first_date.isoformat().startswith("2024-01-01T16:00:00+00:00")


def test_get_calendar_and_latest_bar_from_cache() -> None:
    provider = AshareDataProvider(fetcher=_mock_fetcher_ok)
    provider.load_bars(symbols=["000001.SZ"], start="2024-01-02", end="2024-01-04")

    calendar = provider.get_calendar(start="2024-01-03", end="2024-01-04")
    assert len(calendar) == 2

    latest = provider.get_latest_bar_asof("000001.SZ", "2024-01-03 23:59:59+00:00")
    assert latest is not None
    assert latest["symbol"] == "000001.SZ"


def test_validate_dataset_strict_mode_raises() -> None:
    def bad_fetcher(
        symbol: str,
        timeframe: str,
        start: pd.Timestamp | None,
        end: pd.Timestamp | None,
    ) -> pd.DataFrame:
        del symbol, timeframe, start, end
        return pd.DataFrame(
            {
                "date": ["2024-01-02", "2024-01-02"],
                "open": [10.0, 10.1],
                "high": [10.2, 10.3],
                "low": [9.9, 10.0],
                "close": [10.1, 10.2],
                "volume": [100000, 100200],
            }
        )

    provider = AshareDataProvider(fetcher=bad_fetcher)
    provider.load_bars(symbols=["000001.SZ"], start="2024-01-02", end="2024-01-03")

    result = provider.validate_dataset()
    assert not result.is_valid
    assert any("Duplicate (symbol, timestamp)" in error for error in result.errors)


def test_register_dataset_not_supported() -> None:
    provider = AshareDataProvider(fetcher=_mock_fetcher_ok)
    with pytest.raises(DataConfigError):
        provider.register_dataset("x", {})


def test_load_bars_requires_symbols() -> None:
    provider = AshareDataProvider(fetcher=_mock_fetcher_ok)
    with pytest.raises(DataLoadError):
        provider.load_bars(symbols=None)


def test_get_before_load_raises() -> None:
    provider = AshareDataProvider(fetcher=_mock_fetcher_ok)
    with pytest.raises(DataLoadError):
        provider.get_calendar()
    with pytest.raises(DataLoadError):
        provider.get_latest_bar_asof("000001.SZ", "2024-01-03")
    with pytest.raises(DataLoadError):
        provider.validate_dataset()
