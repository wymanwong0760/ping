"""test_catalog_calendar 测试用例。"""
from __future__ import annotations

import pandas as pd

from quant_system.data import DataCatalog, TradingCalendar


def test_catalog_register_and_reload(tmp_path) -> None:
    catalog_path = tmp_path / "datasets.json"
    catalog = DataCatalog(catalog_path=catalog_path)

    catalog.register_dataset(
        "bars_csv",
        {
            "path": "data/raw/sample_bars_multi.csv",
            "format": "csv",
            "timeframe": "1d",
            "timezone": "Asia/Shanghai",
            "column_mapping": {},
        },
    )

    reloaded = DataCatalog(catalog_path=catalog_path)
    assert reloaded.list_datasets() == ["bars_csv"]
    metadata = reloaded.get_dataset("bars_csv")
    assert metadata.file_format == "csv"
    assert metadata.timeframe == "1d"


def test_trading_calendar_filtering() -> None:
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2024-01-02 09:30:00+00:00",
                    "2024-01-03 09:30:00+00:00",
                    "2024-01-04 09:30:00+00:00",
                ]
            ),
            "symbol": ["000001.SZ", "000001.SZ", "000001.SZ"],
        }
    )
    calendar = TradingCalendar()

    ts = calendar.get_calendar(start="2024-01-03", end="2024-01-04", bars=bars)

    assert len(ts) == 2
    assert str(ts[0].date()) == "2024-01-03"
    assert str(ts[1].date()) == "2024-01-04"
