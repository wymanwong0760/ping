"""test_local_file_provider 测试用例。"""
from __future__ import annotations

import pandas as pd
import pytest

from quant_system.core.schema import BAR_STANDARD_COLUMNS
from quant_system.data import DataCatalog, LocalFileDataProvider


def _build_catalog(tmp_path, entries: dict) -> DataCatalog:
    catalog_path = tmp_path / "datasets.json"
    catalog = DataCatalog(catalog_path=catalog_path)
    for name, metadata in entries.items():
        catalog.register_dataset(name, metadata)
    return catalog


def test_load_bars_from_csv_and_filter_symbols(tmp_path) -> None:
    csv_path = tmp_path / "bars.csv"
    src = pd.read_csv("tests/fixtures/data/bars_multi.csv")
    src.to_csv(csv_path, index=False)

    catalog = _build_catalog(
        tmp_path,
        {
            "bars_csv": {
                "path": str(csv_path),
                "format": "csv",
                "timeframe": "1d",
                "timezone": "Asia/Shanghai",
                "column_mapping": {},
            }
        },
    )
    provider = LocalFileDataProvider(catalog=catalog)

    bars = provider.load_bars(symbols=["000001.SZ"], dataset_name="bars_csv")

    assert list(bars.columns) == BAR_STANDARD_COLUMNS
    assert bars["symbol"].unique().tolist() == ["000001.SZ"]
    assert len(bars) == 3


def test_load_bars_from_parquet(tmp_path) -> None:
    parquet_path = tmp_path / "bars.parquet"
    src = pd.read_csv("tests/fixtures/data/bars_multi.csv")

    pyarrow = pytest.importorskip("pyarrow")
    assert pyarrow is not None
    src.to_parquet(parquet_path, index=False)

    catalog = _build_catalog(
        tmp_path,
        {
            "bars_parquet": {
                "path": str(parquet_path),
                "format": "parquet",
                "timeframe": "1d",
                "timezone": "Asia/Shanghai",
                "column_mapping": {},
            }
        },
    )
    provider = LocalFileDataProvider(catalog=catalog)

    bars = provider.load_bars(symbols=["000001.SZ", "000002.SZ"], dataset_name="bars_parquet")

    assert len(bars) == 6
    assert sorted(bars["symbol"].unique().tolist()) == ["000001.SZ", "000002.SZ"]


def test_column_mapping_and_default_symbol(tmp_path) -> None:
    csv_path = tmp_path / "bars_mapped.csv"
    src = pd.read_csv("tests/fixtures/data/bars_mapped.csv")
    src.to_csv(csv_path, index=False)

    catalog = _build_catalog(
        tmp_path,
        {
            "bars_mapped": {
                "path": str(csv_path),
                "format": "csv",
                "timeframe": "1d",
                "timezone": "Asia/Shanghai",
                "column_mapping": {
                    "trade_time": "timestamp",
                    "ticker": "symbol",
                    "open_px": "open",
                    "high_px": "high",
                    "low_px": "low",
                    "close_px": "close",
                    "vol": "volume",
                },
            }
        },
    )
    provider = LocalFileDataProvider(catalog=catalog)

    bars = provider.load_bars(symbols=["000001.SZ"], dataset_name="bars_mapped")

    assert list(bars.columns) == BAR_STANDARD_COLUMNS
    assert len(bars) == 3
    assert float(bars.iloc[0]["open"]) == pytest.approx(10.0)


def test_multi_symbols_and_time_range_filter(tmp_path) -> None:
    csv_path = tmp_path / "bars.csv"
    src = pd.read_csv("tests/fixtures/data/bars_multi.csv")
    src.to_csv(csv_path, index=False)

    catalog = _build_catalog(
        tmp_path,
        {
            "bars_csv": {
                "path": str(csv_path),
                "format": "csv",
                "timeframe": "1d",
                "timezone": "Asia/Shanghai",
                "column_mapping": {},
            }
        },
    )
    provider = LocalFileDataProvider(catalog=catalog)

    bars = provider.load_bars(
        symbols=["000001.SZ", "000002.SZ"],
        start="2024-01-03",
        end="2024-01-03 23:59:59",
        dataset_name="bars_csv",
    )

    assert len(bars) == 2
    assert sorted(bars["symbol"].unique().tolist()) == ["000001.SZ", "000002.SZ"]


def test_get_latest_bar_asof(tmp_path) -> None:
    csv_path = tmp_path / "bars.csv"
    src = pd.read_csv("tests/fixtures/data/bars_multi.csv")
    src.to_csv(csv_path, index=False)

    catalog = _build_catalog(
        tmp_path,
        {
            "bars_csv": {
                "path": str(csv_path),
                "format": "csv",
                "timeframe": "1d",
                "timezone": "Asia/Shanghai",
                "column_mapping": {},
            }
        },
    )
    provider = LocalFileDataProvider(catalog=catalog)

    row = provider.get_latest_bar_asof(
        symbol="000001.SZ",
        ts="2024-01-03 12:00:00",
        dataset_name="bars_csv",
    )

    assert row is not None
    assert row["symbol"] == "000001.SZ"
    assert str(row["timestamp"].date()) == "2024-01-03"
