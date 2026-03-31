"""test_ashare_downloader 测试用例。"""
from __future__ import annotations

import pandas as pd
import pytest

from quant_system.data import AshareDownloader, DataCatalog, LocalFileDataProvider
from quant_system.data.exceptions import DataConfigError


def _mock_fetcher(
    symbol: str,
    timeframe: str,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
) -> pd.DataFrame:
    del timeframe, start, end
    if symbol == "000001.SZ":
        return pd.DataFrame(
            {
                "date": ["2024-01-02", "2024-01-03"],
                "open": [10.0, 10.1],
                "high": [10.2, 10.3],
                "low": [9.9, 10.0],
                "close": [10.1, 10.2],
                "volume": [100000, 120000],
                "amount": [1010000, 1224000],
            }
        )
    return pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "open": [20.0, 20.1],
            "high": [20.3, 20.4],
            "low": [19.8, 19.9],
            "close": [20.1, 20.2],
            "volume": [80000, 82000],
            "停牌": ["正常", "停牌"],
        }
    )


def test_download_parquet_success_with_fields(tmp_path) -> None:
    pyarrow = pytest.importorskip("pyarrow")
    assert pyarrow is not None

    downloader = AshareDownloader(fetcher=_mock_fetcher)
    output = tmp_path / "ashare.parquet"

    result = downloader.download(
        symbols=["000001.SZ", "sh600000"],
        start="2024-01-02",
        end="2024-01-03",
        output_path=output,
        fields=[
            "timestamp",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
        ],
    )

    assert output.exists()
    assert result.row_count == 4
    assert result.output_format == "parquet"
    assert result.provider_used == "akshare"
    assert result.columns == [
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    ]

    saved = pd.read_parquet(output)
    assert list(saved.columns) == result.columns
    assert sorted(saved["symbol"].unique().tolist()) == ["000001.SZ", "600000.SH"]


def test_download_rejects_unknown_fields(tmp_path) -> None:
    downloader = AshareDownloader(fetcher=_mock_fetcher)
    with pytest.raises(DataConfigError):
        downloader.download(
            symbols=["000001.SZ"],
            output_path=tmp_path / "x.parquet",
            fields=["timestamp", "symbol", "open", "bad_field"],
        )


def test_download_rejects_missing_required_fields(tmp_path) -> None:
    downloader = AshareDownloader(fetcher=_mock_fetcher)
    with pytest.raises(DataConfigError):
        downloader.download(
            symbols=["000001.SZ"],
            output_path=tmp_path / "x.parquet",
            fields=["timestamp", "symbol", "open", "high", "low", "close"],
        )


def test_download_overwrite_flag(tmp_path) -> None:
    pyarrow = pytest.importorskip("pyarrow")
    assert pyarrow is not None

    downloader = AshareDownloader(fetcher=_mock_fetcher)
    output = tmp_path / "ashare.parquet"

    downloader.download(symbols=["000001.SZ"], output_path=output)
    with pytest.raises(DataConfigError):
        downloader.download(symbols=["000001.SZ"], output_path=output, overwrite=False)

    downloader.download(symbols=["000001.SZ"], output_path=output, overwrite=True)


def test_download_output_readable_by_local_provider(tmp_path) -> None:
    pyarrow = pytest.importorskip("pyarrow")
    assert pyarrow is not None

    output = tmp_path / "ashare.parquet"
    downloader = AshareDownloader(fetcher=_mock_fetcher)
    downloader.download(symbols=["000001.SZ", "sh600000"], output_path=output)

    catalog = DataCatalog(catalog_path=tmp_path / "datasets.json")
    catalog.register_dataset(
        "downloaded",
        {
            "path": str(output),
            "format": "parquet",
            "timeframe": "1d",
            "timezone": "UTC",
            "column_mapping": {},
        },
    )

    provider = LocalFileDataProvider(catalog=catalog)
    bars = provider.load_bars(symbols=["000001.SZ", "600000.SH"], dataset_name="downloaded")

    assert len(bars) == 4
    assert sorted(bars["symbol"].unique().tolist()) == ["000001.SZ", "600000.SH"]
