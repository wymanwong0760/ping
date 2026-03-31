"""下载 A 股数据到本地 parquet 示例。"""
from __future__ import annotations

from quant_system.data import download_ashare_bars


if __name__ == "__main__":
    result = download_ashare_bars(
        symbols=["000001.SZ", "600000.SH"],
        start="2024-01-01",
        end="2024-03-01",
        output_path="data/raw/ashare_000001_600000.parquet",
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
        output_format="parquet",
        overwrite=True,
    )
    print(result)
