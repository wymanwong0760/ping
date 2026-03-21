"""load_single_symbol 示例脚本。"""
from __future__ import annotations

import logging
from pathlib import Path

from quant_system.data import DataCatalog, LocalFileDataProvider

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    catalog_path = root / "data" / "catalog" / "datasets.example.json"

    catalog = DataCatalog(catalog_path=catalog_path)
    provider = LocalFileDataProvider(catalog=catalog)

    bars = provider.load_bars(symbols=["000001.SZ"], dataset_name="sample_single_csv")

    print("Loaded rows:", len(bars))
    print("Columns:", list(bars.columns))
    print(bars.head().to_string(index=False))


if __name__ == "__main__":
    main()
