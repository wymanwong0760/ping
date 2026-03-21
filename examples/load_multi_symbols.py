"""load_multi_symbols 示例脚本。"""
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

    bars = provider.load_bars(
        symbols=["000001.SZ", "000002.SZ"],
        start="2024-01-02",
        end="2024-01-04 23:59:59",
        dataset_name="sample_multi_csv",
    )

    print("Available symbols:", provider.get_available_symbols("sample_multi_csv"))
    print("Loaded rows:", len(bars))
    print(bars.to_string(index=False))


if __name__ == "__main__":
    main()
