"""show_strategy_outputs 示例脚本。"""
from __future__ import annotations

from pathlib import Path

from quant_system.data import DataCatalog, LocalFileDataProvider
from quant_system.strategy import (
    CrossSectionalMomentumStrategy,
    DualMovingAverageStrategy,
    StrategyConfig,
    run_strategy_with_provider,
)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    catalog = DataCatalog(catalog_path=root / "data" / "catalog" / "datasets.example.json")
    provider = LocalFileDataProvider(catalog=catalog)

    dma = DualMovingAverageStrategy(
        StrategyConfig(
            source="dual_ma_showcase",
            warmup_bars=3,
            rebalance="daily",
            params={"short_window": 2, "long_window": 3},
        )
    )
    dma_result = run_strategy_with_provider(
        provider=provider,
        strategy=dma,
        symbols=["000001.SZ", "000002.SZ"],
        start="2024-01-02",
        end="2024-01-04 23:59:59",
        dataset_name="sample_multi_csv",
    )

    momentum = CrossSectionalMomentumStrategy(
        StrategyConfig(
            source="momentum_showcase",
            warmup_bars=3,
            rebalance="daily",
            params={"lookback_periods": 2, "top_k": 1},
        )
    )
    momentum_result = run_strategy_with_provider(
        provider=provider,
        strategy=momentum,
        symbols=["000001.SZ", "000002.SZ"],
        start="2024-01-02",
        end="2024-01-04 23:59:59",
        dataset_name="sample_multi_csv",
    )

    print("=== Signals ===")
    for signal in dma_result.signals:
        print(signal)

    print("=== Targets ===")
    for target in momentum_result.targets:
        print(target)


if __name__ == "__main__":
    main()
