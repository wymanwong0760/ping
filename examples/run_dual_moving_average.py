"""run_dual_moving_average 示例脚本。"""
from __future__ import annotations

from pathlib import Path

from quant_system.data import DataCatalog, LocalFileDataProvider
from quant_system.strategy import (
    DualMovingAverageStrategy,
    StrategyConfig,
    run_strategy_with_provider,
)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    catalog = DataCatalog(catalog_path=root / "data" / "catalog" / "datasets.example.json")
    provider = LocalFileDataProvider(catalog=catalog)

    strategy = DualMovingAverageStrategy(
        StrategyConfig(
            source="dual_ma_example",
            warmup_bars=3,
            rebalance="daily",
            missing_data="skip_symbol",
            params={"short_window": 2, "long_window": 3},
        )
    )

    result = run_strategy_with_provider(
        provider=provider,
        strategy=strategy,
        symbols=["000001.SZ", "000002.SZ"],
        start="2024-01-02",
        end="2024-01-04 23:59:59",
        dataset_name="sample_multi_csv",
    )

    print("Signal count:", len(result.signals))
    for signal in result.signals:
        print(
            {
                "timestamp": signal.timestamp.isoformat(),
                "symbol": signal.symbol,
                "direction": signal.direction,
                "strength": round(signal.strength, 6),
                "source": signal.source,
            }
        )


if __name__ == "__main__":
    main()
