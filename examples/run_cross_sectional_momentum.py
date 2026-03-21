"""run_cross_sectional_momentum 示例脚本。"""
from __future__ import annotations

from pathlib import Path

from quant_system.data import DataCatalog, LocalFileDataProvider
from quant_system.strategy import (
    CrossSectionalMomentumStrategy,
    StrategyConfig,
    run_strategy_with_provider,
)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    catalog = DataCatalog(catalog_path=root / "data" / "catalog" / "datasets.example.json")
    provider = LocalFileDataProvider(catalog=catalog)

    strategy = CrossSectionalMomentumStrategy(
        StrategyConfig(
            source="cross_sectional_mom_example",
            warmup_bars=3,
            rebalance="daily",
            missing_data="skip_symbol",
            params={"lookback_periods": 2, "top_k": 1},
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

    print("Target count:", len(result.targets))
    for target in result.targets:
        print(
            {
                "timestamp": target.timestamp.isoformat(),
                "symbol": target.symbol,
                "target_weight": target.target_weight,
                "source": target.source,
                "rank": target.metadata.get("rank"),
                "momentum_return": round(target.metadata.get("momentum_return", 0.0), 6),
            }
        )


if __name__ == "__main__":
    main()
