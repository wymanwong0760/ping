"""test_cross_sectional_momentum 测试用例。"""
from __future__ import annotations

import pandas as pd

from quant_system.strategy import (
    CrossSectionalMomentumStrategy,
    StrategyConfig,
    StrategyRunner,
)


def test_cross_sectional_momentum_ranking_output() -> None:
    timestamps = pd.to_datetime(
        [
            "2024-01-02 09:30:00+00:00",
            "2024-01-03 09:30:00+00:00",
            "2024-01-04 09:30:00+00:00",
            "2024-01-05 09:30:00+00:00",
        ]
    )
    bars = pd.DataFrame(
        {
            "timestamp": list(timestamps) * 3,
            "symbol": ["A"] * 4 + ["B"] * 4 + ["C"] * 4,
            "close": [10.0, 11.0, 12.0, 13.0, 10.0, 9.0, 8.0, 7.0, 10.0, 10.2, 10.4, 10.6],
        }
    )

    strategy = CrossSectionalMomentumStrategy(
        StrategyConfig(
            source="mom",
            rebalance="daily",
            params={"lookback_periods": 2, "top_k": 2},
        )
    )

    result = StrategyRunner(strategy).run(bars=bars, universe=["A", "B", "C"])

    assert result.targets
    latest_targets = [
        target
        for target in result.targets
        if str(target.timestamp.date()) == "2024-01-05"
    ]

    by_symbol = {target.symbol: target for target in latest_targets}
    assert by_symbol["A"].target_weight == 0.5
    assert by_symbol["C"].target_weight == 0.5
    assert by_symbol["B"].target_weight == 0.0
