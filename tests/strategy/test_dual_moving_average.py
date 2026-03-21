"""test_dual_moving_average 测试用例。"""
from __future__ import annotations

import pandas as pd

from quant_system.strategy import DualMovingAverageStrategy, StrategyConfig, StrategyRunner


def test_dual_moving_average_signal_correctness() -> None:
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2024-01-02 09:30:00+00:00",
                    "2024-01-03 09:30:00+00:00",
                    "2024-01-04 09:30:00+00:00",
                    "2024-01-05 09:30:00+00:00",
                ]
            ),
            "symbol": ["000001.SZ"] * 4,
            "close": [10.0, 11.0, 12.0, 13.0],
        }
    )

    strategy = DualMovingAverageStrategy(
        StrategyConfig(
            source="dma",
            params={"short_window": 2, "long_window": 3},
            rebalance="daily",
            warmup_bars=0,
        )
    )

    result = StrategyRunner(strategy).run(bars=bars, universe=["000001.SZ"])

    assert len(result.signals) == 2
    assert all(signal.symbol == "000001.SZ" for signal in result.signals)
    assert all(signal.direction == "long" for signal in result.signals)
    assert all(signal.strength > 0 for signal in result.signals)
