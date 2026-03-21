"""test_ashare_backtest_integration 测试用例。"""
from __future__ import annotations

import pandas as pd

from quant_system.backtest import BacktestConfig, run_backtest_with_provider
from quant_system.data import AshareDataProvider
from quant_system.strategy import DualMovingAverageStrategy, StrategyConfig


def _mock_fetcher_trending(
    symbol: str,
    timeframe: str,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
) -> pd.DataFrame:
    del timeframe, start, end
    if symbol == "000001.SZ":
        closes = [10.0, 10.2, 10.4, 10.6, 10.8]
    else:
        closes = [20.0, 20.3, 20.6, 20.9, 21.2]

    return pd.DataFrame(
        {
            "date": [
                "2024-01-02",
                "2024-01-03",
                "2024-01-04",
                "2024-01-05",
                "2024-01-08",
            ],
            "open": closes,
            "high": [x + 0.1 for x in closes],
            "low": [x - 0.1 for x in closes],
            "close": closes,
            "volume": [100000, 110000, 120000, 130000, 140000],
            "amount": [v * p for v, p in zip([100000, 110000, 120000, 130000, 140000], closes)],
        }
    )


def test_run_backtest_with_mocked_ashare_provider_generates_trades() -> None:
    provider = AshareDataProvider(fetcher=_mock_fetcher_trending)
    strategy = DualMovingAverageStrategy(
        StrategyConfig(
            source="ashare_integration_test",
            warmup_bars=0,
            rebalance="daily",
            missing_data="skip_symbol",
            params={"short_window": 2, "long_window": 3},
        )
    )

    result = run_backtest_with_provider(
        provider=provider,
        strategy=strategy,
        symbols=["000001.SZ", "600000.SH"],
        config=BacktestConfig(
            initial_cash=100_000.0,
            fill_mode="current_close",
            signal_position_size=100.0,
            annualization_factor=252,
        ),
        start="2024-01-02",
        end="2024-01-08 23:59:59",
        timeframe="1d",
    )

    assert len(result.fills) > 0
    assert result.metrics["total_trades"] > 0
    assert not result.equity_curve.empty
    assert "cumulative_return" in result.summary()
