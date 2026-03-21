"""test_metrics 测试用例。"""
from __future__ import annotations

import pandas as pd
import pytest

from quant_system.backtest.metrics import build_drawdown_series, compute_metrics


def test_max_drawdown_computation() -> None:
    equity = pd.Series(
        [1_000.0, 1_100.0, 1_050.0, 1_200.0, 900.0],
        index=pd.to_datetime(
            [
                "2024-01-02 09:30:00+00:00",
                "2024-01-03 09:30:00+00:00",
                "2024-01-04 09:30:00+00:00",
                "2024-01-05 09:30:00+00:00",
                "2024-01-08 09:30:00+00:00",
            ],
            utc=True,
        ),
    )

    drawdown = build_drawdown_series(equity)
    assert float(drawdown.min()) == pytest.approx(-0.25)


def test_metrics_are_computable() -> None:
    equity = pd.Series(
        [1_000.0, 1_010.0, 1_020.0, 1_005.0, 1_030.0],
        index=pd.to_datetime(
            [
                "2024-01-02 09:30:00+00:00",
                "2024-01-03 09:30:00+00:00",
                "2024-01-04 09:30:00+00:00",
                "2024-01-05 09:30:00+00:00",
                "2024-01-08 09:30:00+00:00",
            ],
            utc=True,
        ),
    )

    metrics = compute_metrics(
        equity_curve=equity,
        closed_trade_pnls=[10.0, -5.0, 12.0],
        total_turnover=4_000.0,
        total_fills=6,
        annualization_factor=252,
    )

    expected_keys = {
        "cumulative_return",
        "annualized_return",
        "annualized_volatility",
        "sharpe_ratio",
        "max_drawdown",
        "win_rate",
        "turnover",
        "total_trades",
    }
    assert expected_keys.issubset(metrics.keys())
    assert metrics["total_trades"] == pytest.approx(6.0)
    assert 0.0 <= metrics["win_rate"] <= 1.0
