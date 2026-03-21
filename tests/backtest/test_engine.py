"""test_engine 测试用例。"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from quant_system.backtest import BacktestConfig, BacktestEngine
from quant_system.core import Signal
from quant_system.risk import RiskConfig, RiskEngine


def _bars_multi() -> pd.DataFrame:
    rows = [
        {
            "timestamp": "2024-01-02 09:30:00+00:00",
            "symbol": "000001.SZ",
            "open": 10.0,
            "high": 10.2,
            "low": 9.9,
            "close": 10.1,
        },
        {
            "timestamp": "2024-01-02 09:30:00+00:00",
            "symbol": "000002.SZ",
            "open": 20.0,
            "high": 20.5,
            "low": 19.8,
            "close": 20.1,
        },
        {
            "timestamp": "2024-01-03 09:30:00+00:00",
            "symbol": "000001.SZ",
            "open": 10.3,
            "high": 10.6,
            "low": 10.2,
            "close": 10.5,
        },
        {
            "timestamp": "2024-01-03 09:30:00+00:00",
            "symbol": "000002.SZ",
            "open": 20.4,
            "high": 20.8,
            "low": 20.2,
            "close": 20.6,
        },
        {
            "timestamp": "2024-01-04 09:30:00+00:00",
            "symbol": "000001.SZ",
            "open": 10.4,
            "high": 10.7,
            "low": 10.3,
            "close": 10.6,
        },
        {
            "timestamp": "2024-01-04 09:30:00+00:00",
            "symbol": "000002.SZ",
            "open": 20.5,
            "high": 20.9,
            "low": 20.3,
            "close": 20.7,
        },
    ]
    return pd.DataFrame(rows)


def _signal(ts: str, symbol: str, direction: str) -> Signal:
    return Signal(
        timestamp=datetime.fromisoformat(ts.replace("Z", "+00:00")),
        symbol=symbol,
        direction=direction,  # type: ignore[arg-type]
        strength=1.0,
        source="test",
    )


def test_next_open_mode_respects_execution_timing() -> None:
    bars = _bars_multi()
    signals = [
        _signal("2024-01-02T09:30:00+00:00", "000001.SZ", "long"),
        _signal("2024-01-03T09:30:00+00:00", "000001.SZ", "flat"),
    ]

    engine = BacktestEngine(BacktestConfig(initial_cash=1_000.0, signal_position_size=10.0))
    result = engine.run(bars=bars, signals=signals)

    assert len(result.fills) == 2
    assert result.fills[0].timestamp == datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc)
    assert result.fills[0].price == pytest.approx(10.3)
    assert result.fills[1].timestamp == datetime(2024, 1, 4, 9, 30, tzinfo=timezone.utc)
    assert result.fills[1].side == "sell"


def test_multi_symbol_backtest_runs() -> None:
    bars = _bars_multi()
    signals = [
        _signal("2024-01-02T09:30:00+00:00", "000001.SZ", "long"),
        _signal("2024-01-02T09:30:00+00:00", "000002.SZ", "long"),
        _signal("2024-01-03T09:30:00+00:00", "000001.SZ", "flat"),
        _signal("2024-01-03T09:30:00+00:00", "000002.SZ", "flat"),
    ]

    engine = BacktestEngine(BacktestConfig(initial_cash=10_000.0, signal_position_size=10.0))
    result = engine.run(bars=bars, signals=signals)

    assert len(result.snapshots) == 3
    assert len(result.fills) == 4
    assert "cumulative_return" in result.metrics
    assert "max_drawdown" in result.metrics


def test_backtest_with_risk_engine_runs_and_records_decisions() -> None:
    bars = _bars_multi().copy()
    bars["is_suspended"] = False
    bars.loc[(bars["timestamp"] == "2024-01-02 09:30:00+00:00") & (bars["symbol"] == "000002.SZ"), "is_suspended"] = True

    signals = [
        _signal("2024-01-02T09:30:00+00:00", "000001.SZ", "long"),
        _signal("2024-01-02T09:30:00+00:00", "000002.SZ", "long"),
    ]

    risk_config = RiskConfig()
    risk_config.max_symbol_position.max_abs_qty = 8.0
    risk_config.max_symbol_position.max_weight = None
    engine = BacktestEngine(
        BacktestConfig(initial_cash=10_000.0, signal_position_size=10.0),
        risk_engine=RiskEngine(risk_config),
    )
    result = engine.run(bars=bars, signals=signals)

    assert result.risk_decisions
    assert result.risk_audit_logs
    symbols = {item.symbol for item in result.risk_decisions}
    assert {"000001.SZ", "000002.SZ"}.issubset(symbols)
    by_symbol = {item.symbol: item for item in result.risk_decisions}
    assert by_symbol["000001.SZ"].action == "modify"
    assert by_symbol["000002.SZ"].action == "reject"
