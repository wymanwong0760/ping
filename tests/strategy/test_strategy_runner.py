"""test_strategy_runner 测试用例。"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from quant_system.core import Signal
from quant_system.strategy import (
    MissingDataError,
    SignalStrategy,
    StrategyConfig,
    StrategyContext,
    StrategyRunner,
)


class _EchoSignalStrategy(SignalStrategy):
    """策略实现。"""
    def generate_signals(self, context: StrategyContext) -> list[Signal]:
        return [
            Signal(
                timestamp=context.timestamp.to_pydatetime(),
                symbol=symbol,
                direction="flat",
                strength=0.0,
                source=self.config.source,
            )
            for symbol in context.universe
        ]


class _ProbeNoLookaheadStrategy(SignalStrategy):
    """策略实现。"""
    def generate_signals(self, context: StrategyContext) -> list[Signal]:
        records = context.state.setdefault("records", [])
        records.append(
            {
                "timestamp": context.timestamp,
                "max_history_ts": context.bars["timestamp"].max(),
                "history_size": len(context.bars),
            }
        )
        symbol = context.universe[0]
        return [
            Signal(
                timestamp=context.timestamp.to_pydatetime(),
                symbol=symbol,
                direction="flat",
                strength=0.0,
                source=self.config.source,
            )
        ]


def _make_bars() -> pd.DataFrame:
    timestamps = pd.to_datetime(
        [
            "2024-01-02 09:30:00+00:00",
            "2024-01-03 09:30:00+00:00",
            "2024-01-04 09:30:00+00:00",
            "2024-01-05 09:30:00+00:00",
        ]
    )
    rows: list[dict] = []
    closes_by_symbol = {
        "000001.SZ": [10.0, 10.5, 10.8, 11.0],
        "000002.SZ": [20.0, 20.1, 20.2, 20.3],
    }
    for symbol, closes in closes_by_symbol.items():
        for ts, close in zip(timestamps, closes):
            rows.append({"timestamp": ts, "symbol": symbol, "close": close})
    return pd.DataFrame(rows)


def test_warmup_skips_early_rebalances() -> None:
    bars = _make_bars()
    strategy = _EchoSignalStrategy(
        StrategyConfig(source="echo", warmup_bars=3, rebalance="daily")
    )

    result = StrategyRunner(strategy).run(bars=bars)

    # 关键逻辑说明。
    assert len(result.signals) == 4
    first_ts = min(signal.timestamp for signal in result.signals)
    assert first_ts == datetime(2024, 1, 4, 9, 30, tzinfo=timezone.utc)


def test_missing_data_policy_skip_timestamp() -> None:
    bars = _make_bars()
    # 在某个时间戳移除一个标的
    bars = bars[
        ~(
            (bars["symbol"] == "000002.SZ")
            & (bars["timestamp"] == pd.Timestamp("2024-01-03 09:30:00+00:00"))
        )
    ].reset_index(drop=True)

    strategy = _EchoSignalStrategy(
        StrategyConfig(source="echo", missing_data="skip_timestamp")
    )
    result = StrategyRunner(strategy).run(
        bars=bars,
        universe=["000001.SZ", "000002.SZ"],
    )

    signal_dates = sorted({signal.timestamp.date().isoformat() for signal in result.signals})
    assert signal_dates == ["2024-01-02", "2024-01-04", "2024-01-05"]


def test_missing_data_policy_raise() -> None:
    bars = _make_bars()
    bars = bars[
        ~(
            (bars["symbol"] == "000002.SZ")
            & (bars["timestamp"] == pd.Timestamp("2024-01-03 09:30:00+00:00"))
        )
    ].reset_index(drop=True)

    strategy = _EchoSignalStrategy(StrategyConfig(source="echo", missing_data="raise"))

    with pytest.raises(MissingDataError):
        StrategyRunner(strategy).run(bars=bars, universe=["000001.SZ", "000002.SZ"])


def test_rebalance_frequency_weekly_vs_daily() -> None:
    timestamps = pd.to_datetime(
        [
            "2024-01-01 09:30:00+00:00",
            "2024-01-02 09:30:00+00:00",
            "2024-01-03 09:30:00+00:00",
            "2024-01-04 09:30:00+00:00",
            "2024-01-05 09:30:00+00:00",
            "2024-01-08 09:30:00+00:00",
            "2024-01-09 09:30:00+00:00",
            "2024-01-10 09:30:00+00:00",
            "2024-01-11 09:30:00+00:00",
            "2024-01-12 09:30:00+00:00",
        ]
    )
    bars = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": ["000001.SZ"] * len(timestamps),
            "close": [10 + i * 0.1 for i in range(len(timestamps))],
        }
    )

    daily = StrategyRunner(
        _EchoSignalStrategy(StrategyConfig(source="echo", rebalance="daily"))
    ).run(bars=bars)
    weekly = StrategyRunner(
        _EchoSignalStrategy(StrategyConfig(source="echo", rebalance="weekly"))
    ).run(bars=bars)

    assert len(daily.signals) == 10
    assert len(weekly.signals) == 2


def test_runner_context_has_no_future_data() -> None:
    bars = _make_bars()
    state: dict = {}
    strategy = _ProbeNoLookaheadStrategy(StrategyConfig(source="probe", rebalance="daily"))

    result = StrategyRunner(strategy).run(
        bars=bars[bars["symbol"] == "000001.SZ"],
        universe=["000001.SZ"],
        state=state,
    )

    assert len(result.signals) == 4
    assert "records" in state
    assert len(state["records"]) == 4
    history_sizes = [record["history_size"] for record in state["records"]]
    assert history_sizes == [1, 2, 3, 4]
    for record in state["records"]:
        assert record["max_history_ts"] == record["timestamp"]
