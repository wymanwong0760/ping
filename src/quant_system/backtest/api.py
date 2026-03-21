"""回测高层接口。

提供两类入口：
- 直接使用外部 bars 数据运行回测；
- 通过数据提供器加载数据后运行回测。
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

import pandas as pd

from quant_system.data import BaseDataProvider
from quant_system.risk import RiskConfig, RiskEngine
from quant_system.strategy import BaseStrategy, StrategyRunner

from .config import BacktestConfig
from .engine import BacktestEngine
from .models import BacktestResult


def run_backtest(
    strategy: BaseStrategy,
    bars: pd.DataFrame,
    config: BacktestConfig | None = None,
    universe: Sequence[str] | None = None,
    state: Mapping[str, Any] | None = None,
    risk_engine: RiskEngine | None = None,
    risk_config: RiskConfig | None = None,
) -> BacktestResult:
    """基于已给定 bars 运行策略并执行回测。

    会先运行策略生成 signals/targets，再交由 `BacktestEngine` 执行。
    若同时提供 `risk_engine` 与 `risk_config`，优先使用显式传入的 `risk_engine`。
    """
    strategy_result = StrategyRunner(strategy).run(bars=bars, universe=universe, state=state)
    engine = BacktestEngine(
        config=config or BacktestConfig(),
        risk_engine=risk_engine or (RiskEngine(risk_config) if risk_config is not None else None),
    )
    return engine.run(
        bars=bars,
        signals=strategy_result.signals,
        targets=strategy_result.targets,
    )


def run_backtest_with_provider(
    provider: BaseDataProvider,
    strategy: BaseStrategy,
    symbols: Sequence[str] | str | None,
    config: BacktestConfig | None = None,
    start: pd.Timestamp | str | None = None,
    end: pd.Timestamp | str | None = None,
    timeframe: str = "1d",
    dataset_name: str | None = None,
    state: Mapping[str, Any] | None = None,
    risk_engine: RiskEngine | None = None,
    risk_config: RiskConfig | None = None,
) -> BacktestResult:
    """通过数据提供器加载 bars 后运行回测。"""
    bars = provider.load_bars(
        symbols=symbols,
        start=start,
        end=end,
        timeframe=timeframe,
        dataset_name=dataset_name,
    )
    universe = [symbols] if isinstance(symbols, str) else list(symbols) if symbols else None
    return run_backtest(
        strategy=strategy,
        bars=bars,
        config=config,
        universe=universe,
        state=state,
        risk_engine=risk_engine,
        risk_config=risk_config,
    )
