"""高层接口定义。"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

import pandas as pd

from quant_system.data import BaseDataProvider

from .base import BaseStrategy
from .config import StrategyConfig
from .cross_sectional_momentum import CrossSectionalMomentumStrategy
from .dual_moving_average import DualMovingAverageStrategy
from .exceptions import StrategyConfigError
from .runner import StrategyRunResult, StrategyRunner

_STRATEGY_REGISTRY: dict[str, type[BaseStrategy]] = {
    "dual_moving_average": DualMovingAverageStrategy,
    "cross_sectional_momentum": CrossSectionalMomentumStrategy,
}


def create_strategy(strategy_type: str, config: StrategyConfig) -> BaseStrategy:
    """创建并返回实例。"""
    strategy_cls = _STRATEGY_REGISTRY.get(strategy_type)
    if strategy_cls is None:
        raise StrategyConfigError(f"Unknown strategy_type: {strategy_type}")
    return strategy_cls(config=config)


def run_strategy(
    strategy: BaseStrategy,
    bars: pd.DataFrame,
    universe: Sequence[str] | None = None,
    state: Mapping[str, Any] | None = None,
) -> StrategyRunResult:
    """执行流程并返回结果。"""
    return StrategyRunner(strategy).run(bars=bars, universe=universe, state=state)


def run_strategy_with_provider(
    provider: BaseDataProvider,
    strategy: BaseStrategy,
    symbols: Sequence[str] | str | None,
    start: pd.Timestamp | str | None = None,
    end: pd.Timestamp | str | None = None,
    timeframe: str = "1d",
    dataset_name: str | None = None,
    state: Mapping[str, Any] | None = None,
) -> StrategyRunResult:
    """执行流程并返回结果。"""
    bars = provider.load_bars(
        symbols=symbols,
        start=start,
        end=end,
        timeframe=timeframe,
        dataset_name=dataset_name,
    )
    universe = [symbols] if isinstance(symbols, str) else list(symbols) if symbols else None
    return run_strategy(strategy=strategy, bars=bars, universe=universe, state=state)
