"""strategy 模块导出。"""

from .api import create_strategy, run_strategy, run_strategy_with_provider
from .base import BaseStrategy, SignalStrategy, TargetStrategy
from .config import MissingDataPolicy, RebalanceFrequency, StrategyConfig
from .context import StrategyContext
from .cross_sectional_momentum import CrossSectionalMomentumStrategy
from .dual_moving_average import DualMovingAverageStrategy
from .exceptions import (
    FutureDataLeakError,
    MissingDataError,
    StrategyConfigError,
    StrategyDataError,
    StrategyError,
)
from .generators import SignalGenerator, TargetGenerator
from .runner import StrategyRunResult, StrategyRunner

__all__ = [
    "StrategyConfig",
    "RebalanceFrequency",
    "MissingDataPolicy",
    "StrategyContext",
    "BaseStrategy",
    "SignalStrategy",
    "TargetStrategy",
    "SignalGenerator",
    "TargetGenerator",
    "StrategyRunner",
    "StrategyRunResult",
    "DualMovingAverageStrategy",
    "CrossSectionalMomentumStrategy",
    "create_strategy",
    "run_strategy",
    "run_strategy_with_provider",
    "StrategyError",
    "StrategyConfigError",
    "StrategyDataError",
    "MissingDataError",
    "FutureDataLeakError",
]
