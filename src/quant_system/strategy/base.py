"""抽象基类与接口定义。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Sequence

from quant_system.core import Signal, TargetPosition

from .config import StrategyConfig
from .context import StrategyContext


class BaseStrategy(ABC):
    """策略基类。"""

    def __init__(self, config: StrategyConfig) -> None:
        self.config = config

    @property
    def name(self) -> str:
        """策略名称。"""
        return self.__class__.__name__

    def get_param(self, key: str, default: Any = None) -> Any:
        """读取参数并提供默认值。"""
        return self.config.params.get(key, default)


class SignalStrategy(BaseStrategy, ABC):
    """信号型策略接口。"""

    @abstractmethod
    def generate_signals(self, context: StrategyContext) -> Sequence[Signal]:
        """生成交易信号。"""


class TargetStrategy(BaseStrategy, ABC):
    """目标仓位型策略接口。"""

    @abstractmethod
    def generate_targets(self, context: StrategyContext) -> Sequence[TargetPosition]:
        """生成目标仓位。"""
