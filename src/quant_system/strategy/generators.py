"""生成器协议定义。"""
from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from quant_system.core import Signal, TargetPosition

from .context import StrategyContext


@runtime_checkable
class SignalGenerator(Protocol):
    """信号生成器协议。"""

    def generate_signals(self, context: StrategyContext) -> Sequence[Signal]:
        """生成交易信号。"""


@runtime_checkable
class TargetGenerator(Protocol):
    """目标仓位生成器协议。"""

    def generate_targets(self, context: StrategyContext) -> Sequence[TargetPosition]:
        """生成目标仓位。"""
