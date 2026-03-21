"""配置对象定义。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from .exceptions import StrategyConfigError

RebalanceFrequency = Literal["daily", "weekly"]
MissingDataPolicy = Literal["skip_symbol", "skip_timestamp", "raise"]


@dataclass(slots=True)
class StrategyConfig:
    """策略配置数据类。"""

    source: str
    warmup_bars: int = 0
    rebalance: RebalanceFrequency = "daily"
    missing_data: MissingDataPolicy = "skip_symbol"
    params: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.source.strip():
            raise StrategyConfigError("StrategyConfig.source must be non-empty")
        if self.warmup_bars < 0:
            raise StrategyConfigError("StrategyConfig.warmup_bars must be >= 0")
        if self.rebalance not in {"daily", "weekly"}:
            raise StrategyConfigError(
                f"Unsupported rebalance frequency: {self.rebalance}"
            )
        if self.missing_data not in {"skip_symbol", "skip_timestamp", "raise"}:
            raise StrategyConfigError(
                f"Unsupported missing_data policy: {self.missing_data}"
            )
