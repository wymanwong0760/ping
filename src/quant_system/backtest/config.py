"""配置对象定义。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .exceptions import BacktestConfigError

FillMode = Literal["next_open", "current_close"]


@dataclass(slots=True)
class BacktestConfig:
    """回测配置数据类。"""

    initial_cash: float = 1_000_000.0
    fill_mode: FillMode = "next_open"
    commission_bps: float = 0.0
    commission_per_order: float = 0.0
    slippage_bps: float = 0.0
    allow_short: bool = False
    signal_position_size: float = 100.0
    annualization_factor: int = 252

    def __post_init__(self) -> None:
        if self.initial_cash <= 0:
            raise BacktestConfigError("initial_cash must be > 0")
        if self.fill_mode not in {"next_open", "current_close"}:
            raise BacktestConfigError(f"Unsupported fill_mode: {self.fill_mode}")
        if self.commission_bps < 0:
            raise BacktestConfigError("commission_bps must be >= 0")
        if self.commission_per_order < 0:
            raise BacktestConfigError("commission_per_order must be >= 0")
        if self.slippage_bps < 0:
            raise BacktestConfigError("slippage_bps must be >= 0")
        if self.signal_position_size <= 0:
            raise BacktestConfigError("signal_position_size must be > 0")
        if self.annualization_factor <= 0:
            raise BacktestConfigError("annualization_factor must be > 0")
