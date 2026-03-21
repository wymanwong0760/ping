"""回测配置对象定义。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .exceptions import BacktestConfigError

FillMode = Literal["next_open", "current_close"]


@dataclass(slots=True)
class BacktestConfig:
    """回测配置。

    关键字段：
    - `fill_mode`：决定订单在下一根 bar 还是当前 bar 成交；
    - `signal_position_size`：信号策略默认目标仓位基数；
    - `annualization_factor`：收益与波动年化因子。
    """

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
