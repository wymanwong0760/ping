"""配置对象定义。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .exceptions import ExecutionConfigError

FillMode = Literal["next_open", "current_close"]
UntradablePolicy = Literal["reject", "keep_pending"]


@dataclass(slots=True)
class ExecutionConfig:
    """配置数据类。"""

    fill_mode: FillMode = "next_open"
    commission_bps: float = 0.0
    commission_per_order: float = 0.0
    slippage_bps: float = 0.0
    untradable_policy: UntradablePolicy = "reject"

    def __post_init__(self) -> None:
        if self.fill_mode not in {"next_open", "current_close"}:
            raise ExecutionConfigError(f"Unsupported fill_mode: {self.fill_mode}")
        if self.commission_bps < 0:
            raise ExecutionConfigError("commission_bps must be >= 0")
        if self.commission_per_order < 0:
            raise ExecutionConfigError("commission_per_order must be >= 0")
        if self.slippage_bps < 0:
            raise ExecutionConfigError("slippage_bps must be >= 0")
        if self.untradable_policy not in {"reject", "keep_pending"}:
            raise ExecutionConfigError(
                f"Unsupported untradable_policy: {self.untradable_policy}"
            )
