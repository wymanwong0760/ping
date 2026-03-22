"""执行配置对象定义。

该模块用于统一描述执行层撮合行为与成本模型参数，
其术语与行为定义应与 `docs/execution_module.md` 保持一致。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .exceptions import ExecutionConfigError

FillMode = Literal["next_open", "current_close"]
UntradablePolicy = Literal["reject", "keep_pending"]


@dataclass(slots=True)
class ExecutionConfig:
    """执行配置数据类。

    字段说明:
        fill_mode:
            - `next_open`: 订单不会在提交当根 bar 成交，最早在后续 bar 的 open 尝试成交；
            - `current_close`: 可在当前 bar 按 close（market）尝试成交。
        commission_bps:
            按成交金额比例收取的手续费（单位 bps，1 bps = 0.01%）。
        commission_per_order:
            每笔订单固定手续费。
        slippage_bps:
            按成交金额比例估算的滑点成本（单位 bps）。
        untradable_policy:
            - `reject`: 标的不可交易时直接拒单；
            - `keep_pending`: 标的不可交易时继续保留挂单。
    """

    fill_mode: FillMode = "next_open"
    commission_bps: float = 0.0
    commission_per_order: float = 0.0
    slippage_bps: float = 0.0
    untradable_policy: UntradablePolicy = "reject"

    def __post_init__(self) -> None:
        """执行配置合法性校验。

        该校验在实例化阶段尽早失败，避免运行期才暴露非法参数。
        """
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
