"""执行层数据模型定义。

本模块仅承载执行过程中的数据结构，不包含撮合计算逻辑。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from quant_system.core import Fill, OrderRequest

ExecutionOrderStatus = Literal["pending", "filled", "rejected", "canceled"]


@dataclass(slots=True)
class ExecutionOrder:
    """执行订单对象。

    字段语义:
        order_id: 执行层内部唯一订单 ID。
        request: 原始订单请求（来自上游下单链路）。
        status: 执行状态，范围见 `ExecutionOrderStatus`。
        created_at: 订单进入执行引擎的时间。
        executable_at: 订单最早可执行时间（与时序门控一起生效）。
    """

    order_id: str
    request: OrderRequest
    status: ExecutionOrderStatus
    created_at: datetime
    executable_at: datetime


@dataclass(slots=True)
class ExecutionReject:
    """执行拒单记录。

    用于记录订单在执行阶段被拒绝的事实与原因，便于审计与回放。
    """

    timestamp: datetime
    order_id: str
    symbol: str
    reason: str
    # 默认来源为执行引擎本身；上层若有需要可在外部扩展来源维度。
    source: str = "execution_engine"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionStepResult:
    """单步执行结果容器。

    该对象表示某个 bar 执行完成后的快照，包含：
    - fills: 本步新生成的成交记录；
    - rejects: 本步新生成的拒单记录；
    - pending_orders: 本步结束后仍待执行的订单。
    """

    timestamp: datetime
    fills: list[Fill] = field(default_factory=list)
    rejects: list[ExecutionReject] = field(default_factory=list)
    pending_orders: list[ExecutionOrder] = field(default_factory=list)

    @property
    def open_order_count(self) -> int:
        """返回当前快照中的待执行订单数量。"""
        return len(self.pending_orders)
