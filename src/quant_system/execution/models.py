"""数据模型定义。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from quant_system.core import Fill, OrderRequest

ExecutionOrderStatus = Literal["pending", "filled", "rejected", "canceled"]


@dataclass(slots=True)
class ExecutionOrder:
    """执行订单对象。"""

    order_id: str
    request: OrderRequest
    status: ExecutionOrderStatus
    created_at: datetime
    executable_at: datetime


@dataclass(slots=True)
class ExecutionReject:
    """执行拒单记录。"""

    timestamp: datetime
    order_id: str
    symbol: str
    reason: str
    source: str = "execution_engine"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionStepResult:
    """单步执行结果容器。"""

    timestamp: datetime
    fills: list[Fill] = field(default_factory=list)
    rejects: list[ExecutionReject] = field(default_factory=list)
    pending_orders: list[ExecutionOrder] = field(default_factory=list)

    @property
    def open_order_count(self) -> int:
        """返回当前待执行订单数。"""
        return len(self.pending_orders)
