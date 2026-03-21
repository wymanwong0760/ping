"""数据模型定义。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

Direction = Literal["long", "short", "flat"]
OrderSide = Literal["buy", "sell"]
OrderType = Literal["market", "limit"]


@dataclass(slots=True)
class Bar:
    """`Bar` 类。"""

    timestamp: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float | None = None
    adj_factor: float | None = None
    is_suspended: bool | None = None


@dataclass(slots=True)
class Signal:
    """`Signal` 类。"""

    timestamp: datetime
    symbol: str
    direction: Direction
    strength: float
    source: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TargetPosition:
    """`TargetPosition` 类。"""

    timestamp: datetime
    symbol: str
    source: str
    target_weight: float | None = None
    target_qty: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OrderRequest:
    """`OrderRequest` 类。"""

    timestamp: datetime
    symbol: str
    side: OrderSide
    quantity: float
    order_type: OrderType
    source: str
    limit_price: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Fill:
    """`Fill` 类。"""

    timestamp: datetime
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    commission: float
    slippage: float
    order_id: str


@dataclass(slots=True)
class PortfolioSnapshot:
    """`PortfolioSnapshot` 类。"""

    timestamp: datetime
    cash: float
    equity: float
    positions: dict[str, float]
    leverage: float
    drawdown: float
