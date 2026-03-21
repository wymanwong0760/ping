"""引擎实现。"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Mapping

import pandas as pd

from quant_system.core import Fill, OrderRequest

from .config import ExecutionConfig
from .models import ExecutionOrder, ExecutionReject, ExecutionStepResult

logger = logging.getLogger(__name__)


class ExecutionEngine:
    """执行引擎实现。"""

    def __init__(self, config: ExecutionConfig | None = None) -> None:
        self.config = config or ExecutionConfig()
        self._order_seq = 0
        self._open_orders: list[ExecutionOrder] = []

    def submit_orders(
        self,
        timestamp: datetime,
        orders: list[OrderRequest],
    ) -> list[str]:
        """提交订单请求并返回订单 ID 列表。"""
        ts = self._normalize_timestamp(timestamp)
        executable_at = ts
        order_ids: list[str] = []

        for request in orders:
            order_id = self._next_order_id()
            exec_order = ExecutionOrder(
                order_id=order_id,
                request=request,
                status="pending",
                created_at=ts.to_pydatetime(),
                executable_at=executable_at.to_pydatetime(),
            )
            self._open_orders.append(exec_order)
            order_ids.append(order_id)

        logger.debug("Submitted %s execution orders at %s", len(order_ids), ts)
        return order_ids

    def on_bar(
        self,
        timestamp: datetime,
        bars_by_symbol: Mapping[str, Mapping[str, Any]],
    ) -> ExecutionStepResult:
        """处理一个 bar 并返回执行结果。"""
        ts = self._normalize_timestamp(timestamp)
        fills: list[Fill] = []
        rejects: list[ExecutionReject] = []
        remaining: list[ExecutionOrder] = []

        for execution_order in self._open_orders:
            created_at = self._normalize_timestamp(execution_order.created_at)
            if self.config.fill_mode == "next_open" and ts <= created_at:
                remaining.append(execution_order)
                continue
            if self._normalize_timestamp(execution_order.executable_at) > ts:
                remaining.append(execution_order)
                continue

            order = execution_order.request
            bar = bars_by_symbol.get(str(order.symbol))
            if bar is None:
                remaining.append(execution_order)
                continue

            tradable, reason = self._check_tradable(symbol=str(order.symbol), bar=bar)
            if not tradable:
                if self.config.untradable_policy == "keep_pending":
                    remaining.append(execution_order)
                    continue

                execution_order.status = "rejected"
                rejects.append(
                    ExecutionReject(
                        timestamp=ts.to_pydatetime(),
                        order_id=execution_order.order_id,
                        symbol=str(order.symbol),
                        reason=reason,
                        metadata={"policy": self.config.untradable_policy},
                    )
                )
                continue

            fill_price = self.resolve_fill_price(order=order, bar=bar, fill_mode=self.config.fill_mode)
            if fill_price is None:
                remaining.append(execution_order)
                continue

            commission, slippage = self.compute_costs(
                quantity=float(order.quantity),
                price=float(fill_price),
                commission_bps=self.config.commission_bps,
                commission_per_order=self.config.commission_per_order,
                slippage_bps=self.config.slippage_bps,
            )
            fills.append(
                Fill(
                    timestamp=ts.to_pydatetime(),
                    symbol=str(order.symbol),
                    side=order.side,
                    quantity=float(order.quantity),
                    price=float(fill_price),
                    commission=commission,
                    slippage=slippage,
                    order_id=execution_order.order_id,
                )
            )
            execution_order.status = "filled"

        self._open_orders = remaining
        return ExecutionStepResult(
            timestamp=ts.to_pydatetime(),
            fills=fills,
            rejects=rejects,
            pending_orders=[item for item in self._open_orders],
        )

    def cancel_order(self, order_id: str) -> bool:
        """取消待执行订单。"""
        for index, execution_order in enumerate(self._open_orders):
            if execution_order.order_id != order_id:
                continue
            execution_order.status = "canceled"
            del self._open_orders[index]
            return True
        return False

    def get_open_orders(self) -> list[ExecutionOrder]:
        """返回当前待执行订单列表。"""
        return list(self._open_orders)

    @staticmethod
    def resolve_fill_price(
        order: OrderRequest,
        bar: Mapping[str, Any],
        fill_mode: str,
    ) -> float | None:
        """根据订单与行情决定成交价。"""
        open_price = float(bar["open"])
        high_price = float(bar["high"])
        low_price = float(bar["low"])
        close_price = float(bar["close"])

        if order.order_type == "market":
            return open_price if fill_mode == "next_open" else close_price

        if order.order_type == "limit":
            if order.limit_price is None:
                return None
            limit = float(order.limit_price)
            if order.side == "buy" and low_price <= limit:
                return limit
            if order.side == "sell" and high_price >= limit:
                return limit
            return None

        return None

    @staticmethod
    def compute_costs(
        *,
        quantity: float,
        price: float,
        commission_bps: float,
        commission_per_order: float,
        slippage_bps: float,
    ) -> tuple[float, float]:
        """计算手续费与滑点成本。"""
        notional = abs(float(quantity) * float(price))
        commission = notional * (float(commission_bps) / 10_000.0) + float(
            commission_per_order
        )
        slippage = notional * (float(slippage_bps) / 10_000.0)
        return float(commission), float(slippage)

    @staticmethod
    def _check_tradable(symbol: str, bar: Mapping[str, Any]) -> tuple[bool, str]:
        if bool(bar.get("is_suspended", False)):
            return False, f"symbol {symbol} is suspended"
        if "is_tradable" in bar and not bool(bar.get("is_tradable")):
            return False, f"symbol {symbol} is not tradable"
        return True, ""

    @staticmethod
    def _normalize_timestamp(value: datetime) -> pd.Timestamp:
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            return timestamp.tz_localize("UTC")
        return timestamp.tz_convert("UTC")

    def _next_order_id(self) -> str:
        self._order_seq += 1
        return f"EXEC-{self._order_seq:08d}"
