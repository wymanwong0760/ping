"""撮合经纪模拟实现。"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Mapping

from quant_system.core import Fill, OrderRequest
from quant_system.execution import ExecutionEngine

from .config import BacktestConfig

logger = logging.getLogger(__name__)


class SimBroker:
    """`SimBroker` 类。"""

    def __init__(self, config: BacktestConfig) -> None:
        self.config = config
        self._order_seq = 0

    def match_orders(
        self,
        timestamp: datetime,
        bars_by_symbol: Mapping[str, Mapping[str, float]],
        orders: list[OrderRequest],
    ) -> tuple[list[Fill], list[OrderRequest]]:
        """执行 `match_orders`。"""
        fills: list[Fill] = []
        remaining: list[OrderRequest] = []

        for order in orders:
            bar = bars_by_symbol.get(str(order.symbol))
            if bar is None:
                remaining.append(order)
                continue

            trade_price = self._resolve_fill_price(order=order, bar=bar)
            if trade_price is None:
                remaining.append(order)
                continue

            commission, slippage = ExecutionEngine.compute_costs(
                quantity=float(order.quantity),
                price=trade_price,
                commission_bps=self.config.commission_bps,
                commission_per_order=self.config.commission_per_order,
                slippage_bps=self.config.slippage_bps,
            )

            fill = Fill(
                timestamp=timestamp,
                symbol=str(order.symbol),
                side=order.side,
                quantity=float(order.quantity),
                price=trade_price,
                commission=commission,
                slippage=slippage,
                order_id=self._next_order_id(),
            )
            fills.append(fill)

            logger.debug(
                "Matched order symbol=%s side=%s qty=%.4f type=%s fill_price=%.6f",
                order.symbol,
                order.side,
                order.quantity,
                order.order_type,
                trade_price,
            )

        return fills, remaining

    def _resolve_fill_price(
        self,
        order: OrderRequest,
        bar: Mapping[str, float],
    ) -> float | None:
        return ExecutionEngine.resolve_fill_price(
            order=order,
            bar=bar,
            fill_mode=self.config.fill_mode,
        )

    def _next_order_id(self) -> str:
        self._order_seq += 1
        return f"SIM-{self._order_seq:08d}"
