"""撮合经纪模拟实现。

该模块仅负责“订单 -> 成交”转换：
- 按 bar 与订单规则判断是否可成交；
- 计算成交价格、手续费与滑点；
- 返回成交列表与未成交订单。

资金、持仓与盈亏更新由 `Ledger` 负责，不在此模块处理。
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Mapping

from quant_system.core import Fill, OrderRequest
from quant_system.execution import ExecutionEngine

from .config import BacktestConfig

logger = logging.getLogger(__name__)


class SimBroker:
    """回测撮合器。

    在给定时间点对一批订单尝试撮合，无法成交的订单原样返回。
    """

    def __init__(self, config: BacktestConfig) -> None:
        self.config = config
        self._order_seq = 0

    def match_orders(
        self,
        timestamp: datetime,
        bars_by_symbol: Mapping[str, Mapping[str, float]],
        orders: list[OrderRequest],
    ) -> tuple[list[Fill], list[OrderRequest]]:
        """撮合订单并返回 `(fills, remaining_orders)`。"""
        fills: list[Fill] = []
        remaining: list[OrderRequest] = []

        for order in orders:
            bar = bars_by_symbol.get(str(order.symbol))
            # 当前 symbol 在该时点无行情，订单继续挂起。
            if bar is None:
                remaining.append(order)
                continue

            trade_price = self._resolve_fill_price(order=order, bar=bar)
            # 有行情但不满足成交条件（如价格未触达/停牌）时继续保留订单。
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
        """基于执行模块规则解析成交价。

        该方法是对 `ExecutionEngine.resolve_fill_price` 的薄封装，
        便于在回测内统一使用 `BacktestConfig.fill_mode`。
        """
        return ExecutionEngine.resolve_fill_price(
            order=order,
            bar=bar,
            fill_mode=self.config.fill_mode,
        )

    def _next_order_id(self) -> str:
        self._order_seq += 1
        return f"SIM-{self._order_seq:08d}"
