"""执行引擎实现。

本模块负责在 bar 数据上模拟订单执行流程，核心职责包括：
1) 接收订单并维护 pending 队列；
2) 在每个 bar 到来时按时序与撮合规则尝试成交；
3) 在不可交易场景下按策略拒单或继续挂单；
4) 产出 fills / rejects / pending_orders 单步结果。

注意：本模块不负责资金账本与绩效统计。
"""
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
    """执行引擎。

    订单在本引擎内的主要状态流转为：
    - pending：已提交，等待可执行时点与撮合条件；
    - filled：已成交；
    - rejected：因不可交易等原因被拒绝；
    - canceled：在 pending 阶段被主动撤销。

    引擎维护 `_open_orders` 作为当前待处理订单集合。
    """

    def __init__(self, config: ExecutionConfig | None = None) -> None:
        # 未显式传入时使用默认执行配置，确保行为稳定可预测。
        self.config = config or ExecutionConfig()
        self._order_seq = 0
        # 仅保存尚未完成（pending）的执行订单。
        self._open_orders: list[ExecutionOrder] = []

    def submit_orders(
        self,
        timestamp: datetime,
        orders: list[OrderRequest],
    ) -> list[str]:
        """提交订单请求并返回订单 ID 列表。

        参数:
            timestamp: 提交时点，会被归一化到 UTC。
            orders: 上游（通常已过风控）传入的订单请求。

        返回:
            本次提交生成的执行层订单 ID 列表。

        说明:
            `created_at` 与 `executable_at` 在提交时先记录为同一时点，
            实际“同 bar 是否允许成交”由 `on_bar` 中的 `fill_mode` 分支控制。
        """
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
        """处理一个 bar 并返回执行结果。

        处理流程（按顺序）：
        1) 对每个 pending 订单做时序检查（`fill_mode` 与 `executable_at`）；
        2) 检查对应标的是否存在行情数据；
        3) 检查是否可交易（停牌/不可交易）；
        4) 解析成交价（market/limit）；
        5) 计算手续费与滑点并生成 Fill；
        6) 回写本步后的 pending 列表。

        返回:
            `ExecutionStepResult`，包含本步成交、拒单与剩余挂单快照。
        """
        ts = self._normalize_timestamp(timestamp)
        fills: list[Fill] = []
        rejects: list[ExecutionReject] = []
        remaining: list[ExecutionOrder] = []

        for execution_order in self._open_orders:
            created_at = self._normalize_timestamp(execution_order.created_at)
            # next_open 模式下，同一 bar 提交的订单不能在该 bar 成交，避免时序泄漏。
            if self.config.fill_mode == "next_open" and ts <= created_at:
                remaining.append(execution_order)
                continue
            # 尚未到可执行时点时继续保留挂单。
            if self._normalize_timestamp(execution_order.executable_at) > ts:
                remaining.append(execution_order)
                continue

            order = execution_order.request
            bar = bars_by_symbol.get(str(order.symbol))
            # 无行情数据时不强行拒单，继续等待后续 bar。
            if bar is None:
                remaining.append(execution_order)
                continue

            tradable, reason = self._check_tradable(symbol=str(order.symbol), bar=bar)
            if not tradable:
                # keep_pending: 保留挂单，等待后续重新判定可交易性。
                if self.config.untradable_policy == "keep_pending":
                    remaining.append(execution_order)
                    continue

                # reject: 直接拒单并记录原因，便于审计与回放。
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
            # 未得到成交价（例如限价未触发）则继续挂单。
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

        # 单步处理结束后，统一以 remaining 作为新的 open orders。
        self._open_orders = remaining
        return ExecutionStepResult(
            timestamp=ts.to_pydatetime(),
            fills=fills,
            rejects=rejects,
            pending_orders=[item for item in self._open_orders],
        )

    def cancel_order(self, order_id: str) -> bool:
        """取消待执行订单。

        仅对当前仍处于 pending 的订单生效。

        返回:
            True 表示找到并撤销；False 表示未找到对应挂单。
        """
        for index, execution_order in enumerate(self._open_orders):
            if execution_order.order_id != order_id:
                continue
            execution_order.status = "canceled"
            del self._open_orders[index]
            return True
        return False

    def get_open_orders(self) -> list[ExecutionOrder]:
        """返回当前待执行订单列表。

        返回的是内部列表的浅拷贝，避免调用方直接修改引擎内部状态。
        """
        return list(self._open_orders)

    @staticmethod
    def resolve_fill_price(
        order: OrderRequest,
        bar: Mapping[str, Any],
        fill_mode: str,
    ) -> float | None:
        """根据订单与行情决定成交价。

        规则:
        - market 单:
          - `next_open` 使用 open；
          - `current_close` 使用 close。
        - limit 单:
          - buy 需满足 `low <= limit_price`；
          - sell 需满足 `high >= limit_price`；
          - 未触发返回 None，表示继续挂单。
        """
        open_price = float(bar["open"])
        high_price = float(bar["high"])
        low_price = float(bar["low"])
        close_price = float(bar["close"])

        if order.order_type == "market":
            return open_price if fill_mode == "next_open" else close_price

        if order.order_type == "limit":
            # limit 单必须提供限价，否则视为不可成交。
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
        """计算手续费与滑点成本。

        计算口径:
        - 成交金额 `notional = abs(quantity * price)`
        - 手续费 `commission = notional * commission_bps / 10000 + commission_per_order`
        - 滑点成本 `slippage = notional * slippage_bps / 10000`
        """
        notional = abs(float(quantity) * float(price))
        commission = notional * (float(commission_bps) / 10_000.0) + float(
            commission_per_order
        )
        slippage = notional * (float(slippage_bps) / 10_000.0)
        return float(commission), float(slippage)

    @staticmethod
    def _check_tradable(symbol: str, bar: Mapping[str, Any]) -> tuple[bool, str]:
        """检查标的在当前 bar 是否可交易。

        判定规则:
        - `is_suspended=True` 视为不可交易；
        - 若存在 `is_tradable` 字段且为 False，视为不可交易。

        返回:
            (是否可交易, 原因说明)
        """
        if bool(bar.get("is_suspended", False)):
            return False, f"symbol {symbol} is suspended"
        if "is_tradable" in bar and not bool(bar.get("is_tradable")):
            return False, f"symbol {symbol} is not tradable"
        return True, ""

    @staticmethod
    def _normalize_timestamp(value: datetime) -> pd.Timestamp:
        """将时间统一转换为 UTC 时区的 `pd.Timestamp`。"""
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            return timestamp.tz_localize("UTC")
        return timestamp.tz_convert("UTC")

    def _next_order_id(self) -> str:
        """生成递增的执行订单 ID（形如 `EXEC-00000001`）。"""
        self._order_seq += 1
        return f"EXEC-{self._order_seq:08d}"
