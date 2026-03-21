"""订单规模转换实现。

该模块负责把策略输出（signals/targets）转换为可执行订单，
核心是“目标仓位 - 当前仓位 = 调仓增量（delta）”。
"""
from __future__ import annotations

import logging
from typing import Mapping, Sequence

from quant_system.core import OrderRequest, Signal, TargetPosition

from .config import BacktestConfig
from .exceptions import OrderGenerationError

logger = logging.getLogger(__name__)


class OrderSizer:
    """订单规模转换器。

    支持 signal-based 与 target-based 两种下单输入。
    """

    def __init__(self, config: BacktestConfig) -> None:
        self.config = config

    def orders_from_signals(
        self,
        signals: Sequence[Signal],
        current_positions: Mapping[str, float],
    ) -> list[OrderRequest]:
        """将信号列表转换为订单请求。

        转换过程：
        1) 根据信号方向得到目标仓位；
        2) 与当前仓位求差得到 delta；
        3) 仅对非零 delta 生成市价单。
        """
        orders: list[OrderRequest] = []
        for signal in signals:
            # 强度下限为 1，避免弱信号把基础仓位缩到 0 附近导致频繁噪声交易。
            strength = max(abs(float(signal.strength)), 1.0)
            base_qty = self.config.signal_position_size * strength

            if signal.direction == "long":
                target_qty = base_qty
            elif signal.direction == "short":
                # 不允许做空时，short 信号会被裁剪为平仓目标（0 仓位）。
                target_qty = -base_qty if self.config.allow_short else 0.0
            else:
                target_qty = 0.0

            current_qty = float(current_positions.get(signal.symbol, 0.0))
            delta = target_qty - current_qty
            # 近零差值视为无需调仓，避免浮点误差触发无意义订单。
            if abs(delta) < 1e-12:
                continue

            side = "buy" if delta > 0 else "sell"
            orders.append(
                OrderRequest(
                    timestamp=signal.timestamp,
                    symbol=signal.symbol,
                    side=side,
                    quantity=abs(delta),
                    order_type="market",
                    source=signal.source,
                    metadata={"signal_direction": signal.direction, **signal.metadata},
                )
            )

        logger.debug("Generated %s orders from %s signals", len(orders), len(signals))
        return orders

    def orders_from_targets(
        self,
        targets: Sequence[TargetPosition],
        current_positions: Mapping[str, float],
        equity: float,
        reference_prices: Mapping[str, float],
    ) -> list[OrderRequest]:
        """将目标仓位列表转换为订单请求。

        支持 `target_qty` 与 `target_weight` 两种表达；
        统一先解出目标股数，再与当前仓位求差生成 delta 订单。
        """
        orders: list[OrderRequest] = []

        for target in targets:
            target_qty = self._resolve_target_qty(
                target=target,
                equity=equity,
                reference_prices=reference_prices,
            )
            if not self.config.allow_short:
                target_qty = max(target_qty, 0.0)

            current_qty = float(current_positions.get(target.symbol, 0.0))
            delta = target_qty - current_qty
            if abs(delta) < 1e-12:
                continue

            side = "buy" if delta > 0 else "sell"
            orders.append(
                OrderRequest(
                    timestamp=target.timestamp,
                    symbol=target.symbol,
                    side=side,
                    quantity=abs(delta),
                    order_type="market",
                    source=target.source,
                    metadata={"target_qty": target_qty, **target.metadata},
                )
            )

        logger.debug("Generated %s orders from %s targets", len(orders), len(targets))
        return orders

    @staticmethod
    def _resolve_target_qty(
        target: TargetPosition,
        equity: float,
        reference_prices: Mapping[str, float],
    ) -> float:
        """解析目标仓位对应的目标股数。

        优先级：
        - 若给定 `target_qty`，直接使用；
        - 否则使用 `equity * target_weight / reference_price` 换算。

        当 `target_weight` 路径缺少有效价格时抛出异常。
        """
        has_weight = target.target_weight is not None
        has_qty = target.target_qty is not None

        if not has_weight and not has_qty:
            raise OrderGenerationError(
                f"TargetPosition must contain target_weight or target_qty: {target.symbol}"
            )

        if has_qty:
            return float(target.target_qty or 0.0)

        price = float(reference_prices.get(target.symbol, 0.0))
        if price <= 0:
            raise OrderGenerationError(
                f"Missing or invalid reference price for weight target: {target.symbol}"
            )
        return float(equity) * float(target.target_weight or 0.0) / price
