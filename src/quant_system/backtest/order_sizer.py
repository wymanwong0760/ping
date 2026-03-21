"""订单规模转换实现。"""
from __future__ import annotations

import logging
from typing import Mapping, Sequence

from quant_system.core import OrderRequest, Signal, TargetPosition

from .config import BacktestConfig
from .exceptions import OrderGenerationError

logger = logging.getLogger(__name__)


class OrderSizer:
    """`OrderSizer` 类。"""

    def __init__(self, config: BacktestConfig) -> None:
        self.config = config

    def orders_from_signals(
        self,
        signals: Sequence[Signal],
        current_positions: Mapping[str, float],
    ) -> list[OrderRequest]:
        """执行 `orders_from_signals`。"""
        orders: list[OrderRequest] = []
        for signal in signals:
            strength = max(abs(float(signal.strength)), 1.0)
            base_qty = self.config.signal_position_size * strength

            if signal.direction == "long":
                target_qty = base_qty
            elif signal.direction == "short":
                target_qty = -base_qty if self.config.allow_short else 0.0
            else:
                target_qty = 0.0

            current_qty = float(current_positions.get(signal.symbol, 0.0))
            delta = target_qty - current_qty
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
        """执行 `orders_from_targets`。"""
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
