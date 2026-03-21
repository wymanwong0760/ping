"""组合账本与记账实现。"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from quant_system.core import Fill, PortfolioSnapshot

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PositionRecord:
    """记录对象。"""

    quantity: float = 0.0
    avg_cost: float = 0.0


class Ledger:
    """`Ledger` 类。"""

    def __init__(self, initial_cash: float) -> None:
        self.initial_cash = float(initial_cash)
        self.cash = float(initial_cash)
        self.positions: dict[str, PositionRecord] = {}
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.turnover_notional = 0.0
        self.fills: list[Fill] = []
        self.closed_trade_pnls: list[float] = []
        self._peak_equity = float(initial_cash)

    def apply_fill(self, fill: Fill) -> None:
        """应用处理并返回结果。"""
        symbol = str(fill.symbol)
        qty = float(fill.quantity)
        price = float(fill.price)
        commission = float(fill.commission)
        slippage = float(fill.slippage)
        total_cost = commission + slippage

        if qty <= 0:
            raise ValueError("fill.quantity must be > 0")

        signed_qty = qty if fill.side == "buy" else -qty
        position = self.positions.get(symbol, PositionRecord())
        old_qty = position.quantity
        old_avg = position.avg_cost
        new_qty = old_qty + signed_qty

        if fill.side == "buy":
            self.cash -= qty * price + total_cost
        else:
            self.cash += qty * price - total_cost

        self.turnover_notional += abs(qty * price)

        realized_delta = 0.0
        if old_qty == 0 or (old_qty > 0 and signed_qty > 0) or (old_qty < 0 and signed_qty < 0):
            if abs(new_qty) > 0:
                new_avg = (
                    (old_avg * abs(old_qty) + price * abs(signed_qty)) / abs(new_qty)
                    if old_qty != 0
                    else price
                )
            else:
                new_avg = 0.0
        else:
            closing_qty = min(abs(old_qty), abs(signed_qty))
            if old_qty > 0:
                realized_delta = (price - old_avg) * closing_qty
            else:
                realized_delta = (old_avg - price) * closing_qty

            if abs(new_qty) < 1e-12:
                new_avg = 0.0
                new_qty = 0.0
            elif old_qty * new_qty < 0:
                new_avg = price
            else:
                new_avg = old_avg

        if realized_delta != 0.0:
            self.realized_pnl += realized_delta
            self.closed_trade_pnls.append(realized_delta)

        if abs(new_qty) < 1e-12:
            self.positions.pop(symbol, None)
        else:
            self.positions[symbol] = PositionRecord(quantity=new_qty, avg_cost=new_avg)

        self.fills.append(fill)

        logger.debug(
            "Applied fill symbol=%s side=%s qty=%.4f price=%.6f cash=%.2f realized=%.2f",
            symbol,
            fill.side,
            qty,
            price,
            self.cash,
            self.realized_pnl,
        )

    def mark_to_market(self, timestamp: datetime, close_prices: dict[str, float]) -> PortfolioSnapshot:
        """标记并返回结果。"""
        market_value = 0.0
        gross_exposure = 0.0
        unrealized = 0.0

        for symbol, position in self.positions.items():
            price = float(close_prices.get(symbol, position.avg_cost))
            qty = position.quantity
            market_value += qty * price
            gross_exposure += abs(qty * price)
            if qty >= 0:
                unrealized += (price - position.avg_cost) * qty
            else:
                unrealized += (position.avg_cost - price) * abs(qty)

        self.unrealized_pnl = unrealized
        equity = self.cash + market_value
        if equity > self._peak_equity:
            self._peak_equity = equity

        drawdown = 0.0
        if self._peak_equity > 0:
            drawdown = (equity / self._peak_equity) - 1.0

        leverage = (gross_exposure / equity) if equity > 0 else 0.0

        return PortfolioSnapshot(
            timestamp=timestamp,
            cash=self.cash,
            equity=equity,
            positions={symbol: pos.quantity for symbol, pos in self.positions.items()},
            leverage=leverage,
            drawdown=drawdown,
        )

    def get_position_qty(self, symbol: str) -> float:
        """获取并返回结果。"""
        position = self.positions.get(symbol)
        if position is None:
            return 0.0
        return position.quantity

    def get_avg_cost(self, symbol: str) -> float:
        """获取并返回结果。"""
        position = self.positions.get(symbol)
        if position is None:
            return 0.0
        return position.avg_cost
