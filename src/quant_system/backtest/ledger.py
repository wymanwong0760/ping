"""组合账本与记账实现。

账本负责维护回测中的账户状态：
- 现金余额；
- 各标的持仓与持仓成本；
- 已实现/未实现盈亏；
- 换手与权益快照。

约定：持仓数量 `quantity > 0` 表示多头，`quantity < 0` 表示空头。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from quant_system.core import Fill, PortfolioSnapshot

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PositionRecord:
    """单标的持仓记录。"""

    quantity: float = 0.0
    avg_cost: float = 0.0


class Ledger:
    """回测账本。

    负责接收成交并更新账户状态，提供盯市与持仓查询能力。
    """

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
        """将一笔成交写入账本并更新现金/持仓/已实现盈亏。

        处理三类仓位变化：
        - 同向加仓（或从 0 开仓）：更新加权平均成本；
        - 反向减仓：按平掉部分计算已实现盈亏；
        - 反手：平掉原方向后，剩余仓位以当前成交价重置成本。
        """
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
            # 买入占用现金，费用（佣金+滑点）同向扣减。
            self.cash -= qty * price + total_cost
        else:
            # 卖出回收现金，费用仍然扣减。
            self.cash += qty * price - total_cost

        self.turnover_notional += abs(qty * price)

        realized_delta = 0.0
        # 同向交易（或从空仓开仓）：仅更新持仓与均价，不产生已实现盈亏。
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
            # 反向交易：先按可对冲数量确认已实现盈亏。
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
            # 近零仓位按平仓处理，避免浮点误差导致“幽灵仓位”。
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
        """按当前价格盯市并生成账户快照。

        若某标的在 `close_prices` 中缺失，则回退到其 `avg_cost` 估值，
        以保证快照可计算且不会引入 NaN。
        """
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
        # 当权益非正时不再计算杠杆，避免产生无意义或发散值。

        return PortfolioSnapshot(
            timestamp=timestamp,
            cash=self.cash,
            equity=equity,
            positions={symbol: pos.quantity for symbol, pos in self.positions.items()},
            leverage=leverage,
            drawdown=drawdown,
        )

    def get_position_qty(self, symbol: str) -> float:
        """返回指定标的当前持仓数量；不存在则为 0。"""
        position = self.positions.get(symbol)
        if position is None:
            return 0.0
        return position.quantity

    def get_avg_cost(self, symbol: str) -> float:
        """返回指定标的当前持仓均价；不存在则为 0。"""
        position = self.positions.get(symbol)
        if position is None:
            return 0.0
        return position.avg_cost
