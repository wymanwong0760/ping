"""风控规则实现。"""
from __future__ import annotations

from dataclasses import replace
from typing import Any, Mapping

from quant_system.core import OrderRequest

from .base import BaseRiskRule
from .config import (
    DailyTurnoverRuleConfig,
    DrawdownCircuitBreakerRuleConfig,
    MaxLeverageRuleConfig,
    MaxSymbolPositionRuleConfig,
    TradabilityRuleConfig,
    UniverseFilterRuleConfig,
)
from .models import RiskContext, RuleResult

_EPS = 1e-12


def _signed_quantity(order: OrderRequest) -> float:
    return float(order.quantity) if order.side == "buy" else -float(order.quantity)


def _clone_order(order: OrderRequest, *, signed_delta: float) -> OrderRequest:
    side = "buy" if signed_delta > 0 else "sell"
    return replace(order, side=side, quantity=abs(float(signed_delta)))


def _resolve_price(symbol: str, context: RiskContext, fallback: float | None = None) -> float:
    if symbol in context.close_prices:
        return float(context.close_prices[symbol])
    payload: Mapping[str, Any] = context.market_by_symbol.get(symbol, {})
    if "close" in payload:
        return float(payload["close"])
    if "open" in payload:
        return float(payload["open"])
    if fallback is not None:
        return float(fallback)
    return 0.0


class UniverseFilterRule(BaseRiskRule):
    """规则实现。"""
    def __init__(self, config: UniverseFilterRuleConfig) -> None:
        super().__init__(name="universe_filter")
        self.config = config

    def evaluate_order(self, request: OrderRequest, context: RiskContext) -> RuleResult:
        if not self.config.enabled:
            return RuleResult(action="pass")

        symbol = request.symbol
        if symbol in self.config.blacklist:
            return RuleResult(
                action="reject",
                reason=f"symbol {symbol} is in blacklist",
                metadata={"symbol": symbol},
            )
        if self.config.whitelist and symbol not in self.config.whitelist:
            return RuleResult(
                action="reject",
                reason=f"symbol {symbol} not in whitelist",
                metadata={"symbol": symbol},
            )
        return RuleResult(action="pass")


class TradabilityRule(BaseRiskRule):
    """规则实现。"""
    def __init__(self, config: TradabilityRuleConfig) -> None:
        super().__init__(name="tradability")
        self.config = config

    def evaluate_order(self, request: OrderRequest, context: RiskContext) -> RuleResult:
        if not self.config.enabled:
            return RuleResult(action="pass")

        symbol = request.symbol
        state = context.market_by_symbol.get(symbol)
        if state is None:
            if self.config.reject_if_missing_market_state:
                return RuleResult(
                    action="reject",
                    reason=f"missing market state for {symbol}",
                    metadata={"symbol": symbol},
                )
            return RuleResult(action="pass")

        if bool(state.get("is_suspended", False)):
            return RuleResult(
                action="reject",
                reason=f"symbol {symbol} is suspended",
                metadata={"symbol": symbol},
            )

        if "is_tradable" in state and not bool(state.get("is_tradable")):
            return RuleResult(
                action="reject",
                reason=f"symbol {symbol} is not tradable",
                metadata={"symbol": symbol},
            )

        return RuleResult(action="pass")


class DrawdownCircuitBreakerRule(BaseRiskRule):
    """规则实现。"""
    def __init__(self, config: DrawdownCircuitBreakerRuleConfig) -> None:
        super().__init__(name="drawdown_circuit_breaker")
        self.config = config

    def evaluate_order(self, request: OrderRequest, context: RiskContext) -> RuleResult:
        if not self.config.enabled:
            return RuleResult(action="pass")

        threshold = -float(self.config.max_drawdown)
        if float(context.snapshot.drawdown) <= threshold:
            return RuleResult(
                action="reject",
                reason=(
                    f"drawdown {context.snapshot.drawdown:.6f} breached max "
                    f"-{self.config.max_drawdown:.6f}"
                ),
                metadata={"drawdown": context.snapshot.drawdown, "max_drawdown": self.config.max_drawdown},
            )
        return RuleResult(action="pass")


class MaxSymbolPositionRule(BaseRiskRule):
    """规则实现。"""
    def __init__(self, config: MaxSymbolPositionRuleConfig) -> None:
        super().__init__(name="max_symbol_position")
        self.config = config

    def evaluate_order(self, request: OrderRequest, context: RiskContext) -> RuleResult:
        if not self.config.enabled:
            return RuleResult(action="pass")

        symbol = request.symbol
        current_qty = float(context.snapshot.positions.get(symbol, 0.0))
        signed_delta = _signed_quantity(request)
        proposed_qty = current_qty + signed_delta

        limits: list[float] = []
        if self.config.max_abs_qty is not None:
            limits.append(float(self.config.max_abs_qty))

        if self.config.max_weight is not None:
            price = _resolve_price(symbol, context)
            equity = float(context.snapshot.equity)
            if price > 0 and equity > 0:
                limits.append(float(self.config.max_weight) * equity / price)

        if not limits:
            return RuleResult(action="pass")

        max_abs_qty = min(limits)
        if abs(proposed_qty) <= max_abs_qty + _EPS:
            return RuleResult(action="pass")

        clipped_qty = max(min(proposed_qty, max_abs_qty), -max_abs_qty)
        clipped_delta = clipped_qty - current_qty

        if abs(clipped_delta) <= _EPS:
            return RuleResult(
                action="reject",
                reason=(
                    f"symbol {symbol} position exceeds limit and no additional capacity: "
                    f"proposed={proposed_qty:.6f}, limit={max_abs_qty:.6f}"
                ),
                metadata={
                    "symbol": symbol,
                    "proposed_qty": proposed_qty,
                    "current_qty": current_qty,
                    "max_abs_qty": max_abs_qty,
                },
            )

        modified = _clone_order(request, signed_delta=clipped_delta)
        return RuleResult(
            action="modify",
            reason=f"clip symbol {symbol} position to max_abs_qty={max_abs_qty:.6f}",
            metadata={
                "symbol": symbol,
                "current_qty": current_qty,
                "proposed_qty": proposed_qty,
                "clipped_qty": clipped_qty,
                "max_abs_qty": max_abs_qty,
            },
            modified_order=modified,
        )


class MaxLeverageRule(BaseRiskRule):
    """规则实现。"""
    def __init__(self, config: MaxLeverageRuleConfig) -> None:
        super().__init__(name="max_leverage")
        self.config = config

    def evaluate_order(self, request: OrderRequest, context: RiskContext) -> RuleResult:
        if not self.config.enabled:
            return RuleResult(action="pass")

        symbol = request.symbol
        equity = float(context.snapshot.equity)
        if equity <= 0:
            return RuleResult(
                action="reject",
                reason="equity <= 0, leverage check cannot pass",
                metadata={"equity": equity},
            )

        price = _resolve_price(symbol, context)
        if price <= 0:
            return RuleResult(
                action="reject",
                reason=f"invalid price for leverage check: {symbol}",
                metadata={"symbol": symbol},
            )

        current_qty = float(context.snapshot.positions.get(symbol, 0.0))
        signed_delta = _signed_quantity(request)
        proposed_qty = current_qty + signed_delta

        current_gross = float(context.snapshot.leverage) * equity
        current_symbol_gross = abs(current_qty * price)
        proposed_symbol_gross = abs(proposed_qty * price)
        proposed_gross = current_gross - current_symbol_gross + proposed_symbol_gross
        max_gross = float(self.config.max_leverage) * equity

        if proposed_gross <= max_gross + _EPS:
            return RuleResult(action="pass")

        if proposed_symbol_gross <= current_symbol_gross + _EPS:
            return RuleResult(action="pass")

        allowed_symbol_gross = max_gross - (current_gross - current_symbol_gross)
        if allowed_symbol_gross <= 0:
            return RuleResult(
                action="reject",
                reason="portfolio gross leverage already above limit; reject gross-increasing order",
                metadata={
                    "current_gross": current_gross,
                    "max_gross": max_gross,
                    "proposed_gross": proposed_gross,
                },
            )

        clipped_abs_qty = allowed_symbol_gross / price
        clipped_qty = max(min(proposed_qty, clipped_abs_qty), -clipped_abs_qty)
        clipped_delta = clipped_qty - current_qty
        if abs(clipped_delta) <= _EPS:
            return RuleResult(
                action="reject",
                reason="order cannot increase leverage within remaining gross capacity",
                metadata={
                    "symbol": symbol,
                    "current_gross": current_gross,
                    "max_gross": max_gross,
                    "allowed_symbol_gross": allowed_symbol_gross,
                },
            )

        modified = _clone_order(request, signed_delta=clipped_delta)
        return RuleResult(
            action="modify",
            reason=f"clip order to satisfy max_leverage={self.config.max_leverage:.6f}",
            metadata={
                "symbol": symbol,
                "current_gross": current_gross,
                "proposed_gross": proposed_gross,
                "max_gross": max_gross,
                "clipped_qty": clipped_qty,
            },
            modified_order=modified,
        )


class DailyTurnoverRule(BaseRiskRule):
    """规则实现。"""
    def __init__(self, config: DailyTurnoverRuleConfig) -> None:
        super().__init__(name="daily_turnover")
        self.config = config

    def evaluate_order(self, request: OrderRequest, context: RiskContext) -> RuleResult:
        if not self.config.enabled:
            return RuleResult(action="pass")

        equity = float(context.snapshot.equity)
        limits: list[float] = []
        if self.config.max_notional is not None:
            limits.append(float(self.config.max_notional))
        if self.config.max_ratio_of_equity is not None and equity > 0:
            limits.append(float(self.config.max_ratio_of_equity) * equity)

        if not limits:
            return RuleResult(action="pass")

        budget = min(limits)
        consumed = float(context.daily_turnover)
        remaining = budget - consumed

        price = _resolve_price(request.symbol, context)
        if price <= 0:
            return RuleResult(
                action="reject",
                reason=f"invalid price for turnover check: {request.symbol}",
                metadata={"symbol": request.symbol},
            )

        order_notional = float(request.quantity) * price
        if order_notional <= remaining + _EPS:
            return RuleResult(action="pass")

        if remaining <= _EPS:
            return RuleResult(
                action="reject",
                reason="daily turnover budget exhausted",
                metadata={"budget": budget, "consumed": consumed, "remaining": remaining},
            )

        clipped_qty = remaining / price
        if clipped_qty <= _EPS:
            return RuleResult(
                action="reject",
                reason="remaining turnover too small to place order",
                metadata={"remaining": remaining, "price": price},
            )

        signed_delta = _signed_quantity(request)
        modified = _clone_order(
            request,
            signed_delta=(clipped_qty if signed_delta > 0 else -clipped_qty),
        )
        return RuleResult(
            action="modify",
            reason="clip order to remaining daily turnover budget",
            metadata={
                "budget": budget,
                "consumed": consumed,
                "remaining": remaining,
                "order_notional": order_notional,
            },
            modified_order=modified,
        )
