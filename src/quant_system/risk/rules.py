"""风控规则实现。

规则层仅负责“局部约束判断”，统一返回 `RuleResult(pass/modify/reject)`；
最终订单是否通过、是否短路以及审计聚合由 `RiskEngine` 负责。
"""
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

# 浮点比较容忍度：用于边界比较，避免因机器误差导致“等于上限”被误判超限。
_EPS = 1e-12


def _signed_quantity(order: OrderRequest) -> float:
    """把订单方向编码为带符号数量（buy 为正，sell 为负）。"""
    return float(order.quantity) if order.side == "buy" else -float(order.quantity)


def _clone_order(order: OrderRequest, *, signed_delta: float) -> OrderRequest:
    """基于带符号增量克隆订单并回写 side/quantity。"""
    side = "buy" if signed_delta > 0 else "sell"
    return replace(order, side=side, quantity=abs(float(signed_delta)))


def _resolve_price(symbol: str, context: RiskContext, fallback: float | None = None) -> float:
    """解析规则估算价格并支持可选回退值。

    优先级：`close_prices -> market.close -> market.open -> fallback -> 0.0`。
    """
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
    """标的池过滤规则。

    语义：
    - 命中 blacklist：reject；
    - 配置了 whitelist 且标的不在其中：reject；
    - 其余情况：pass。
    """
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
            # whitelist 非空时按“仅允许列表内”解释，显式拒绝外部标的。
            return RuleResult(
                action="reject",
                reason=f"symbol {symbol} not in whitelist",
                metadata={"symbol": symbol},
            )
        return RuleResult(action="pass")


class TradabilityRule(BaseRiskRule):
    """可交易性规则。

    检查行情状态完整性与交易状态（停牌/不可交易），
    用于在进入撮合前提前拒绝明显不可执行订单。
    """
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
            # 允许缺状态放行时，不在此规则层阻断，由后续规则/执行层继续判定。
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
    """回撤熔断规则。

    当组合当前回撤跌破阈值（更负）时拒绝新订单，
    防止在回撤超限阶段继续放大风险暴露。
    """
    def __init__(self, config: DrawdownCircuitBreakerRuleConfig) -> None:
        super().__init__(name="drawdown_circuit_breaker")
        self.config = config

    def evaluate_order(self, request: OrderRequest, context: RiskContext) -> RuleResult:
        if not self.config.enabled:
            return RuleResult(action="pass")

        threshold = -float(self.config.max_drawdown)
        # 回撤为负值，数值越小（更负）代表回撤越深，因此用 <= 判定触发熔断。
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
    """单标的仓位上限规则。

    同时支持绝对股数上限（`max_abs_qty`）与权益权重上限（`max_weight`）。
    当二者同时存在时取更严格约束；超限时优先裁剪，无法再裁剪则拒单。
    """
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
                # 将权重上限转换为可持仓股数上限，与 max_abs_qty 统一比较。
                limits.append(float(self.config.max_weight) * equity / price)

        if not limits:
            return RuleResult(action="pass")

        # 多约束同时存在时取最严格限制，避免宽松约束掩盖严格约束。
        max_abs_qty = min(limits)
        if abs(proposed_qty) <= max_abs_qty + _EPS:
            # 使用 EPS 容忍边界误差，避免浮点微差导致“贴边订单”被误拒。
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
    """组合杠杆上限规则。

    基于“当前组合总敞口 + 订单变动后目标敞口”进行比较：
    - 未超限：pass；
    - 可在剩余容量内裁剪：modify；
    - 无容量或无法有效裁剪：reject。
    """
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
        # 可分配给当前标的的剩余敞口 = 组合上限 - 其他标的已占用敞口。
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
    """日内换手预算规则。

    预算可由固定名义额（`max_notional`）与权益比例（`max_ratio_of_equity`）共同决定，
    二者同时存在时取最严格预算。超预算时优先裁剪，预算耗尽则拒绝。
    """
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
        # daily_turnover 由引擎维护为“当日累计已消耗换手”，remaining 为可继续下单预算。
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
            # 预算已耗尽（含浮点容忍范围），直接拒绝避免生成无效小单。
            return RuleResult(
                action="reject",
                reason="daily turnover budget exhausted",
                metadata={"budget": budget, "consumed": consumed, "remaining": remaining},
            )

        clipped_qty = remaining / price
        if clipped_qty <= _EPS:
            # 虽有剩余额度但不足以形成有效下单数量，按拒单处理更可解释。
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
