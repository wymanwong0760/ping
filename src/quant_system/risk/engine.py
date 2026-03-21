"""风控引擎实现。"""
from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import date
from typing import Sequence

from quant_system.core import OrderRequest, TargetPosition

from .base import BaseRiskRule
from .config import RiskConfig
from .exceptions import RiskRuleError
from .models import RiskAuditRecord, RiskContext, RiskDecision, RuleResult
from .rules import (
    DailyTurnoverRule,
    DrawdownCircuitBreakerRule,
    MaxLeverageRule,
    MaxSymbolPositionRule,
    TradabilityRule,
    UniverseFilterRule,
)

logger = logging.getLogger(__name__)


class RiskEngine:
    """风控引擎。"""

    def __init__(self, config: RiskConfig | None = None) -> None:
        self.config = config or RiskConfig()
        self.rules = self._build_rules(self.config)
        self._turnover_by_date: dict[date, float] = {}

    def evaluate_orders(
        self,
        requests: Sequence[OrderRequest],
        context: RiskContext,
    ) -> tuple[list[OrderRequest], list[RiskDecision], list[RiskAuditRecord]]:
        """逐笔评估订单请求。"""
        accepted: list[OrderRequest] = []
        decisions: list[RiskDecision] = []
        audits: list[RiskAuditRecord] = []

        for request in requests:
            decision, audit_rows = self._evaluate_one_order(request=request, context=context)
            decisions.append(decision)
            audits.extend(audit_rows)
            if decision.action in {"approve", "modify"} and decision.final_request is not None:
                accepted.append(decision.final_request)
                price = self._resolve_price(decision.final_request.symbol, context)
                if price > 0:
                    trading_day = context.timestamp.date()
                    self._turnover_by_date[trading_day] = (
                        self._turnover_by_date.get(trading_day, 0.0)
                        + float(decision.final_request.quantity) * float(price)
                    )

        return accepted, decisions, audits

    def evaluate_targets(
        self,
        requests: Sequence[TargetPosition],
        context: RiskContext,
    ) -> tuple[list[TargetPosition], list[RiskDecision], list[RiskAuditRecord]]:
        """逐笔评估目标仓位请求。"""
        accepted: list[TargetPosition] = []
        decisions: list[RiskDecision] = []
        audits: list[RiskAuditRecord] = []

        for request in requests:
            current: TargetPosition = request
            triggered = False
            rejected = False
            final_rule = "risk_engine"
            reason = "approved"

            for rule in self.rules:
                try:
                    result = rule.evaluate_target(current, context)
                except Exception as exc:  # pragma: no cover
                    raise RiskRuleError(f"rule={rule.name} evaluate_target failed: {exc}") from exc

                if result.action == "pass":
                    continue

                triggered = True
                final_rule = rule.name
                reason = result.reason or result.action

                if result.action == "reject":
                    rejected = True
                    audits.append(
                        self._build_audit(
                            timestamp=context.timestamp,
                            symbol=current.symbol,
                            rule_name=rule.name,
                            action="reject",
                            reason=reason,
                            original=asdict(request),
                            updated=None,
                            metadata=result.metadata,
                        )
                    )
                    break

                if result.action == "modify" and result.modified_target is not None:
                    previous = current
                    current = result.modified_target
                    audits.append(
                        self._build_audit(
                            timestamp=context.timestamp,
                            symbol=current.symbol,
                            rule_name=rule.name,
                            action="modify",
                            reason=reason,
                            original=asdict(previous),
                            updated=asdict(current),
                            metadata=result.metadata,
                        )
                    )

            if rejected:
                decisions.append(
                    RiskDecision(
                        timestamp=context.timestamp,
                        symbol=request.symbol,
                        action="reject",
                        rule_name=final_rule,
                        reason=reason,
                        original_request=request,
                        final_request=None,
                    )
                )
                continue

            action = "modify" if triggered and current != request else "approve"
            decisions.append(
                RiskDecision(
                    timestamp=context.timestamp,
                    symbol=request.symbol,
                    action=action,
                    rule_name=(final_rule if triggered else "risk_engine"),
                    reason=(reason if triggered else "approved"),
                    original_request=request,
                    final_request=current,
                )
            )
            accepted.append(current)

        return accepted, decisions, audits

    def get_daily_turnover(self, trading_day: date) -> float:
        """返回指定交易日累计换手额。"""
        return float(self._turnover_by_date.get(trading_day, 0.0))

    def _evaluate_one_order(
        self,
        request: OrderRequest,
        context: RiskContext,
    ) -> tuple[RiskDecision, list[RiskAuditRecord]]:
        current: OrderRequest = request
        audits: list[RiskAuditRecord] = []
        triggered = False
        final_rule = "risk_engine"
        final_reason = "approved"

        trading_day = context.timestamp.date()
        base_turnover = float(context.daily_turnover)

        for rule in self.rules:
            rule_context = RiskContext(
                timestamp=context.timestamp,
                snapshot=context.snapshot,
                close_prices=context.close_prices,
                market_by_symbol=context.market_by_symbol,
                daily_turnover=base_turnover + self._turnover_by_date.get(trading_day, 0.0),
                metadata=dict(context.metadata),
            )
            result = self._safe_evaluate_order(rule=rule, request=current, context=rule_context)
            if result.action == "pass":
                continue

            triggered = True
            final_rule = rule.name
            final_reason = result.reason or result.action

            if result.action == "reject":
                audits.append(
                    self._build_audit(
                        timestamp=context.timestamp,
                        symbol=current.symbol,
                        rule_name=rule.name,
                        action="reject",
                        reason=final_reason,
                        original=asdict(request),
                        updated=None,
                        metadata=result.metadata,
                    )
                )
                logger.info(
                    "Risk rejected order symbol=%s rule=%s reason=%s",
                    current.symbol,
                    rule.name,
                    final_reason,
                )
                return (
                    RiskDecision(
                        timestamp=context.timestamp,
                        symbol=request.symbol,
                        action="reject",
                        rule_name=rule.name,
                        reason=final_reason,
                        original_request=request,
                        final_request=None,
                    ),
                    audits,
                )

            if result.action == "modify" and result.modified_order is not None:
                previous = current
                current = result.modified_order
                audits.append(
                    self._build_audit(
                        timestamp=context.timestamp,
                        symbol=current.symbol,
                        rule_name=rule.name,
                        action="modify",
                        reason=final_reason,
                        original=asdict(previous),
                        updated=asdict(current),
                        metadata=result.metadata,
                    )
                )
                logger.info(
                    "Risk modified order symbol=%s rule=%s reason=%s",
                    current.symbol,
                    rule.name,
                    final_reason,
                )

        decision_action = "modify" if triggered and current != request else "approve"
        return (
            RiskDecision(
                timestamp=context.timestamp,
                symbol=request.symbol,
                action=decision_action,
                rule_name=(final_rule if triggered else "risk_engine"),
                reason=(final_reason if triggered else "approved"),
                original_request=request,
                final_request=current,
            ),
            audits,
        )

    @staticmethod
    def _build_rules(config: RiskConfig) -> list[BaseRiskRule]:
        registry: dict[str, BaseRiskRule] = {
            "universe_filter": UniverseFilterRule(config.universe_filter),
            "tradability": TradabilityRule(config.tradability),
            "drawdown_circuit_breaker": DrawdownCircuitBreakerRule(config.drawdown_circuit_breaker),
            "max_symbol_position": MaxSymbolPositionRule(config.max_symbol_position),
            "max_leverage": MaxLeverageRule(config.max_leverage),
            "daily_turnover": DailyTurnoverRule(config.daily_turnover),
        }
        return [registry[name] for name in config.rule_order]

    @staticmethod
    def _safe_evaluate_order(
        rule: BaseRiskRule,
        request: OrderRequest,
        context: RiskContext,
    ) -> RuleResult:
        try:
            return rule.evaluate_order(request, context)
        except Exception as exc:  # pragma: no cover
            raise RiskRuleError(f"rule={rule.name} evaluate_order failed: {exc}") from exc

    @staticmethod
    def _build_audit(
        *,
        timestamp,
        symbol: str,
        rule_name: str,
        action: str,
        reason: str,
        original: dict,
        updated: dict | None,
        metadata: dict,
    ) -> RiskAuditRecord:
        return RiskAuditRecord(
            timestamp=timestamp,
            symbol=symbol,
            rule_name=rule_name,
            action=action,  # type: ignore[arg-type]
            reason=reason,
            original=original,
            updated=updated,
            metadata=dict(metadata),
        )

    @staticmethod
    def _resolve_price(symbol: str, context: RiskContext) -> float:
        if symbol in context.close_prices:
            return float(context.close_prices[symbol])
        payload = context.market_by_symbol.get(symbol, {})
        if "close" in payload:
            return float(payload["close"])
        if "open" in payload:
            return float(payload["open"])
        return 0.0
