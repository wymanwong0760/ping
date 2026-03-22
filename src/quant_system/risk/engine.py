"""风控引擎实现。

该模块负责把多条风控规则按 `RiskConfig.rule_order` 串联执行，
并将规则层 `pass/modify/reject` 聚合为引擎层 `approve/modify/reject`。
同时维护可审计轨迹（`RiskAuditRecord`）与日内换手累计。
"""
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
    """风控引擎。

    决策语义：
    - 任一规则返回 `reject`：立即终止后续规则并输出 `reject`；
    - 任一规则返回 `modify`：应用修改后继续评估后续规则；
    - 全部规则仅 `pass`：输出 `approve`。

    设计上将“规则动作”和“最终决策”分层，便于调用方区分：
    规则层是局部判断，引擎层是全链路聚合结果。
    """

    def __init__(self, config: RiskConfig | None = None) -> None:
        self.config = config or RiskConfig()
        self.rules = self._build_rules(self.config)
        self._turnover_by_date: dict[date, float] = {}

    def evaluate_orders(
        self,
        requests: Sequence[OrderRequest],
        context: RiskContext,
    ) -> tuple[list[OrderRequest], list[RiskDecision], list[RiskAuditRecord]]:
        """逐笔评估订单请求并返回通过单、决策与审计记录。

        返回值关系：
        - accepted: 最终可进入执行层的订单（approve/modify 且带 final_request）；
        - decisions: 每笔原始请求对应一条最终决策；
        - audits: 命中 `modify/reject` 的规则轨迹（可多条）。

        注意：
        仅当订单最终被接受时，才会按最终请求估算并累计当日换手，
        以确保 `daily_turnover` 规则看到的是“已通过风控”的真实消耗。
        """
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
                    # 仅累计最终通过风控的订单名义额，避免把 reject 请求误计入预算消耗。
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
        """逐笔评估目标仓位请求。

        与 `evaluate_orders` 保持一致的聚合语义：
        - `reject` 立即短路；
        - `modify` 记录审计并继续后续规则；
        - 最终输出 accepted / decisions / audits 三元组。
        """
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
                    # 与订单风控保持一致：reject 后短路，后续规则不再评估。
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
                    # modify 不终止链路，后续规则基于“修改后请求”继续评估。

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
        """评估单笔订单并产出最终决策与规则级审计。

        关键口径：
        `rule_context.daily_turnover` = `context.daily_turnover`（外部已发生）
        + `self._turnover_by_date[trading_day]`（本引擎当日已接受并累计）。
        这样可确保同一批次订单按顺序评估时，后续订单能看到前序订单对预算的占用。
        """
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
                # 规则侧看到的是“基础换手 + 当日已通过风控的累计换手”。
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
                # reject 为硬短路：当前订单立即结束评估，直接输出最终 reject 决策。
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
                # modify 为软约束：回写修改结果后，继续让后续规则做二次约束。

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
        """按 `rule_order` 构建规则实例列表。

        注册表提供“规则名 -> 实例”的稳定映射，最终执行顺序完全由
        `RiskConfig.rule_order` 决定，使优先级策略可配置且可测试。
        """
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
        """构建统一审计记录。

        字段语义：
        - original: 规则命中前输入；
        - updated: 仅 modify 时存在，表示规则调整后的输出；
        - reject 时 updated 为空，表示链路终止且无可执行请求。
        """
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
        """解析风控估算价格。

        回退优先级：
        1) `context.close_prices[symbol]`
        2) `context.market_by_symbol[symbol]["close"]`
        3) `context.market_by_symbol[symbol]["open"]`
        4) `0.0`（表示缺价，交由上层规则决定 reject/跳过）
        """
        if symbol in context.close_prices:
            return float(context.close_prices[symbol])
        payload = context.market_by_symbol.get(symbol, {})
        if "close" in payload:
            return float(payload["close"])
        if "open" in payload:
            return float(payload["open"])
        return 0.0
