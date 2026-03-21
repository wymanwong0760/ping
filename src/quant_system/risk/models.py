"""数据模型定义。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Mapping

from quant_system.core import OrderRequest, PortfolioSnapshot, TargetPosition

RiskAction = Literal["approve", "modify", "reject"]
RuleAction = Literal["pass", "modify", "reject"]


@dataclass(slots=True)
class RiskContext:
    """风控运行上下文。"""

    timestamp: datetime
    snapshot: PortfolioSnapshot
    close_prices: Mapping[str, float] = field(default_factory=dict)
    market_by_symbol: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    daily_turnover: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RuleResult:
    """单条规则评估结果。"""

    action: RuleAction = "pass"
    reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    modified_order: OrderRequest | None = None
    modified_target: TargetPosition | None = None


@dataclass(slots=True)
class RiskAuditRecord:
    """风控审计记录。"""

    timestamp: datetime
    symbol: str
    rule_name: str
    action: Literal["modify", "reject"]
    reason: str
    original: dict[str, Any]
    updated: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RiskDecision:
    """风控决策结果。"""

    timestamp: datetime
    symbol: str
    action: RiskAction
    rule_name: str
    reason: str
    original_request: OrderRequest | TargetPosition
    final_request: OrderRequest | TargetPosition | None
    metadata: dict[str, Any] = field(default_factory=dict)
