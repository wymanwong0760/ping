"""风控模块数据模型定义。

本模块承载四层语义对象：
1) 上下文（`RiskContext`）：规则计算输入；
2) 规则结果（`RuleResult`）：单条规则输出；
3) 最终决策（`RiskDecision`）：引擎聚合结论；
4) 审计记录（`RiskAuditRecord`）：命中轨迹留痕。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Mapping

from quant_system.core import OrderRequest, PortfolioSnapshot, TargetPosition

# 引擎层最终动作：对外暴露给 broker/backtest 的聚合决策。
RiskAction = Literal["approve", "modify", "reject"]
# 规则层动作：单条规则内部语义，需经引擎聚合后才形成最终 RiskAction。
RuleAction = Literal["pass", "modify", "reject"]


@dataclass(slots=True)
class RiskContext:
    """风控运行上下文。

    说明：
    - `daily_turnover` 为当前评估时点已消耗的当日换手（由引擎动态维护）；
    - `close_prices` / `market_by_symbol` 为价格与交易状态来源；
    - `metadata` 用于承载调用方扩展信息，不参与默认规则计算。
    """

    timestamp: datetime
    snapshot: PortfolioSnapshot
    close_prices: Mapping[str, float] = field(default_factory=dict)
    market_by_symbol: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    daily_turnover: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RuleResult:
    """单条规则评估结果。

    字段约定：
    - `action="pass"` 时通常不携带 modified_*；
    - `action="modify"` 时应携带对应的 `modified_order` 或 `modified_target`；
    - `action="reject"` 时 modified_* 应为空。
    """

    action: RuleAction = "pass"
    reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    modified_order: OrderRequest | None = None
    modified_target: TargetPosition | None = None


@dataclass(slots=True)
class RiskAuditRecord:
    """风控审计记录。

    仅记录命中规则（modify/reject）。
    其中 `original/updated` 用于回放规则前后输入输出差异。
    """

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
    """风控决策结果。

    对每笔原始请求输出一条最终结论：
    - `original_request` 始终是入口请求；
    - `final_request` 在 approve/modify 时存在，在 reject 时为空；
    - `rule_name/reason` 标识最终生效规则及原因。
    """

    timestamp: datetime
    symbol: str
    action: RiskAction
    rule_name: str
    reason: str
    original_request: OrderRequest | TargetPosition
    final_request: OrderRequest | TargetPosition | None
    metadata: dict[str, Any] = field(default_factory=dict)
