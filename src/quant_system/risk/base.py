"""风控规则抽象基类与扩展契约。"""
from __future__ import annotations

from quant_system.core import OrderRequest, TargetPosition

from .models import RiskContext, RuleResult


class BaseRiskRule:
    """风控规则基类。

    约束：子类应保持纯评估语义，不直接修改引擎状态；
    通过返回 `RuleResult` 把 pass/modify/reject 交由引擎统一编排。
    """

    name: str

    def __init__(self, name: str) -> None:
        self.name = name

    def evaluate_order(self, request: OrderRequest, context: RiskContext) -> RuleResult:
        """评估订单请求。

        基类默认返回 `pass`，表示“无约束”。
        子类按需覆写并返回 modify/reject。
        """
        return RuleResult(action="pass")

    def evaluate_target(self, request: TargetPosition, context: RiskContext) -> RuleResult:
        """评估目标仓位请求。

        默认同样返回 `pass`，保持与订单评估一致的扩展契约。
        """
        return RuleResult(action="pass")
