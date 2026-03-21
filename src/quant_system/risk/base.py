"""抽象基类与接口定义。"""
from __future__ import annotations

from quant_system.core import OrderRequest, TargetPosition

from .models import RiskContext, RuleResult


class BaseRiskRule:
    """风控规则基类。"""

    name: str

    def __init__(self, name: str) -> None:
        self.name = name

    def evaluate_order(self, request: OrderRequest, context: RiskContext) -> RuleResult:
        """评估订单请求。"""
        return RuleResult(action="pass")

    def evaluate_target(self, request: TargetPosition, context: RiskContext) -> RuleResult:
        """评估目标仓位请求。"""
        return RuleResult(action="pass")
