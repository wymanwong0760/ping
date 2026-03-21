"""异常类型定义。"""
from __future__ import annotations


class RiskError(Exception):
    """风控模块基类异常。"""


class RiskConfigError(RiskError):
    """风控配置异常。"""


class RiskRuleError(RiskError):
    """风控规则执行异常。"""
