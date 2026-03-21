"""异常类型定义。"""
from __future__ import annotations


class StrategyError(Exception):
    """策略模块基类异常。"""


class StrategyConfigError(StrategyError):
    """策略配置异常。"""


class StrategyDataError(StrategyError):
    """策略数据异常。"""


class MissingDataError(StrategyDataError):
    """策略运行时数据缺失异常。"""


class FutureDataLeakError(StrategyDataError):
    """策略运行时发现未来数据泄漏异常。"""
