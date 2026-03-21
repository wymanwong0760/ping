"""异常类型定义。"""
from __future__ import annotations


class BacktestError(Exception):
    """回测模块基础异常。"""


class BacktestConfigError(BacktestError):
    """回测配置异常。"""


class BacktestDataError(BacktestError):
    """回测数据异常。"""


class OrderGenerationError(BacktestError):
    """订单生成异常。"""
