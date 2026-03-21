"""异常类型定义。"""
from __future__ import annotations


class DataError(Exception):
    """数据模块基础异常。"""


class DataConfigError(DataError):
    """数据配置异常。"""


class DataLoadError(DataError):
    """数据加载异常。"""


class DataValidationError(DataError):
    """数据校验异常。"""
