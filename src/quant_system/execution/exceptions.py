"""异常类型定义。"""
from __future__ import annotations


class ExecutionError(Exception):
    """执行模块基础异常。"""


class ExecutionConfigError(ExecutionError):
    """执行配置异常。"""


class ExecutionStateError(ExecutionError):
    """执行状态异常。"""
