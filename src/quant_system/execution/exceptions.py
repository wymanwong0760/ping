"""执行模块异常类型定义。

异常分层用于区分配置问题与运行状态问题，便于调用方按类型处理。
"""
from __future__ import annotations


class ExecutionError(Exception):
    """执行模块基础异常。

    适合作为统一捕获执行模块异常的基类。
    """


class ExecutionConfigError(ExecutionError):
    """执行配置异常。

    在 `ExecutionConfig` 初始化校验失败时抛出。
    """


class ExecutionStateError(ExecutionError):
    """执行状态异常。

    用于表示执行流程中的状态不一致或非法状态转换。
    """
