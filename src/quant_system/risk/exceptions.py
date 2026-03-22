"""风控模块异常类型定义。

异常分层用于区分配置问题与运行期规则执行问题，
便于调用方按类型进行恢复或告警。
"""
from __future__ import annotations


class RiskError(Exception):
    """风控模块基础异常。

    适合作为统一捕获风控相关错误的根类型。
    """


class RiskConfigError(RiskError):
    """风控配置异常。

    在配置对象初始化或参数校验失败时抛出。
    """


class RiskRuleError(RiskError):
    """风控规则执行异常。

    用于封装规则运行期异常（例如规则内部报错），
    由引擎统一抛出以携带 rule 名称等上下文。
    """
