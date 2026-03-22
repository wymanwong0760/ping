"""风控配置对象定义。

该模块用于描述：
- 各规则的开关与参数约束；
- 规则执行顺序（`rule_order`，即优先级）；
- 启动期参数校验（不合法配置尽早失败）。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .exceptions import RiskConfigError


@dataclass(slots=True)
class MaxSymbolPositionRuleConfig:
    """单标的仓位限制配置。

    - `max_abs_qty`: 单标的绝对持仓上限（股数）；
    - `max_weight`: 单标的权益权重上限（0, 1]；
    两者可同时配置，规则执行时取更严格约束。
    """

    enabled: bool = True
    max_abs_qty: float | None = None
    max_weight: float | None = 0.2

    def __post_init__(self) -> None:
        if self.max_abs_qty is not None and self.max_abs_qty <= 0:
            raise RiskConfigError("max_abs_qty must be > 0 when provided")
        if self.max_weight is not None and not (0.0 < self.max_weight <= 1.0):
            raise RiskConfigError("max_weight must be in (0, 1]")


@dataclass(slots=True)
class MaxLeverageRuleConfig:
    """杠杆上限配置。

    `max_leverage` 为组合总敞口 / 权益的上限，必须大于 0。
    """

    enabled: bool = True
    max_leverage: float = 1.0

    def __post_init__(self) -> None:
        if self.max_leverage <= 0:
            raise RiskConfigError("max_leverage must be > 0")


@dataclass(slots=True)
class DailyTurnoverRuleConfig:
    """日内换手预算配置。

    - `max_notional`: 固定名义额预算；
    - `max_ratio_of_equity`: 按权益比例给出的预算；
    两者同时存在时按更严格预算生效。
    """

    enabled: bool = True
    max_notional: float | None = None
    max_ratio_of_equity: float | None = 1.0

    def __post_init__(self) -> None:
        if self.max_notional is not None and self.max_notional <= 0:
            raise RiskConfigError("max_notional must be > 0 when provided")
        if self.max_ratio_of_equity is not None and self.max_ratio_of_equity <= 0:
            raise RiskConfigError("max_ratio_of_equity must be > 0 when provided")


@dataclass(slots=True)
class UniverseFilterRuleConfig:
    """标的池过滤配置。

    通过 blacklist/whitelist 定义可交易标的范围。
    """

    enabled: bool = True
    blacklist: set[str] = field(default_factory=set)
    whitelist: set[str] = field(default_factory=set)


@dataclass(slots=True)
class TradabilityRuleConfig:
    """可交易性检查配置。

    `reject_if_missing_market_state` 控制缺失行情状态时是拒单还是放行。
    """

    enabled: bool = True
    reject_if_missing_market_state: bool = True


@dataclass(slots=True)
class DrawdownCircuitBreakerRuleConfig:
    """回撤熔断配置。

    `max_drawdown` 取值范围 (0, 1)，表示可容忍的最大回撤比例。
    """

    enabled: bool = True
    max_drawdown: float = 0.15

    def __post_init__(self) -> None:
        if not (0.0 < self.max_drawdown < 1.0):
            raise RiskConfigError("max_drawdown must be in (0, 1)")


@dataclass(slots=True)
class RiskConfig:
    """风控引擎总配置。

    `rule_order` 定义规则执行顺序（即优先级）。
    规则名越靠前，越先参与判定，越可能先触发短路或修改。
    """

    rule_order: tuple[str, ...] = (
        "universe_filter",
        "tradability",
        "drawdown_circuit_breaker",
        "max_symbol_position",
        "max_leverage",
        "daily_turnover",
    )
    max_symbol_position: MaxSymbolPositionRuleConfig = field(
        default_factory=MaxSymbolPositionRuleConfig
    )
    max_leverage: MaxLeverageRuleConfig = field(default_factory=MaxLeverageRuleConfig)
    daily_turnover: DailyTurnoverRuleConfig = field(default_factory=DailyTurnoverRuleConfig)
    universe_filter: UniverseFilterRuleConfig = field(default_factory=UniverseFilterRuleConfig)
    tradability: TradabilityRuleConfig = field(default_factory=TradabilityRuleConfig)
    drawdown_circuit_breaker: DrawdownCircuitBreakerRuleConfig = field(
        default_factory=DrawdownCircuitBreakerRuleConfig
    )

    def __post_init__(self) -> None:
        """在启动期校验 `rule_order` 合法性。

        若包含未注册规则名则立即抛错，避免运行中才暴露配置问题。
        """
        allowed = {
            "universe_filter",
            "tradability",
            "drawdown_circuit_breaker",
            "max_symbol_position",
            "max_leverage",
            "daily_turnover",
        }
        unknown = [name for name in self.rule_order if name not in allowed]
        if unknown:
            raise RiskConfigError(f"Unsupported rule names in rule_order: {unknown}")
