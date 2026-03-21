"""配置对象定义。"""
from __future__ import annotations

from dataclasses import dataclass, field

from .exceptions import RiskConfigError


@dataclass(slots=True)
class MaxSymbolPositionRuleConfig:
    """单标的仓位限制配置。"""

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
    """杠杆上限配置。"""

    enabled: bool = True
    max_leverage: float = 1.0

    def __post_init__(self) -> None:
        if self.max_leverage <= 0:
            raise RiskConfigError("max_leverage must be > 0")


@dataclass(slots=True)
class DailyTurnoverRuleConfig:
    """日内换手预算配置。"""

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
    """标的池过滤配置。"""

    enabled: bool = True
    blacklist: set[str] = field(default_factory=set)
    whitelist: set[str] = field(default_factory=set)


@dataclass(slots=True)
class TradabilityRuleConfig:
    """可交易性检查配置。"""

    enabled: bool = True
    reject_if_missing_market_state: bool = True


@dataclass(slots=True)
class DrawdownCircuitBreakerRuleConfig:
    """回撤熔断配置。"""

    enabled: bool = True
    max_drawdown: float = 0.15

    def __post_init__(self) -> None:
        if not (0.0 < self.max_drawdown < 1.0):
            raise RiskConfigError("max_drawdown must be in (0, 1)")


@dataclass(slots=True)
class RiskConfig:
    """风控引擎配置。"""

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
