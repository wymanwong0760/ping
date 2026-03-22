"""risk.rules 单规则行为测试。

覆盖点：各规则在 pass/modify/reject 三类动作上的关键边界。
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from quant_system.core import OrderRequest, PortfolioSnapshot
from quant_system.risk import (
    DailyTurnoverRule,
    DailyTurnoverRuleConfig,
    DrawdownCircuitBreakerRule,
    DrawdownCircuitBreakerRuleConfig,
    MaxLeverageRule,
    MaxLeverageRuleConfig,
    MaxSymbolPositionRule,
    MaxSymbolPositionRuleConfig,
    RiskContext,
    TradabilityRule,
    TradabilityRuleConfig,
    UniverseFilterRule,
    UniverseFilterRuleConfig,
)


def _snapshot(*, positions: dict[str, float] | None = None, equity: float = 1000.0, leverage: float = 0.0, drawdown: float = 0.0) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        cash=equity,
        equity=equity,
        positions=positions or {},
        leverage=leverage,
        drawdown=drawdown,
    )


def _order(symbol: str = "000001.SZ", side: str = "buy", qty: float = 100.0) -> OrderRequest:
    return OrderRequest(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        symbol=symbol,
        side=side,  # type: ignore[arg-type]
        quantity=qty,
        order_type="market",
        source="test",
    )


def test_max_symbol_position_rule_clips_order() -> None:
    # 仓位上限超限时应 modify，裁剪为允许增量。
    rule = MaxSymbolPositionRule(
        MaxSymbolPositionRuleConfig(enabled=True, max_abs_qty=80.0, max_weight=None)
    )
    context = RiskContext(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        snapshot=_snapshot(positions={"000001.SZ": 10.0}),
        close_prices={"000001.SZ": 10.0},
    )

    result = rule.evaluate_order(_order(qty=100.0), context)
    assert result.action == "modify"
    assert result.modified_order is not None
    assert result.modified_order.side == "buy"
    assert result.modified_order.quantity == pytest.approx(70.0)


def test_universe_filter_rule_rejects_blacklist() -> None:
    # 黑名单命中应直接 reject。
    rule = UniverseFilterRule(
        UniverseFilterRuleConfig(enabled=True, blacklist={"000001.SZ"}, whitelist=set())
    )
    context = RiskContext(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        snapshot=_snapshot(),
    )
    result = rule.evaluate_order(_order(symbol="000001.SZ"), context)
    assert result.action == "reject"


def test_drawdown_circuit_breaker_rejects_when_breached() -> None:
    # 当前回撤超过阈值（更负）时应触发熔断拒单。
    rule = DrawdownCircuitBreakerRule(
        DrawdownCircuitBreakerRuleConfig(enabled=True, max_drawdown=0.10)
    )
    context = RiskContext(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        snapshot=_snapshot(drawdown=-0.12),
    )
    result = rule.evaluate_order(_order(), context)
    assert result.action == "reject"


def test_tradability_rule_rejects_suspended_symbol() -> None:
    # 停牌标的应被可交易性规则拒绝。
    rule = TradabilityRule(TradabilityRuleConfig(enabled=True))
    context = RiskContext(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        snapshot=_snapshot(),
        market_by_symbol={"000001.SZ": {"is_suspended": True, "close": 10.0}},
    )
    result = rule.evaluate_order(_order(symbol="000001.SZ"), context)
    assert result.action == "reject"


def test_max_leverage_rule_clips_order() -> None:
    # 杠杆上限约束下应对超限订单进行裁剪。
    rule = MaxLeverageRule(MaxLeverageRuleConfig(enabled=True, max_leverage=0.5))
    context = RiskContext(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        snapshot=_snapshot(positions={"000001.SZ": 20.0}, equity=1000.0, leverage=0.4),
        close_prices={"000001.SZ": 10.0},
    )

    result = rule.evaluate_order(_order(symbol="000001.SZ", qty=20.0), context)
    assert result.action == "modify"
    assert result.modified_order is not None
    assert result.modified_order.quantity == pytest.approx(10.0)


def test_daily_turnover_rule_clips_order() -> None:
    # 日内预算剩余不足时应 modify 到可用额度。
    rule = DailyTurnoverRule(
        DailyTurnoverRuleConfig(enabled=True, max_notional=1000.0, max_ratio_of_equity=None)
    )
    context = RiskContext(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        snapshot=_snapshot(equity=1000.0),
        close_prices={"000001.SZ": 10.0},
        daily_turnover=700.0,
    )

    result = rule.evaluate_order(_order(qty=50.0), context)
    assert result.action == "modify"
    assert result.modified_order is not None
    assert result.modified_order.quantity == pytest.approx(30.0)
