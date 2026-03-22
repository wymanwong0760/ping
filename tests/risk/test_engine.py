"""RiskEngine 行为测试。

覆盖点：规则链聚合、reject 短路、modify 结果落地与审计记录输出。
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from quant_system.core import OrderRequest, PortfolioSnapshot
from quant_system.risk import RiskConfig, RiskContext, RiskEngine


def _snapshot(*, positions: dict[str, float] | None = None, equity: float = 1000.0, leverage: float = 0.0, drawdown: float = 0.0) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        cash=equity,
        equity=equity,
        positions=positions or {},
        leverage=leverage,
        drawdown=drawdown,
    )


def _order(symbol: str, qty: float) -> OrderRequest:
    return OrderRequest(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        symbol=symbol,
        side="buy",
        quantity=qty,
        order_type="market",
        source="test",
    )


def test_risk_engine_multi_rules_are_predictable() -> None:
    # 覆盖多规则联动：同一批次里同时出现 modify（仓位裁剪）与 reject（黑名单）。
    config = RiskConfig()
    config.universe_filter.blacklist.add("000002.SZ")
    config.max_symbol_position.max_abs_qty = 80.0
    config.max_symbol_position.max_weight = None
    config.max_leverage.max_leverage = 0.8
    config.daily_turnover.max_notional = 2_000.0
    config.daily_turnover.max_ratio_of_equity = None

    engine = RiskEngine(config)
    context = RiskContext(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        snapshot=_snapshot(positions={"000001.SZ": 10.0}, equity=1_000.0, leverage=0.1),
        close_prices={"000001.SZ": 10.0, "000002.SZ": 20.0},
        market_by_symbol={
            "000001.SZ": {"close": 10.0, "is_suspended": False},
            "000002.SZ": {"close": 20.0, "is_suspended": False},
        },
    )

    orders = [_order("000001.SZ", 100.0), _order("000002.SZ", 10.0)]
    accepted, decisions, audits = engine.evaluate_orders(orders, context)

    assert len(accepted) == 1
    assert accepted[0].symbol == "000001.SZ"
    assert accepted[0].quantity == pytest.approx(70.0)
    by_symbol = {item.symbol: item for item in decisions}
    assert by_symbol["000001.SZ"].action == "modify"
    assert by_symbol["000001.SZ"].rule_name == "max_symbol_position"
    assert by_symbol["000002.SZ"].action == "reject"
    assert by_symbol["000002.SZ"].rule_name == "universe_filter"

    assert any(row.action == "modify" and row.symbol == "000001.SZ" for row in audits)
    assert any(row.action == "reject" and row.symbol == "000002.SZ" for row in audits)


def test_risk_engine_drawdown_circuit_breaker_rejects_all() -> None:
    # 覆盖回撤熔断短路：订单应全部 reject，并产生对应审计记录。
    config = RiskConfig()
    config.drawdown_circuit_breaker.max_drawdown = 0.05
    engine = RiskEngine(config)

    context = RiskContext(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        snapshot=_snapshot(drawdown=-0.08),
        close_prices={"000001.SZ": 10.0},
        market_by_symbol={"000001.SZ": {"close": 10.0, "is_suspended": False}},
    )

    accepted, decisions, audits = engine.evaluate_orders([_order("000001.SZ", 10.0)], context)
    assert accepted == []
    assert decisions[0].action == "reject"
    assert decisions[0].rule_name == "drawdown_circuit_breaker"
    assert len(audits) == 1
