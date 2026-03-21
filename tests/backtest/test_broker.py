"""test_broker 测试用例。"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from quant_system.backtest import BacktestConfig, SimBroker
from quant_system.core import OrderRequest


def _order(order_type: str = "market", side: str = "buy", limit_price: float | None = None) -> OrderRequest:
    return OrderRequest(
        timestamp=datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc),
        symbol="000001.SZ",
        side=side,  # type: ignore[arg-type]
        quantity=10.0,
        order_type=order_type,  # type: ignore[arg-type]
        limit_price=limit_price,
        source="test",
    )


def _bar() -> dict[str, float]:
    return {"open": 10.0, "high": 10.4, "low": 9.8, "close": 10.2}


def test_market_order_next_open_fill_and_cost_models() -> None:
    broker = SimBroker(
        BacktestConfig(
            fill_mode="next_open",
            commission_bps=10.0,
            commission_per_order=1.0,
            slippage_bps=20.0,
        )
    )

    fills, remaining = broker.match_orders(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        bars_by_symbol={"000001.SZ": _bar()},
        orders=[_order(order_type="market")],
    )

    assert len(remaining) == 0
    assert len(fills) == 1
    fill = fills[0]
    assert fill.price == pytest.approx(10.0)
    notional = 100.0
    assert fill.commission == pytest.approx(notional * 0.001 + 1.0)
    assert fill.slippage == pytest.approx(notional * 0.002)


def test_limit_order_fill_when_touched() -> None:
    broker = SimBroker(BacktestConfig(fill_mode="next_open"))
    order = _order(order_type="limit", side="buy", limit_price=9.9)

    fills, remaining = broker.match_orders(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        bars_by_symbol={"000001.SZ": _bar()},
        orders=[order],
    )

    assert len(fills) == 1
    assert len(remaining) == 0
    assert fills[0].price == pytest.approx(9.9)


def test_limit_order_remains_when_not_touched() -> None:
    broker = SimBroker(BacktestConfig(fill_mode="next_open"))
    order = _order(order_type="limit", side="buy", limit_price=9.5)

    fills, remaining = broker.match_orders(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        bars_by_symbol={"000001.SZ": _bar()},
        orders=[order],
    )

    assert len(fills) == 0
    assert len(remaining) == 1


def test_market_order_current_close_mode() -> None:
    broker = SimBroker(BacktestConfig(fill_mode="current_close"))

    fills, _ = broker.match_orders(
        timestamp=datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc),
        bars_by_symbol={"000001.SZ": _bar()},
        orders=[_order(order_type="market")],
    )

    assert fills[0].price == pytest.approx(10.2)
