"""test_matching 测试用例。"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from quant_system.core import OrderRequest
from quant_system.execution import ExecutionEngine


def _order(order_type: str, side: str = "buy", limit_price: float | None = None) -> OrderRequest:
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


def test_resolve_fill_price_market_modes() -> None:
    order = _order("market")
    assert ExecutionEngine.resolve_fill_price(order, _bar(), "next_open") == pytest.approx(10.0)
    assert ExecutionEngine.resolve_fill_price(order, _bar(), "current_close") == pytest.approx(10.2)


def test_resolve_fill_price_limit_rules() -> None:
    buy = _order("limit", "buy", 9.9)
    sell = _order("limit", "sell", 10.3)
    miss = _order("limit", "buy", 9.5)

    assert ExecutionEngine.resolve_fill_price(buy, _bar(), "next_open") == pytest.approx(9.9)
    assert ExecutionEngine.resolve_fill_price(sell, _bar(), "next_open") == pytest.approx(10.3)
    assert ExecutionEngine.resolve_fill_price(miss, _bar(), "next_open") is None


def test_compute_costs_matches_backtest_semantics() -> None:
    commission, slippage = ExecutionEngine.compute_costs(
        quantity=10.0,
        price=10.0,
        commission_bps=10.0,
        commission_per_order=1.0,
        slippage_bps=20.0,
    )
    assert commission == pytest.approx(100.0 * 0.001 + 1.0)
    assert slippage == pytest.approx(100.0 * 0.002)
