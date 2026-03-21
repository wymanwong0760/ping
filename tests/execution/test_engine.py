"""test_engine 测试用例。"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from quant_system.core import OrderRequest
from quant_system.execution import ExecutionConfig, ExecutionEngine


def _order(
    *,
    symbol: str = "000001.SZ",
    side: str = "buy",
    order_type: str = "market",
    limit_price: float | None = None,
    quantity: float = 10.0,
) -> OrderRequest:
    return OrderRequest(
        timestamp=datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc),
        symbol=symbol,
        side=side,  # type: ignore[arg-type]
        quantity=quantity,
        order_type=order_type,  # type: ignore[arg-type]
        limit_price=limit_price,
        source="test",
    )


def _bar(*, open_price: float = 10.0, high: float = 10.4, low: float = 9.8, close: float = 10.2) -> dict[str, float]:
    return {"open": open_price, "high": high, "low": low, "close": close}


def test_market_order_current_close_fills_same_step() -> None:
    engine = ExecutionEngine(ExecutionConfig(fill_mode="current_close"))
    ts = datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)

    order_ids = engine.submit_orders(ts, [_order(order_type="market")])
    result = engine.on_bar(ts, {"000001.SZ": _bar(close=10.3)})

    assert order_ids == ["EXEC-00000001"]
    assert len(result.fills) == 1
    assert result.fills[0].order_id == "EXEC-00000001"
    assert result.fills[0].price == pytest.approx(10.3)
    assert result.open_order_count == 0


def test_market_order_next_open_waits_until_next_bar() -> None:
    engine = ExecutionEngine(ExecutionConfig(fill_mode="next_open"))
    t0 = datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc)

    engine.submit_orders(t0, [_order(order_type="market")])
    step0 = engine.on_bar(t0, {"000001.SZ": _bar(open_price=10.0)})
    step1 = engine.on_bar(t1, {"000001.SZ": _bar(open_price=10.6)})

    assert step0.fills == []
    assert step0.open_order_count == 1
    assert len(step1.fills) == 1
    assert step1.fills[0].price == pytest.approx(10.6)
    assert step1.open_order_count == 0


def test_limit_order_stays_pending_until_touched() -> None:
    engine = ExecutionEngine(ExecutionConfig(fill_mode="current_close"))
    ts = datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)

    engine.submit_orders(ts, [_order(order_type="limit", limit_price=9.5)])
    step0 = engine.on_bar(ts, {"000001.SZ": _bar(low=9.8)})
    step1 = engine.on_bar(ts, {"000001.SZ": _bar(low=9.4)})

    assert step0.fills == []
    assert step0.open_order_count == 1
    assert len(step1.fills) == 1
    assert step1.fills[0].price == pytest.approx(9.5)


def test_untradable_symbol_rejected_by_default_policy() -> None:
    engine = ExecutionEngine(ExecutionConfig(fill_mode="current_close", untradable_policy="reject"))
    ts = datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)

    engine.submit_orders(ts, [_order(order_type="market")])
    result = engine.on_bar(ts, {"000001.SZ": {**_bar(), "is_suspended": True}})

    assert result.fills == []
    assert len(result.rejects) == 1
    assert "suspended" in result.rejects[0].reason
    assert result.open_order_count == 0


def test_untradable_symbol_can_keep_pending() -> None:
    engine = ExecutionEngine(
        ExecutionConfig(fill_mode="current_close", untradable_policy="keep_pending")
    )
    ts = datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)

    engine.submit_orders(ts, [_order(order_type="market")])
    step0 = engine.on_bar(ts, {"000001.SZ": {**_bar(), "is_suspended": True}})
    step1 = engine.on_bar(ts, {"000001.SZ": _bar(close=10.8)})

    assert step0.fills == []
    assert step0.rejects == []
    assert step0.open_order_count == 1
    assert len(step1.fills) == 1
    assert step1.fills[0].price == pytest.approx(10.8)


def test_cancel_open_order() -> None:
    engine = ExecutionEngine(ExecutionConfig(fill_mode="next_open"))
    ts = datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)

    order_id = engine.submit_orders(ts, [_order(order_type="market")])[0]
    assert engine.cancel_order(order_id) is True
    assert engine.get_open_orders() == []
