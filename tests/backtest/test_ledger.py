"""test_ledger 测试用例。"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from quant_system.backtest.ledger import Ledger
from quant_system.core import Fill


def _ts(day: int) -> datetime:
    return datetime(2024, 1, day, 9, 30, tzinfo=timezone.utc)


def test_cash_and_position_update_after_buy_and_sell() -> None:
    ledger = Ledger(initial_cash=1_000.0)

    buy = Fill(
        timestamp=_ts(2),
        symbol="000001.SZ",
        side="buy",
        quantity=10,
        price=10.0,
        commission=1.0,
        slippage=0.5,
        order_id="SIM-0001",
    )
    ledger.apply_fill(buy)

    assert ledger.cash == pytest.approx(898.5)
    assert ledger.get_position_qty("000001.SZ") == pytest.approx(10.0)
    assert ledger.get_avg_cost("000001.SZ") == pytest.approx(10.0)

    sell = Fill(
        timestamp=_ts(3),
        symbol="000001.SZ",
        side="sell",
        quantity=4,
        price=11.0,
        commission=1.0,
        slippage=0.5,
        order_id="SIM-0002",
    )
    ledger.apply_fill(sell)

    assert ledger.cash == pytest.approx(941.0)
    assert ledger.get_position_qty("000001.SZ") == pytest.approx(6.0)
    assert ledger.realized_pnl == pytest.approx(4.0)


def test_mark_to_market_unrealized_and_snapshot_fields() -> None:
    ledger = Ledger(initial_cash=1_000.0)
    ledger.apply_fill(
        Fill(
            timestamp=_ts(2),
            symbol="000001.SZ",
            side="buy",
            quantity=10,
            price=10.0,
            commission=0.0,
            slippage=0.0,
            order_id="SIM-0001",
        )
    )

    snapshot = ledger.mark_to_market(
        timestamp=_ts(2),
        close_prices={"000001.SZ": 10.5},
    )

    assert snapshot.cash == pytest.approx(900.0)
    assert snapshot.equity == pytest.approx(1_005.0)
    assert snapshot.positions["000001.SZ"] == pytest.approx(10.0)
    assert ledger.unrealized_pnl == pytest.approx(5.0)
