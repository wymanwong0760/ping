"""run_execution_demo 示例脚本。"""
from __future__ import annotations

from datetime import datetime, timezone

from quant_system.core import OrderRequest
from quant_system.execution import ExecutionConfig, create_execution_engine, run_execution_step


def _print_step(name: str, result) -> None:
    print(name)
    print(f"- timestamp: {result.timestamp}")
    print(f"- fills: {len(result.fills)}")
    for fill in result.fills:
        print(
            f"  - order_id={fill.order_id} symbol={fill.symbol} side={fill.side} "
            f"qty={fill.quantity:.2f} price={fill.price:.4f} "
            f"commission={fill.commission:.6f} slippage={fill.slippage:.6f}"
        )
    print(f"- rejects: {len(result.rejects)}")
    for reject in result.rejects:
        print(
            f"  - order_id={reject.order_id} symbol={reject.symbol} reason={reject.reason}"
        )
    print(f"- pending_orders: {result.open_order_count}")


def main() -> None:
    engine = create_execution_engine(
        ExecutionConfig(
            fill_mode="next_open",
            commission_bps=3.0,
            commission_per_order=1.0,
            slippage_bps=2.0,
            untradable_policy="reject",
        )
    )

    t0 = datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 3, 9, 30, tzinfo=timezone.utc)

    orders = [
        OrderRequest(
            timestamp=t0,
            symbol="000001.SZ",
            side="buy",
            quantity=100.0,
            order_type="market",
            source="execution_demo",
        )
    ]

    step0 = run_execution_step(
        engine=engine,
        timestamp=t0,
        bars_by_symbol={
            "000001.SZ": {"open": 10.0, "high": 10.5, "low": 9.9, "close": 10.2}
        },
        orders=orders,
    )
    _print_step("Step 0 (submit at t0, next_open mode):", step0)

    step1 = run_execution_step(
        engine=engine,
        timestamp=t1,
        bars_by_symbol={
            "000001.SZ": {"open": 10.6, "high": 10.8, "low": 10.4, "close": 10.7}
        },
    )
    _print_step("Step 1 (execute at t1 open):", step1)


if __name__ == "__main__":
    main()
