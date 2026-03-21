"""run_backtest_ashare_demo 示例脚本。"""
from __future__ import annotations

from pathlib import Path

from quant_system.backtest import BacktestConfig, export_result, run_backtest
from quant_system.data import AshareDataProvider
from quant_system.strategy import DualMovingAverageStrategy, StrategyConfig, StrategyRunner


def main() -> None:
    root = Path(__file__).resolve().parents[1]

    symbols = ["000001.SZ", "600000.SH"]
    strategy = DualMovingAverageStrategy(
        StrategyConfig(
            source="backtest_ashare_dual_ma",
            warmup_bars=0,
            rebalance="daily",
            missing_data="skip_symbol",
            params={"short_window": 5, "long_window": 20},
        )
    )

    provider = AshareDataProvider(strict_validation=False)
    bars = provider.load_bars(
        symbols=symbols,
        start="2024-01-01",
        end="2024-12-31 23:59:59",
        timeframe="1d",
    )

    timestamps = sorted(bars["timestamp"].unique().tolist())
    print("Input bars:")
    print(f"- rows: {len(bars)}")
    print(f"- symbols: {sorted(bars['symbol'].unique().tolist())}")
    print(f"- timestamps: {len(timestamps)}")
    if timestamps:
        print(f"- range: {timestamps[0]} -> {timestamps[-1]}")

    strategy_result = StrategyRunner(strategy).run(bars=bars, universe=symbols)
    print("Strategy outputs:")
    print(f"- signals: {len(strategy_result.signals)}")
    print(f"- targets: {len(strategy_result.targets)}")

    result = run_backtest(
        strategy=strategy,
        bars=bars,
        config=BacktestConfig(
            initial_cash=100_000.0,
            fill_mode="current_close",
            signal_position_size=100.0,
            commission_bps=3.0,
            slippage_bps=2.0,
            annualization_factor=252,
        ),
        universe=symbols,
    )

    print("Backtest summary:")
    for key, value in sorted(result.summary().items()):
        if isinstance(value, float):
            print(f"- {key}: {value:.6f}")
        else:
            print(f"- {key}: {value}")

    print("Execution diagnostics:")
    print(f"- orders: {len(result.orders)}")
    print(f"- fills: {len(result.fills)}")
    if not result.fills:
        print("- no fills generated. Try one of:")
        print("  * wider date range")
        print("  * smaller long_window")
        print("  * verify symbols are valid A-share tickers")

    output_dir = root / "data" / "processed" / "backtest_outputs"
    exported = export_result(
        result=result,
        output_dir=output_dir,
        prefix="dual_ma_ashare_demo",
        formats=("csv", "json"),
    )
    print("Exported files:")
    for name, path in exported.items():
        print(f"- {name}: {path}")


if __name__ == "__main__":
    main()
