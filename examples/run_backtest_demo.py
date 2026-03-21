"""run_backtest_demo 示例脚本。"""
from __future__ import annotations

from pathlib import Path

from quant_system.backtest import BacktestConfig, export_result, run_backtest
from quant_system.data import DataCatalog, LocalFileDataProvider
from quant_system.strategy import DualMovingAverageStrategy, StrategyConfig, StrategyRunner


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    catalog = DataCatalog(catalog_path=root / "data" / "catalog" / "datasets.example.json")
    provider = LocalFileDataProvider(catalog=catalog)

    symbols = ["000001.SZ", "000002.SZ"]
    strategy = DualMovingAverageStrategy(
        StrategyConfig(
            source="backtest_demo_dual_ma",
            warmup_bars=0,
            rebalance="daily",
            missing_data="skip_symbol",
            params={"short_window": 2, "long_window": 3},
        )
    )

    config = BacktestConfig(
        initial_cash=100_000.0,
        fill_mode="current_close",
        signal_position_size=100.0,
        commission_bps=3.0,
        slippage_bps=2.0,
        annualization_factor=252,
    )

    bars = provider.load_bars(
        symbols=symbols,
        start="2024-01-02",
        end="2024-01-04 23:59:59",
        dataset_name="sample_multi_csv",
    )
    timestamps = sorted(bars["timestamp"].unique().tolist())
    print("Input bars:")
    print(f"- rows: {len(bars)}")
    print(f"- symbols: {sorted(bars['symbol'].unique().tolist())}")
    print(f"- timestamps: {len(timestamps)}")
    print(f"- range: {timestamps[0]} -> {timestamps[-1]}")

    strategy_result = StrategyRunner(strategy).run(bars=bars, universe=symbols)
    print("Strategy outputs:")
    print(f"- signals: {len(strategy_result.signals)}")
    print(f"- targets: {len(strategy_result.targets)}")

    result = run_backtest(
        strategy=strategy,
        bars=bars,
        config=config,
        universe=symbols,
    )

    summary = result.summary()
    print("Backtest summary:")
    for key in sorted(summary):
        value = summary[key]
        if isinstance(value, float):
            print(f"- {key}: {value:.6f}")
        else:
            print(f"- {key}: {value}")

    print("Execution diagnostics:")
    print(f"- orders: {len(result.orders)}")
    print(f"- fills: {len(result.fills)}")
    if not result.fills:
        print("- no fills generated. Try one of:")
        print("  * longer date range")
        print("  * smaller long_window")
        print("  * fill_mode='current_close' (already set in this demo)")
        print("  * verify symbols exist in selected dataset")

    output_dir = root / "data" / "processed" / "backtest_outputs"
    exported = export_result(
        result=result,
        output_dir=output_dir,
        prefix="dual_ma_demo",
        formats=("csv", "json"),
    )
    print("Exported files:")
    for name, path in exported.items():
        print(f"- {name}: {path}")


if __name__ == "__main__":
    main()
