"""test_visualizers 测试用例。"""
from __future__ import annotations

from pathlib import Path

import pytest

from quant_system.backtest import BacktestConfig, export_plots, run_backtest_with_provider
from quant_system.data import DataCatalog, LocalFileDataProvider
from quant_system.strategy import DualMovingAverageStrategy, StrategyConfig


def _build_provider() -> LocalFileDataProvider:
    root = Path(__file__).resolve().parents[2]
    catalog = DataCatalog(catalog_path=root / "data" / "catalog" / "datasets.example.json")
    return LocalFileDataProvider(catalog=catalog)


def _build_strategy() -> DualMovingAverageStrategy:
    return DualMovingAverageStrategy(
        StrategyConfig(
            source="viz_unit_test",
            warmup_bars=0,
            rebalance="daily",
            params={"short_window": 2, "long_window": 3},
        )
    )


def test_export_plots_writes_png_files(tmp_path: Path) -> None:
    pytest.importorskip("matplotlib")

    result = run_backtest_with_provider(
        provider=_build_provider(),
        strategy=_build_strategy(),
        symbols=["000001.SZ", "000002.SZ"],
        config=BacktestConfig(
            initial_cash=10_000.0,
            fill_mode="current_close",
            signal_position_size=10.0,
            annualization_factor=252,
        ),
        start="2024-01-02",
        end="2024-01-04 23:59:59",
        dataset_name="sample_multi_csv",
    )

    exported = export_plots(
        result=result,
        output_dir=tmp_path,
        prefix="unit_plot",
        formats=("png",),
    )

    assert set(exported.keys()) == {"plot_equity_png", "plot_drawdown_png"}
    for path in exported.values():
        assert path.exists()
        assert path.suffix == ".png"
        assert path.stat().st_size > 0
