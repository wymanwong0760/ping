"""test_api_e2e 测试用例。"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from quant_system.backtest import BacktestConfig, export_result, run_backtest_with_provider
from quant_system.risk import RiskConfig
from quant_system.data import DataCatalog, LocalFileDataProvider
from quant_system.strategy import DualMovingAverageStrategy, StrategyConfig


def _build_provider() -> LocalFileDataProvider:
    root = Path(__file__).resolve().parents[2]
    catalog = DataCatalog(catalog_path=root / "data" / "catalog" / "datasets.example.json")
    return LocalFileDataProvider(catalog=catalog)


def _build_strategy() -> DualMovingAverageStrategy:
    return DualMovingAverageStrategy(
        StrategyConfig(
            source="dma_e2e",
            warmup_bars=0,
            rebalance="daily",
            params={"short_window": 2, "long_window": 3},
        )
    )


def test_run_backtest_with_provider_e2e() -> None:
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

    assert len(result.snapshots) == 3
    assert len(result.fills) == 2
    assert {fill.symbol for fill in result.fills} == {"000001.SZ", "000002.SZ"}
    assert result.metrics["total_trades"] == pytest.approx(2.0)
    assert "max_drawdown" in result.metrics
    assert not result.equity_curve.empty


def test_run_backtest_with_provider_e2e_with_risk() -> None:
    risk_config = RiskConfig()
    risk_config.max_symbol_position.max_abs_qty = 8.0
    risk_config.max_symbol_position.max_weight = None

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
        risk_config=risk_config,
    )

    assert result.risk_decisions
    assert result.risk_audit_logs
    assert all(item.action in {"approve", "modify", "reject"} for item in result.risk_decisions)


def test_export_result_writes_csv_and_json(tmp_path: Path) -> None:
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

    exported = export_result(
        result=result,
        output_dir=tmp_path,
        prefix="e2e",
        formats=("csv", "json"),
    )

    assert set(exported.keys()) == {"csv", "json"}
    assert exported["csv"].exists()
    assert exported["json"].exists()
    assert result.export_paths["csv"] == exported["csv"]
    assert result.export_paths["json"] == exported["json"]

    equity_df = pd.read_csv(exported["csv"])
    assert {"timestamp", "equity", "drawdown"}.issubset(equity_df.columns)

    payload = json.loads(exported["json"].read_text(encoding="utf-8"))
    assert "summary" in payload
    assert "metrics" in payload
    assert "fills" in payload
