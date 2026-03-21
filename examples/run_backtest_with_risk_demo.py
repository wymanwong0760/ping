"""run_backtest_with_risk_demo 示例脚本。"""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from quant_system.backtest import BacktestConfig, run_backtest_with_provider
from quant_system.data import DataCatalog, LocalFileDataProvider
from quant_system.risk import RiskConfig
from quant_system.strategy import DualMovingAverageStrategy, StrategyConfig


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    catalog = DataCatalog(catalog_path=root / "data" / "catalog" / "datasets.example.json")
    provider = LocalFileDataProvider(catalog=catalog)

    strategy = DualMovingAverageStrategy(
        StrategyConfig(
            source="backtest_risk_demo_dual_ma",
            warmup_bars=0,
            rebalance="daily",
            missing_data="skip_symbol",
            params={"short_window": 2, "long_window": 3},
        )
    )

    risk_config = RiskConfig()
    risk_config.max_symbol_position.max_abs_qty = 50.0
    risk_config.max_symbol_position.max_weight = None
    risk_config.max_leverage.max_leverage = 0.8
    risk_config.daily_turnover.max_ratio_of_equity = 0.5
    risk_config.universe_filter.blacklist.add("000002.SZ")

    result = run_backtest_with_provider(
        provider=provider,
        strategy=strategy,
        symbols=["000001.SZ", "000002.SZ"],
        config=BacktestConfig(
            initial_cash=100_000.0,
            fill_mode="next_open",
            signal_position_size=100.0,
            commission_bps=3.0,
            slippage_bps=2.0,
            annualization_factor=252,
        ),
        start="2024-01-02",
        end="2024-01-04 23:59:59",
        dataset_name="sample_multi_csv",
        risk_config=risk_config,
    )

    print("Backtest summary:")
    for key, value in sorted(result.summary().items()):
        if isinstance(value, float):
            print(f"- {key}: {value:.6f}")
        else:
            print(f"- {key}: {value}")

    print("Risk decisions:")
    for decision in result.risk_decisions:
        print(
            f"- {decision.timestamp} {decision.symbol} action={decision.action} "
            f"rule={decision.rule_name} reason={decision.reason}"
        )

    print("Risk audits:")
    for audit in result.risk_audit_logs:
        payload = asdict(audit)
        print(f"- {payload}")


if __name__ == "__main__":
    main()
