"""回测结果数据模型定义。"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from quant_system.core import Fill, OrderRequest, PortfolioSnapshot

from .config import BacktestConfig

if TYPE_CHECKING:
    from quant_system.risk import RiskAuditRecord, RiskDecision


@dataclass(slots=True)
class BacktestResult:
    """回测输出容器。

    同时保存过程数据（orders/fills/snapshots）与统计结果（metrics/equity_curve）。
    """

    config: BacktestConfig
    snapshots: list[PortfolioSnapshot] = field(default_factory=list)
    orders: list[OrderRequest] = field(default_factory=list)
    fills: list[Fill] = field(default_factory=list)
    metrics: dict[str, float] = field(default_factory=dict)
    equity_curve: pd.Series = field(default_factory=lambda: pd.Series(dtype="float64"))
    drawdown_series: pd.Series = field(default_factory=lambda: pd.Series(dtype="float64"))
    closed_trade_pnls: list[float] = field(default_factory=list)
    risk_decisions: list[RiskDecision] = field(default_factory=list)
    risk_audit_logs: list[RiskAuditRecord] = field(default_factory=list)
    export_paths: dict[str, Path] = field(default_factory=dict)

    def summary(self) -> dict[str, Any]:
        """返回回测摘要视图，便于展示与导出。"""
        last_equity = float(self.equity_curve.iloc[-1]) if not self.equity_curve.empty else 0.0
        return {
            "initial_cash": self.config.initial_cash,
            "ending_equity": last_equity,
            "total_trades": int(self.metrics.get("total_trades", 0)),
            **self.metrics,
        }
