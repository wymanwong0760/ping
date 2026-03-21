"""backtest 模块公共导出。

对外暴露回测引擎、配置、结果模型、指标与高层 API。
"""

from .api import run_backtest, run_backtest_with_provider
from .broker import SimBroker
from .config import BacktestConfig, FillMode
from .engine import BacktestEngine
from .exceptions import (
    BacktestConfigError,
    BacktestDataError,
    BacktestError,
    OrderGenerationError,
)
from .exporters import export_result
from .visualizers import export_plots
from .metrics import build_drawdown_series, build_equity_curve, compute_metrics, to_monthly_returns
from .models import BacktestResult

__all__ = [
    "FillMode",
    "BacktestConfig",
    "BacktestResult",
    "BacktestEngine",
    "SimBroker",
    "run_backtest",
    "run_backtest_with_provider",
    "export_result",
    "export_plots",
    "build_equity_curve",
    "build_drawdown_series",
    "compute_metrics",
    "to_monthly_returns",
    "BacktestError",
    "BacktestConfigError",
    "BacktestDataError",
    "OrderGenerationError",
]
