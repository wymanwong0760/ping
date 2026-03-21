"""回测结果可视化导出实现。"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Sequence

import pandas as pd

from .models import BacktestResult

PlotFormat = Literal["png"]


def _require_pyplot() -> Any:
    try:
        import matplotlib

        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt
    except ImportError as exc:  # pragma: no cover - 环境相关分支
        raise RuntimeError(
            "matplotlib is required for plot export. Install optional dependency with: pip install -e '.[viz]'"
        ) from exc
    return plt


def export_plots(
    result: BacktestResult,
    output_dir: str | Path,
    prefix: str = "backtest_result",
    formats: Sequence[PlotFormat] = ("png",),
) -> dict[str, Path]:
    """导出回测图表并返回输出路径。"""
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    exported: dict[str, Path] = {}
    for fmt in formats:
        if fmt != "png":
            raise ValueError(f"Unsupported plot format: {fmt}")

        plt = _require_pyplot()
        index = pd.to_datetime(result.equity_curve.index, utc=True)

        equity_path = target_dir / f"{prefix}.equity.{fmt}"
        fig, ax = plt.subplots(figsize=(10, 4))
        if not result.equity_curve.empty:
            ax.plot(index, result.equity_curve.values, label="equity")
            ax.legend(loc="best")
        ax.set_title("Equity Curve")
        ax.set_xlabel("timestamp")
        ax.set_ylabel("equity")
        fig.tight_layout()
        fig.savefig(equity_path)
        plt.close(fig)
        exported[f"plot_equity_{fmt}"] = equity_path

        drawdown = result.drawdown_series
        if not drawdown.empty and not result.equity_curve.empty:
            drawdown = drawdown.reindex(result.equity_curve.index)

        drawdown_path = target_dir / f"{prefix}.drawdown.{fmt}"
        fig, ax = plt.subplots(figsize=(10, 4))
        if not drawdown.empty:
            ax.plot(index, drawdown.values, color="tab:red", label="drawdown")
            ax.legend(loc="best")
        ax.set_title("Drawdown")
        ax.set_xlabel("timestamp")
        ax.set_ylabel("drawdown")
        fig.tight_layout()
        fig.savefig(drawdown_path)
        plt.close(fig)
        exported[f"plot_drawdown_{fmt}"] = drawdown_path

    result.export_paths.update(exported)
    return exported
