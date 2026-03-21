"""回测结果导出实现。

支持把回测结果导出为：
- CSV：权益与回撤时间序列；
- JSON：摘要、指标与成交明细；
- Plot(PNG)：权益与回撤图。
"""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Literal, Sequence

import pandas as pd

from .models import BacktestResult
from .visualizers import export_plots

ExportFormat = Literal["csv", "json", "plot_png"]


def export_result(
    result: BacktestResult,
    output_dir: str | Path,
    prefix: str = "backtest_result",
    formats: Sequence[ExportFormat] = ("csv",),
) -> dict[str, Path]:
    """导出回测结果并返回各格式输出路径。"""
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    exported: dict[str, Path] = {}
    for fmt in formats:
        if fmt == "plot_png":
            exported.update(export_plots(result=result, output_dir=target_dir, prefix=prefix, formats=("png",)))
            continue

        if fmt == "csv":
            path = target_dir / f"{prefix}.equity.csv"
            equity_df = pd.DataFrame(
                {
                    "timestamp": pd.to_datetime(result.equity_curve.index, utc=True).astype(str),
                    "equity": result.equity_curve.values,
                    "drawdown": result.drawdown_series.reindex(result.equity_curve.index).values,
                }
            )
            equity_df.to_csv(path, index=False)
            exported["csv"] = path
            continue

        if fmt == "json":
            path = target_dir / f"{prefix}.summary.json"
            payload = {
                "summary": result.summary(),
                "metrics": result.metrics,
                "fills": [asdict(fill) for fill in result.fills],
            }
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            exported["json"] = path
            continue

        raise ValueError(f"Unsupported export format: {fmt}")

    result.export_paths.update(exported)
    return exported
