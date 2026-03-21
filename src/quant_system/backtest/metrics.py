"""绩效指标计算实现。

本模块聚焦“快照序列 -> 可分析时间序列/指标”：
- 从账户快照构建权益曲线；
- 从权益曲线构建回撤序列；
- 计算收益、波动、夏普、胜率、换手等聚合指标。
"""
from __future__ import annotations

import math

import pandas as pd


def build_equity_curve(snapshots: list) -> pd.Series:
    """将快照列表转换为按时间索引的权益曲线。"""
    if not snapshots:
        return pd.Series(dtype="float64")
    index = pd.to_datetime([snapshot.timestamp for snapshot in snapshots], utc=True)
    values = [float(snapshot.equity) for snapshot in snapshots]
    return pd.Series(values, index=index, dtype="float64")


def build_drawdown_series(equity_curve: pd.Series) -> pd.Series:
    """基于权益曲线计算逐时点回撤 `(equity / running_peak - 1)`。"""
    if equity_curve.empty:
        return pd.Series(dtype="float64")
    running_peak = equity_curve.cummax()
    drawdown = (equity_curve / running_peak) - 1.0
    return drawdown.astype("float64")


def compute_metrics(
    equity_curve: pd.Series,
    closed_trade_pnls: list[float],
    total_turnover: float,
    annualization_factor: int,
    total_fills: int = 0,
) -> dict[str, float]:
    """计算回测核心指标。

    指标口径：
    - `cumulative_return`：期末权益相对期初权益收益率；
    - `annualized_return`：按 `annualization_factor` 年化；
    - `annualized_volatility`：收益序列标准差年化；
    - `sharpe_ratio`：年化收益 / 年化波动（无无风险利率项）；
    - `max_drawdown`：回撤序列最小值；
    - `win_rate`：已平仓盈亏中盈利笔数占比；
    - `turnover`：总换手名义金额 / 期初权益；
    - `total_trades`：成交笔数（fills）。
    """
    if equity_curve.empty:
        return {
            "cumulative_return": 0.0,
            "annualized_return": 0.0,
            "annualized_volatility": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "win_rate": 0.0,
            "turnover": 0.0,
            "total_trades": 0.0,
        }

    returns = equity_curve.pct_change().fillna(0.0)
    periods = max(len(equity_curve) - 1, 1)

    start = float(equity_curve.iloc[0])
    end = float(equity_curve.iloc[-1])
    cumulative_return = (end / start - 1.0) if start > 0 else 0.0

    annualized_return = (1.0 + cumulative_return) ** (annualization_factor / periods) - 1.0
    annualized_volatility = float(returns.std(ddof=0) * math.sqrt(annualization_factor))
    sharpe_ratio = annualized_return / annualized_volatility if annualized_volatility > 0 else 0.0

    drawdown = build_drawdown_series(equity_curve)
    max_drawdown = float(drawdown.min()) if not drawdown.empty else 0.0

    closed_trades = float(len(closed_trade_pnls))
    wins = sum(1 for pnl in closed_trade_pnls if pnl > 0)
    win_rate = (wins / closed_trades) if closed_trades > 0 else 0.0
    turnover = total_turnover / start if start > 0 else 0.0

    return {
        "cumulative_return": float(cumulative_return),
        "annualized_return": float(annualized_return),
        "annualized_volatility": float(annualized_volatility),
        "sharpe_ratio": float(sharpe_ratio),
        "max_drawdown": float(max_drawdown),
        "win_rate": float(win_rate),
        "turnover": float(turnover),
        "total_trades": float(total_fills),
    }


def to_monthly_returns(equity_curve: pd.Series) -> pd.Series:
    """将权益曲线重采样到月末并输出月收益率序列。"""
    if equity_curve.empty:
        return pd.Series(dtype="float64")
    monthly_equity = equity_curve.resample("ME").last()
    return monthly_equity.pct_change().dropna()
