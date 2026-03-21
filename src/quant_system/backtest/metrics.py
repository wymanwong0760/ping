"""绩效指标计算实现。"""
from __future__ import annotations

import math

import pandas as pd


def build_equity_curve(snapshots: list) -> pd.Series:
    """构建权益曲线并返回结果。"""
    if not snapshots:
        return pd.Series(dtype="float64")
    index = pd.to_datetime([snapshot.timestamp for snapshot in snapshots], utc=True)
    values = [float(snapshot.equity) for snapshot in snapshots]
    return pd.Series(values, index=index, dtype="float64")


def build_drawdown_series(equity_curve: pd.Series) -> pd.Series:
    """构建回撤序列并返回结果。"""
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
    """计算绩效指标并返回结果。"""
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
    """转换为月收益率序列并返回结果。"""
    if equity_curve.empty:
        return pd.Series(dtype="float64")
    monthly_equity = equity_curve.resample("ME").last()
    return monthly_equity.pct_change().dropna()
