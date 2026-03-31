"""回测引擎实现。

该模块负责串联回测主流程：
1) 读取并标准化 bars；
2) 在每个时间点根据 signals/targets 生成订单；
3) 可选接入风控过滤订单；
4) 通过经纪撮合生成成交；
5) 由账本更新现金/持仓并做盯市；
6) 汇总权益曲线与绩效指标。

`fill_mode` 决定订单生成与成交时序：
- `next_open`：当前时点生成订单，下一时点才可执行；
- `current_close`：当前时点生成订单并在同一时点尝试执行。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence

import pandas as pd

from quant_system.core import Fill, OrderRequest, PortfolioSnapshot, Signal, TargetPosition
from quant_system.risk import RiskDecision, RiskEngine

from .broker import SimBroker
from .config import BacktestConfig
from .exceptions import BacktestDataError
from .ledger import Ledger
from .metrics import build_drawdown_series, build_equity_curve, compute_metrics
from .models import BacktestResult
from .order_sizer import OrderSizer

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PendingOrder:
    """待执行订单包装。

    `order` 为原始订单，`executable_at` 表示最早允许撮合的时间点。
    在 `next_open` 模式下，订单会被推迟到下一根 bar 才执行。
    """

    order: OrderRequest
    executable_at: pd.Timestamp


class BacktestEngine:
    """回测引擎。

    负责组织订单生成、风控评估、撮合执行、记账与绩效汇总。
    """

    def __init__(self, config: BacktestConfig, risk_engine: RiskEngine | None = None) -> None:
        self.config = config
        self.risk_engine = risk_engine
        self.broker = SimBroker(config)
        self.order_sizer = OrderSizer(config)

    def run(
        self,
        bars: pd.DataFrame,
        signals: Sequence[Signal] | None = None,
        targets: Sequence[TargetPosition] | None = None,
    ) -> BacktestResult:
        """执行一次回测并返回结构化结果。

        约束与行为：
        - `signals` 与 `targets` 不能在同一次 `run` 中同时消费；
        - bars 会先做字段与类型标准化（含 UTC 时间统一）；
        - `next_open` 模式先执行历史 pending，再生成新单并挂到下一时点；
        - `current_close` 模式在当前时点生成订单并立即尝试撮合；
        - 最后一个时间点在 `next_open` 下没有下一根 bar，新生成订单不会执行。
        """
        prepared = self._prepare_bars(bars)
        if prepared.empty:
            return BacktestResult(config=self.config)

        signals = list(signals or [])
        targets = list(targets or [])
        if signals and targets:
            raise BacktestDataError("signals and targets cannot be consumed together in one run")

        signal_by_ts = self._group_signals_by_timestamp(signals)
        target_by_ts = self._group_targets_by_timestamp(targets)

        timestamps = [pd.Timestamp(ts) for ts in sorted(prepared["timestamp"].unique())]
        next_ts_map: dict[pd.Timestamp, pd.Timestamp] = {
            ts: timestamps[index + 1]
            for index, ts in enumerate(timestamps)
            if index + 1 < len(timestamps)
        }

        ledger = Ledger(initial_cash=self.config.initial_cash)
        orders: list[OrderRequest] = []
        fills: list[Fill] = []
        snapshots: list[PortfolioSnapshot] = []
        pending: list[PendingOrder] = []
        risk_decisions: list[RiskDecision] = []
        risk_audit_logs: list[Any] = []

        for timestamp in timestamps:
            bar_slice = prepared[prepared["timestamp"] == timestamp]
            bars_by_symbol = self._bars_to_dict(bar_slice)
            close_prices = {symbol: payload["close"] for symbol, payload in bars_by_symbol.items()}

            if self.config.fill_mode == "next_open":
                # 先处理历史挂单：这些订单在前序时点生成，到当前 bar 才可执行。
                pending, new_fills = self._execute_pending(
                    timestamp=timestamp,
                    bars_by_symbol=bars_by_symbol,
                    pending=pending,
                )
                for fill in new_fills:
                    ledger.apply_fill(fill)
                fills.extend(new_fills)

                snapshot = ledger.mark_to_market(
                    timestamp=timestamp.to_pydatetime(),
                    close_prices=close_prices,
                )
                snapshots.append(snapshot)

                new_orders = self._build_orders_for_timestamp(
                    timestamp=timestamp,
                    signal_by_ts=signal_by_ts,
                    target_by_ts=target_by_ts,
                    ledger=ledger,
                    close_prices=close_prices,
                    equity=snapshot.equity,
                )
                # 风控在“生成订单之后、撮合之前”介入，不修改账本历史，仅过滤待执行请求。
                filtered_orders, decisions, audits = self._apply_risk(
                    orders=new_orders,
                    timestamp=timestamp,
                    snapshot=snapshot,
                    bars_by_symbol=bars_by_symbol,
                    close_prices=close_prices,
                )
                risk_decisions.extend(decisions)
                risk_audit_logs.extend(audits)
                orders.extend(filtered_orders)

                next_ts = next_ts_map.get(timestamp)
                # next_open 需要“下一根 bar”作为执行载体；最后时点无 next_ts，因此只能丢弃新单。
                if next_ts is None and filtered_orders:
                    logger.info(
                        "Skip %s orders at last timestamp=%s due to next_open mode",
                        len(filtered_orders),
                        timestamp,
                    )
                if next_ts is not None:
                    pending.extend(
                        [PendingOrder(order=order, executable_at=next_ts) for order in filtered_orders]
                    )
            else:
                # current_close 路径：当前时点生成订单，并在同一时点进入撮合尝试。
                new_orders = self._build_orders_for_timestamp(
                    timestamp=timestamp,
                    signal_by_ts=signal_by_ts,
                    target_by_ts=target_by_ts,
                    ledger=ledger,
                    close_prices=close_prices,
                    equity=(snapshots[-1].equity if snapshots else self.config.initial_cash),
                )
                risk_snapshot = snapshots[-1] if snapshots else self._bootstrap_snapshot(timestamp)
                filtered_orders, decisions, audits = self._apply_risk(
                    orders=new_orders,
                    timestamp=timestamp,
                    snapshot=risk_snapshot,
                    bars_by_symbol=bars_by_symbol,
                    close_prices=close_prices,
                )
                risk_decisions.extend(decisions)
                risk_audit_logs.extend(audits)
                orders.extend(filtered_orders)
                pending.extend(
                    [PendingOrder(order=order, executable_at=timestamp) for order in filtered_orders]
                )

                pending, new_fills = self._execute_pending(
                    timestamp=timestamp,
                    bars_by_symbol=bars_by_symbol,
                    pending=pending,
                )
                for fill in new_fills:
                    ledger.apply_fill(fill)
                fills.extend(new_fills)

                snapshot = ledger.mark_to_market(
                    timestamp=timestamp.to_pydatetime(),
                    close_prices=close_prices,
                )
                snapshots.append(snapshot)

        equity_curve = build_equity_curve(snapshots)
        drawdown_series = build_drawdown_series(equity_curve)
        metrics = compute_metrics(
            equity_curve=equity_curve,
            closed_trade_pnls=ledger.closed_trade_pnls,
            total_turnover=ledger.turnover_notional,
            total_fills=len(fills),
            annualization_factor=self.config.annualization_factor,
        )

        logger.info(
            "Backtest completed snapshots=%s orders=%s fills=%s cumret=%.6f",
            len(snapshots),
            len(orders),
            len(fills),
            metrics.get("cumulative_return", 0.0),
        )

        return BacktestResult(
            config=self.config,
            snapshots=snapshots,
            orders=orders,
            fills=fills,
            metrics=metrics,
            equity_curve=equity_curve,
            drawdown_series=drawdown_series,
            closed_trade_pnls=list(ledger.closed_trade_pnls),
            risk_decisions=risk_decisions,
            risk_audit_logs=risk_audit_logs,
        )

    def _build_orders_for_timestamp(
        self,
        timestamp: pd.Timestamp,
        signal_by_ts: Mapping[pd.Timestamp, list[Signal]],
        target_by_ts: Mapping[pd.Timestamp, list[TargetPosition]],
        ledger: Ledger,
        close_prices: Mapping[str, float],
        equity: float,
    ) -> list[OrderRequest]:
        current_positions = {symbol: record.quantity for symbol, record in ledger.positions.items()}
        orders: list[OrderRequest] = []

        signals = signal_by_ts.get(timestamp, [])
        targets = target_by_ts.get(timestamp, [])

        if signals:
            orders.extend(
                self.order_sizer.orders_from_signals(
                    signals=signals,
                    current_positions=current_positions,
                )
            )
        if targets:
            orders.extend(
                self.order_sizer.orders_from_targets(
                    targets=targets,
                    current_positions=current_positions,
                    equity=equity,
                    reference_prices=close_prices,
                )
            )
        return orders

    def _apply_risk(
        self,
        orders: Sequence[OrderRequest],
        timestamp: pd.Timestamp,
        snapshot: PortfolioSnapshot,
        bars_by_symbol: Mapping[str, Mapping[str, float]],
        close_prices: Mapping[str, float],
    ) -> tuple[list[OrderRequest], list[RiskDecision], list[Any]]:
        if self.risk_engine is None or not orders:
            return list(orders), [], []

        from quant_system.risk import RiskContext

        context = RiskContext(
            timestamp=timestamp.to_pydatetime(),
            snapshot=snapshot,
            close_prices=close_prices,
            market_by_symbol=bars_by_symbol,
            daily_turnover=0.0,
        )
        return self.risk_engine.evaluate_orders(requests=orders, context=context)

    def _execute_pending(
        self,
        timestamp: pd.Timestamp,
        bars_by_symbol: Mapping[str, Mapping[str, float]],
        pending: list[PendingOrder],
    ) -> tuple[list[PendingOrder], list[Fill]]:
        """执行当前时点可撮合的挂单。

        返回 `(new_pending, matched_fills)`：
        - `new_pending` 包含未来时点订单 + 本时点未成交订单；
        - `matched_fills` 为本时点成功成交结果。
        """
        # 到期订单才允许撮合；未到期订单保持原计划执行时间。
        eligible = [item for item in pending if item.executable_at <= timestamp]
        future = [item for item in pending if item.executable_at > timestamp]
        if not eligible:
            return future, []

        matched, remaining_orders = self.broker.match_orders(
            timestamp=timestamp.to_pydatetime(),
            bars_by_symbol=bars_by_symbol,
            orders=[item.order for item in eligible],
        )

        # 本时点未成交订单保留，并可在后续时点继续尝试撮合。
        remaining_pending = [
            PendingOrder(order=order, executable_at=timestamp) for order in remaining_orders
        ]
        return future + remaining_pending, matched

    @staticmethod
    def _prepare_bars(bars: pd.DataFrame) -> pd.DataFrame:
        """标准化并校验输入 bars。

        要求至少包含 `timestamp/symbol/open/high/low/close`；
        输出统一为 UTC 时间、数值型 OHLC，并按 `timestamp,symbol` 排序。
        """
        if bars.empty:
            return bars.copy()

        required = {"timestamp", "symbol", "open", "high", "low", "close"}
        missing = sorted(required - set(bars.columns))
        if missing:
            raise BacktestDataError(f"Bars missing required columns: {missing}")

        prepared = bars.copy()
        prepared["timestamp"] = pd.to_datetime(prepared["timestamp"], errors="coerce", utc=True)
        if prepared["timestamp"].isna().any():
            raise BacktestDataError("Bars contain invalid timestamp values")

        prepared["symbol"] = prepared["symbol"].astype("string")
        for column in ["open", "high", "low", "close"]:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
        if prepared[["open", "high", "low", "close"]].isna().any().any():
            raise BacktestDataError("Bars contain invalid OHLC numeric values")

        if "is_suspended" in prepared.columns:
            # 缺失停牌标记默认视为可交易，避免 None 破坏后续布尔判断。
            prepared["is_suspended"] = (
                prepared["is_suspended"].astype("boolean").fillna(False).astype(bool)
            )

        return prepared.sort_values(["timestamp", "symbol"]).reset_index(drop=True)

    @staticmethod
    def _bars_to_dict(bars: pd.DataFrame) -> dict[str, dict[str, float]]:
        payload: dict[str, dict[str, float]] = {}
        for row in bars.itertuples(index=False):
            item: dict[str, float] = {
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "close": float(row.close),
            }
            if hasattr(row, "is_suspended"):
                item["is_suspended"] = bool(getattr(row, "is_suspended"))
            payload[str(row.symbol)] = item
        return payload

    @staticmethod
    def _group_signals_by_timestamp(signals: Sequence[Signal]) -> dict[pd.Timestamp, list[Signal]]:
        grouped: dict[pd.Timestamp, list[Signal]] = {}
        for signal in signals:
            ts = BacktestEngine._normalize_timestamp(signal.timestamp)
            grouped.setdefault(ts, []).append(signal)
        return grouped

    @staticmethod
    def _group_targets_by_timestamp(
        targets: Sequence[TargetPosition],
    ) -> dict[pd.Timestamp, list[TargetPosition]]:
        grouped: dict[pd.Timestamp, list[TargetPosition]] = {}
        for target in targets:
            ts = BacktestEngine._normalize_timestamp(target.timestamp)
            grouped.setdefault(ts, []).append(target)
        return grouped

    @staticmethod
    def _normalize_timestamp(value: datetime) -> pd.Timestamp:
        """将时间戳归一化到 UTC。"""
        timestamp = pd.Timestamp(value)
        # 对 naive datetime 约定为 UTC，本地时区转换由调用方在入参前处理。
        if timestamp.tzinfo is None:
            return timestamp.tz_localize("UTC")
        return timestamp.tz_convert("UTC")

    def _bootstrap_snapshot(self, timestamp: pd.Timestamp) -> PortfolioSnapshot:
        return PortfolioSnapshot(
            timestamp=timestamp.to_pydatetime(),
            cash=self.config.initial_cash,
            equity=self.config.initial_cash,
            positions={},
            leverage=0.0,
            drawdown=0.0,
        )
