"""引擎实现。"""
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
    """`PendingOrder` 类。"""

    order: OrderRequest
    executable_at: pd.Timestamp


class BacktestEngine:
    """引擎实现。"""

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
        """执行流程并返回结果。"""
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
        eligible = [item for item in pending if item.executable_at <= timestamp]
        future = [item for item in pending if item.executable_at > timestamp]
        if not eligible:
            return future, []

        matched, remaining_orders = self.broker.match_orders(
            timestamp=timestamp.to_pydatetime(),
            bars_by_symbol=bars_by_symbol,
            orders=[item.order for item in eligible],
        )

        remaining_pending = [
            PendingOrder(order=order, executable_at=timestamp) for order in remaining_orders
        ]
        return future + remaining_pending, matched

    @staticmethod
    def _prepare_bars(bars: pd.DataFrame) -> pd.DataFrame:
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
            prepared["is_suspended"] = prepared["is_suspended"].fillna(False).astype(bool)

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
        timestamp = pd.Timestamp(value)
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
