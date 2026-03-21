"""运行器实现。"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import pandas as pd

from quant_system.core import Signal, TargetPosition

from .base import BaseStrategy, SignalStrategy, TargetStrategy
from .context import StrategyContext
from .exceptions import FutureDataLeakError, MissingDataError, StrategyDataError, StrategyError

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StrategyRunResult:
    """结果容器。"""
    signals: list[Signal] = field(default_factory=list)
    targets: list[TargetPosition] = field(default_factory=list)


class StrategyRunner:
    """`StrategyRunner` 类。"""
    def __init__(self, strategy: BaseStrategy) -> None:
        self.strategy = strategy

    def run(
        self,
        bars: pd.DataFrame,
        universe: Sequence[str] | None = None,
        state: Mapping[str, Any] | None = None,
    ) -> StrategyRunResult:
        """执行流程并返回结果。"""
        normalized = self._prepare_bars(bars)
        expected_universe = self._resolve_universe(normalized, universe)
        rebalance_timestamps = self._select_rebalance_timestamps(
            normalized["timestamp"], self.strategy.config.rebalance
        )

        logger.info(
            "Run strategy=%s timestamps=%s universe=%s rebalance=%s warmup=%s",
            self.strategy.name,
            len(rebalance_timestamps),
            expected_universe,
            self.strategy.config.rebalance,
            self.strategy.config.warmup_bars,
        )

        result = StrategyRunResult()
        shared_state: dict[str, Any]
        if isinstance(state, dict):
            shared_state = state
        else:
            shared_state = dict(state or {})

        for index, timestamp in enumerate(rebalance_timestamps):
            if index + 1 < self.strategy.config.warmup_bars:
                continue

            history = normalized[normalized["timestamp"] <= timestamp]
            if history.empty:
                continue

            max_history_ts = history["timestamp"].max()
            if max_history_ts > timestamp:
                raise FutureDataLeakError(
                    f"Future data detected at {timestamp}: max history ts {max_history_ts}"
                )

            current_symbols = sorted(
                history.loc[history["timestamp"] == timestamp, "symbol"]
                .astype(str)
                .dropna()
                .unique()
                .tolist()
            )
            missing_symbols = sorted(set(expected_universe) - set(current_symbols))

            if missing_symbols:
                policy = self.strategy.config.missing_data
                if policy == "raise":
                    raise MissingDataError(
                        f"Missing symbols at {timestamp}: {missing_symbols}"
                    )
                if policy == "skip_timestamp":
                    logger.info(
                        "Skip timestamp=%s due to missing symbols=%s",
                        timestamp,
                        missing_symbols,
                    )
                    continue

            if missing_symbols and self.strategy.config.missing_data == "skip_symbol":
                active_universe = [
                    symbol for symbol in expected_universe if symbol in current_symbols
                ]
            else:
                active_universe = list(expected_universe)

            context = StrategyContext(
                timestamp=timestamp,
                bars=history,
                universe=active_universe,
                expected_universe=list(expected_universe),
                missing_symbols=missing_symbols,
                state=shared_state,
            )

            if isinstance(self.strategy, SignalStrategy):
                outputs = self.strategy.generate_signals(context)
                result.signals.extend(
                    self._validate_signals(outputs, timestamp, expected_universe)
                )
            elif isinstance(self.strategy, TargetStrategy):
                outputs = self.strategy.generate_targets(context)
                result.targets.extend(
                    self._validate_targets(outputs, timestamp, expected_universe)
                )
            else:
                raise StrategyError(f"Unsupported strategy type: {type(self.strategy)}")

        logger.info(
            "Finished strategy=%s signals=%s targets=%s",
            self.strategy.name,
            len(result.signals),
            len(result.targets),
        )
        return result

    @staticmethod
    def _prepare_bars(bars: pd.DataFrame) -> pd.DataFrame:
        if bars.empty:
            return bars.copy()

        required_columns = {"timestamp", "symbol"}
        missing_required = sorted(required_columns - set(bars.columns))
        if missing_required:
            raise StrategyDataError(
                f"Bars missing required columns for strategy run: {missing_required}"
            )

        prepared = bars.copy()
        prepared["timestamp"] = pd.to_datetime(prepared["timestamp"], errors="coerce", utc=True)
        if prepared["timestamp"].isna().any():
            raise StrategyDataError("Bars contain invalid timestamp values")
        prepared["symbol"] = prepared["symbol"].astype("string")

        return prepared.sort_values(["timestamp", "symbol"]).reset_index(drop=True)

    @staticmethod
    def _resolve_universe(bars: pd.DataFrame, universe: Sequence[str] | None) -> list[str]:
        if universe is None:
            return sorted(bars["symbol"].dropna().astype(str).unique().tolist())

        symbols = [str(symbol) for symbol in universe]
        if not symbols:
            raise StrategyDataError("Universe must not be empty")
        return sorted(dict.fromkeys(symbols))

    @staticmethod
    def _select_rebalance_timestamps(
        timestamps: pd.Series,
        rebalance: str,
    ) -> list[pd.Timestamp]:
        unique_ts = pd.Series(pd.Index(timestamps.dropna().unique())).sort_values().tolist()
        if rebalance == "daily":
            return [pd.Timestamp(ts) for ts in unique_ts]

        ts_series = pd.Series(pd.to_datetime(unique_ts, utc=True))
        iso_calendar = ts_series.dt.isocalendar()
        weekly = (
            ts_series.groupby([iso_calendar["year"], iso_calendar["week"]])
            .max()
            .sort_values()
            .tolist()
        )
        return [pd.Timestamp(ts) for ts in weekly]

    @staticmethod
    def _normalize_timestamp(value: Any) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")

    def _validate_signals(
        self,
        outputs: Sequence[Signal],
        timestamp: pd.Timestamp,
        expected_universe: Sequence[str],
    ) -> list[Signal]:
        validated: list[Signal] = []
        for item in outputs:
            if not isinstance(item, Signal):
                raise StrategyDataError(f"Strategy output is not Signal: {type(item)}")
            if str(item.symbol) not in expected_universe:
                raise StrategyDataError(f"Signal symbol out of universe: {item.symbol}")
            if self._normalize_timestamp(item.timestamp) != timestamp:
                raise StrategyDataError(
                    f"Signal timestamp mismatch: {item.timestamp} != {timestamp}"
                )
            validated.append(item)
        return validated

    def _validate_targets(
        self,
        outputs: Sequence[TargetPosition],
        timestamp: pd.Timestamp,
        expected_universe: Sequence[str],
    ) -> list[TargetPosition]:
        validated: list[TargetPosition] = []
        for item in outputs:
            if not isinstance(item, TargetPosition):
                raise StrategyDataError(
                    f"Strategy output is not TargetPosition: {type(item)}"
                )
            if str(item.symbol) not in expected_universe:
                raise StrategyDataError(f"Target symbol out of universe: {item.symbol}")
            if self._normalize_timestamp(item.timestamp) != timestamp:
                raise StrategyDataError(
                    f"Target timestamp mismatch: {item.timestamp} != {timestamp}"
                )
            validated.append(item)
        return validated
