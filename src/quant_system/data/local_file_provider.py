"""本地文件数据提供器实现。

该模块负责把离线数据文件（CSV/Parquet）转换为系统统一的 bars 结构，
并在加载阶段完成：
1. 数据集定位与读取；
2. 列映射、时间戳标准化与数值字段清洗；
3. 按 symbols/start/end 过滤；
4. 可选严格校验（strict_validation）。

设计目标是让上层回测/策略模块始终面对一致的数据格式，降低数据源差异带来的分支复杂度。
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from quant_system.core.schema import (
    BAR_OPTIONAL_COLUMNS,
    BAR_REQUIRED_COLUMNS,
    BAR_STANDARD_COLUMNS,
    DEFAULT_TIMEFRAME,
)

from .base import BaseDataProvider
from .calendar import TradingCalendar
from .catalog import DataCatalog, DatasetMetadata
from .exceptions import DataLoadError, DataValidationError
from .validator import DataValidator, ValidationResult

logger = logging.getLogger(__name__)


class LocalFileDataProvider(BaseDataProvider):
    """基于本地文件与目录元信息的数据提供器。

    该实现遵循 `BaseDataProvider` 统一接口，核心执行链路为：
    `load_bars -> _read_dataset -> _normalize -> _apply_filters`。

    其中：
    - `catalog` 负责解析数据集路径、格式与列映射配置；
    - `validator` 负责数据质量校验；
    - `calendar` 负责从 bars 提取交易时间轴；
    - `strict_validation` 控制是否在加载时将校验失败视为硬错误。
    """

    def __init__(
        self,
        catalog: DataCatalog,
        validator: DataValidator | None = None,
        calendar: TradingCalendar | None = None,
        strict_validation: bool = False,
    ) -> None:
        self.catalog = catalog
        self.validator = validator or DataValidator()
        self.calendar = calendar or TradingCalendar()
        self.strict_validation = strict_validation

    def load_bars(
        self,
        symbols: Sequence[str] | str | None,
        start: pd.Timestamp | str | None = None,
        end: pd.Timestamp | str | None = None,
        timeframe: str = DEFAULT_TIMEFRAME,
        dataset_name: str | None = None,
    ) -> pd.DataFrame:
        """加载并返回标准化后的 bars。

        主执行流程：
        1. 通过 catalog 解析目标数据集元信息；
        2. 校验请求周期与数据集周期是否一致；
        3. 读取原始文件；
        4. 执行标准化（列映射、时间统一、类型转换、必要列检查）；
        5. 应用 symbols/start/end 过滤；
        6. 返回按 `timestamp,symbol` 排序的结果。

        注意：本方法只做“数据访问与规范化”，不承担策略层业务语义。
        """
        metadata = self.catalog.get_dataset(dataset_name)

        if timeframe and metadata.timeframe and timeframe != metadata.timeframe:
            raise DataLoadError(
                f"Dataset timeframe mismatch: request={timeframe}, dataset={metadata.timeframe}"
            )

        logger.info(
            "Loading bars from dataset=%s path=%s format=%s",
            metadata.name,
            metadata.path,
            metadata.file_format,
        )
        raw_df = self._read_dataset(metadata)
        normalized = self._normalize(raw_df, metadata)
        result = self._apply_filters(normalized, symbols=symbols, start=start, end=end)

        logger.info(
            "Loaded dataset=%s rows=%s symbols=%s",
            metadata.name,
            len(result),
            sorted(result["symbol"].dropna().unique().tolist()) if not result.empty else [],
        )
        return result

    def get_available_symbols(self, dataset_name: str | None = None) -> list[str]:
        """返回数据集中可用标的。"""
        bars = self.load_bars(symbols=None, dataset_name=dataset_name)
        return sorted(bars["symbol"].dropna().astype(str).unique().tolist())

    def get_calendar(
        self,
        start: pd.Timestamp | str | None = None,
        end: pd.Timestamp | str | None = None,
        dataset_name: str | None = None,
    ) -> pd.DatetimeIndex:
        """返回交易日历。"""
        bars = self.load_bars(symbols=None, dataset_name=dataset_name)
        return self.calendar.get_calendar(bars=bars, start=start, end=end)

    def get_latest_bar_asof(
        self,
        symbol: str,
        ts: pd.Timestamp | str,
        dataset_name: str | None = None,
    ) -> pd.Series | None:
        """返回 symbol 在 ts 时点可见的最新 bar。"""
        bars = self.load_bars(symbols=[symbol], end=ts, dataset_name=dataset_name)
        if bars.empty:
            return None
        return bars.iloc[-1]

    def validate_dataset(
        self,
        dataset_name: str | None = None,
        null_ratio_threshold: float = 0.3,
    ) -> ValidationResult:
        """执行数据校验。"""
        metadata = self.catalog.get_dataset(dataset_name)
        raw_df = self._read_dataset(metadata)
        normalized = self._normalize(raw_df, metadata, sort_output=False)
        result = self.validator.validate(
            normalized,
            null_ratio_threshold=null_ratio_threshold,
        )
        logger.info(
            "Validation finished dataset=%s valid=%s errors=%s warnings=%s",
            metadata.name,
            result.is_valid,
            len(result.errors),
            len(result.warnings),
        )
        if self.strict_validation and not result.is_valid:
            raise DataValidationError(
                f"Dataset '{metadata.name}' validation failed: {result.errors}"
            )
        return result

    def register_dataset(self, name: str, metadata: Mapping[str, Any]) -> None:
        """注册数据集。"""
        self.catalog.register_dataset(name=name, metadata=dict(metadata))
        logger.info("Dataset registered: %s", name)

    def _read_dataset(self, metadata: DatasetMetadata) -> pd.DataFrame:
        path = Path(metadata.path)
        if not path.exists():
            raise DataLoadError(f"Dataset file not found: {path}")

        try:
            if metadata.file_format == "csv":
                return pd.read_csv(path)
            if metadata.file_format == "parquet":
                return pd.read_parquet(path)
        except Exception as exc:  # pragma: no cover
            raise DataLoadError(f"Failed to read dataset '{metadata.name}': {exc}") from exc

        raise DataLoadError(
            f"Unsupported dataset format '{metadata.file_format}' for {metadata.name}"
        )

    def _normalize(
        self,
        df: pd.DataFrame,
        metadata: DatasetMetadata,
        run_validation: bool = True,
        sort_output: bool = True,
    ) -> pd.DataFrame:
        """将原始数据标准化为系统 bars Schema。

        关键步骤：
        - 根据 `metadata.column_mapping` 做字段重命名，统一输入源差异；
        - 保证 `symbol` 与 `timestamp` 存在（必要时使用 `default_symbol` 回填）；
        - 把时间戳统一转换到 UTC，确保跨模块时间比较一致；
        - 把 OHLCV 等数值列转换为数值类型（失败转 NaN，交由校验器识别）；
        - 自动补齐可选列，最终按 `BAR_STANDARD_COLUMNS` 投影输出；
        - 在严格模式下可触发校验失败即抛错。

        返回值满足回测/策略/执行模块的统一输入契约。
        """
        mapped = df.rename(columns=metadata.column_mapping)

        if "symbol" not in mapped.columns:
            if metadata.default_symbol:
                mapped["symbol"] = metadata.default_symbol
            else:
                raise DataLoadError(
                    f"Dataset '{metadata.name}' has no symbol column and no default_symbol"
                )

        if "timestamp" not in mapped.columns:
            raise DataLoadError(f"Dataset '{metadata.name}' missing timestamp column")

        mapped["timestamp"] = self._normalize_timestamps(
            mapped["timestamp"], metadata.timezone
        )

        numeric_columns = ["open", "high", "low", "close", "volume", "amount", "adj_factor"]
        for column in numeric_columns:
            if column in mapped.columns:
                mapped[column] = pd.to_numeric(mapped[column], errors="coerce")

        if "is_suspended" in mapped.columns:
            mapped["is_suspended"] = mapped["is_suspended"].astype("boolean")

        for optional_col in BAR_OPTIONAL_COLUMNS:
            if optional_col not in mapped.columns:
                mapped[optional_col] = pd.NA

        missing_required = [col for col in BAR_REQUIRED_COLUMNS if col not in mapped.columns]
        if missing_required:
            raise DataLoadError(
                f"Dataset '{metadata.name}' missing required columns: {missing_required}"
            )

        normalized = mapped[BAR_STANDARD_COLUMNS].copy()
        normalized["symbol"] = normalized["symbol"].astype("string")

        if sort_output:
            normalized = normalized.sort_values(["timestamp", "symbol"]).reset_index(drop=True)

        if run_validation and self.strict_validation:
            result = self.validator.validate(normalized)
            if not result.is_valid:
                raise DataValidationError(
                    f"Dataset '{metadata.name}' validation failed: {result.errors}"
                )
        return normalized

    def _apply_filters(
        self,
        df: pd.DataFrame,
        symbols: Sequence[str] | str | None,
        start: pd.Timestamp | str | None,
        end: pd.Timestamp | str | None,
    ) -> pd.DataFrame:
        """对标准化 bars 应用标的与时间区间过滤。

        过滤规则：
        - `symbols` 不为 None 时，仅保留请求标的；
        - `start/end` 会先统一转换为 UTC，再做闭区间筛选；
        - 输出始终按 `timestamp,symbol` 排序，确保下游可重复性。
        """
        result = df
        if symbols is not None:
            symbol_list = [symbols] if isinstance(symbols, str) else list(symbols)
            result = result[result["symbol"].isin(symbol_list)]

        start_ts = self._to_utc_timestamp(start) if start is not None else None
        end_ts = self._to_utc_timestamp(end) if end is not None else None

        if start_ts is not None:
            result = result[result["timestamp"] >= start_ts]
        if end_ts is not None:
            result = result[result["timestamp"] <= end_ts]

        return result.sort_values(["timestamp", "symbol"]).reset_index(drop=True)

    @staticmethod
    def _normalize_timestamps(series: pd.Series, timezone: str) -> pd.Series:
        ts = pd.to_datetime(series, errors="coerce", utc=False)
        if getattr(ts.dt, "tz", None) is None:
            return ts.dt.tz_localize(timezone).dt.tz_convert("UTC")
        return ts.dt.tz_convert("UTC")

    @staticmethod
    def _to_utc_timestamp(value: pd.Timestamp | str) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")
