"""A 股行情下载器。"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

import pandas as pd

from quant_system.core.schema import BAR_REQUIRED_COLUMNS, BAR_STANDARD_COLUMNS, DEFAULT_TIMEFRAME

from .ashare_common import (
    normalize_symbol_frame,
    normalize_symbol_list,
    to_local_timestamp,
    to_utc_timestamp,
)
from .exceptions import DataConfigError, DataLoadError, DataValidationError
from .validator import DataValidator

AshareFetcher = Callable[[str, str, pd.Timestamp | None, pd.Timestamp | None], pd.DataFrame]


@dataclass(slots=True)
class DownloadResult:
    """下载结果。"""

    output_path: str
    output_format: str
    row_count: int
    symbols: list[str]
    start: pd.Timestamp | None
    end: pd.Timestamp | None
    columns: list[str]
    provider_used: str


class AshareDownloader:
    """A 股数据下载与落盘服务。"""

    def __init__(
        self,
        *,
        timezone: str = "Asia/Shanghai",
        fetcher: AshareFetcher | None = None,
        provider_name: str = "akshare",
        validator: DataValidator | None = None,
    ) -> None:
        from .ashare_common import fetch_with_akshare

        self.timezone = timezone
        self.fetcher = fetcher or fetch_with_akshare
        self.provider_name = provider_name
        self.validator = validator or DataValidator()

    def download(
        self,
        *,
        symbols: Sequence[str] | str,
        output_path: str | Path,
        start: pd.Timestamp | str | None = None,
        end: pd.Timestamp | str | None = None,
        timeframe: str = DEFAULT_TIMEFRAME,
        fields: Sequence[str] | None = None,
        output_format: str = "parquet",
        overwrite: bool = False,
        strict_validation: bool = True,
    ) -> DownloadResult:
        """下载 A 股 bars 并落盘。"""
        symbol_list = normalize_symbol_list(symbols)
        start_ts_local = (
            to_local_timestamp(start, timezone=self.timezone) if start is not None else None
        )
        end_ts_local = to_local_timestamp(end, timezone=self.timezone) if end is not None else None

        frames: list[pd.DataFrame] = []
        for symbol in symbol_list:
            raw = pd.DataFrame(self.fetcher(symbol, timeframe, start_ts_local, end_ts_local))
            if raw.empty:
                raise DataLoadError(f"Ashare returned empty data for symbol '{symbol}'")
            frames.append(normalize_symbol_frame(raw, symbol=symbol, timezone=self.timezone))

        bars = pd.concat(frames, ignore_index=True).sort_values(["timestamp", "symbol"])
        bars = bars.reset_index(drop=True)

        selected_columns = self._resolve_fields(fields)
        bars = bars[selected_columns].copy()

        if strict_validation:
            result = self.validator.validate(bars)
            if not result.is_valid:
                raise DataValidationError(f"Downloaded dataset validation failed: {result.errors}")

        path = Path(output_path)
        if path.exists() and not overwrite:
            raise DataConfigError(f"Output file already exists: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)

        output_format_normalized = str(output_format).strip().lower()
        if output_format_normalized == "parquet":
            bars.to_parquet(path, index=False)
        elif output_format_normalized == "csv":
            bars.to_csv(path, index=False)
        else:
            raise DataConfigError(
                f"Unsupported output_format '{output_format}'. Use parquet or csv."
            )

        return DownloadResult(
            output_path=str(path),
            output_format=output_format_normalized,
            row_count=len(bars),
            symbols=symbol_list,
            start=to_utc_timestamp(start) if start is not None else None,
            end=to_utc_timestamp(end) if end is not None else None,
            columns=selected_columns,
            provider_used=self.provider_name,
        )

    @staticmethod
    def _resolve_fields(fields: Sequence[str] | None) -> list[str]:
        if fields is None:
            return list(BAR_STANDARD_COLUMNS)

        requested = [str(item).strip() for item in fields]
        if not requested:
            raise DataConfigError("fields cannot be empty")

        unknown = [item for item in requested if item not in BAR_STANDARD_COLUMNS]
        if unknown:
            raise DataConfigError(f"Unknown fields: {unknown}")

        missing_required = [col for col in BAR_REQUIRED_COLUMNS if col not in requested]
        if missing_required:
            raise DataConfigError(
                "fields must include required columns: "
                f"{missing_required}"
            )

        ordered = [col for col in BAR_STANDARD_COLUMNS if col in requested]
        return ordered


def download_ashare_bars(
    *,
    symbols: Sequence[str] | str,
    output_path: str | Path,
    start: pd.Timestamp | str | None = None,
    end: pd.Timestamp | str | None = None,
    timeframe: str = DEFAULT_TIMEFRAME,
    fields: Sequence[str] | None = None,
    output_format: str = "parquet",
    overwrite: bool = False,
) -> DownloadResult:
    """下载 A 股 bars 并保存到本地。"""
    downloader = AshareDownloader()
    return downloader.download(
        symbols=symbols,
        output_path=output_path,
        start=start,
        end=end,
        timeframe=timeframe,
        fields=fields,
        output_format=output_format,
        overwrite=overwrite,
    )
