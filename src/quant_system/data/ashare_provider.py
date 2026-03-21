"""A 股在线数据提供器实现。

该模块负责在线拉取 A 股行情并转换为统一 bars Schema，核心链路包括：
1. 标的与时间参数标准化；
2. 在线源拉取（优先 Ashare/ashare，失败回退 akshare）；
3. 字段映射与数据清洗；
4. 统一时区、补齐标准列并过滤时间区间；
5. 缓存最近一次结果，支持 calendar/asof/validate 查询。

目标是把“多数据源 + 多字段命名”的复杂性收敛在数据层内部，保证上层使用一致接口。
"""
from __future__ import annotations

import importlib
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from quant_system.core.schema import (
    BAR_OPTIONAL_COLUMNS,
    BAR_REQUIRED_COLUMNS,
    BAR_STANDARD_COLUMNS,
    DEFAULT_TIMEFRAME,
)

from .base import BaseDataProvider
from .calendar import TradingCalendar
from .exceptions import DataConfigError, DataLoadError, DataValidationError
from .validator import DataValidator, ValidationResult

AshareFetcher = Callable[[str, str, pd.Timestamp | None, pd.Timestamp | None], pd.DataFrame]


class AshareDataProvider(BaseDataProvider):
    """A 股在线行情数据提供器。

    与本地文件 provider 不同，该实现按请求即时拉取数据，并在内存缓存最近一次加载结果。
    主执行路径：
    `load_bars -> _fetch_symbol_data -> _normalize_symbol_frame -> _apply_filters`。

    设计要点：
    - 兼容 Ashare/ashare 与 akshare 的 API 差异；
    - 统一符号格式（`000001.SZ`）与时间语义（统一输出 UTC）；
    - 对外暴露与 `BaseDataProvider` 一致的方法集合，便于上层无缝切换数据源。
    """

    def __init__(
        self,
        validator: DataValidator | None = None,
        calendar: TradingCalendar | None = None,
        strict_validation: bool = False,
        timezone: str = "Asia/Shanghai",
        fetcher: AshareFetcher | None = None,
    ) -> None:
        self.validator = validator or DataValidator()
        self.calendar = calendar or TradingCalendar()
        self.strict_validation = strict_validation
        self.timezone = timezone
        self.fetcher = fetcher
        self._cached_bars = pd.DataFrame(columns=BAR_STANDARD_COLUMNS)
        self._last_symbols: list[str] = []

    def load_bars(
        self,
        symbols: Sequence[str] | str | None,
        start: pd.Timestamp | str | None = None,
        end: pd.Timestamp | str | None = None,
        timeframe: str = DEFAULT_TIMEFRAME,
        dataset_name: str | None = None,
    ) -> pd.DataFrame:
        """按请求拉取并返回标准化 bars。

        执行步骤：
        1. 归一化 symbols 与起止时间（本地时区语义）；
        2. 逐标的拉取原始数据；
        3. 对每个标的做字段与类型标准化；
        4. 合并后按 symbols/start/end 做闭区间过滤；
        5. 可选严格校验并更新内部缓存。

        `dataset_name` 对在线源无意义，保留该参数仅为满足统一接口。
        """
        del dataset_name
        if symbols is None:
            raise DataLoadError("AshareDataProvider requires symbols to be specified")

        symbol_list = self._normalize_symbol_list(symbols)
        start_ts_local = self._to_local_timestamp(start) if start is not None else None
        end_ts_local = self._to_local_timestamp(end) if end is not None else None

        frames: list[pd.DataFrame] = []
        for symbol in symbol_list:
            raw = self._fetch_symbol_data(
                symbol=symbol,
                timeframe=timeframe,
                start=start_ts_local,
                end=end_ts_local,
            )
            frames.append(self._normalize_symbol_frame(raw=raw, symbol=symbol))

        if not frames:
            return pd.DataFrame(columns=BAR_STANDARD_COLUMNS)

        bars = pd.concat(frames, ignore_index=True)
        bars = self._apply_filters(bars, symbols=symbol_list, start=start, end=end)

        if self.strict_validation:
            result = self.validator.validate(bars)
            if not result.is_valid:
                raise DataValidationError(f"Ashare dataset validation failed: {result.errors}")

        self._cached_bars = bars.copy()
        self._last_symbols = symbol_list
        return bars

    def get_available_symbols(self, dataset_name: str | None = None) -> list[str]:
        """获取并返回结果。"""
        del dataset_name
        return sorted(self._last_symbols)

    def get_calendar(
        self,
        start: pd.Timestamp | str | None = None,
        end: pd.Timestamp | str | None = None,
        dataset_name: str | None = None,
    ) -> pd.DatetimeIndex:
        """获取并返回结果。"""
        del dataset_name
        if self._cached_bars.empty:
            raise DataLoadError("No cached Ashare bars. Call load_bars first.")

        start_ts = (
            self._to_filter_utc_timestamp(start, is_end=False)
            if start is not None
            else None
        )
        end_ts = self._to_filter_utc_timestamp(end, is_end=True) if end is not None else None
        return self.calendar.get_calendar(self._cached_bars, start=start_ts, end=end_ts)

    def get_latest_bar_asof(
        self,
        symbol: str,
        ts: pd.Timestamp | str,
        dataset_name: str | None = None,
    ) -> pd.Series | None:
        """获取并返回结果。"""
        if self._cached_bars.empty:
            raise DataLoadError("No cached Ashare bars. Call load_bars first.")

        normalized_symbol = self._normalize_symbol(symbol)
        ts_utc = self._to_utc_timestamp(ts)
        view = self._cached_bars[
            (self._cached_bars["symbol"] == normalized_symbol)
            & (self._cached_bars["timestamp"] <= ts_utc)
        ]
        if view.empty:
            return None
        return view.iloc[-1]

    def validate_dataset(
        self,
        dataset_name: str | None = None,
        null_ratio_threshold: float = 0.3,
    ) -> ValidationResult:
        """校验并返回结果。"""
        del dataset_name
        if self._cached_bars.empty:
            raise DataLoadError("No cached Ashare bars. Call load_bars first.")
        return self.validator.validate(
            self._cached_bars,
            null_ratio_threshold=null_ratio_threshold,
        )

    def register_dataset(self, name: str, metadata: Mapping[str, Any]) -> None:
        """注册相关信息。"""
        name, metadata
        raise DataConfigError("AshareDataProvider does not support catalog registration")

    def _fetch_symbol_data(
        self,
        symbol: str,
        timeframe: str,
        start: pd.Timestamp | None,
        end: pd.Timestamp | None,
    ) -> pd.DataFrame:
        fetcher = self.fetcher or self._fetch_with_ashare
        raw = fetcher(symbol, timeframe, start, end)
        frame = pd.DataFrame(raw)
        if frame.empty:
            raise DataLoadError(f"Ashare returned empty data for symbol '{symbol}'")
        return frame

    def _fetch_with_ashare(
        self,
        symbol: str,
        timeframe: str,
        start: pd.Timestamp | None,
        end: pd.Timestamp | None,
    ) -> pd.DataFrame:
        """通过在线库拉取单标的数据，含多级回退逻辑。

        回退顺序：
        1. 尝试导入 `Ashare`，失败后尝试 `ashare`；
        2. 若存在 `get_price`，以多组参数签名重试（适配不同版本函数签名）；
        3. Ashare 路径不可用时回退到 `akshare`；
        4. akshare 主接口为空且周期为日线时，再尝试日线备用接口。

        这样做的目的不是“隐藏错误”，而是尽量吸收三方库版本差异，提高在线抓取成功率。
        """
        ashare_error: Exception | None = None

        try:
            module = importlib.import_module("Ashare")
        except ModuleNotFoundError:
            try:
                module = importlib.import_module("ashare")
            except ModuleNotFoundError:
                module = None

        if module is not None:
            get_price = getattr(module, "get_price", None)
            if get_price is None:
                ashare_attr = getattr(module, "Ashare", None)
                get_price = getattr(ashare_attr, "get_price", None)

            if get_price is not None:
                end_date = end.strftime("%Y-%m-%d") if end is not None else None
                count = self._estimate_count(start=start, end=end)
                code = self._to_ashare_symbol(symbol)

                attempts = [
                    {
                        "code": code,
                        "frequency": timeframe,
                        "count": count,
                        "end_date": end_date,
                    },
                    {
                        "symbol": code,
                        "frequency": timeframe,
                        "count": count,
                        "end_date": end_date,
                    },
                    {
                        "code": code,
                        "frequency": timeframe,
                        "end_date": end_date,
                    },
                    {
                        "symbol": code,
                        "frequency": timeframe,
                        "end_date": end_date,
                    },
                ]

                last_type_error: TypeError | None = None
                for kwargs in attempts:
                    try:
                        payload = {k: v for k, v in kwargs.items() if v is not None}
                        return pd.DataFrame(get_price(**payload))
                    except TypeError as exc:
                        last_type_error = exc
                        continue
                    except Exception as exc:  # pragma: no cover - network/runtime branch
                        ashare_error = exc
                        break

                if ashare_error is None and last_type_error is not None:
                    ashare_error = last_type_error
            else:
                ashare_error = DataLoadError("Cannot find get_price API from Ashare package")

        try:
            akshare = importlib.import_module("akshare")
        except ModuleNotFoundError as exc:
            raise DataLoadError(
                "No available online provider module. Tried Ashare/ashare and akshare. "
                f"Ashare error: {ashare_error}; akshare import error: {exc}"
            ) from exc

        period = self._to_akshare_period(timeframe)
        normalized = self._normalize_symbol(symbol)
        code, _suffix = normalized.split(".", 1)
        start_date = start.strftime("%Y%m%d") if start is not None else None
        end_date = end.strftime("%Y%m%d") if end is not None else None

        akshare_symbol = self._to_ashare_symbol(symbol)
        frame: pd.DataFrame | None = None
        primary_error: Exception | None = None

        try:
            frame = akshare.stock_zh_a_hist(
                symbol=code,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust="",
            )
        except Exception as exc:
            primary_error = exc

        if (frame is None or pd.DataFrame(frame).empty) and period == "daily":
            try:
                daily = akshare.stock_zh_a_daily(symbol=akshare_symbol, adjust="")
                frame = pd.DataFrame(daily)
                if not frame.empty and "date" in frame.columns:
                    date_series = pd.to_datetime(frame["date"], errors="coerce")
                    frame = frame.assign(date=date_series)
                    if start_date is not None:
                        frame = frame[frame["date"] >= pd.to_datetime(start_date)]
                    if end_date is not None:
                        frame = frame[frame["date"] <= pd.to_datetime(end_date)]
                    frame = frame.assign(date=frame["date"].dt.strftime("%Y-%m-%d"))
            except Exception as exc:
                if primary_error is None:
                    primary_error = exc

        if frame is None:
            raise DataLoadError(f"akshare fetch failed for '{symbol}': {primary_error}")

        return pd.DataFrame(frame)


    def _normalize_symbol_frame(self, raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """标准化单标的原始行情为统一 bars 结构。

        处理内容：
        - 兼容中英文与不同供应商字段名（如 date/日期、close/收盘）；
        - 构造并校验 timestamp 列；
        - 将时间统一转为 UTC；
        - 统一 symbol 表达；
        - 把价格/成交量等列转为数值；
        - 归一化停牌标识并补齐可选列；
        - 检查必须列后按标准列顺序输出。

        该方法是在线源“脏数据 -> 干净 bars”转换的核心入口。
        """
        df = raw.copy()
        rename_map = {
            "date": "timestamp",
            "datetime": "timestamp",
            "time": "timestamp",
            "trade_date": "timestamp",
            "日期": "timestamp",
            "Open": "open",
            "open_price": "open",
            "开盘": "open",
            "High": "high",
            "high_price": "high",
            "最高": "high",
            "Low": "low",
            "low_price": "low",
            "最低": "low",
            "Close": "close",
            "close_price": "close",
            "收盘": "close",
            "Volume": "volume",
            "vol": "volume",
            "成交量": "volume",
            "Amount": "amount",
            "money": "amount",
            "成交额": "amount",
            "adj": "adj_factor",
            "adjfactor": "adj_factor",
            "复权因子": "adj_factor",
            "suspended": "is_suspended",
            "suspend": "is_suspended",
            "停牌": "is_suspended",
        }
        df = df.rename(columns=rename_map)

        if "timestamp" not in df.columns:
            reset = df.reset_index()
            df = reset.rename(columns={reset.columns[0]: "timestamp"})

        if "timestamp" not in df.columns:
            raise DataLoadError(f"Ashare bars missing timestamp for '{symbol}'")

        timestamps = pd.to_datetime(df["timestamp"], errors="coerce", utc=False)
        if timestamps.isna().all():
            raise DataLoadError(f"Ashare bars contain invalid timestamps for '{symbol}'")
        if getattr(timestamps.dt, "tz", None) is None:
            df["timestamp"] = timestamps.dt.tz_localize(self.timezone).dt.tz_convert("UTC")
        else:
            df["timestamp"] = timestamps.dt.tz_convert("UTC")

        df["symbol"] = self._normalize_symbol(symbol)

        for col in ["open", "high", "low", "close", "volume", "amount", "adj_factor"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "is_suspended" in df.columns:
            df["is_suspended"] = self._normalize_suspend_flag(df["is_suspended"])

        for optional_col in BAR_OPTIONAL_COLUMNS:
            if optional_col not in df.columns:
                df[optional_col] = pd.NA

        missing_required = [col for col in BAR_REQUIRED_COLUMNS if col not in df.columns]
        if missing_required:
            raise DataLoadError(
                f"Ashare bars missing required columns for '{symbol}': {missing_required}"
            )

        normalized = df[BAR_STANDARD_COLUMNS].copy()
        normalized["symbol"] = normalized["symbol"].astype("string")
        normalized = normalized.dropna(subset=["timestamp"]).sort_values(
            ["timestamp", "symbol"]
        )
        return normalized.reset_index(drop=True)

    @staticmethod
    def _normalize_suspend_flag(series: pd.Series) -> pd.Series:
        true_values = {"1", "true", "yes", "y", "停牌", "t"}
        false_values = {"0", "false", "no", "n", "正常", "f"}

        normalized = series.astype("string").str.lower().str.strip()
        out = pd.Series(pd.NA, index=series.index, dtype="boolean")
        out[normalized.isin(true_values)] = True
        out[normalized.isin(false_values)] = False
        return out

    def _apply_filters(
        self,
        df: pd.DataFrame,
        symbols: Sequence[str],
        start: pd.Timestamp | str | None,
        end: pd.Timestamp | str | None,
    ) -> pd.DataFrame:
        result = df[df["symbol"].isin(symbols)]

        start_ts = (
            self._to_filter_utc_timestamp(start, is_end=False)
            if start is not None
            else None
        )
        end_ts = self._to_filter_utc_timestamp(end, is_end=True) if end is not None else None

        if start_ts is not None:
            result = result[result["timestamp"] >= start_ts]
        if end_ts is not None:
            result = result[result["timestamp"] <= end_ts]

        return result.sort_values(["timestamp", "symbol"]).reset_index(drop=True)

    @staticmethod
    def _normalize_symbol_list(symbols: Sequence[str] | str) -> list[str]:
        raw = [symbols] if isinstance(symbols, str) else list(symbols)
        normalized = [AshareDataProvider._normalize_symbol(item) for item in raw]
        deduplicated = sorted(set(normalized))
        if not deduplicated:
            raise DataLoadError("No valid symbols provided")
        return deduplicated

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        text = str(symbol).strip().upper()
        if "." in text:
            code, suffix = text.split(".", 1)
            if suffix in {"SH", "SZ"}:
                return f"{code}.{suffix}"

        lowered = text.lower()
        if lowered.startswith("sh") and len(lowered) >= 8:
            return f"{lowered[2:8].upper()}.SH"
        if lowered.startswith("sz") and len(lowered) >= 8:
            return f"{lowered[2:8].upper()}.SZ"

        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) != 6:
            raise DataLoadError(f"Unsupported symbol format: '{symbol}'")
        suffix = "SH" if digits.startswith(("5", "6", "9")) else "SZ"
        return f"{digits}.{suffix}"

    @staticmethod
    def _to_ashare_symbol(symbol: str) -> str:
        normalized = AshareDataProvider._normalize_symbol(symbol)
        code, suffix = normalized.split(".", 1)
        exchange = "sh" if suffix == "SH" else "sz"
        return f"{exchange}{code}"

    @staticmethod
    def _to_akshare_period(timeframe: str) -> str:
        mapping = {
            "1d": "daily",
            "d": "daily",
            "day": "daily",
            "daily": "daily",
            "1w": "weekly",
            "w": "weekly",
            "week": "weekly",
            "weekly": "weekly",
            "1m": "monthly",
            "m": "monthly",
            "month": "monthly",
            "monthly": "monthly",
        }
        key = str(timeframe).strip().lower()
        period = mapping.get(key)
        if period is None:
            raise DataLoadError(
                f"Unsupported timeframe '{timeframe}' for akshare; use daily/weekly/monthly."
            )
        return period

    @staticmethod
    def _to_utc_timestamp(value: pd.Timestamp | str) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")

    def _to_filter_utc_timestamp(self, value: pd.Timestamp | str, is_end: bool) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if isinstance(value, str):
            text = value.strip()
            if len(text) == 10 and text[4] == "-" and text[7] == "-" and is_end:
                ts = ts + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
        if ts.tzinfo is None:
            return ts.tz_localize(self.timezone).tz_convert("UTC")
        return ts.tz_convert("UTC")

    def _to_local_timestamp(self, value: pd.Timestamp | str) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            return ts.tz_localize(self.timezone)
        return ts.tz_convert(self.timezone)

    @staticmethod
    def _estimate_count(start: pd.Timestamp | None, end: pd.Timestamp | None) -> int:
        if start is not None and end is not None:
            days = (end.normalize() - start.normalize()).days + 1
            return max(days + 5, 30)
        if start is not None:
            now = pd.Timestamp.now(tz=start.tz)
            days = (now.normalize() - start.normalize()).days + 1
            return max(days + 5, 30)
        return 500
