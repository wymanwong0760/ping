"""A 股数据公共工具。"""
from __future__ import annotations

import importlib
from typing import Sequence

import pandas as pd

from quant_system.core.schema import BAR_OPTIONAL_COLUMNS, BAR_REQUIRED_COLUMNS, BAR_STANDARD_COLUMNS

from .exceptions import DataLoadError


def normalize_symbol_list(symbols: Sequence[str] | str) -> list[str]:
    """归一化并去重 symbols。"""
    raw = [symbols] if isinstance(symbols, str) else list(symbols)
    normalized = [normalize_symbol(item) for item in raw]
    deduplicated = sorted(set(normalized))
    if not deduplicated:
        raise DataLoadError("No valid symbols provided")
    return deduplicated


def normalize_symbol(symbol: str) -> str:
    """统一 symbol 形态为 000001.SZ / 600000.SH。"""
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


def to_ashare_symbol(symbol: str) -> str:
    """转换为 ashare/akshare 常见代码格式，如 sh600000。"""
    normalized = normalize_symbol(symbol)
    code, suffix = normalized.split(".", 1)
    exchange = "sh" if suffix == "SH" else "sz"
    return f"{exchange}{code}"


def to_akshare_period(timeframe: str) -> str:
    """转换 timeframe 到 akshare period。"""
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


def to_utc_timestamp(value: pd.Timestamp | str) -> pd.Timestamp:
    """转换为 UTC 时间戳。"""
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def to_filter_utc_timestamp(
    value: pd.Timestamp | str,
    *,
    is_end: bool,
    timezone: str,
) -> pd.Timestamp:
    """过滤场景时间转换（支持 end 日期字符串闭区间）。"""
    ts = pd.Timestamp(value)
    if isinstance(value, str):
        text = value.strip()
        if len(text) == 10 and text[4] == "-" and text[7] == "-" and is_end:
            ts = ts + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    if ts.tzinfo is None:
        return ts.tz_localize(timezone).tz_convert("UTC")
    return ts.tz_convert("UTC")


def to_local_timestamp(value: pd.Timestamp | str, *, timezone: str) -> pd.Timestamp:
    """转换到本地时区时间戳。"""
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize(timezone)
    return ts.tz_convert(timezone)


def estimate_count(start: pd.Timestamp | None, end: pd.Timestamp | None) -> int:
    """估算 ashare 拉取条数。"""
    if start is not None and end is not None:
        days = (end.normalize() - start.normalize()).days + 1
        return max(days + 5, 30)
    if start is not None:
        now = pd.Timestamp.now(tz=start.tz)
        days = (now.normalize() - start.normalize()).days + 1
        return max(days + 5, 30)
    return 500


def normalize_suspend_flag(series: pd.Series) -> pd.Series:
    """归一化停牌字段到 boolean。"""
    true_values = {"1", "true", "yes", "y", "停牌", "t"}
    false_values = {"0", "false", "no", "n", "正常", "f"}

    normalized = series.astype("string").str.lower().str.strip()
    out = pd.Series(pd.NA, index=series.index, dtype="boolean")
    out[normalized.isin(true_values)] = True
    out[normalized.isin(false_values)] = False
    return out


def normalize_symbol_frame(raw: pd.DataFrame, *, symbol: str, timezone: str) -> pd.DataFrame:
    """标准化单标的原始行情为统一 bars 结构。"""
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
        df["timestamp"] = timestamps.dt.tz_localize(timezone).dt.tz_convert("UTC")
    else:
        df["timestamp"] = timestamps.dt.tz_convert("UTC")

    df["symbol"] = normalize_symbol(symbol)

    for col in ["open", "high", "low", "close", "volume", "amount", "adj_factor"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "amount" in df.columns:
        df["amount"] = df["amount"].astype("Float64")
    if "adj_factor" in df.columns:
        df["adj_factor"] = df["adj_factor"].astype("Float64")

    if "is_suspended" in df.columns:
        df["is_suspended"] = normalize_suspend_flag(df["is_suspended"]).astype("boolean")

    for optional_col in BAR_OPTIONAL_COLUMNS:
        if optional_col not in df.columns:
            if optional_col in {"amount", "adj_factor"}:
                df[optional_col] = pd.Series(pd.NA, index=df.index, dtype="Float64")
            elif optional_col == "is_suspended":
                df[optional_col] = pd.Series(pd.NA, index=df.index, dtype="boolean")
            else:
                df[optional_col] = pd.NA

    missing_required = [col for col in BAR_REQUIRED_COLUMNS if col not in df.columns]
    if missing_required:
        raise DataLoadError(
            f"Ashare bars missing required columns for '{symbol}': {missing_required}"
        )

    normalized = df[BAR_STANDARD_COLUMNS].copy()
    normalized["symbol"] = normalized["symbol"].astype("string")
    normalized = normalized.dropna(subset=["timestamp"]).sort_values(["timestamp", "symbol"])
    return normalized.reset_index(drop=True)


def apply_symbol_time_filters(
    df: pd.DataFrame,
    *,
    symbols: Sequence[str],
    start: pd.Timestamp | str | None,
    end: pd.Timestamp | str | None,
    timezone: str,
) -> pd.DataFrame:
    """按 symbols/start/end 过滤并排序。"""
    result = df[df["symbol"].isin(symbols)]

    start_ts = (
        to_filter_utc_timestamp(start, is_end=False, timezone=timezone)
        if start is not None
        else None
    )
    end_ts = (
        to_filter_utc_timestamp(end, is_end=True, timezone=timezone) if end is not None else None
    )

    if start_ts is not None:
        result = result[result["timestamp"] >= start_ts]
    if end_ts is not None:
        result = result[result["timestamp"] <= end_ts]

    return result.sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def fetch_with_akshare(
    symbol: str,
    timeframe: str,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
) -> pd.DataFrame:
    """使用 akshare 拉取单标的行情。"""
    try:
        akshare = importlib.import_module("akshare")
    except ModuleNotFoundError as exc:
        raise DataLoadError(f"akshare import error: {exc}") from exc

    period = to_akshare_period(timeframe)
    normalized = normalize_symbol(symbol)
    code, _suffix = normalized.split(".", 1)
    start_date = start.strftime("%Y%m%d") if start is not None else None
    end_date = end.strftime("%Y%m%d") if end is not None else None

    akshare_symbol = to_ashare_symbol(symbol)
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
