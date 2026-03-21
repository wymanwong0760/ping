"""交易日历辅助实现。

该模块从已加载 bars 中提取可交易时间轴，避免上层重复处理时间去重、
排序与区间裁剪逻辑。
"""
from __future__ import annotations

import pandas as pd


class TradingCalendar:
    """交易日历工具。

    输入为包含 `timestamp` 列的 bars，输出去重且有序的 DatetimeIndex。
    同时支持 start/end 过滤，便于策略运行器与回测引擎复用同一时间轴定义。
    """

    def get_calendar(
        self,
        bars: pd.DataFrame,
        start: pd.Timestamp | str | None = None,
        end: pd.Timestamp | str | None = None,
    ) -> pd.DatetimeIndex:
        """从 bars 提取交易时点并返回日历索引。

        处理细节：
        - 自动解析并剔除非法时间戳；
        - 对时间去重并升序排序；
        - 使用统一时区语义执行 start/end 过滤。
        """
        if "timestamp" not in bars.columns:
            return pd.DatetimeIndex([])

        ts = pd.to_datetime(bars["timestamp"], errors="coerce", utc=False).dropna()
        unique_ts = pd.DatetimeIndex(sorted(ts.unique()))

        start_ts = self._to_timestamp(start, is_end=False)
        end_ts = self._to_timestamp(end, is_end=True)

        if start_ts is not None:
            unique_ts = unique_ts[unique_ts >= start_ts]
        if end_ts is not None:
            unique_ts = unique_ts[unique_ts <= end_ts]
        return unique_ts

    @staticmethod
    def _to_timestamp(
        value: pd.Timestamp | str | None,
        is_end: bool,
    ) -> pd.Timestamp | None:
        if value is None:
            return None
        ts = pd.Timestamp(value)

        if isinstance(value, str) and TradingCalendar._is_date_only(value) and is_end:
            ts = ts + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)

        if ts.tzinfo is None:
            return ts.tz_localize("UTC")
        return ts.tz_convert("UTC")

    @staticmethod
    def _is_date_only(value: str) -> bool:
        text = value.strip()
        return len(text) == 10 and text[4] == "-" and text[7] == "-"
