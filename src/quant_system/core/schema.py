"""模式常量定义。"""
from __future__ import annotations

BAR_STANDARD_COLUMNS: list[str] = [
    "timestamp",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adj_factor",
    "is_suspended",
]

BAR_REQUIRED_COLUMNS: list[str] = [
    "timestamp",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
]

BAR_OPTIONAL_COLUMNS: list[str] = ["amount", "adj_factor", "is_suspended"]

DEFAULT_TIMEFRAME = "1d"
DEFAULT_TIMEZONE = "UTC"
