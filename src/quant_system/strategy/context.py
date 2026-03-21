"""运行时上下文定义。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass(slots=True)
class StrategyContext:
    """策略运行上下文。"""

    timestamp: pd.Timestamp
    bars: pd.DataFrame
    universe: list[str]
    expected_universe: list[str]
    missing_symbols: list[str] = field(default_factory=list)
    state: dict[str, Any] = field(default_factory=dict)
