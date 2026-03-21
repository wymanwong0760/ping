"""抽象基类与接口定义。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping, Sequence

import pandas as pd

from .validator import ValidationResult


class BaseDataProvider(ABC):
    """数据提供器抽象基类。"""

    @abstractmethod
    def load_bars(
        self,
        symbols: Sequence[str] | str | None,
        start: pd.Timestamp | str | None = None,
        end: pd.Timestamp | str | None = None,
        timeframe: str = "1d",
        dataset_name: str | None = None,
    ) -> pd.DataFrame:
        """加载行情数据。"""

    @abstractmethod
    def get_available_symbols(self, dataset_name: str | None = None) -> list[str]:
        """返回可用标的列表。"""

    @abstractmethod
    def get_calendar(
        self,
        start: pd.Timestamp | str | None = None,
        end: pd.Timestamp | str | None = None,
        dataset_name: str | None = None,
    ) -> pd.DatetimeIndex:
        """返回交易日历。"""

    @abstractmethod
    def get_latest_bar_asof(
        self,
        symbol: str,
        ts: pd.Timestamp | str,
        dataset_name: str | None = None,
    ) -> pd.Series | None:
        """返回指定时点可见的最新 bar。"""

    @abstractmethod
    def validate_dataset(
        self,
        dataset_name: str | None = None,
        null_ratio_threshold: float = 0.3,
    ) -> ValidationResult:
        """执行数据校验。"""

    @abstractmethod
    def register_dataset(
        self,
        name: str,
        metadata: Mapping[str, Any],
    ) -> None:
        """注册数据集元信息。"""
