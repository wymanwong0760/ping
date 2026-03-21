"""双均线策略（Dual Moving Average）。

该策略基于短期均线与长期均线的相对位置生成方向信号：
- 当短期均线高于长期均线时，输出 `long`；
- 否则输出 `flat`（空仓/不持有）。

实现目标是保持逻辑直观、可解释，适合作为趋势跟随类入门策略模板。
"""
from __future__ import annotations

import logging
from typing import Sequence

import pandas as pd

from quant_system.core import Signal

from .base import SignalStrategy
from .config import StrategyConfig
from .context import StrategyContext
from .exceptions import StrategyConfigError

logger = logging.getLogger(__name__)


class DualMovingAverageStrategy(SignalStrategy):
    """基于双均线关系生成信号的趋势策略。

    参数来源于 `StrategyConfig.params`：
    - `short_window`: 短周期均线窗口，默认 5；
    - `long_window`: 长周期均线窗口，默认 20。

    约束：`0 < short_window < long_window`。
    """

    def __init__(self, config: StrategyConfig) -> None:
        super().__init__(config)
        self.short_window = int(self.get_param("short_window", 5))
        self.long_window = int(self.get_param("long_window", 20))
        if self.short_window <= 0:
            raise StrategyConfigError("short_window must be > 0")
        if self.long_window <= 0:
            raise StrategyConfigError("long_window must be > 0")
        if self.short_window >= self.long_window:
            raise StrategyConfigError("short_window must be smaller than long_window")

    def generate_signals(self, context: StrategyContext) -> Sequence[Signal]:
        """在当前调仓时点生成每个标的的方向信号。

        处理流程：
        1. 对每个标的提取历史 close 序列；
        2. 若数据不足 `long_window`，跳过该标的；
        3. 计算短均线与长均线；
        4. 依据短长均线关系输出 `long/flat`。

        `strength` 使用 `(short_ma - long_ma) / abs(long_ma)`，用于表达趋势强弱。
        """
        signals: list[Signal] = []

        for symbol in context.universe:
            symbol_bars = context.bars[context.bars["symbol"] == symbol].sort_values("timestamp")
            # 统一转成数值并丢弃脏值，避免均线计算被字符串/空值污染。
            closes = pd.to_numeric(symbol_bars["close"], errors="coerce").dropna()
            if len(closes) < self.long_window:
                # 历史长度不足以构造长均线时，不对该标的给出信号。
                continue

            # 使用最新窗口计算短均线与长均线。
            short_ma = float(closes.tail(self.short_window).mean())
            long_ma = float(closes.tail(self.long_window).mean())
            if long_ma == 0:
                # 防止强度计算发生除零。
                continue

            # 本策略当前仅输出 long/flat，不输出 short。
            if short_ma > long_ma:
                direction = "long"
                strength = (short_ma - long_ma) / abs(long_ma)
            else:
                direction = "flat"
                strength = 0.0

            signals.append(
                Signal(
                    timestamp=context.timestamp.to_pydatetime(),
                    symbol=str(symbol),
                    direction=direction,
                    strength=float(strength),
                    source=self.config.source,
                    metadata={
                        # 记录关键中间量，便于回测分析与调试。
                        "short_window": self.short_window,
                        "long_window": self.long_window,
                        "short_ma": short_ma,
                        "long_ma": long_ma,
                    },
                )
            )

        logger.debug(
            "DualMovingAverageStrategy generated %s signals at %s",
            len(signals),
            context.timestamp,
        )
        return signals
