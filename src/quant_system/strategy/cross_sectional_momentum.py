"""横截面动量策略（Cross-Sectional Momentum）。

该策略在同一时点对多个标的做相对强弱比较：
1. 计算每个标的在观察窗口内的累计收益；
2. 按动量从高到低排序；
3. 对前 `top_k` 个标的等权配置，其余标的权重设为 0。

适用于“强者恒强”假设下的截面轮动场景。
"""
from __future__ import annotations

import logging
from typing import Sequence

import pandas as pd

from quant_system.core import TargetPosition

from .base import TargetStrategy
from .config import StrategyConfig
from .context import StrategyContext
from .exceptions import StrategyConfigError

logger = logging.getLogger(__name__)


class CrossSectionalMomentumStrategy(TargetStrategy):
    """按截面动量排名生成目标仓位的策略。

    参数来源于 `StrategyConfig.params`：
    - `lookback_periods`: 动量观察期（收益计算窗口长度），默认 5；
    - `top_k`: 入选标的数量，默认 3。

    约束：`lookback_periods > 0` 且 `top_k > 0`。
    """

    def __init__(self, config: StrategyConfig) -> None:
        super().__init__(config)
        self.lookback_periods = int(self.get_param("lookback_periods", 5))
        self.top_k = int(self.get_param("top_k", 3))
        if self.lookback_periods <= 0:
            raise StrategyConfigError("lookback_periods must be > 0")
        if self.top_k <= 0:
            raise StrategyConfigError("top_k must be > 0")

    def generate_targets(self, context: StrategyContext) -> Sequence[TargetPosition]:
        """在当前调仓时点生成目标权重。

        对每个标的：
        - 取最近 `lookback_periods + 1` 个收盘价；
        - 用 `(end_price / start_price) - 1` 作为动量收益；
        - 数据不足或起始价格为 0 时跳过。

        最终将前 `top_k` 名设为等权，其余为 0。
        """
        momentum_scores: dict[str, float] = {}

        for symbol in context.universe:
            symbol_bars = context.bars[context.bars["symbol"] == symbol].sort_values("timestamp")
            # 转成数值并去除缺失，保证收益计算稳定。
            closes = pd.to_numeric(symbol_bars["close"], errors="coerce").dropna()
            if len(closes) < self.lookback_periods + 1:
                # 至少需要 N+1 个点，才能计算 N 期收益。
                continue

            trailing_window = closes.tail(self.lookback_periods + 1)
            start_price = float(trailing_window.iloc[0])
            end_price = float(trailing_window.iloc[-1])
            if start_price == 0:
                # 避免除零，直接跳过异常价格序列。
                continue

            momentum_scores[str(symbol)] = (end_price / start_price) - 1.0

        if not momentum_scores:
            return []

        # 从高到低排序，选择动量最强的 top_k 标的。
        ranked = sorted(momentum_scores.items(), key=lambda item: item[1], reverse=True)
        chosen_count = min(self.top_k, len(ranked))
        selected_symbols = {symbol for symbol, _ in ranked[:chosen_count]}
        selected_weight = 1.0 / chosen_count

        targets: list[TargetPosition] = []
        for symbol in sorted(momentum_scores.keys()):
            targets.append(
                TargetPosition(
                    timestamp=context.timestamp.to_pydatetime(),
                    symbol=symbol,
                    source=self.config.source,
                    target_weight=selected_weight if symbol in selected_symbols else 0.0,
                    metadata={
                        # 保留排名与动量值，便于事后归因分析。
                        "momentum_return": float(momentum_scores[symbol]),
                        "lookback_periods": self.lookback_periods,
                        "rank": next(
                            index for index, (name, _) in enumerate(ranked, start=1) if name == symbol
                        ),
                    },
                )
            )

        logger.debug(
            "CrossSectionalMomentumStrategy generated %s targets at %s",
            len(targets),
            context.timestamp,
        )
        return targets
