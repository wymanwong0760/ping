"""执行模块高层接口定义。

本模块提供轻量工厂与单步执行封装，方便外部以最小样板代码调用执行引擎。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from quant_system.core import OrderRequest

from .config import ExecutionConfig
from .engine import ExecutionEngine
from .models import ExecutionStepResult


def create_execution_engine(config: ExecutionConfig | None = None) -> ExecutionEngine:
    """创建并返回执行引擎实例。

    参数:
        config: 执行配置；为 None 时使用 `ExecutionConfig()` 默认配置。
    """
    return ExecutionEngine(config=config)


def run_execution_step(
    engine: ExecutionEngine,
    timestamp: datetime,
    bars_by_symbol: Mapping[str, Mapping[str, Any]],
    orders: list[OrderRequest] | None = None,
) -> ExecutionStepResult:
    """执行一个行情步并返回执行结果。

    调用顺序:
        1) 若提供 `orders`，先提交到引擎；
        2) 再调用 `on_bar` 处理当前 bar。

    说明:
        最终是否在本步成交由引擎内部时序与撮合规则决定（例如 `fill_mode`）。
    """
    if orders:
        engine.submit_orders(timestamp=timestamp, orders=orders)
    return engine.on_bar(timestamp=timestamp, bars_by_symbol=bars_by_symbol)
