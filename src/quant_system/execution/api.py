"""高层接口定义。"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from quant_system.core import OrderRequest

from .config import ExecutionConfig
from .engine import ExecutionEngine
from .models import ExecutionStepResult


def create_execution_engine(config: ExecutionConfig | None = None) -> ExecutionEngine:
    """创建并返回执行引擎实例。"""
    return ExecutionEngine(config=config)


def run_execution_step(
    engine: ExecutionEngine,
    timestamp: datetime,
    bars_by_symbol: Mapping[str, Mapping[str, Any]],
    orders: list[OrderRequest] | None = None,
) -> ExecutionStepResult:
    """执行一个行情步并返回执行结果。"""
    if orders:
        engine.submit_orders(timestamp=timestamp, orders=orders)
    return engine.on_bar(timestamp=timestamp, bars_by_symbol=bars_by_symbol)
