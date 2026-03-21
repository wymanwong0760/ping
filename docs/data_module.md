# 数据模块（Data Module）

本模块为 quant_system MVP 提供统一的数据层，供 strategy/backtest/risk/execution 复用。

## 1. 数据目录规范

- `data/raw/`：原始历史数据文件（CSV/Parquet）
- `data/processed/`：预处理后数据（当前预留）
- `data/catalog/`：数据集注册配置（样例：`datasets.example.json`）

## 2. 标准字段（内部 long-format）

标准列：

`[timestamp, symbol, open, high, low, close, volume, amount, adj_factor, is_suspended]`

其中必需列：

`[timestamp, symbol, open, high, low, close, volume]`

说明：
- `timestamp` 统一转换为 UTC 时区时间。
- 数据输出按 `timestamp, symbol` 升序，避免未来函数。

## 3. 关键接口

- `BaseDataProvider`
  - `load_bars(symbols, start=None, end=None, timeframe="1d", dataset_name=None)`
  - `get_available_symbols(dataset_name=None)`
  - `get_calendar(start=None, end=None, dataset_name=None)`
  - `get_latest_bar_asof(symbol, ts, dataset_name=None)`
  - `validate_dataset(dataset_name=None, null_ratio_threshold=0.3)`
  - `register_dataset(name, metadata)`

- `LocalFileDataProvider`
  - 本地 CSV/Parquet 读取与标准化实现。

- `DataCatalog`
  - 维护数据集元数据（路径、格式、列映射、时区、timeframe）。

- `DataValidator`
  - 质量检查：缺列、时间非法、重复时间戳、非单调、`high < low`、负成交量、空值比例、symbol 缺失。

- `TradingCalendar`
  - 从 bars 构建交易时间序列并支持区间过滤。

## 4. 注册新数据集

可通过 `data/catalog/datasets.example.json` 或运行时注册：

```python
provider.register_dataset(
    "my_dataset",
    {
        "path": "data/raw/my_bars.csv",
        "format": "csv",
        "timeframe": "1d",
        "timezone": "Asia/Shanghai",
        "column_mapping": {
            "trade_time": "timestamp",
            "ticker": "symbol"
        }
    }
)
```

## 5. 被 strategy/backtest 调用方式

```python
bars = provider.load_bars(
    symbols=["000001.SZ", "000002.SZ"],
    start="2024-01-01",
    end="2024-03-01",
    timeframe="1d",
    dataset_name="sample_multi_csv",
)
```

返回值始终为标准 long-format DataFrame，可直接输入策略层或回测层。
