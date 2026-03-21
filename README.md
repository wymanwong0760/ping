# ping

## Data Module Quick Start

### Install

```bash
python3 -m pip install -e ".[dev]"
```

### Install Ashare (optional)

```bash
python3 -m pip install -e ".[ashare]"
```

> Note: the `ashare` extra installs `akshare` as the default online A-share data backend in this project. If you already have `Ashare/ashare` in your environment, the provider will try it first and then fall back to `akshare`.

### Install Visualization (optional)

```bash
python3 -m pip install -e ".[viz]"
```

### Run examples

```bash
python3 examples/load_single_symbol.py
python3 examples/load_multi_symbols.py
python3 examples/run_dual_moving_average.py
python3 examples/run_cross_sectional_momentum.py
python3 examples/show_strategy_outputs.py
python3 examples/run_backtest_demo.py
python3 examples/run_backtest_ashare_demo.py
python3 examples/run_execution_demo.py
```


### Run tests

```bash
pytest -q
```

### Notes

- If backtest outputs `total_trades: 0`, first check `fill_mode` and sample length.
- `next_open` requires a following bar to fill; with very short samples it can lead to no fills.
- Prefer `fill_mode="current_close"` in demos when you want quick visible trades.

### Docs

See docs for:
- `docs/data_module.md`：数据目录规范、标准字段定义、数据集注册方式
- `docs/strategy_module.md`：策略框架、样例策略、扩展方式、无未来函数约束
- `docs/strategies/dual_moving_average/README.md`：双均线策略说明与流程图
- `docs/strategies/cross_sectional_momentum/README.md`：横截面动量策略说明与流程图
- `docs/backtest_module.md`：bar-level 回测引擎、撮合/账本/绩效、导出与高层 API
- `docs/risk_module.md`：风控规则引擎、审计记录、回测链路中的风险决策
- `docs/execution_module.md`：paper trading 执行引擎、订单生命周期、撮合时序
