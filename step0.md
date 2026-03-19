你现在扮演“资深量化系统架构师 + Python 工程师 + 测试工程师”。

请在当前仓库中增量开发，不要推翻已有实现；优先复用已有代码，必要时只做最小重构，并说明原因。

项目目标：
构建一个量化交易系统 MVP，面向股票中低频策略，先支持：
1. 本地历史数据加载
2. 信号生成
3. 回测
4. 风控
5. 模拟执行（paper trading）

工程原则：
- 简单、稳定、可测试、可扩展、可观测
- 单机版优先，不做微服务，不做分布式
- 配置驱动
- 代码必须有类型标注、docstring、日志、错误处理
- 不要引入不必要的重量依赖
- 不确定处做合理假设，并写入 README 或 docs

统一目录（如不存在则创建）：
- src/quant_system/core
- src/quant_system/data
- src/quant_system/strategy
- src/quant_system/backtest
- src/quant_system/risk
- src/quant_system/execution
- tests
- examples
- docs

统一核心对象约定（如尚未定义，请先定义在 core 模块中）：
1. Bar
   - timestamp
   - symbol
   - open
   - high
   - low
   - close
   - volume
   - amount（可选）
   - adj_factor（可选）
   - is_suspended（可选）

2. Signal
   - timestamp
   - symbol
   - direction（long / short / flat）
   - strength
   - source
   - metadata

3. TargetPosition
   - timestamp
   - symbol
   - target_weight（可选）
   - target_qty（可选）
   - source
   - metadata
   约束：target_weight 和 target_qty 至少一个有值，但不要同时作为主逻辑冲突使用。

4. OrderRequest
   - timestamp
   - symbol
   - side（buy / sell）
   - quantity
   - order_type（market / limit）
   - limit_price（可选）
   - source
   - metadata

5. Fill
   - timestamp
   - symbol
   - side
   - quantity
   - price
   - commission
   - slippage
   - order_id

6. PortfolioSnapshot
   - timestamp
   - cash
   - equity
   - positions
   - leverage
   - drawdown

统一数据格式约定：
- 内部历史行情尽量统一为 long-format DataFrame
- 推荐标准列：
  [timestamp, symbol, open, high, low, close, volume, amount, adj_factor, is_suspended]
- 时间必须可排序，禁止未来函数

统一工作方式：
1. 先扫描仓库并总结现状
2. 给出简短实施计划（控制在 10 条以内）
3. 列出将新增/修改的文件
4. 直接开始编码，不要停留在纯讨论
5. 编码后运行测试
6. 输出：
   - 改动摘要
   - 关键设计说明
   - 运行方式
   - 后续建议

硬性要求：
- 每个对外能力至少提供 1 个 examples 示例
- 每个模块至少提供必要的单元测试
- 所有公共接口都要可读、可扩展
- 不允许把回测、风控、执行逻辑混进策略层
- 不允许使用未来数据
- 不要为了“看起来高级”而过度设计

现在我接下来会给你一个具体模块，请按以上约束直接开发。