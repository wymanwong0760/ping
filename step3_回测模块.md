基于现有仓库与共用约束，请开发“回测模块”。

目标：
实现一个可运行、可测试、可扩展的 bar-level 回测引擎，能够消费 data 模块和 strategy 模块的输出，并生成绩效结果。

设计原则：
- 回测层负责按时间顺序推进模拟
- 回测层负责维护现金、持仓、净值、交易记录
- 回测层可以使用“内部模拟撮合器”，但设计上要与后续 execution 模块兼容
- 严禁未来函数和错误撮合时序

必须实现的能力：

一、回测主引擎
请实现：
- BacktestEngine
- 回测配置对象
- 回测运行主入口
- 结果对象 BacktestResult 或等价结构

要求：
- 能接收数据模块输出
- 能接收策略模块输出
- 能按时间顺序推进
- 能支持多标的

二、组合与账本
请实现最小但清晰的组合账本能力：
- 现金
- 当前持仓
- 持仓均价
- 组合净值
- 已实现盈亏
- 未实现盈亏
- 交易记录

三、模拟成交
请实现最小撮合能力（供回测内使用）：
- market order
- limit order（最小支持）
- fill 模式可配置，例如：
  - next_open
  - current_close
- 手续费模型
- 滑点模型
- 成交后更新现金与持仓

要求：
- 撮合逻辑与策略逻辑分开
- 接口设计预留给 execution 模块复用或替换

四、绩效分析
请输出至少以下指标：
- cumulative return
- annualized return
- annualized volatility
- sharpe ratio
- max drawdown
- win rate
- turnover
- total trades

如实现方便，可额外输出：
- monthly returns
- equity curve
- drawdown series

五、回测输出
请提供：
- 结构化结果对象
- CSV / JSON 导出能力（至少一种）
- 简单 summary 输出

六、端到端示例
请提供至少一个完整例子：
- 从数据模块读取样例数据
- 使用策略模块的一个策略
- 运行回测
- 输出 summary

七、测试
至少覆盖：
- 交易后现金和持仓变化正确
- 手续费和滑点生效
- next_open 模式符合时序
- max drawdown 计算正确
- 绩效指标可计算
- 多标的回测可运行

八、文档
说明至少包括：
- 回测引擎结构
- 如何接入新策略
- 如何切换撮合模式
- 回测的已知假设
- 哪些点为 execution 模块预留了接口

实现约束：
- 不要接真实交易接口
- 不要做图形化前端
- 不要用未来 bar 成交当前信号
- 不要把风控规则硬编码进回测主循环
- 如有需要，可以增加最小内部 broker abstraction，但要保持清晰

交付验收标准：
- examples/backtest_demo 类似脚本可直接运行
- 能输出回测 summary
- pytest 可以通过
- strategy 输出能直接接到 backtest
- 结果对象足够支撑后面插入 risk 和 execution

完成后请输出：
1. 改动文件清单
2. 回测引擎结构说明
3. 如何运行端到端示例
4. 绩效指标是如何计算的
5. 为 risk / execution 预留了哪些接口