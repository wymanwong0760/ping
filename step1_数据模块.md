基于我刚才给你的共用约束，请开发“数据模块”。

目标：
构建一个可供策略、回测、风控、执行复用的数据层，先支持本地 CSV / Parquet 历史数据，面向股票中低频系统 MVP。

范围：
- 只做本地文件数据源
- 不接外部行情 API
- 重点是“标准化、可校验、可加载、可复用”

必须实现的能力：

一、项目骨架初始化
如果仓库为空或结构不完整，请先创建基础项目骨架，包括：
- src/quant_system/core
- src/quant_system/data
- tests
- examples
- docs
并补充必要的 pyproject / package 初始化文件（如果缺失）。

二、核心数据接口
请设计并实现以下数据接口或等价抽象：
- BaseDataProvider
- LocalFileDataProvider
- DataCatalog
- TradingCalendar
- DataValidator

要求：
- 接口清晰
- 便于后续替换数据源
- 不要耦合到具体策略

三、历史数据加载能力
请支持：
- CSV 加载
- Parquet 加载
- 单标的加载
- 多标的加载
- 按时间范围加载
- 按周期加载（至少支持日线；分钟线结构要兼容）
- 列名映射配置
- 基础时区处理
- 返回统一标准格式 DataFrame

建议暴露的 API（名字可调整，但语义要保留）：
- load_bars(symbols, start=None, end=None, timeframe="1d")
- get_available_symbols()
- get_calendar(start=None, end=None)
- get_latest_bar_asof(symbol, ts)
- validate_dataset(...)
- register_dataset(...)

四、数据质量校验
请实现至少以下校验：
- 必需列缺失
- 时间列缺失或非法
- 重复时间戳
- 非单调时间
- 非法价格（如 high < low）
- 非法成交量（负数）
- 空值比例检查
- symbol 缺失

五、数据目录与配置
请定义清晰的数据目录规范，例如：
- data/raw/
- data/processed/
- data/catalog/
并提供样例配置文件，让后续模块知道如何找到数据。

六、示例数据与示例脚本
请提供一个最小样例数据集（可以是很小的 mock 数据），并提供至少 2 个 examples：
1. 加载单标的数据
2. 加载多标的数据并打印标准化结果

七、测试
请编写必要测试，至少覆盖：
- CSV 加载
- Parquet 加载
- 列映射
- 多标的加载
- 校验器识别坏数据
- get_latest_bar_asof 的行为

八、文档
请编写 docs 或 README 片段，说明：
- 数据目录规范
- 标准字段定义
- 如何注册新数据集
- 如何被 strategy / backtest 调用

实现约束：
- 不要实现远程数据抓取
- 不要写成一次性脚本，要写成可复用模块
- 内部格式尽量统一
- 允许后续扩展到分钟线
- 第一优先级是“清晰、稳、可测”

交付验收标准：
- examples 可以直接运行
- pytest 可以通过
- 能从样例数据中加载 2 个 symbol 的历史 bars
- 下游模块可以直接调用数据 API
- 文档足够让下一个模块继续开发

完成后请输出：
1. 改动文件清单
2. 核心接口说明
3. 如何运行 examples
4. 如何运行 tests
5. 还预留了哪些扩展点