# VNpy数据读取器

这个脚本直接从vnpy的SQLite数据库文件读取K线数据并以DataFrame格式返回，不依赖于chart组件。

## 文件说明

- `data_reader.py`: 主要的數據读取脚本，直接连接SQLite数据库
- `example_usage.py`: 使用示例脚本
- `check_db.py`: 数据库检查工具
- `test_query.py`: 数据库查询测试脚本

## 功能特性

- **直接数据库访问**：直接连接SQLite数据库，不依赖chart组件
- **智能重采样**：自动从1分钟数据重采样生成更高时间周期的数据
- **多交易所支持**：自动识别SHFE、CZCE、DCE、INE等交易所
- **多种时间周期**：支持'1m', '5m', '15m', '30m', '60m', '1D', '1W'
- **日期过滤**：支持自定义开始和结束日期
- **DataFrame输出**：返回标准pandas DataFrame格式

## 实现原理

1. **数据库定位**：自动查找`.vntrader/database.db`文件
2. **合约识别**：根据合约代码前缀自动判断交易所
3. **数据查询**：优先查询目标时间周期数据，如果不存在则使用1分钟数据
4. **重采样处理**：使用pandas的resample功能生成更高时间周期的K线数据

## 命令行使用

### 基本用法

```bash
# 使用默认参数
python data_reader.py

# 指定合约和时间范围
python data_reader.py rb8888 1D 2022-01-01 2022-12-31

# 显示图表界面
python data_reader.py rb8888 1D 2022-01-01 2022-12-31 --show-chart
```

### 参数说明

- `symbol`: 合约代码，如 'rb8888' (默认: rb8888)
- `timeframe`: 时间周期，如 '1D' (默认: 1D)
- `start_date`: 开始日期，如 '2022-01-01' (默认: 2022-01-01)
- `end_date`: 结束日期，如 '2022-12-31' (默认: 无限制)
- `--show-chart`: 显示图表界面

## Python代码中使用

```python
from data_reader import load_vnpy_data

# 加载日线数据
df = load_vnpy_data('rb8888', '1D', '2022-01-01', '2022-12-31')

# 加载5分钟数据
df_5m = load_vnpy_data('rb8888', '5m', '2022-01-01', '2022-01-02')

# 显示图表界面
df_with_chart = load_vnpy_data('rb8888', '1D', '2022-01-01', '2022-12-31', show_chart=True)

print(df.head())
```

## 数据格式

返回的DataFrame包含以下列：

- `date`: 日期时间 (格式: 'YYYYMMDD HH:MM:SS')
- `open`: 开盘价
- `high`: 最高价
- `low`: 最低价
- `close`: 收盘价
- `volume`: 成交量

## 支持的合约类型

脚本会根据合约代码自动判断交易所：

- 上海期货交易所 (SHFE): rb, hc, cu, al, zn, pb, ni, sn, au, ag
- 郑州商品交易所 (CZCE): 其他合约

## 示例运行

运行示例脚本查看完整的使用演示：

```bash
python example_usage.py
```

这将展示：
- 基本数据加载
- 分钟数据加载
- 数据分析和统计
- 不同合约的数据加载

## 数据库结构

vnpy使用SQLite数据库存储数据，主要表结构：

- **dbbardata**: K线数据表
  - symbol: 合约代码
  - exchange: 交易所
  - datetime: 时间戳
  - interval: 时间周期 ('1m', 'd', 'w')
  - open_price, high_price, low_price, close_price: OHLC价格
  - volume: 成交量

- **dbbaroverview**: 数据概览表
  - 记录每个合约的数据统计信息

## 注意事项

1. **数据来源**：需要先通过vnpy的数据录制功能收集数据到数据库
2. **时间周期**：数据库通常只存储1分钟数据，其他周期通过重采样生成
3. **日期格式**：输入日期格式为 'YYYY-MM-DD'，输出为 'YYYY-MM-DD HH:MM:SS'
4. **性能**：大量数据的重采样可能需要一些时间
5. **交易所识别**：脚本会根据合约代码自动判断交易所

## 故障排除

如果遇到数据加载失败：

1. 检查数据库连接和配置
2. 确认合约代码和交易所正确
3. 验证日期范围是否合理
4. 检查数据库中是否存在对应时间周期的数据
