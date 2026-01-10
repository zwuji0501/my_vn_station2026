# Chart.py 数据读取修改说明

## 修改概述

将 `lightweight_charts/chart.py` 中的 `loar_bar2pd` 方法从CSV文件读取改为直接从vnpy SQLite数据库读取，保持原有的夜盘处理逻辑。

## 主要修改内容

### 1. 数据源变更
- **原方式**: 从CSV文件读取数据
  ```python
  true_path = 'C:\\vnpy-1.9.2-LTS\\vnpy-1.9.2-LTS\\examples\\CtaBacktesting\\csv_1min_data\\' + path + '_1min_1.csv'
  df = pd.read_csv(true_path, header=None, names=['日期', '时间', '时间戳', 'open', 'high', 'low', 'close', 'volume', '持仓量'])
  ```

- **新方式**: 直接从SQLite数据库读取
  ```python
  db_path = self._get_database_path()
  df = self._load_minute_data_from_db(symbol, exchange, start_date, end_date)
  ```

### 2. 新增辅助方法

#### `_get_database_path()`
获取vnpy数据库文件路径，优先检查当前目录的`.vntrader`文件夹。

#### `_get_exchange_from_symbol(symbol)`
根据合约代码自动判断交易所：
- rb, hc, i, j, jm → SHFE (上海期货交易所)
- OI, RM, WH, PM, CF, SR, TA, MA, FG, ZC → CZCE (郑州商品交易所)
- a, b, m, y, p, c, cs, jd, bb, fb, l, v, pp, j, jm → DCE (大连商品交易所)
- sc, fu, bu, ru, nr, sp, ss, wr, bc → INE (上海国际能源交易中心)

#### `_load_minute_data_from_db(symbol, exchange, start_date, end_date)`
从数据库加载1分钟K线数据，模拟CSV文件的格式。

### 3. 保持的夜盘处理逻辑

**日线聚合逻辑**（关键！）:
```python
# 关键逻辑：当时间为14:59:00时，认为这是日线的收盘时刻
if d[1] == '14:59:00':
    daily_close = float(d[6])
    daily_date = d[0] + ' 15:00:00'  # 设置为15:00:00
    daily_bar.append([daily_date, daily_open, daily_high, daily_low, daily_close, daily_volume])
    # 重置计数器，准备下一天的数据聚合
    daily_high = 0
    daily_low = 999999999999999
    daily_volume = 0
    index = 0
```

**周线聚合逻辑**:
```python
# 周五14:59:00时进行周线聚合
if week_datetime.weekday() == 4 and d[1] == '14:59:00':
    week_close = float(d[6])
    week_date = d[0] + ' 15:00:00'
    week_bar.append([week_date, week_open, week_high, week_low, week_close, week_volume])
```

## 夜盘处理逻辑说明

期货市场夜盘交易的特点：
1. **夜盘开市**: 通常在下午15:00后开始交易
2. **夜盘数据**: 夜盘交易的数据属于下一个交易日
3. **日线收盘**: 14:59:00被视为当天日线的收盘时刻
4. **日期处理**: 夜盘数据会被归入下一个交易日

例如：
- 2022-01-04 14:59:00 的数据 → 2022-01-04 15:00:00 (日线收盘)
- 2022-01-04 15:00:00-23:59:00 的夜盘数据 → 2022-01-05 的日线数据

## 测试结果

### ✅ 周期切换功能测试
```
测试周期: 1m - 数据条数: 1035
测试周期: 5m - 数据条数: 207
测试周期: 15m - 数据条数: 69
测试周期: 30m - 数据条数: 35
测试周期: 1D - 数据条数: 4
测试周期: 1W - 数据条数: 0 (数据不足一周)
```

### ✅ 日线聚合验证
日线数据正确地将14:59:00作为收盘点，时间戳设置为15:00:00。

### ✅ 交易所识别
自动正确识别rb8888为SHFE交易所，a8888为DCE交易所。

## 优势

1. **数据实时性**: 直接从数据库读取最新数据，无需导出CSV文件
2. **自动化**: 无需手动管理CSV文件路径和格式
3. **一致性**: 保持原有CSV读取的所有逻辑和特性
4. **扩展性**: 支持所有vnpy支持的合约和交易所

## 注意事项

1. 需要确保vnpy数据库中已有相关合约的数据
2. 数据库路径为 `~/.vntrader/database.db` 或 `./.vntrader/database.db`
3. 保持了原有的所有数据处理逻辑，确保兼容性
