#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
VNpy 1分钟数据预处理为日线数据
将数据库中的1分钟K线数据按夜盘处理逻辑聚合为日线数据并导入数据库
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime
import argparse


class BarDataPreprocessor:
    """K线数据预处理器，支持日线和小时线"""

    def __init__(self, db_path=None):
        """初始化预处理器"""
        if db_path is None:
            # 自动查找数据库路径
            current_dir = os.getcwd()
            vntrader_dir = os.path.join(current_dir, ".vntrader")
            if os.path.exists(vntrader_dir):
                db_path = os.path.join(vntrader_dir, "database.db")

            if db_path is None or not os.path.exists(db_path):
                home_dir = os.path.expanduser("~")
                vntrader_dir = os.path.join(home_dir, ".vntrader")
                db_path = os.path.join(vntrader_dir, "database.db")

        self.db_path = db_path
        print(f"数据库路径: {self.db_path}")

        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"数据库文件不存在: {self.db_path}")

    def _get_exchange_from_symbol(self, symbol):
        """根据合约代码确定交易所"""
        if symbol.startswith(('rb', 'hc', 'i', 'j', 'jm')):
            return 'SHFE'  # 上海期货交易所
        elif symbol.startswith(('OI', 'RM', 'WH', 'PM', 'CF', 'SR', 'TA', 'MA', 'FG', 'ZC')):
            return 'CZCE'  # 郑州商品交易所
        elif symbol.startswith(('cu', 'al', 'zn', 'pb', 'ni', 'sn', 'au', 'ag')):
            return 'SHFE'
        elif symbol.startswith(('a', 'b', 'm', 'y', 'p', 'c', 'cs', 'jd', 'bb', 'fb', 'l', 'v', 'pp', 'j', 'jm')):
            return 'DCE'  # 大连商品交易所
        elif symbol.startswith(('sc', 'fu', 'bu', 'ru', 'nr', 'sp', 'ss', 'wr', 'bc')):
            return 'INE'  # 上海国际能源交易中心
        else:
            return 'CZCE'  # 默认郑州商品交易所

    def get_available_symbols(self):
        """获取数据库中所有有1分钟数据的合约"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 查询有1分钟数据的所有合约
        cursor.execute("""
            SELECT DISTINCT symbol, exchange, COUNT(*) as count
            FROM dbbardata
            WHERE interval = '1m'
            GROUP BY symbol, exchange
            ORDER BY symbol
        """)

        symbols = cursor.fetchall()
        conn.close()

        return symbols

    def get_symbols_needing_update(self, hours_threshold: int = 24) -> list:
        """
        获取需要更新的合约（基于最后数据时间）

        Args:
            hours_threshold: 时间阈值（小时），超过此时间未更新的合约需要更新

        Returns:
            list: 需要更新的合约列表 [(symbol, exchange, last_update), ...]
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 查询有1分钟数据且最后更新时间超过阈值的合约
        cursor.execute("""
            SELECT symbol, exchange, MAX(datetime) as last_update, COUNT(*) as data_count
            FROM dbbardata
            WHERE interval = '1m'
            GROUP BY symbol, exchange
            HAVING last_update < datetime('now', '-{} hours')
            ORDER BY last_update ASC
        """.format(hours_threshold))

        symbols = cursor.fetchall()
        conn.close()

        return symbols

    def get_last_aggregation_time(self, symbol: str, exchange: str, interval: str) -> datetime:
        """
        获取合约最后聚合时间

        Args:
            symbol: 合约代码
            exchange: 交易所
            interval: 时间周期 ('1h', 'd')

        Returns:
            datetime: 最后聚合时间，如果没有则返回None
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT MAX(datetime) FROM dbbardata
            WHERE symbol = ? AND exchange = ? AND interval = ?
        """, (symbol, exchange, interval))

        result = cursor.fetchone()[0]
        conn.close()

        if result:
            try:
                return datetime.fromisoformat(result.replace(' ', 'T'))
            except (ValueError, AttributeError):
                return None

        return None

    def get_new_minute_data_count(self, symbol: str, exchange: str, since_time: datetime) -> int:
        """
        获取指定时间之后的新1分钟数据条数

        Args:
            symbol: 合约代码
            exchange: 交易所
            since_time: 起始时间

        Returns:
            int: 新数据条数
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*) FROM dbbardata
            WHERE symbol = ? AND exchange = ? AND interval = '1m'
            AND datetime > ?
        """, (symbol, exchange, since_time.isoformat()))

        count = cursor.fetchone()[0]
        conn.close()

        return count

    def aggregate_hourly_data(self, symbol, exchange, force_update=False, incremental=True):
        """
        将指定合约的1分钟数据聚合为小时线数据

        Args:
            symbol: 合约代码
            exchange: 交易所
            force_update: 是否强制更新已存在的小时线数据
            incremental: 是否启用增量聚合（只聚合新增数据）
        """
        print(f"开始处理 {symbol} ({exchange}) 的小时线数据聚合...")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 检查是否已有小时线数据，并决定聚合策略
        cursor.execute("""
            SELECT COUNT(*) FROM dbbardata
            WHERE symbol = ? AND exchange = ? AND interval = '1h'
        """, (symbol, exchange))

        existing_count = cursor.fetchone()[0]

        # 确定聚合起始时间
        start_time = None
        if existing_count > 0 and not force_update and incremental:
            # 增量模式：只聚合新增数据
            last_agg_time = self.get_last_aggregation_time(symbol, exchange, '1h')
            if last_agg_time:
                start_time = last_agg_time
                print(f"  {symbol} 启用增量聚合，从 {last_agg_time} 开始")
            else:
                print(f"  {symbol} 无法确定最后聚合时间，将重新生成所有数据")
        elif existing_count > 0 and force_update:
            print(f"  {symbol} 已存在 {existing_count} 条小时线数据，将重新生成")
            # 删除现有的小时线数据
            cursor.execute("""
                DELETE FROM dbbardata
                WHERE symbol = ? AND exchange = ? AND interval = '1h'
            """, (symbol, exchange))
            conn.commit()
        elif existing_count == 0:
            print(f"  {symbol} 首次生成小时线数据")

        # 查询该合约的1分钟数据（支持增量）
        query = """
            SELECT datetime, open_price, high_price, low_price, close_price, volume
            FROM dbbardata
            WHERE symbol = ? AND exchange = ? AND interval = '1m'
        """
        params = [symbol, exchange]

        if start_time:
            query += " AND datetime > ?"
            params.append(start_time.isoformat())

        query += " ORDER BY datetime"
        cursor.execute(query, params)

        rows = cursor.fetchall()

        if not rows:
            print(f"  {symbol} 没有找到1分钟数据")
            conn.close()
            return 0

        print(f"  找到 {len(rows)} 条1分钟数据，开始聚合小时线...")

        # 聚合小时线数据
        hourly_data = []
        hourly_high = 0
        hourly_low = 999999999999999
        hourly_volume = 0
        index = 0
        current_hour = None

        for row in rows:
            datetime_str, open_price, high_price, low_price, close_price, volume = row

            # 解析日期和时间
            dt = datetime.fromisoformat(datetime_str.replace(' ', 'T'))
            hour_key = dt.strftime('%Y%m%d %H')  # 按小时分组

            # 如果是新的小时，重置计数器
            if current_hour != hour_key:
                if index > 0:  # 保存上一小时的数据
                    hourly_datetime = datetime.strptime(f"{current_hour}:00:00", '%Y%m%d %H:%M:%S')
                    hourly_data.append({
                        'symbol': symbol,
                        'exchange': exchange,
                        'datetime': hourly_datetime.isoformat(' ', 'seconds'),
                        'interval': '1h',
                        'volume': hourly_volume,
                        'turnover': 0.0,
                        'open_interest': 0.0,
                        'open_price': hourly_open,
                        'high_price': hourly_high,
                        'low_price': hourly_low,
                        'close_price': hourly_close
                    })

                # 初始化新小时
                current_hour = hour_key
                hourly_open = float(open_price)
                hourly_high = float(high_price)
                hourly_low = float(low_price)
                hourly_close = float(close_price)
                hourly_volume = float(volume)
                index = 1
            else:
                # 更新当前小时的数据
                hourly_high = max(float(high_price), hourly_high)
                hourly_low = min(float(low_price), hourly_low)
                hourly_close = float(close_price)
                hourly_volume += float(volume)
                index += 1

        # 保存最后一个小时的数据
        if index > 0 and current_hour:
            hourly_datetime = datetime.strptime(f"{current_hour}:00:00", '%Y%m%d %H:%M:%S')
            hourly_data.append({
                'symbol': symbol,
                'exchange': exchange,
                'datetime': hourly_datetime.isoformat(' ', 'seconds'),
                'interval': '1h',
                'volume': hourly_volume,
                'turnover': 0.0,
                'open_interest': 0.0,
                'open_price': hourly_open,
                'high_price': hourly_high,
                'low_price': hourly_low,
                'close_price': hourly_close
            })

        # 批量插入小时线数据
        if hourly_data:
            cursor.executemany("""
                INSERT INTO dbbardata
                (symbol, exchange, datetime, interval, volume, turnover, open_interest,
                 open_price, high_price, low_price, close_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [(d['symbol'], d['exchange'], d['datetime'], d['interval'],
                   d['volume'], d['turnover'], d['open_interest'],
                   d['open_price'], d['high_price'], d['low_price'], d['close_price'])
                  for d in hourly_data])

            conn.commit()
            print(f"  成功插入 {len(hourly_data)} 条小时线数据")

            # 更新dbbaroverview表
            cursor.execute("""
                SELECT COUNT(*), MIN(datetime), MAX(datetime)
                FROM dbbardata
                WHERE symbol = ? AND exchange = ? AND interval = '1h'
            """, (symbol, exchange))

            count, start_date, end_date = cursor.fetchone()

            # 检查是否已存在概览记录
            cursor.execute("""
                SELECT id FROM dbbaroverview
                WHERE symbol = ? AND exchange = ? AND interval = '1h'
            """, (symbol, exchange))

            existing = cursor.fetchone()

            if existing:
                # 更新现有记录
                cursor.execute("""
                    UPDATE dbbaroverview
                    SET count = ?, start = ?, end = ?
                    WHERE symbol = ? AND exchange = ? AND interval = '1h'
                """, (count, start_date, end_date, symbol, exchange))
            else:
                # 插入新记录
                cursor.execute("""
                    INSERT INTO dbbaroverview (symbol, exchange, interval, count, start, end)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (symbol, exchange, '1h', count, start_date, end_date))

            conn.commit()
            print(f"  更新了dbbaroverview表")

        conn.close()
        return len(hourly_data)

    def aggregate_daily_data(self, symbol, exchange, force_update=False, incremental=True):
        """
        将指定合约的1分钟数据聚合为日线数据

        Args:
            symbol: 合约代码
            exchange: 交易所
            force_update: 是否强制更新已存在的日线数据
        """
        print(f"开始处理 {symbol} ({exchange}) 的日线数据聚合...")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 检查是否已有日线数据，并决定聚合策略
        cursor.execute("""
            SELECT COUNT(*) FROM dbbardata
            WHERE symbol = ? AND exchange = ? AND interval = 'd'
        """, (symbol, exchange))

        existing_count = cursor.fetchone()[0]

        # 确定聚合起始时间
        start_time = None
        if existing_count > 0 and not force_update and incremental:
            # 增量模式：只聚合新增数据
            last_agg_time = self.get_last_aggregation_time(symbol, exchange, 'd')
            if last_agg_time:
                start_time = last_agg_time
                print(f"  {symbol} 启用增量聚合，从 {last_agg_time} 开始")
            else:
                print(f"  {symbol} 无法确定最后聚合时间，将重新生成所有数据")
        elif existing_count > 0 and force_update:
            print(f"  {symbol} 已存在 {existing_count} 条日线数据，将重新生成")
            # 删除现有的日线数据
            cursor.execute("""
                DELETE FROM dbbardata
                WHERE symbol = ? AND exchange = ? AND interval = 'd'
            """, (symbol, exchange))
            conn.commit()
        elif existing_count == 0:
            print(f"  {symbol} 首次生成日线数据")

        # 查询该合约的1分钟数据（支持增量）
        query = """
            SELECT datetime, open_price, high_price, low_price, close_price, volume
            FROM dbbardata
            WHERE symbol = ? AND exchange = ? AND interval = '1m'
        """
        params = [symbol, exchange]

        if start_time:
            query += " AND datetime > ?"
            params.append(start_time.isoformat())

        query += " ORDER BY datetime"
        cursor.execute(query, params)

        rows = cursor.fetchall()

        if not rows:
            print(f"  {symbol} 没有找到1分钟数据")
            conn.close()
            return 0

        print(f"  找到 {len(rows)} 条1分钟数据，开始聚合...")

        # 聚合日线数据
        daily_data = []
        daily_high = 0
        daily_low = 999999999999999
        daily_volume = 0
        index = 0
        last_data = None

        for row in rows:
            datetime_str, open_price, high_price, low_price, close_price, volume = row

            # 解析日期和时间
            dt = datetime.fromisoformat(datetime_str.replace(' ', 'T'))
            date_str = dt.strftime('%Y%m%d')
            time_str = dt.strftime('%H:%M:%S')

            d = [date_str, time_str, str(int(dt.timestamp())),
                 open_price, high_price, low_price, close_price, volume]

            if index == 0:
                daily_open = float(d[3])
            daily_high = max(float(d[4]), daily_high)
            daily_low = min(float(d[5]), daily_low)
            daily_close = float(d[6])
            daily_volume += float(d[7])
            daily_date = d[0] + ' 15:00:00'
            index += 1

            # 关键逻辑：当时间为14:59:00时，认为这是日线的收盘时刻
            if d[1] == '14:59:00':
                daily_close = float(d[6])
                daily_date = d[0] + ' 15:00:00'
                daily_datetime = datetime.strptime(daily_date, '%Y%m%d %H:%M:%S')

                daily_data.append({
                    'symbol': symbol,
                    'exchange': exchange,
                    'datetime': daily_datetime.isoformat(' ', 'seconds'),
                    'interval': 'd',
                    'volume': daily_volume,
                    'turnover': 0.0,  # 日线数据通常没有turnover
                    'open_interest': 0.0,  # 可以后续更新
                    'open_price': daily_open,
                    'high_price': daily_high,
                    'low_price': daily_low,
                    'close_price': daily_close
                })

                # 重置计数器
                daily_high = 0
                daily_low = 999999999999999
                daily_volume = 0
                index = 0

            last_data = d

        # 处理最后一天的数据（如果没有在14:59:00结束）
        if index != 0 and (not last_data or last_data[1] != '14:59:00'):
            daily_close = float(last_data[6])
            daily_date = last_data[0] + ' ' + last_data[1]
            daily_datetime = datetime.strptime(daily_date, '%Y%m%d %H:%M:%S')

            daily_data.append({
                'symbol': symbol,
                'exchange': exchange,
                'datetime': daily_datetime.isoformat(' ', 'seconds'),
                'interval': 'd',
                'volume': daily_volume,
                'turnover': 0.0,
                'open_interest': 0.0,
                'open_price': daily_open,
                'high_price': daily_high,
                'low_price': daily_low,
                'close_price': daily_close
            })

        # 批量插入日线数据
        if daily_data:
            cursor.executemany("""
                INSERT INTO dbbardata
                (symbol, exchange, datetime, interval, volume, turnover, open_interest,
                 open_price, high_price, low_price, close_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [(d['symbol'], d['exchange'], d['datetime'], d['interval'],
                   d['volume'], d['turnover'], d['open_interest'],
                   d['open_price'], d['high_price'], d['low_price'], d['close_price'])
                  for d in daily_data])

            conn.commit()
            print(f"  成功插入 {len(daily_data)} 条日线数据")

            # 更新dbbaroverview表
            cursor.execute("""
                SELECT COUNT(*), MIN(datetime), MAX(datetime)
                FROM dbbardata
                WHERE symbol = ? AND exchange = ? AND interval = 'd'
            """, (symbol, exchange))

            count, start_date, end_date = cursor.fetchone()

            # 检查是否已存在概览记录
            cursor.execute("""
                SELECT id FROM dbbaroverview
                WHERE symbol = ? AND exchange = ? AND interval = 'd'
            """, (symbol, exchange))

            existing = cursor.fetchone()

            if existing:
                # 更新现有记录
                cursor.execute("""
                    UPDATE dbbaroverview
                    SET count = ?, start = ?, end = ?
                    WHERE symbol = ? AND exchange = ? AND interval = 'd'
                """, (count, start_date, end_date, symbol, exchange))
            else:
                # 插入新记录
                cursor.execute("""
                    INSERT INTO dbbaroverview (symbol, exchange, interval, count, start, end)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (symbol, exchange, 'd', count, start_date, end_date))

            conn.commit()
            print(f"  更新了dbbaroverview表")

        conn.close()
        return len(daily_data)

    def process_symbol(self, symbol, exchange, intervals=None, force_update=False):
        """
        处理单个合约的指定周期数据

        Args:
            symbol: 合约代码
            exchange: 交易所
            intervals: 要处理的周期列表，默认['1h', '1D']
            force_update: 是否强制更新
        """
        if intervals is None:
            intervals = ['1h', '1D']

        total_processed = 0

        for interval in intervals:
            try:
                if interval.lower() in ['1h', 'h', 'hour', 'hourly']:
                    processed = self.aggregate_hourly_data(symbol, exchange, force_update, incremental=not force_update)
                elif interval.lower() in ['1d', 'd', 'day', 'daily']:
                    processed = self.aggregate_daily_data(symbol, exchange, force_update, incremental=not force_update)
                else:
                    print(f"  不支持的时间周期: {interval}")
                    continue

                total_processed += processed

            except Exception as e:
                print(f"处理 {symbol} ({exchange}) {interval}周期时出错: {e}")
                continue

        return total_processed

    def process_all_symbols(self, intervals=None, force_update=False, symbol_filter=None):
        """
        处理所有合约的指定周期数据

        Args:
            intervals: 要处理的周期列表，默认['1h', '1D']
            force_update: 是否强制更新
            symbol_filter: 合约过滤器函数，返回True则处理
        """
        if intervals is None:
            intervals = ['1h', '1D']

        symbols = self.get_available_symbols()
        print(f"找到 {len(symbols)} 个有1分钟数据的合约")
        print(f"将处理周期: {intervals}")

        total_processed = 0
        success_count = 0

        for symbol, exchange, count in symbols:
            if symbol_filter and not symbol_filter(symbol, exchange):
                continue

            try:
                processed = self.process_symbol(symbol, exchange, intervals, force_update)
                total_processed += processed
                if processed > 0:
                    success_count += 1

                print(f"进度: {success_count}/{len(symbols)} 合约处理完成")

            except Exception as e:
                print(f"处理 {symbol} ({exchange}) 时出错: {e}")
                continue

        print("\n处理完成!")
        interval_names = []
        for interval in intervals:
            if interval.lower() in ['1h', 'h', 'hour', 'hourly']:
                interval_names.append('小时线')
            elif interval.lower() in ['1d', 'd', 'day', 'daily']:
                interval_names.append('日线')
            else:
                interval_names.append(interval)

        print(f"成功处理 {success_count} 个合约")
        print(f"共生成 {total_processed} 条{'+'.join(interval_names)}数据")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='VNpy K线数据预处理器')
    parser.add_argument('--db-path', help='数据库文件路径')
    parser.add_argument('--force-update', action='store_true', help='强制更新已存在的数据')
    parser.add_argument('--symbol', help='指定处理的合约代码')
    parser.add_argument('--exchange', help='指定交易所')
    parser.add_argument('--intervals', nargs='+', default=['1h', '1D'],
                       choices=['1h', '1d', '1D', 'h', 'd', 'hour', 'day', 'hourly', 'daily'],
                       help='要处理的周期，默认同时处理小时线和日线')

    args = parser.parse_args()

    try:
        preprocessor = BarDataPreprocessor(args.db_path)

        # 标准化周期参数
        intervals = []
        for interval in args.intervals:
            if interval.lower() in ['1h', 'h', 'hour', 'hourly']:
                intervals.append('1h')
            elif interval.lower() in ['1d', 'd', 'day', 'daily']:
                intervals.append('1D')
            else:
                intervals.append(interval)

        # 去重
        intervals = list(set(intervals))

        if args.symbol:
            # 处理指定合约
            exchange = args.exchange or preprocessor._get_exchange_from_symbol(args.symbol)
            count = preprocessor.process_symbol(args.symbol, exchange, intervals, args.force_update)

            interval_names = []
            for interval in intervals:
                if interval == '1h':
                    interval_names.append('小时线')
                elif interval == '1D':
                    interval_names.append('日线')
                else:
                    interval_names.append(interval)

            print(f"处理完成，共生成 {count} 条{'+'.join(interval_names)}数据")
        else:
            # 处理所有合约
            symbol_filter = None
            if args.exchange:
                # 只处理指定交易所的合约
                def exchange_filter(symbol, exchange):
                    return exchange == args.exchange
                symbol_filter = exchange_filter

            preprocessor.process_all_symbols(intervals, args.force_update, symbol_filter)

    except Exception as e:
        print(f"程序执行出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
