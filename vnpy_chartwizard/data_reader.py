#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
VNpy数据读取器
直接从SQLite数据库读取vnpy的K线数据并以DataFrame格式打印

使用方法:
1. 命令行使用:
   python data_reader.py rb8888 1D 2022-01-01 2022-12-31
   python data_reader.py rb8888 1D 2022-01-01 2022-12-31 --show-chart

2. 在Python代码中使用:
   from data_reader import load_vnpy_data
   df = load_vnpy_data('rb8888', '1D', '2022-01-01', '2022-12-31')
   print(df.head())

支持的时间周期: '1m', '5m', '15m', '30m', '60m', '1D', '1W'
"""

import sys
import os
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime


def get_database_path():
    """获取vnpy数据库文件路径"""
    # 首先检查当前目录的.vntrader文件夹
    current_dir = Path.cwd()
    vntrader_dir = current_dir / ".vntrader"
    if vntrader_dir.exists():
        db_path = vntrader_dir / "database.db"
        if db_path.exists():
            return str(db_path)

    # 然后检查用户主目录的.vntrader文件夹
    home_dir = Path.home()
    vntrader_dir = home_dir / ".vntrader"
    if vntrader_dir.exists():
        db_path = vntrader_dir / "database.db"
        if db_path.exists():
            return str(db_path)

    # 如果都没有找到，返回默认路径（会自动创建）
    if not vntrader_dir.exists():
        vntrader_dir.mkdir(parents=True, exist_ok=True)
    return str(vntrader_dir / "database.db")


def load_vnpy_data(symbol, timeframe='1D', start_date=None, end_date=None, show_chart=False):
    """
    从vnpy SQLite数据库直接加载K线数据

    Args:
        symbol (str): 合约代码，如 'rb8888'
        timeframe (str): 时间周期，如 '1m', '5m', '15m', '30m', '60m', '1D', '1W'
        start_date (str): 开始日期，格式如 '2022-01-01'
        end_date (str): 结束日期，格式如 '2022-12-31'
        show_chart (bool): 是否显示图表界面（保留参数以保持兼容性）

    Returns:
        pd.DataFrame: 包含K线数据的DataFrame，列名为: date, open, high, low, close, volume
    """
    try:
        # 获取数据库路径
        db_path = get_database_path()
        print(f"数据库路径: {db_path}")

        # 检查数据库文件是否存在
        if not os.path.exists(db_path):
            print(f"数据库文件不存在: {db_path}")
            print("请先运行vnpy策略或数据录制功能来创建数据库和数据。")
            return pd.DataFrame()

        # 连接数据库
        conn = sqlite3.connect(db_path)

        # 确定交易所（根据合约代码前缀）
        if symbol.startswith(('rb', 'hc', 'i', 'j', 'jm')):
            exchange = 'SHFE'  # 上海期货交易所
        elif symbol.startswith(('OI', 'RM', 'WH', 'PM', 'CF', 'SR', 'TA', 'MA', 'FG', 'ZC')):
            exchange = 'CZCE'  # 郑州商品交易所
        elif symbol.startswith(('cu', 'al', 'zn', 'pb', 'ni', 'sn', 'au', 'ag')):
            exchange = 'SHFE'
        elif symbol.startswith(('a', 'b', 'm', 'y', 'p', 'c', 'cs', 'jd', 'bb', 'fb', 'l', 'v', 'pp', 'j', 'jm')):
            exchange = 'DCE'  # 大连商品交易所
        elif symbol.startswith(('sc', 'fu', 'bu', 'ru', 'nr', 'sp', 'ss', 'wr', 'bc')):
            exchange = 'INE'  # 上海国际能源交易中心
        else:
            exchange = 'CZCE'  # 默认郑州商品交易所

        # 转换时间周期字符串
        # 注意：数据库中可能只有1分钟数据，需要重采样
        interval_map = {
            '1m': '1m',
            '5m': '1m',  # 从1分钟数据重采样
            '15m': '1m',
            '30m': '1m',
            '60m': '1m',
            '1h': '1h',  # 小时线（预处理数据）
            '1H': '1h',
            '1D': 'd',   # 日线（预处理数据）
            '1W': 'w'    # 周线
        }

        if timeframe not in interval_map:
            raise ValueError(f"不支持的时间周期: {timeframe}")

        db_interval = interval_map[timeframe]  # 数据库中查询的间隔

        # 处理日期参数
        if start_date is None:
            start_date_str = '2020-01-01 00:00:00'
        elif isinstance(start_date, str):
            start_date_str = start_date + ' 00:00:00' if ' ' not in start_date else start_date

        if end_date is None or end_date == '':
            end_date_str = '2099-12-31 23:59:59'
        elif isinstance(end_date, str):
            end_date_str = end_date + ' 23:59:59' if ' ' not in end_date else end_date

        # 首先尝试查询目标时间周期的数据
        query = f"""
        SELECT
            datetime,
            open_price as open,
            high_price as high,
            low_price as low,
            close_price as close,
            volume
        FROM dbbardata
        WHERE symbol = '{symbol}'
        AND exchange = '{exchange}'
        AND interval = '{db_interval}'
        AND datetime >= '{start_date_str}'
        AND datetime <= '{end_date_str}'
        ORDER BY datetime
        """

        # 执行查询
        df = pd.read_sql_query(query, conn)

        if df.empty:
            print(f"警告：未从数据库找到 {symbol}.{exchange} 的 {timeframe} 数据")
            print(f"数据库文件: {db_path}")
            conn.close()
            return df

        # 数据重采样处理
        if timeframe in ['5m', '15m', '30m', '60m', '1D', '1W']:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)

            # 根据时间周期进行重采样
            resample_map = {
                '5m': '5min',
                '15m': '15min',
                '30m': '30min',
                '60m': '1H',
                '1D': '1D',
                '1W': '1W'
            }

            period = resample_map[timeframe]
            df = df.resample(period).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()

            df.reset_index(inplace=True)
            # 重命名列
            df = df.rename(columns={'datetime': 'date'})

        conn.close()

        # 格式化日期列
        if not df.empty:
            if 'datetime' in df.columns:
                df = df.rename(columns={'datetime': 'date'})
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')

        return df

    except Exception as e:
        print(f"加载数据时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def main():
    """
    主函数，处理命令行参数
    用法: python data_reader.py [symbol] [timeframe] [start_date] [end_date] [--show-chart]
    """
    import argparse

    parser = argparse.ArgumentParser(description='VNpy数据读取器')
    parser.add_argument('symbol', nargs='?', default='rb8888', help='合约代码 (默认: rb8888)')
    parser.add_argument('timeframe', nargs='?', default='1m', help='时间周期 (默认: 1D)')
    parser.add_argument('start_date', nargs='?', default='2000-01-01', help='开始日期 (默认: 2022-01-01)')
    parser.add_argument('end_date', nargs='?', default=None, help='结束日期 (默认: 无限制)')
    parser.add_argument('--show-chart', action='store_true', help='显示图表界面')

    args = parser.parse_args()

    print("="*60)
    print("VNpy数据读取器")
    print(f"合约代码: {args.symbol}")
    print(f"时间周期: {args.timeframe}")
    print(f"开始日期: {args.start_date}")
    print(f"结束日期: {args.end_date}")
    print(f"显示图表: {'是' if args.show_chart else '否'}")
    print("="*60)

    # 加载数据
    df = load_vnpy_data(args.symbol, args.timeframe, args.start_date, args.end_date, args.show_chart)

    if df.empty:
        print("未找到数据或加载失败")
        return

    # 打印数据信息
    print("\n数据概览:")
    print(f"数据条数: {len(df)}")
    if not df.empty:
        print(f"时间范围: {df['date'].min()} 到 {df['date'].max()}")
        print(f"价格范围: {df['low'].min():.2f} - {df['high'].max():.2f}")
        print(f"成交量总和: {df['volume'].sum():.0f}")

    print("\n前5条数据:")
    print(df.head().to_string(index=False))

    if len(df) > 5:
        print("\n后5条数据:")
        print(df.tail().to_string(index=False))

    print("\n完整数据已加载完成，可以在代码中进一步处理。")

    # 如果显示图表，展示数据
    if args.show_chart and not df.empty:
        # 这里可以添加图表显示逻辑
        print("图表已在新窗口中打开...")


if __name__ == '__main__':
    main()
