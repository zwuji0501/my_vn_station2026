#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
VNpy数据读取器使用示例
演示如何在Python代码中使用data_reader模块
"""

from data_reader import load_vnpy_data
import pandas as pd

def example_basic_usage():
    """基本使用示例"""
    print("=== 基本使用示例 ===")

    # 加载日线数据
    df = load_vnpy_data('rb8888', '1D', '2022-01-01', '2022-01-10')

    if not df.empty:
        print(f"加载了 {len(df)} 条日线数据")
        print(df.head())
    else:
        print("未找到数据")

def example_minute_data():
    """分钟数据加载示例"""
    print("\n=== 分钟数据加载示例 ===")

    # 加载5分钟数据
    df = load_vnpy_data('rb8888', '5m', '2022-01-01', '2022-01-02')

    if not df.empty:
        print(f"加载了 {len(df)} 条5分钟数据")
        print(df.head())
    else:
        print("未找到数据")

def example_data_analysis():
    """数据分析示例"""
    print("\n=== 数据分析示例 ===")

    # 加载一个月的数据
    df = load_vnpy_data('rb8888', '1D', '2022-01-01', '2022-02-01')

    if not df.empty:
        # 基本统计
        print("=== 基本统计 ===")
        print(f"数据条数: {len(df)}")
        print(f"平均收盘价: {df['close'].mean():.2f}")
        print(f"最高价: {df['high'].max():.2f}")
        print(f"最低价: {df['low'].min():.2f}")
        print(f"总成交量: {df['volume'].sum():.0f}")

        # 计算日收益率
        df['returns'] = df['close'].pct_change()
        print(f"\n平均日收益率: {df['returns'].mean():.4f}")
        print(f"收益率标准差: {df['returns'].std():.4f}")

        # 计算移动平均线
        df['MA5'] = df['close'].rolling(5).mean()
        df['MA20'] = df['close'].rolling(20).mean()

        print("\n=== 最近5天数据（带移动平均线）===")
        recent_data = df.tail(5)[['date', 'close', 'MA5', 'MA20']].copy()
        print(recent_data.to_string(index=False))
    else:
        print("未找到数据")

def example_different_symbols():
    """不同合约数据加载示例"""
    print("\n=== 不同合约数据加载示例 ===")

    symbols = ['rb8888', 'hc8888', 'i8888']  # 螺纹钢、热轧卷板、铁矿石

    for symbol in symbols:
        df = load_vnpy_data(symbol, '1D', '2022-01-01', '2022-01-05')
        if not df.empty:
            print(f"{symbol}: 加载了 {len(df)} 条数据")
            print(f"  价格范围: {df['low'].min():.2f} - {df['high'].max():.2f}")
        else:
            print(f"{symbol}: 未找到数据")

if __name__ == '__main__':
    # 运行所有示例
    example_basic_usage()
    example_minute_data()
    example_data_analysis()
    example_different_symbols()

    print("\n=== 使用说明 ===")
    print("1. 直接运行此脚本查看各种使用示例")
    print("2. 在你的代码中导入 load_vnpy_data 函数")
    print("3. 命令行使用: python data_reader.py [symbol] [timeframe] [start_date] [end_date]")
