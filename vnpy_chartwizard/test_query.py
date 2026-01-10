#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试数据库查询
"""

import sqlite3
import os

def test_query():
    """测试数据库查询"""
    db_path = os.path.expanduser('~/.vntrader/database.db')
    print(f"数据库路径: {db_path}")

    if not os.path.exists(db_path):
        print("数据库文件不存在")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 测试a8888日线数据
        cursor.execute("SELECT COUNT(*) FROM dbbardata WHERE symbol='a8888' AND exchange='DCE' AND interval='d'")
        count = cursor.fetchone()[0]
        print(f"a8888 DCE日线数据: {count} 条")

        if count > 0:
            cursor.execute("SELECT datetime, open_price, close_price FROM dbbardata WHERE symbol='a8888' AND exchange='DCE' AND interval='d' ORDER BY datetime LIMIT 5")
            rows = cursor.fetchall()
            print("前5条数据:")
            for row in rows:
                print(f"  {row[0]}: {row[1]} -> {row[2]}")

        # 测试rb8888数据
        cursor.execute("SELECT COUNT(*) FROM dbbardata WHERE symbol LIKE '%rb%'")
        rb_count = cursor.fetchone()[0]
        print(f"\n包含rb的合约数据: {rb_count} 条")

        # 检查rb8888的具体情况
        cursor.execute("SELECT DISTINCT interval, COUNT(*) FROM dbbardata WHERE symbol='rb8888' GROUP BY interval")
        intervals = cursor.fetchall()
        print("rb8888数据统计:")
        for interval, count in intervals:
            print(f"  {interval}: {count} 条")

        if intervals:
            cursor.execute("SELECT MIN(datetime), MAX(datetime) FROM dbbardata WHERE symbol='rb8888'")
            date_range = cursor.fetchone()
            print(f"时间范围: {date_range[0]} 到 {date_range[1]}")

        # 检查有哪些rb开头的合约
        cursor.execute("SELECT DISTINCT symbol, exchange FROM dbbardata WHERE symbol LIKE 'rb%' LIMIT 10")
        rb_contracts = cursor.fetchall()
        print("\nrb开头合约列表:")
        for symbol, exchange in rb_contracts:
            cursor.execute(f"SELECT COUNT(*) FROM dbbardata WHERE symbol='{symbol}' AND exchange='{exchange}'")
            total_count = cursor.fetchone()[0]
            print(f"  {symbol} ({exchange}): {total_count} 条记录")

        conn.close()

    except Exception as e:
        print(f"查询数据库时出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_query()
