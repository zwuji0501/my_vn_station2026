#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查vnpy数据库内容
"""

import sqlite3
import os

def check_database():
    """检查数据库内容"""
    db_path = os.path.expanduser('~/.vntrader/database.db')
    print(f"数据库路径: {db_path}")

    if not os.path.exists(db_path):
        print("数据库文件不存在")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 获取所有表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print(f"数据库中的表: {[t[0] for t in tables]}")

        # 检查每个表的内容
        for table_name, in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"{table_name}: {count} 条记录")

            if count > 0:
                # 显示表结构
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                print(f"  列: {[col[1] for col in columns]}")

                if table_name == 'dbbaroverview':
                    # 显示可用的合约
                    cursor.execute("SELECT DISTINCT symbol, exchange, interval FROM dbbaroverview")
                    contracts = cursor.fetchall()
                    print(f"  可用合约: {len(contracts)} 个")
                    for symbol, exchange, interval in contracts[:10]:  # 只显示前10个
                        print(f"    {symbol} ({exchange}) - {interval}")
                    if len(contracts) > 10:
                        print(f"    ... 还有 {len(contracts) - 10} 个合约")

                # 显示前几条记录
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                rows = cursor.fetchall()
                print(f"  示例数据: {rows[0] if rows else '无'}")

        conn.close()

    except Exception as e:
        print(f"检查数据库时出错: {e}")

if __name__ == '__main__':
    check_database()
