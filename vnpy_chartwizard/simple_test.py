#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简单测试高效数据加载
"""

from lightweight_charts import Chart
import time

def test_loading():
    """测试数据加载"""
    print("测试高效数据加载...")

    chart = Chart(toolbox=False, inner_width=1.0, inner_height=1.0)

    # 测试几个关键周期
    test_cases = [
        ('1h', '小时线'),
        ('1D', '日线'),
        ('1W', '周线'),
    ]

    for period, name in test_cases:
        print(f"\n测试 {period} - {name}")
        start_time = time.time()

        try:
            df = chart.loar_bar2pd('rb8888', period, '2022-01-04', '2022-01-10')
            load_time = time.time() - start_time

            if not df.empty:
                print(".3f"                print(f"数据范围: {df['date'].min()} 到 {df['date'].max()}")
            else:
                print("[失败] 未找到数据")

        except Exception as e:
            print(f"[错误] {e}")

    print("\n测试完成!")

if __name__ == '__main__':
    test_loading()
