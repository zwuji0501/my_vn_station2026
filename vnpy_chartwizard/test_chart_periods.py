#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试chart的周期切换功能
"""

from lightweight_charts import Chart

def test_chart_periods():
    """测试不同周期的数据加载"""
    print("测试Chart的周期切换功能...")

    # 创建Chart实例（不显示界面）
    chart = Chart(toolbox=False, inner_width=1.0, inner_height=1.0)

    # 测试不同周期
    periods = ['1m', '5m', '15m', '30m', '1D', '1W']

    for period in periods:
        print(f"\n测试周期: {period}")
        try:
            df = chart.loar_bar2pd('rb8888', period, '2022-01-04', '2022-01-06')
            if not df.empty:
                print(f"  数据条数: {len(df)}")
                print(f"  时间范围: {df['date'].min()} 到 {df['date'].max()}")
                print(f"  价格范围: {df['low'].min():.2f} - {df['high'].max():.2f}")
            else:
                print("  未找到数据")
        except Exception as e:
            print(f"  错误: {e}")

    print("\n测试完成！")

if __name__ == '__main__':
    test_chart_periods()
