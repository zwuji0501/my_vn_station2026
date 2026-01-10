#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试图表系统的小时线数据加载
"""

from lightweight_charts import Chart

def test_hourly_chart():
    """测试小时线数据加载"""
    print("测试Chart的小时线数据加载...")

    # 创建Chart实例（不显示界面）
    chart = Chart(toolbox=False, inner_width=1.0, inner_height=1.0)

    try:
        # 测试小时线数据
        print("加载rb8888小时线数据...")
        df = chart.loar_bar2pd('rb8888', '1h', '2022-01-04', '2022-01-04')

        if not df.empty:
            print(f"[成功] 加载了 {len(df)} 条小时线数据")
            print(f"时间范围: {df['date'].min()} 到 {df['date'].max()}")
            print(f"价格范围: {df['low'].min():.2f} - {df['high'].max():.2f}")
            print("前3条数据:")
            print(df.head(3).to_string(index=False))
        else:
            print("[失败] 未找到小时线数据")

    except Exception as e:
        print(f"[错误] 测试失败: {e}")
        import traceback
        traceback.print_exc()

    print("\n测试完成！")

if __name__ == '__main__':
    test_hourly_chart()
