#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试高效数据加载逻辑
"""

from lightweight_charts import Chart
import time

def test_efficient_loading():
    """测试不同周期的高效数据加载"""
    print("测试高效数据加载逻辑...")

    # 创建Chart实例（不显示界面）
    chart = Chart(toolbox=False, inner_width=1.0, inner_height=1.0)

    # 测试不同周期的数据加载
    test_periods = [
        ('1m', '1分钟数据'),
        ('5m', '5分钟数据（从1分钟聚合）'),
        ('15m', '15分钟数据（从1分钟聚合）'),
        ('30m', '30分钟数据（从1分钟聚合）'),
        ('1h', '1小时数据（预处理数据）'),
        ('2h', '2小时数据（从1小时聚合）'),
        ('1D', '日线数据（预处理数据）'),
        ('1W', '周线数据（从日线聚合）'),
    ]

    results = {}

    for period, description in test_periods:
        print(f"\n测试 {period} - {description}")
        start_time = time.time()

        try:
            df = chart.loar_bar2pd('rb8888', period, '2022-01-04', '2022-01-06')

            if not df.empty:
                load_time = time.time() - start_time
                results[period] = {
                    'success': True,
                    'count': len(df),
                    'time': load_time,
                    'description': description
                }
                print(".3f")
            else:
                results[period] = {
                    'success': False,
                    'count': 0,
                    'time': time.time() - start_time,
                    'description': description
                }
                print(f"[失败] 未找到数据")

        except Exception as e:
            load_time = time.time() - start_time
            results[period] = {
                'success': False,
                'count': 0,
                'time': load_time,
                'description': description,
                'error': str(e)
            }
            print(f"[错误] {e}")

    # 输出汇总结果
    print("\n" + "="*60)
    print("数据加载效率汇总:")
    print("="*60)

    for period, result in results.items():
        status = "[成功]" if result['success'] else "[失败]"
        count = result['count']
        load_time = result['time']
        desc = result['description']
        print("<6")

    # 分析性能
    print("\n性能分析:")
    print("-" * 30)

    successful_loads = [r for r in results.values() if r['success']]
    if successful_loads:
        avg_time = sum(r['time'] for r in successful_loads) / len(successful_loads)
        min_time = min(r['time'] for r in successful_loads)
        max_time = max(r['time'] for r in successful_loads)

        print(".3f"        print(".3f"        print(".3f"
    # 验证数据聚合逻辑
    print("\n数据聚合验证:")
    print("-" * 30)

    if '1D' in results and '1h' in results and results['1D']['success'] and results['1h']['success']:
        daily_count = results['1D']['count']
        hourly_count = results['1h']['count']
        print(f"日线数据: {daily_count} 条")
        print(f"小时线数据: {hourly_count} 条")

        # 检查日线数量是否合理（大约是小时线的1/24）
        expected_daily = hourly_count / 24
        if abs(daily_count - expected_daily) / expected_daily < 0.5:  # 允许50%的误差
            print("[验证通过] 日线数量与小时线数量关系合理")
        else:
            print(f"[警告] 日线数量({daily_count})与预期({expected_daily:.1f})不符")

    print("\n测试完成！")

if __name__ == '__main__':
    test_efficient_loading()
