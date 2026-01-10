#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试点击事件处理
"""

from lightweight_charts import Chart
import time

def test_click_event():
    """测试点击事件处理"""
    print("测试点击事件处理...")

    # 创建Chart实例
    chart = Chart(toolbox=True, inner_width=1.0, inner_height=1.0)

    # 添加一个测试的点击回调
    def test_callback(data):
        print(f"点击回调被触发: {data}")

    chart.add_click_callback(test_callback)

    # 加载一些数据
    df = chart.loar_bar2pd('rb8888', '1D', '2022-01-04', '2022-01-06')
    if not df.empty:
        print(f"成功加载 {len(df)} 条数据")

        # 设置数据到图表
        chart.set(df)

        # 检查df_temp是否正确设置
        if hasattr(chart, 'df_temp') and chart.df_temp is not None:
            print(f"df_temp已设置，包含 {len(chart.df_temp)} 条记录")
            if 'date' in chart.df_temp.columns:
                print(f"df_temp包含'date'列，数据类型: {chart.df_temp['date'].dtype}")
                print(f"日期范围: {chart.df_temp['date'].min()} 到 {chart.df_temp['date'].max()}")
            else:
                print("警告: df_temp不包含'date'列")
                print(f"可用列: {list(chart.df_temp.columns)}")
        else:
            print("错误: df_temp未设置")

        print("图表设置完成，可以进行点击测试")
        print("请在图表上点击K线来测试点击事件处理")
        print("按Ctrl+C退出...")

        try:
            chart.show(block=True)
        except KeyboardInterrupt:
            print("测试结束")
    else:
        print("加载数据失败")

if __name__ == '__main__':
    test_click_event()
