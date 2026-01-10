#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
回测结果K线图表查看器
基于vnpy_chartwizard/show_chart.py，专门用于查看回测结果
"""

import pandas as pd
from lightweight_charts import Chart, IndicatorManager
import os
import sys


def on_timeframe_selection(chart):
    """时间周期切换回调"""
    global trade_markers, global_symbol, global_start_date, global_end_date

    # 获取新的数据
    new_timeframe = chart.topbar['timeframe'].value
    new_data = chart.loar_bar2pd(global_symbol, new_timeframe, global_start_date, global_end_date, exchange='SHFE')
    if new_data.empty:
        return
    chart.set(new_data, True)

    # 重新加载交易标记（针对新的时间周期）
    if 'global_trades_csv_path' in globals() and global_trades_csv_path:
        # 由于简化了时间处理，交易标记在不同周期下都使用相同的时间
        # 这里暂时不需要重新加载，除非有特殊需求
        pass

    # 更新指标
    if 'indicator_manager' in globals():
        indicator_manager.update_indicators(new_data)


def on_search(chart, searched_string):
    """搜索合约回调"""
    global global_start_date, global_end_date

    try:
        # 从搜索字符串中解析symbol和exchange
        if '.' in searched_string:
            search_symbol = searched_string.split('.')[0]
            search_exchange = searched_string.split('.')[1]
        else:
            search_symbol = searched_string
            search_exchange = 'SHFE'  # 默认交易所

        new_data = chart.loar_bar2pd(search_symbol, chart.topbar['timeframe'].value, global_start_date, global_end_date, exchange=search_exchange)
        if new_data.empty:
            return
        chart.topbar['symbol'].set(searched_string)
        chart.set(new_data)
    except Exception as e:
        print(f"错误：无法加载合约 '{searched_string}' 的数据: {e}")


def load_sig_from_trades_csv(csv_path: str):
    """从trades.csv加载交易信号"""
    try:
        if not os.path.exists(csv_path):
            print(f"警告：交易文件不存在: {csv_path}")
            return None

        # 读取CSV文件
        sig_df = pd.read_csv(csv_path, parse_dates=False)

        if sig_df.empty:
            print("警告：交易文件为空")
            return None

        # 检查必要的列
        required_columns = ['dt', 'direction', 'price', 'volume']
        missing_columns = [col for col in required_columns if col not in sig_df.columns]
        if missing_columns:
            print(f"警告：交易文件缺少必要列: {missing_columns}")
            return None

        # 转换数据格式为图表标记
        markers = []
        for _, row in sig_df.iterrows():
            try:
                # 解析时间
                dt_str = str(row['dt'])
                if len(dt_str) > 19:  # 如果包含微秒，去掉
                    dt_str = dt_str[:19]

                # 简化时间处理：保持交易的原始时间，让图表系统处理匹配
                marker_time = dt_str

                # 确定标记类型和颜色
                direction = str(row['direction']).upper()
                offset = str(row.get('offset', '')).upper()

                # 根据方向和开平仓确定标记样式
                if '买' in direction or 'BUY' in direction or 'LONG' in direction:
                    if '开' in offset or 'OPEN' in offset:
                        marker_type = 'arrow_up'
                        color = '#00ff00'  # 绿色 - 买入开仓
                        position = 'below'
                    else:  # 平仓
                        marker_type = 'circle'
                        color = '#ffffff'  # 白色 - 买入平仓
                        position = 'below'
                elif '卖' in direction or 'SELL' in direction or 'SHORT' in direction:
                    if '开' in offset or 'OPEN' in offset:
                        marker_type = 'arrow_down'
                        color = '#ff0000'  # 红色 - 卖出开仓
                        position = 'above'
                    else:  # 平仓
                        marker_type = 'circle'
                        color = '#ffffff'  # 白色 - 卖出平仓
                        position = 'above'
                else:
                    marker_type = 'circle'
                    color = '#ffff00'  # 黄色 - 其他
                    position = 'below'

                # 创建标记（符合lightweight_charts的marker_list格式）
                marker = {
                    'time': marker_time,
                    'position': position,
                    'shape': marker_type,     # 使用'shape'而不是'type'
                    'color': color,
                    'text': "",  # 只显示符号，不显示文字，避免杂乱
                    'sig': f"{direction}{offset} {row.get('volume', '')}手 @ {row['price']}"
                }
                markers.append(marker)

            except Exception as e:
                print(f"警告：处理交易记录时出错: {e}")
                continue

        print(f"成功加载 {len(markers)} 条交易记录")
        return markers

    except Exception as e:
        print(f"加载交易文件时出错: {e}")
        return None


def run_backtest_chart(symbol: str, start_date: str, end_date: str, trades_csv_path: str):
    """运行回测图表查看器"""
    global indicator_manager, trade_markers

    print(f"启动回测图表查看器 - 合约: {symbol}, 时间: {start_date} 至 {end_date}")
    print(f"交易文件: {trades_csv_path}")

    # 将参数设置为全局变量，供回调函数使用
    global global_symbol, global_start_date, global_end_date, global_trades_csv_path
    global_symbol = symbol
    global_start_date = start_date
    global_end_date = end_date
    global_trades_csv_path = trades_csv_path

    # 创建图表
    chart = Chart(toolbox=True, inner_width=1.0, inner_height=1.0, title=f"回测结果 - {symbol}")

    # 设置图表样式
    chart.legend(True)
    chart.events.search += on_search
    chart.layout(
        background_color='#000000',
        text_color='#FFFFFF',
        font_size=16,
        font_family='Helvetica'
    )
    chart.candle_style(
        up_color='#000000',
        down_color='#a9f9fb',
        border_up_color='#ed4807',
        border_down_color='#a9f9fb',
        wick_up_color='#ed4807',
        wick_down_color='#a9f9fb'
    )
    chart.volume_config(up_color='#ed4807', down_color='#a9f9fb')

    # 顶部输入框和切换器
    chart.topbar.textbox('symbol', f'{symbol} (回测结果)')

    # 加载K线数据
    try:
        df = chart.loar_bar2pd(symbol, '1D', start_date, end_date, exchange='SHFE')
        if df.empty:
            print("警告：未找到K线数据")
            df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])
    except Exception as e:
        print(f"加载K线数据时出错: {e}")
        df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

    # 加载交易信号
    trade_markers = load_sig_from_trades_csv(trades_csv_path)

    # 初始化指标管理器
    indicator_manager = IndicatorManager(chart)

    # 添加一些常用指标
    try:
        if not df.empty:
            # 添加移动平均线
            #indicator_manager.add_ma(df, 5)   # 5日线
            indicator_manager.add_ma(df, 21)  # 21日线
            #indicator_manager.add_ma(df, 60)  # 60日线
    except Exception as e:
        print(f"添加指标时出错: {e}")
        import traceback
        traceback.print_exc()

    # 设置时间周期切换器
    chart.topbar.switcher(
        'timeframe',
        ('1m', '5m', '15m', '30m', '1h', '1D', '1W'),
        default='1D',
        func=on_timeframe_selection
    )

    # 设置主图数据
    if not df.empty:
        chart.set(df)
        print(f"已设置K线数据，共 {len(df)} 条记录")
    else:
        print("警告：没有K线数据可显示")

    # 添加交易标记
    if trade_markers:
        try:
            chart.marker_list(trade_markers)
            print(f"已添加 {len(trade_markers)} 个交易标记到图表")
        except Exception as e:
            print(f"添加交易标记时出错: {e}")

    # 显示图表（阻塞模式，等待用户关闭）
    try:
        print("图表查看器已启动，请查看弹出的图表窗口")
        chart.show(block=True)
        print("图表查看器已关闭")

    except KeyboardInterrupt:
        print("图表查看器被用户中断")
    except Exception as e:
        print(f"显示图表时出错: {e}")
        import traceback
        traceback.print_exc()


def main():
    """主函数 - 从命令行参数运行"""
    if len(sys.argv) < 5:
        print("用法: python backtest_chart_viewer.py <symbol> <start_date> <end_date> <trades_csv_path>")
        print("示例: python backtest_chart_viewer.py rb8888 2022-01-01 2022-12-31 C:/path/to/trades.csv")
        sys.exit(1)

    symbol = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    trades_csv_path = sys.argv[4]

    run_backtest_chart(symbol, start_date, end_date, trades_csv_path)


if __name__ == '__main__':
    main()
