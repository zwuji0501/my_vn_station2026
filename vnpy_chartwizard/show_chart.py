import pandas as pd
from lightweight_charts import Chart, IndicatorManager
import os 
import sys
def on_timeframe_selection(chart):
    # 获取新的数据
    new_data = chart.loar_bar2pd(symbol, chart.topbar['timeframe'].value)
    if new_data.empty:
        return
    chart.set(new_data, True)
    
    indicator_manager.update_indicators(new_data)

def on_search( chart, searched_string):
    try:
        new_data = chart.loar_bar2pd(searched_string, chart.topbar['timeframe'].value)
        if new_data.empty:
            return
        chart.topbar['symbol'].set(searched_string)
        chart.set(new_data)
    except FileNotFoundError:
        print(f"错误：文件 '{searched_string}' 不存在，请检查文件路径是否正确。")


def on_horizontal_line_move( chart, line):
    print(f'Horizontal line moved to: {line.price}')


def handle_chart_click( clicked_kline):
    """处理图表点击事件"""
    pass
    '''
    try:
        from multiprocessing import Process
        from other_chart import run_chart
        Process(target=run_chart, args=(symbol,clicked_kline[0],clicked_kline[1],start_date,end_date,m)).start()
    except Exception as e:
        print(f"Error creating new chart: {str(e)}")
        import traceback
        traceback.print_exc()
    '''




if __name__ == '__main__':
    
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
        start_date = sys.argv[2]
        end_date = sys.argv[3]
    
    #############
    symbol = 'rb8888'
    start_date = '2022-01-01'  # 修改为2021年，因为数据库中rb8888数据从2021年开始
    end_date = ''
    path = 'C:\\vnpy-1.9.2-LTS\\vnpy-1.9.2-LTS\\examples\\CtaBacktesting\\trade.csv'
    if os.path.exists(path):
        sig_df = pd.read_csv(path,parse_dates=False)
        # 将dt列转换为datetime类型并按时间排序
        #sig_df['dt'] = pd.to_datetime(sig_df['dt'])
        trade_symbol = sig_df['symbol'].iloc[0]
        strategy_msg = sig_df['rawData'].iloc[0]


    chart = Chart(toolbox=False, inner_width=1.0, inner_height=1)
    # 初始化图表设置
    chart.legend(True)
    chart.events.search += on_search
    chart.layout(background_color='#000000', text_color='#FFFFFF', font_size=16, font_family='Helvetica')
    chart.candle_style(
        up_color='#000000', down_color='#a9f9fb', border_up_color='#ed4807', 
        border_down_color='#a9f9fb', wick_up_color='#ed4807', wick_down_color='#a9f9fb'
    )
    chart.volume_config(up_color='#ed4807', down_color='#a9f9fb')
    
    # 顶部输入框和切换器
    chart.topbar.textbox('symbol', f'{symbol} {strategy_msg}')

    df = chart.loar_bar2pd(symbol, '1D', start_date, end_date)
    
    #m = chart.load_sig(symbol)
    m = None
        
    
    
    indicator_manager = IndicatorManager(chart)
    #indicator_manager.add_macd(df)
    #indicator_manager.add_sma(30)
    #indicator_manager.add_ema(df,20)
    #indicator_manager.add_boll(df,5)



    chart.topbar.switcher('timeframe', ('1m', '5m', '30m', '60m', '1D', '1W'), default='1D', func=on_timeframe_selection)

    
    
    chart.add_click_callback(handle_chart_click)
    # 添加一些指标

    if m:
        chart.marker_list(m[1])

    # 加载主图数据
    chart.set(df)
    chart.show(block=True)


