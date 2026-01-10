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
    
    try:
        # 读取新脚本模板
        script_path = os.path.join(os.path.dirname(__file__), 'other_chart.py')
        
        # 更新脚本内容
        new_content = f'''from callbacks import ShowChat

        # 参数设置
        SYMBOL = "{symbol}"
        TIMEFRAME = "{chart.topbar['timeframe'].value}"

        if __name__ == '__main__':
            s = ShowChat(SYMBOL)
            s.chart.topbar['timeframe'].set(TIMEFRAME)
            s.show()
        '''
        # 写入更新后的内容
        #with open(script_path, 'w', encoding='utf-8') as f:
        #    f.write(new_content)
        
        # 启动新进程
        
        os.system(script_path)
            
    except Exception as e:
        print(f"Error creating new chart: {str(e)}")
        import traceback
        traceback.print_exc()


def run_chart(symbol, previous_close, current_close, start_date, end_date, m):


    chart = Chart(toolbox=False, inner_width=1.0, inner_height=1,allow_new_instance=False)
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
    chart.topbar.textbox('symbol', symbol)
    
     

    df = chart.loar_bar2pd(symbol, '1m', start_date, end_date)
    
    if chart.load_sig(symbol,greate_pnl=False): 
        marker_list = chart.load_sig(symbol,greate_pnl=False)[1]
    else:
        marker_list = []

    previous_close = previous_close.replace('15:00:00','14:59:00')

    # 取出df中 df[df['date'] < previous_close]的最后一行数据
    #previous_close_data = df[df['date'] < previous_close].iloc[-1]
    #print(previous_close_data)


    # 筛选当日数据
    df = df[df['date'] >= previous_close]
    df = df[df['date'] <= current_close]


    #print('#########################')
    #print(df)


    indicator_manager = IndicatorManager(chart)
    #indicator_manager.add_macd(df)
    #indicator_manager.add_sma(30)
    #indicator_manager.add_ema(df,20)
    #indicator_manager.add_boll(df,5)



    chart.topbar.switcher('timeframe', ('1m', '5m', '30m', '60m', '1D', '1W'), default='1m', func=on_timeframe_selection)

    
    
    #chart.add_click_callback(handle_chart_click)

    if m:
        import datetime
        # 添加信号
        #m[1]在previous_close和current_close之间,time是Timestamp('2024-11-15 22:41:00')
        # 将current_close、previous_close转换为pd.Timestamp
        previous_close = pd.Timestamp(previous_close)
        current_close = pd.Timestamp(current_close)
        sig_list = m[1]
        sig_list = [sig for sig in sig_list if previous_close <= sig['time'] and sig['time'] <= current_close]
        chart.marker_list(sig_list)

    # 加载主图数据
    chart.set(df)
    chart.show(block=True)

if __name__ == '__main__':
    #############
    symbol = sys.argv[1]
    previous_close = sys.argv[2]    
    current_close = sys.argv[3]
    start_date = sys.argv[4]
    end_date = sys.argv[5]
    run_chart(symbol, previous_close, current_close, start_date, end_date, None)

    