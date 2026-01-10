## encoding: UTF-8


import json
from struct import *
import pandas as pd
import os
# import sys
import time
import datetime
import math
import json
import shutil
import csv


def _log_message(msg: str, log_callback=None) -> None:
    """统一的日志输出函数"""
    if log_callback:
        log_callback(msg)
    else:
        print(msg) 
# stock_list = []
# linename=['code','date','open','high','low','close','amout','vol']
# df_all_stock = pd.DataFrame(stock_list, columns=linename)
def miniute2csv_data(dirname, fname, targetDir, log_callback=None):
    ofile=open(dirname + fname, 'rb')
    buf=ofile.read()
    ofile.close()
 

    # 更改文件名
    contract_path = 'C:\\vnpy-1.9.2-LTS\\vnpy-1.9.2-LTS\\examples\\DataRecording\\contract_attribute.json'

    # 初始化合约字典，如果文件不存在或读取失败，使用空字典
    contract_dic = {}
    try:
        with open(contract_path, 'r', encoding='utf-8') as f:
            contract_dic = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        _log_message(f"警告：无法读取合约属性文件 {contract_path}: {e}", log_callback)
        _log_message("将使用原始文件名，不进行合约名称标准化", log_callback)

    short_fname = fname.replace('.lc1','').split('#')[-1].replace('L9','8888')

    # 提取交易合约代号，如rb
    short_fname_code = ''.join([char for char in short_fname if char.isalpha()])

    # 只有在成功加载合约字典且找到对应合约时才进行处理
    if contract_dic and short_fname_code:
        # 将标准是小写的商品代码改回小写
        if short_fname_code not in contract_dic and short_fname_code.lower() in contract_dic:
            short_fname = short_fname.replace(short_fname_code, short_fname_code.lower())
            short_fname_code = short_fname_code.lower()  # 更新代号为小写版本

        # 将郑商所的年月代号改成标准的3位，如RM2405改为RM405,指数保留4位：RM8888
        if short_fname_code in contract_dic:
            try:
                if contract_dic[short_fname_code]["exchange"] == "CZCE" and short_fname[-4:] != '8888':
                    short_fname = short_fname_code + short_fname.split(short_fname_code)[-1][1:]
                    _log_message(f"CZCE：{short_fname}", log_callback)
            except KeyError:
                _log_message(f"警告：合约 {short_fname_code} 的交易所信息不完整，使用原始名称", log_callback)
    else:
        _log_message(f"信息：合约 {short_fname_code} 不在合约属性文件中，使用原始文件名", log_callback)

    _log_message(f"正在转化 {short_fname}", log_callback)


    if os.path.exists(targetDir+ short_fname+'_1min_1.csv'):
        os.remove(targetDir+ short_fname+'_1min_1.csv')

    ifile=open(targetDir+ short_fname+'_1min_1.csv','w')
    num=len(buf)
    no=num/32
    b=0
    e=32
    line=''
    linename=str('date')+','+str('miniute')+','+str('open')+','+str('high')+','+str('low')+','+str('close')+','+str('volume')+','+str('open_interest')+'\n'
    #ifile.write(linename)
 
    t = datetime.datetime.strptime("2012-11-11 00:00:00", "%Y-%m-%d %H:%M:%S")
 
    last_time = '20200101','11:00:00'
    cover_codiction = 0

    first_date_filter = 0
    for i in range(int(no)):
        #a=unpack('IIIIIfII',buf[b:e])
        a = unpack('HHfffffii',buf[b:e])
        r = 894513/1.2534796932185851e-39

        amout = a[6]*r
        #print(amout)
        year=math.floor(a[0]/2048)+2004
        month=math.floor((a[0] % 2048)/100)
        day=(a[0] % 2048) % 100
        hm = (t + datetime.timedelta(minutes=a[1]-1)).strftime("%H:%M:%S")
        

        
        
        #if last_time[1] == '14:59:00' and (hm == '21:00:00' or hm == '21:01:00' or hm == '21:02:00'):
        # 防止没有14:58或21：00或没有夜盘
        if last_time[1][:4] == '14:5' and (hm[:2] != '14'):
            first_date_filter = 1


            old_date = last_time[0]

            cover_codiction = 1

            #print(hm)
        if cover_codiction == 1:
            #print(hm,time_now)
            time_now = datetime.datetime.strptime(hm,'%H:%M:%S')
            new_date = datetime.datetime.strptime(old_date,'%Y%m%d') + datetime.timedelta(days=1)
            new_date = datetime.datetime.strftime(new_date,'%Y%m%d')

            if time_now >= datetime.datetime.strptime('21:00:00','%H:%M:%S'):
                line = old_date + ','+hm+','+'{:.2f}'.format(a[2])+','+'{:.2f}'.format(a[3])+','+'{:.2f}'.format(a[4])+','+'{:.2f}'.format(a[5])+','+'{:.1f}'.format(a[7])+','+str(amout)+'\n'

            elif time_now < datetime.datetime.strptime('03:00:00','%H:%M:%S'):
                line = new_date +','+hm+','+'{:.2f}'.format(a[2])+','+'{:.2f}'.format(a[3])+','+'{:.2f}'.format(a[4])+','+'{:.2f}'.format(a[5])+','+'{:.1f}'.format(a[7])+','+str(amout)+'\n'
                #cover_codiction = 0

            elif time_now >= datetime.datetime.strptime('03:01:00','%H:%M:%S'):
                line = str(year)+'{:02}'.format(month)+'{:02}'.format(day)+','+hm+','+'{:.2f}'.format(a[2])+','+'{:.2f}'.format(a[3])+','+'{:.2f}'.format(a[4])+','+'{:.2f}'.format(a[5])+','+'{:.1f}'.format(a[7])+','+str(amout)+'\n'
                cover_codiction = 0
        else:
        
            line = str(year)+'{:02}'.format(month)+'{:02}'.format(day)+','+hm+','+'{:.2f}'.format(a[2])+','+'{:.2f}'.format(a[3])+','+'{:.2f}'.format(a[4])+','+'{:.2f}'.format(a[5])+','+'{:.1f}'.format(a[7])+','+str(amout)+'\n'
        
        # line = str(year)+'{:02}'.format(month)+'{:02}'.format(day)+','+str(a[1])+','+'{:.2f}'.format(a[2])+','+'{:.2f}'.format(a[3])+','+'{:.2f}'.format(a[4])+','+'{:.2f}'.format(a[5])+','+'{:.2f}'.format(a[6])+','+str(a[7])+'\n'
        # line =str(a[0]) +','+str(a[1])+','+'{:.2f}'.format(a[2])+','+'{:.2f}'.format(a[3])+','+'{:.2f}'.format(a[4])+','+'{:.2f}'.format(a[5])+','+'{:.2f}'.format(a[6])+','+str(a[7])+'\n'

        
        #line = str(year)+'{:02}'.format(month)+'{:02}'.format(day)+','+hm+','+'{:.2f}'.format(a[2])+','+'{:.2f}'.format(a[3])+','+'{:.2f}'.format(a[4])+','+'{:.2f}'.format(a[5])+','+'{:.1f}'.format(a[7])+','+str(amout)+'\n'

        last_time = str(year)+'{:02}'.format(month)+'{:02}'.format(day),hm
        
        n_date_time = datetime.datetime.strptime(line.split(',')[0] + ' ' + line.split(',')[1], '%Y%m%d %H:%M:%S')
        n_date_time_stamp = n_date_time.timestamp()

        new_line = line.replace(line.split(',')[1],line.split(',')[1]+ ','+ str(n_date_time_stamp) )
        
        if first_date_filter == 1:
            ifile.write(new_line)
        b = b+32
        e = e+32


        #print(last_time)

    ifile.close()
    
    #df_gp = pd.read_csv(targetDir + fname + '.csv', sep=',')
    #df_gp.to_excel(targetDir+ fname + '.xlsx')
 
def convert_file_name(target_dir):
# 遍历源文件夹中的所有子文件夹和文件
    for root, dirs, files in os.walk(target_dir):
        for file in files:
            # 获取文件的完整路径
            file_path = os.path.join(root, file)

            #new_file_name = 'new_prefix' + file_name

            os.rename(file_path, file_path.replace('.csv','_1min_1.csv'))


 
def append_1min_1_csv(file_name):

    path = 'C:\\vnpy-1.9.2-LTS\\vnpy-1.9.2-LTS\\examples\\CtaBacktesting\\bar_1min\\bar_1min_1_timestemp_all\\'
    new_path = 'C:\\new_tdxqh\\vipdoc\\ds\\minline\\csv\\'

    if not os.path.exists(path + file_name):
        print('无法找到源文件:',file_name)
        return

    #print(path + file_name)
    append_contract = open(path + file_name, 'r+')  # 以读取和追加模式打开文件
    new_contract_content = ''  # 用于存储新合约文件内容

    try:
        with open(new_path + file_name, 'r') as f:
            new_contract_content = f.read()  # 读取新合约文件的内容
    except Exception as e:
        print(e)
        return file_name
        
    last_timeStamp = ''  # 初始化时间戳变量
    for line in append_contract:
        last_timeStamp = line.split(',')[2]  # 获取追加合约文件中最后一行的时间戳

    # 将新合约文件内容追加到原合约文件
    start = False  # 开始插入标志
    for line in new_contract_content.splitlines():
        if start:
            append_contract.write(line + '\n')  # 写入新合约文件内容
        if line.split(',')[2] == last_timeStamp:
            start = True  # 开始插入新内容

    append_contract.close()

    return None
#append_1min_1_csv('a8888_1min_1.csv')



def connection_all():
    error_list = []
    path = 'C:\\vnpy-1.9.2-LTS\\vnpy-1.9.2-LTS\\examples\\CtaBacktesting\\bar_1min\\bar_1min_1_timestemp_all\\'
    for root, dirs, files in os.walk(path):
        for file in files:
            # 获取文件的完整路径
            file_path = os.path.join(root, file)

            if '8888' in file:
                print('正在接续:',file)
                if append_1min_1_csv(file) is not None:
                    error_list.append(append_1min_1_csv(file))
    print(error_list)
#connection_all()

dirname = 'C:\\new_tdxqh\\vipdoc\\ds\\minline\\'

targetDir='C:\\new_tdxqh\\vipdoc\\ds\\minline\\csv\\'

def dele_file():
    # 列出目录下的所有文件  
    for filename in os.listdir(dirname):  
        file_path = os.path.join(dirname, filename)  
        try:  
            if os.path.isfile(file_path) or os.path.islink(file_path):  
                os.unlink(file_path)  # 删除文件  
            elif os.path.isdir(file_path):  
                shutil.rmtree(file_path)  # 删除目录
            print('删除旧文件minline内成功')  
        except Exception as e:  
            print('Failed to delete %s. Reason: %s' % (file_path, e))

#targetDir='C:\\new_tdxqh\\vipdoc\\ds\\minline\\csv\\main_conctract\\'
# 目标文件夹若不存在，则创建
if not os.path.exists(targetDir):
    os.makedirs(targetDir)


# 检查数据时间戳
def check_timestamp(symbol):
    # 获取合约文件
    file_path = os.path.join(targetDir, symbol + '_1min_1.csv')
    print(file_path)
    if not os.path.exists(file_path):
        print(f'无法找到合约文件: {symbol}')
        return
        
    try:
        # 读取CSV文件，设置列名
        df = pd.read_csv(file_path, names=['date', 'time', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'amount'])
        
        # 检查每一行的时间戳是否大于前一行
        timestamp_errors = []
        prev_timestamp = None
        
        for idx, row in df.iterrows():
            current_timestamp = row['timestamp']
            
            if prev_timestamp is not None and current_timestamp <= prev_timestamp:
                timestamp_errors.append({
                    'index': idx,
                    'current_row': row,
                    'prev_timestamp': prev_timestamp
                })
            
            prev_timestamp = current_timestamp
            
        # 如果发现时间戳错误
        if timestamp_errors:
            print(f'\n{symbol} 发现 {len(timestamp_errors)} 处时间戳异常:')
            for error in timestamp_errors:
                idx = error['index']
                print(f"\n问题位置 {idx}:")
                # 打印前一行、当前行和后一行的数据
                start_idx = max(0, idx-1)
                end_idx = min(idx+1, len(df)-1)
                print(df.loc[start_idx:end_idx, ['date', 'time', 'timestamp']].to_string())
                print(f"前一个时间戳: {error['prev_timestamp']}")
                print(f"当前时间戳: {error['current_row']['timestamp']}")
                print('-' * 50)
            return False
            
        print(f'{symbol} 时间戳检查通过，共 {len(df)} 行数据')
        return True
        
    except Exception as e:
        print(f"处理时间戳时出错: {str(e)}")
        return False

#check_timestamp('rb2505')

def load_symbol():
    cta_path = 'C:\\vnpy-1.9.2-LTS\\vnpy-1.9.2-LTS\\examples\\VnTrader\\CTA_setting.json'
    # 读取CTA配置文件
    try:
        with open(cta_path, 'r', encoding='utf-8') as f:
            cta_setting = json.load(f)
            
        # 提取所有合约代码
        symbols = []
        for strategy in cta_setting:
            if 'vtSymbol' in strategy:
                symbols.append(strategy['vtSymbol'])
                
        return list(set(symbols))  # 去重返回
    except Exception as e:
        print(f'读取CTA配置文件失败: {str(e)}')
        return []



# 批量数据转化
def conver_all(source_dir=None, target_dir=None):
    """
    批量转换TDX K线数据为CSV格式

    Args:
        source_dir: 源数据目录路径，如果为None则使用默认路径
        target_dir: 目标目录路径，如果为None则使用默认路径

    Returns:
        int: 成功转换的文件数量
    """
    if source_dir is None:
        source_dir = dirname
    if target_dir is None:
        target_dir = targetDir

    # 确保目标目录存在
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # 获取文件夹中的所有文件名
    file_list = os.listdir(source_dir)
    count = 0
    for file_name in file_list:
        if '.lc1' in file_name:
            miniute2csv_data(source_dir, file_name, target_dir, log_callback)
            count += 1
    _log_message(f'{count}个合约转化完毕', log_callback)
    return count


def convert_to_vnpy_format(input_file: str, output_file: str = None, log_callback=None) -> bool:
    """
    将TDX转换后的CSV文件转换为vnpy可导入的格式

    Args:
        input_file: 输入的TDX转换后CSV文件路径
        output_file: 输出的vnpy格式CSV文件路径，如果为None则自动生成

    Returns:
        bool: 转换是否成功
    """
    if output_file is None:
        output_file = input_file.replace('_1min_1.csv', '_vnpy_import.csv')
        if output_file == input_file:
            output_file = input_file.replace('.csv', '_vnpy_import.csv')

    try:
        total_in = 0
        total_out = 0
        bad_lines = 0

        # 以文本方式读入，逐行清理
        with open(input_file, "r", encoding="utf-8", errors="ignore", newline="") as f_in:
            raw_lines = []
            for line in f_in:
                line2 = line.strip()
                if not line2:
                    continue
                raw_lines.append(line2)

        # 使用 csv.reader 解析
        reader = csv.reader(raw_lines, delimiter=",")

        # 输出文件：UTF-8（无BOM）
        with open(output_file, "w", encoding="utf-8", newline="") as f_out:
            fieldnames = ["datetime", "open", "high", "low", "close", "volume", "turnover", "open_interest"]
            writer = csv.DictWriter(f_out, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()

            for row in reader:
                total_in += 1

                # 允许行尾多逗号导致的空列，先把末尾空列去掉
                while len(row) > 0 and (row[-1] is None or str(row[-1]).strip() == ""):
                    row.pop()

                # 期望至少 9 列
                if len(row) < 9:
                    bad_lines += 1
                    continue

                # 取前9列（如果有多余列，忽略）
                trade_date = str(row[0]).strip()          # 20210721
                trade_time = str(row[1]).strip()          # 21:00:00
                # epoch = row[2]                          # 1626872400.0（这里不用）
                open_price = str(row[3]).strip()
                high_price = str(row[4]).strip()
                low_price = str(row[5]).strip()
                close_price = str(row[6]).strip()
                volume = str(row[7]).strip()              # 成交量
                open_interest = str(row[8]).strip()       # 持仓量（原amount列）

                # 生成 datetime：YYYY-MM-DD HH:MM:SS
                try:
                    dt = datetime.datetime.strptime(trade_date + trade_time, "%Y%m%d%H:%M:%S")
                    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    bad_lines += 1
                    continue

                # turnover 填 0（通达信数据中没有成交额信息）
                turnover = "0"

                out_row = {
                    "datetime": dt_str,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                    "turnover": turnover,
                    "open_interest": open_interest,
                }

                writer.writerow(out_row)
                total_out += 1

        _log_message(f"vnpy格式转换完成: {input_file} -> {output_file}", log_callback)
        _log_message(f"输入行数: {total_in}, 输出行数: {total_out}, 坏行数: {bad_lines}", log_callback)
        return True

    except Exception as e:
        _log_message(f"转换vnpy格式时出错: {str(e)}", log_callback)
        return False


def conver_all_with_vnpy_format(source_dir=None, target_dir=None, convert_to_vnpy=True, log_callback=None):
    """
    批量转换TDX K线数据为CSV格式，并可选转换为vnpy格式

    Args:
        source_dir: 源数据目录路径，如果为None则使用默认路径
        target_dir: 目标目录路径，如果为None则使用默认路径
        convert_to_vnpy: 是否同时转换为vnpy格式
        log_callback: 日志回调函数，用于输出日志信息

    Returns:
        int: 成功转换的文件数量
    """
    if source_dir is None:
        source_dir = dirname
    if target_dir is None:
        target_dir = targetDir

    # 确保目标目录存在
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # 获取文件夹中的所有文件名
    file_list = os.listdir(source_dir)

    # 计算需要处理的文件总数
    total_files = sum(1 for file_name in file_list if '.lc1' in file_name)
    _log_message(f"开始批量转换，共发现 {total_files} 个 .lc1 文件", log_callback)

    count = 0
    for file_name in file_list:
        if '.lc1' in file_name:
            count += 1
            _log_message(f"[{count}/{total_files}] 正在转换: {file_name}", log_callback)

            # 先转换为原始CSV格式
            miniute2csv_data(source_dir, file_name, target_dir, log_callback)

            if convert_to_vnpy:
                # 获取生成的原始CSV文件名
                short_fname = file_name.replace('.lc1','').split('#')[-1].replace('L9','8888')
                contract_path = 'C:\\vnpy-1.9.2-LTS\\vnpy-1.9.2-LTS\\examples\\DataRecording\\contract_attribute.json'
                with open(contract_path, 'r', encoding='utf-8') as f:
                    contract_dic = json.load(f)

                short_fname_code = ''.join([char for char in short_fname if char.isalpha()])

                if short_fname_code not in contract_dic and short_fname_code.lower() in contract_dic:
                    short_fname = short_fname.replace(short_fname_code,short_fname_code.lower())

                if short_fname_code in contract_dic and contract_dic[short_fname_code]["exchange"] == "CZCE" and short_fname[-4:] != '8888':
                    short_fname = short_fname_code + short_fname.split(short_fname_code)[-1][1:]

                input_csv = os.path.join(target_dir, short_fname + '_1min_1.csv')

                # 转换为vnpy格式
                if os.path.exists(input_csv):
                    convert_to_vnpy_format(input_csv, log_callback=log_callback)

            count += 1
    _log_message(f'=== 数据转换阶段完成 === 共转换 {count} 个合约文件', log_callback)
    return count

# 调用函数


# conver_all_with_vnpy_format()   #转化并转换为vnpy格式 - 注释掉，避免模块导入时自动执行

#connection_all()   #接续

#copy_symbol()   #复制到主力合约约文件


#check_timestamp('rb2505')   # 检查时间的顺序是否正确


 


