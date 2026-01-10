import csv
from datetime import datetime

# =======================
# 配置：输入/输出文件路径
# =======================
csv_name ='rb8888'
INPUT_CSV = csv_name + '.csv'            # 你的原始无表头CSV
OUTPUT_CSV = csv_name + "vnpy_import.csv"     # 输出：可直接导入DataManager的CSV

# =======================
# 解析并转换
# =======================
def normalize_line(line: str) -> str:
    """
    兼容一些文件里可能混入的中文逗号、奇怪空白等。
    """
    if line is None:
        return ""
    line = line.strip()
    # 把中文逗号替换为英文逗号（有些数据源会混入 '，'）
    line = line.replace("，", ",")
    return line

def convert():
    total_in = 0
    total_out = 0
    bad_lines = 0

    # 以文本方式读入，逐行清理，再交给 csv.reader
    with open(INPUT_CSV, "r", encoding="utf-8", errors="ignore", newline="") as f_in:
        raw_lines = []
        for line in f_in:
            line2 = normalize_line(line)
            if not line2:
                continue
            raw_lines.append(line2)

    # 使用 csv.reader 解析
    reader = csv.reader(raw_lines, delimiter=",")

    # 输出文件：UTF-8（无BOM），避免 \ufeffdatetime 问题
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f_out:
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
            volume = str(row[7]).strip()
            open_interest = str(row[8]).strip()

            # 生成 datetime：YYYY-MM-DD HH:MM:SS
            try:
                dt = datetime.strptime(trade_date + trade_time, "%Y%m%d%H:%M:%S")
                dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                bad_lines += 1
                continue

            # turnover 原始没有，填 0
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

    print("转换完成")
    print("输入行数:", total_in)
    print("输出行数:", total_out)
    print("坏行数:", bad_lines)
    print("输出文件:", OUTPUT_CSV)

if __name__ == "__main__":
    convert()
