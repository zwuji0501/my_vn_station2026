import csv
import os
from typing import List, Tuple

from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
)


class CsvSignalStrategy(CtaTemplate):
    """
    CSV信号策略

    该策略读取指定的CSV文件，监控新增的交易信号，
    并根据信号自动执行开仓或平仓操作。

    CSV文件格式要求：
    时间,symbol,方向,开平,价格,数量,?,策略名,?

    参数说明：
    - csv_file_path: CSV文件路径
    - symbol: 交易品种（如ag2604）
    - strategy_name: 策略名称（如"金肯特纳模型"）

    交易逻辑：
    - 每个tick检查CSV文件是否有新增行
    - 只处理匹配当前symbol和strategy_name的信号
    - 跳过策略名为空的行
    - 使用市价下单，不使用信号中的价格
    """

    author = "用Python的交易员"

    # 策略参数
    csv_file_path: str = "C:\\Users\\Administrator\\Desktop\\autoHotKey_script\\mozhu_events.csv"
    vt_symbol: str = "ag2604.SHFE"
    user_strategy_name: str = ""
    strategy_name: str = "金肯特纳模型"

    # 内部变量
    last_line_count: int = 0
    processed_lines: set = set()
    last_tick: TickData = None
    last_tick_prick = 0
    tick_count: int = 0  # tick计数器，用于控制日志输出频率

    parameters = ["csv_file_path", "vt_symbol", "user_strategy_name"]
    variables = ["last_line_count", 'last_tick_prick', 'tick_count']

    def on_init(self) -> None:
        """
        Callback when strategy is inited.
        """
        self.write_log("CSV信号策略初始化")
        self.write_log(f"参数设置 - CSV文件: {self.csv_file_path}, 交易品种: {self.vt_symbol}, 用户策略名: '{self.user_strategy_name}', 默认策略名: '{self.strategy_name}'")

        # 初始化已处理行集合
        self.processed_lines = set()

        # 检查参数
        if not self.csv_file_path:
            self.write_log("错误：未设置CSV文件路径")
            return

        if not self.vt_symbol:
            self.write_log("错误：未设置交易品种")
            return

        # 如果用户没有设置策略名称，使用默认策略名称
        if not self.user_strategy_name:
            self.write_log(f"用户未设置策略名称，使用默认值: '{self.strategy_name}'")
            self.user_strategy_name = self.strategy_name
        else:
            self.write_log(f"使用用户指定的策略名称: '{self.user_strategy_name}'")

        # 检查CSV文件是否存在
        if not os.path.exists(self.csv_file_path):
            self.write_log(f"错误：CSV文件不存在 {self.csv_file_path}")
            return

        # 初始化时读取当前行数
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as f:
                self.last_line_count = sum(1 for line in f if line.strip())
            self.write_log(f"初始化完成，当前CSV文件有{self.last_line_count}行")
        except Exception as e:
            self.write_log(f"读取CSV文件失败: {str(e)}")
            import traceback
            self.write_log(f"详细错误: {traceback.format_exc()}")

    def on_start(self) -> None:
        """
        Callback when strategy is started.
        """
        self.write_log("CSV信号策略启动")

    def on_stop(self) -> None:
        """
        Callback when strategy is stopped.
        """
        self.write_log("CSV信号策略停止")

    def on_tick(self, tick: TickData) -> None:
        """
        Callback of new tick data update.
        """
        self.last_tick = tick
        self.last_tick_prick = tick.ask_price_1
        self.tick_count += 1

        try:
            # 检查是否有新的信号
            new_signals = self._check_new_signals()
            if new_signals:
                self.write_log(f"[{self.tick_count}] 发现{len(new_signals)}个新信号")
                for signal in new_signals:
                    self._process_signal(signal)
        except Exception as e:
            self.write_log(f"[{self.tick_count}] 处理tick数据时出错: {str(e)}")

        self.put_event()

    def on_bar(self, bar: BarData) -> None:
        """
        Callback of new bar data update.
        """
        pass

    def on_order(self, order: OrderData) -> None:
        """
        Callback of new order data update.
        """
        self.put_event()

    def on_trade(self, trade: TradeData) -> None:
        """
        Callback of new trade data update.
        """
        self.put_event()

    def on_stop_order(self, stop_order: StopOrder) -> None:
        """
        Callback of stop order update.
        """
        pass

    def _check_new_signals(self) -> List[Tuple[int, List[str]]]:
        """
        检查CSV文件是否有新增信号
        返回: [(行号, [列数据]), ...]
        """
        if not os.path.exists(self.csv_file_path):
            self.write_log(f"错误：CSV文件不存在 {self.csv_file_path}")
            return []

        new_signals = []
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            current_line_count = len([line for line in lines if line.strip()])

            # 如果行数没有变化，返回空
            if current_line_count <= self.last_line_count:
                return []

            self.write_log(f"发现新增行: {current_line_count - self.last_line_count}行 (总{current_line_count}行)")

            # 读取新增的行
            skipped_empty = 0
            skipped_processed = 0
            skipped_columns = 0
            skipped_no_match = 0

            for i in range(self.last_line_count, current_line_count):
                line = lines[i].strip()

                if not line:
                    skipped_empty += 1
                    continue

                # 跳过已处理的行
                if i in self.processed_lines:
                    skipped_processed += 1
                    continue

                # 解析CSV行
                # CSV格式：时间,symbol,方向,开平,价格,数量,?,策略名,?
                parts = line.split(',')

                if len(parts) < 8:
                    skipped_columns += 1
                    self.write_log(f"跳过第{i+1}行：列数不足({len(parts)}<8)，内容: {line}")
                    continue

                # 检查是否匹配当前策略的symbol和user_strategy_name
                signal_symbol = parts[1].strip()
                signal_strategy = parts[7].strip()

                expected_symbol = self.vt_symbol.split('.')[0]

                # 只有匹配成功的才输出详细日志
                if signal_symbol == expected_symbol and signal_strategy == self.user_strategy_name:
                    self.write_log(f"匹配成功！行{i+1}: symbol='{signal_symbol}', 策略='{signal_strategy}', 内容: {line}")
                    new_signals.append((i, parts))
                    self.processed_lines.add(i)
                else:
                    skipped_no_match += 1
                    # 只在少量匹配失败时输出详情，避免刷屏
                    if skipped_no_match <= 3:
                        self.write_log(f"匹配失败行{i+1}: 期望symbol='{expected_symbol}'实际'{signal_symbol}', 期望策略='{self.user_strategy_name}'实际'{signal_strategy}'")

            # 输出跳过统计
            if skipped_empty > 0 or skipped_processed > 0 or skipped_columns > 0 or skipped_no_match > 0:
                self.write_log(f"新增行处理统计: 成功{len(new_signals)}个, 跳过空行{skipped_empty}个, 已处理{skipped_processed}个, 列数不足{skipped_columns}个, 匹配失败{skipped_no_match}个")

            if skipped_no_match > 3:
                self.write_log(f"... 还有{skipped_no_match - 3}个匹配失败的行未显示详情 (避免刷屏)")

            self.last_line_count = current_line_count

        except Exception as e:
            self.write_log(f"检查新信号时出错: {str(e)}")
            import traceback
            self.write_log(f"详细错误: {traceback.format_exc()}")

        return new_signals

    def _process_signal(self, signal_data: Tuple[int, List[str]]) -> None:
        """
        处理单个信号
        signal_data: (行号, [列数据])
        """
        line_num, parts = signal_data

        self.write_log(f"开始处理信号，行号: {line_num + 1}")

        if not self.last_tick:
            self.write_log("没有最新的tick数据，跳过信号处理")
            return

        try:
            # 解析信号数据
            timestamp = parts[0].strip()
            direction_raw = parts[2].strip()
            action_raw = parts[3].strip()

            # 清理全角空格，提取买卖方向
            direction = direction_raw.replace("　", "").strip()
            action = action_raw.replace("　", "").strip()

            price = float(parts[4].strip())
            volume = int(parts[5].strip())

            self.write_log(f"解析信号数据 - 时间:{timestamp}, 原始方向:'{direction_raw}', 原始开平:'{action_raw}', 清理后方向:'{direction}', 开平:'{action}', 价格:{price}, 数量:{volume}")
            self.write_log(f"当前持仓: {self.pos}, tick价格 - 买一:{self.last_tick.bid_price_1}, 卖一:{self.last_tick.ask_price_1}")

            # 根据方向和开平执行操作，使用市价
            if direction == "买":
                if action == "开":
                    # 买开 - 多头开仓，使用卖一价作为市价
                    self.write_log(f"执行买开操作，数量:{volume}，价格:{self.last_tick.ask_price_1}")
                    self.buy(self.last_tick.ask_price_1, volume, False)
                    self.write_log(f"买开指令已发送")
                elif action == "平":
                    # 买平 - 多头平仓
                    if self.pos < 0:  # 当前为空头
                        cover_volume = min(volume, abs(self.pos))
                        self.write_log(f"执行买平操作，可平数量:{cover_volume}，价格:{self.last_tick.bid_price_1}")
                        self.cover(self.last_tick.bid_price_1, cover_volume, False)
                        self.write_log(f"买平指令已发送")
                    else:
                        self.write_log(f"当前持仓{self.pos}不为负，跳过买平操作")

            elif direction == "卖":
                if action == "开":
                    # 卖开 - 空头开仓，使用买一价作为市价
                    self.write_log(f"执行卖开操作，数量:{volume}，价格:{self.last_tick.bid_price_1}")
                    self.short(self.last_tick.bid_price_1, volume, False)
                    self.write_log(f"卖开指令已发送")
                elif action == "平":
                    # 卖平 - 空头平仓
                    if self.pos > 0:  # 当前为多头
                        sell_volume = min(volume, abs(self.pos))
                        self.write_log(f"执行卖平操作，可平数量:{sell_volume}，价格:{self.last_tick.ask_price_1}")
                        self.sell(self.last_tick.ask_price_1, sell_volume, False)
                        self.write_log(f"卖平指令已发送")
                    else:
                        self.write_log(f"当前持仓{self.pos}不为正，跳过卖平操作")
            else:
                self.write_log(f"未知方向: '{direction}'，跳过处理")

        except Exception as e:
            self.write_log(f"处理信号时出错: {str(e)}, 行号: {line_num + 1}")
            import traceback
            self.write_log(f"详细错误: {traceback.format_exc()}")

