import numpy as np

from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)


class Ma21MacdStrategy(CtaTemplate):
    """"""

    author = "用Python的交易员"

    # 参数
    timeframe: int = 15  # 交易周期（分钟），如15、30、60、240等
    ma_window: int = 21
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    volume_ratio: float = 1.5  # 放量倍数
    stop_loss_pct: float = 0.05  # 固定止损比例 5%
    trailing_pct: float = 0.08  # 跟踪止盈比例 8%
    fixed_size: int = 1

    # 变量
    ma_value: float = 0.0
    macd_value: float = 0.0
    macd_signal_value: float = 0.0
    macd_hist: float = 0.0
    volume_ma: float = 0.0

    intra_trade_high: float = 0.0
    intra_trade_low: float = 0.0
    entry_price: float = 0.0

    parameters = [
        "timeframe",
        "ma_window",
        "macd_fast",
        "macd_slow",
        "macd_signal",
        "volume_ratio",
        "stop_loss_pct",
        "trailing_pct",
        "fixed_size"
    ]

    variables = [
        "ma_value",
        "macd_value",
        "macd_signal_value",
        "macd_hist",
        "volume_ma",
        "intra_trade_high",
        "intra_trade_low",
        "entry_price"
    ]

    def on_init(self) -> None:
        """
        Callback when strategy is inited.
        """
        self.write_log("MA21-MACD策略初始化")

        # 参数验证和修正
        self.timeframe = max(1, min(self.timeframe, 1440))  # 限制在1-1440分钟之间
        self.ma_window = max(5, min(self.ma_window, 200))  # 限制在5-200之间
        self.macd_fast = max(5, min(self.macd_fast, 50))   # 限制在5-50之间
        self.macd_slow = max(10, min(self.macd_slow, 100)) # 限制在10-100之间
        self.macd_signal = max(5, min(self.macd_signal, 50)) # 限制在5-50之间
        self.volume_ratio = max(1.1, min(self.volume_ratio, 5.0)) # 限制在1.1-5.0之间
        self.stop_loss_pct = max(0.01, min(self.stop_loss_pct, 0.02)) # 限制在1%-20%之间
        self.trailing_pct = max(0.01, min(self.trailing_pct, 0.05)) # 限制在1%-30%之间
        self.fixed_size = max(1, self.fixed_size)  # 至少为1

        # 初始化BarGenerator来生成指定周期的K线
        self.bg: BarGenerator = BarGenerator(self.on_bar, self.timeframe, self.on_xmin_bar)
        self.am: ArrayManager = ArrayManager()

        # 加载足够的历史数据（增加数据量以确保指标计算稳定）
        self.load_bar(50)

    def on_start(self) -> None:
        """
        Callback when strategy is started.
        """
        self.write_log("MA21-MACD策略启动")
        self.put_event()

    def on_stop(self) -> None:
        """
        Callback when strategy is stopped.
        """
        self.write_log("MA21-MACD策略停止")
        self.put_event()

    def on_tick(self, tick: TickData) -> None:
        """
        Callback of new tick data update.
        """
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData) -> None:
        """
        Callback of new bar data update.
        """
        # 使用BarGenerator生成指定周期的K线数据
        self.bg.update_bar(bar)

    def on_xmin_bar(self, bar: BarData) -> None:
        """
        Callback of new x-minute bar data update.
        """
        try:
            self.cancel_all()

            am: ArrayManager = self.am
            am.update_bar(bar)
            if not am.inited:
                return

            # 计算技术指标
            self.ma_value = am.sma(self.ma_window)
            macd_values = am.macd(self.macd_fast, self.macd_slow, self.macd_signal)
            self.macd_value, self.macd_signal_value, self.macd_hist = macd_values

            # 计算成交量均线
            if len(am.volume_array) >= self.ma_window:
                # 使用Python内置函数避免numpy多进程序列化问题
                self.volume_ma = sum(am.volume_array[-self.ma_window:]) / self.ma_window
            else:
                self.volume_ma = bar.volume  # 如果数据不足，使用当前成交量

            # 更新持仓后的最高价和最低价
            if self.pos > 0:
                self.intra_trade_high = max(self.intra_trade_high, bar.high_price)
                self.intra_trade_low = min(self.intra_trade_low, bar.low_price)
            elif self.pos < 0:
                self.intra_trade_high = max(self.intra_trade_high, bar.high_price)
                self.intra_trade_low = min(self.intra_trade_low, bar.low_price)

            # 交易逻辑
            if self.pos == 0:
                self.check_entry_signals(bar)
            else:
                self.check_exit_signals(bar)

            self.put_event()
        except Exception as e:
            # 在多进程优化环境中，记录错误但不中断整个优化过程
            self.write_log(f"on_{self.timeframe}min_bar处理出错: {e}")
            return

    def check_entry_signals(self, bar: BarData) -> None:
        """
        检查入场信号
        """
        try:
            # 获取最近5个周期的MACD数据（当前+前4个周期）
            macd_data = self.am.macd(self.macd_fast, self.macd_slow, self.macd_signal, array=True)
            if len(macd_data[0]) < 5:
                return

            macd_values = macd_data[0][-5:]  # 最近5个周期的MACD值
            signal_values = macd_data[1][-5:]  # 最近5个周期的信号值

            # 检查数据有效性
            if len(macd_values) != 5 or len(signal_values) != 5:
                return

            # 检查多头入场条件
            self.check_long_entry(bar, macd_values, signal_values)

            # 检查空头入场条件
            self.check_short_entry(bar, macd_values, signal_values)
        except Exception as e:
            # 在多进程环境中记录错误但不中断优化过程
            self.write_log(f"入场信号检查出错: {e}")
            return

    def check_long_entry(self, bar: BarData, macd_values: np.ndarray, signal_values: np.ndarray) -> None:
        """
        检查多头入场信号
        """
        # 条件1: 放量向上突破21日均线
        volume_breakout = bar.volume > self.volume_ma * self.volume_ratio
        price_breakout_up = bar.close_price > self.ma_value and bar.open_price <= self.ma_value

        if not (volume_breakout and price_breakout_up):
            return

        # 条件2: 前4个周期内发生过金叉，且到突破时没有死叉
        golden_cross_found = False
        golden_cross_index = -1

        # 检查前4个周期是否有金叉（从最早到最晚）
        for i in range(4):  # 检查前4个周期
            if i+1 < len(macd_values) and i+1 < len(signal_values):
                if (macd_values[i] < signal_values[i] and
                    macd_values[i+1] > signal_values[i+1]):
                    golden_cross_found = True
                    golden_cross_index = i + 1  # 金叉发生的周期索引
                    break

        if not golden_cross_found:
            return

        # 检查从金叉发生到当前，是否没有死叉
        has_death_cross = False
        for i in range(golden_cross_index + 1, 5):  # 从金叉后到当前
            if (macd_values[i-1] > signal_values[i-1] and
                macd_values[i] < signal_values[i]):
                has_death_cross = True
                break

        if not has_death_cross:
            self.buy(bar.close_price + 10, self.fixed_size)
            self.entry_price = bar.close_price
            self.intra_trade_high = bar.high_price
            self.intra_trade_low = bar.low_price
            self.write_log(f"多头买入信号触发：价格={bar.close_price}, 均线={self.ma_value:.2f}, 成交量={bar.volume}")

    def check_short_entry(self, bar: BarData, macd_values: np.ndarray, signal_values: np.ndarray) -> None:
        """
        检查空头入场信号
        """
        # 条件1: 放量向下突破21日均线
        volume_breakout = bar.volume > self.volume_ma * self.volume_ratio
        price_breakout_down = bar.close_price < self.ma_value and bar.open_price >= self.ma_value

        if not (volume_breakout and price_breakout_down):
            return

        # 条件2: 前4个周期内发生过死叉，且到突破时没有金叉
        death_cross_found = False
        death_cross_index = -1

        # 检查前4个周期是否有死叉
        for i in range(4):  # 检查前4个周期
            if i+1 < len(macd_values) and i+1 < len(signal_values):
                if (macd_values[i] > signal_values[i] and
                    macd_values[i+1] < signal_values[i+1]):
                    death_cross_found = True
                    death_cross_index = i + 1  # 死叉发生的周期索引
                    break

        if not death_cross_found:
            return

        # 检查从死叉发生到当前，是否没有金叉
        has_golden_cross = False
        for i in range(death_cross_index + 1, 5):  # 从死叉后到当前
            if (macd_values[i-1] < signal_values[i-1] and
                macd_values[i] > signal_values[i]):
                has_golden_cross = True
                break

        if not has_golden_cross:
            self.short(bar.close_price - 10, self.fixed_size)
            self.entry_price = bar.close_price
            self.intra_trade_high = bar.high_price
            self.intra_trade_low = bar.low_price
            self.write_log(f"空头卖出信号触发：价格={bar.close_price}, 均线={self.ma_value:.2f}, 成交量={bar.volume}")

    def check_exit_signals(self, bar: BarData) -> None:
        """
        检查出场信号
        """
        try:
            # 固定比例止损
            if self.pos > 0:
                stop_loss_price = self.entry_price * (1 - self.stop_loss_pct)
                if bar.low_price <= stop_loss_price:
                    self.sell(stop_loss_price - 10, abs(self.pos))
                    self.write_log(f"多头固定止损：价格={stop_loss_price}")
                    return

                # 跟踪止盈
                trailing_stop_price = self.intra_trade_high * (1 - self.trailing_pct)
                if bar.low_price <= trailing_stop_price:
                    self.sell(trailing_stop_price - 10 , abs(self.pos))
                    self.write_log(f"多头跟踪止盈：价格={trailing_stop_price}")
                    return

            elif self.pos < 0:
                stop_loss_price = self.entry_price * (1 + self.stop_loss_pct)
                if bar.high_price >= stop_loss_price:
                    self.cover(stop_loss_price + 10, abs(self.pos))
                    self.write_log(f"空头固定止损：价格={stop_loss_price}")
                    return

                # 跟踪止盈
                trailing_stop_price = self.intra_trade_low * (1 + self.trailing_pct)
                if bar.high_price >= trailing_stop_price:
                    self.cover(trailing_stop_price + 10, abs(self.pos))
                    self.write_log(f"空头跟踪止盈：价格={trailing_stop_price}")
                    return
        except Exception as e:
            # 在多进程环境中记录错误但不中断优化过程
            self.write_log(f"出场信号检查出错: {e}")
            return

    def on_order(self, order: OrderData) -> None:
        """
        Callback of new order data update.
        """
        pass

    def on_trade(self, trade: TradeData) -> None:
        """
        Callback of new trade data update.
        """
        if trade.direction.name == "LONG" and trade.offset.name == "OPEN":
            self.entry_price = trade.price
            self.intra_trade_high = trade.price
            self.intra_trade_low = trade.price
        elif trade.direction.name == "SHORT" and trade.offset.name == "OPEN":
            self.entry_price = trade.price
            self.intra_trade_high = trade.price
            self.intra_trade_low = trade.price

        self.put_event()

    def on_stop_order(self, stop_order: StopOrder) -> None:
        """
        Callback of stop order update.
        """
        pass
