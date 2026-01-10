from datetime import time
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
from vnpy.trader.constant import Interval


class DailyMaBreakoutStrategy(CtaTemplate):
    """"""
    author = "用Python的交易员"

    # 参数
    timeframe: int = 1440  # 交易周期（分钟），1440代表日线
    ma_window: int = 20  # 均线周期
    stop_loss_pct: float = 0.05  # 固定止损比例 5%
    trailing_pct: float = 0.08  # 跟踪止盈比例 8%
    use_fixed_stop: bool = True  # 是否启用固定止损
    use_trailing_stop: bool = True  # 是否启用跟踪止盈
    fixed_size: int = 1

    # 变量
    ma_value: float = 0.0
    intra_trade_high: float = 0.0
    intra_trade_low: float = 0.0
    entry_price: float = 0.0

    parameters = [
        "timeframe",
        "ma_window",
        "stop_loss_pct",
        "trailing_pct",
        "use_fixed_stop",
        "use_trailing_stop",
        "fixed_size"
    ]

    variables = [
        "ma_value",
        "intra_trade_high",
        "intra_trade_low",
        "entry_price"
    ]

    def on_init(self) -> None:
        """
        Callback when strategy is inited.
        """
        self.write_log("日线均线突破策略初始化")

        # 参数验证和修正
        self.timeframe = max(1, min(self.timeframe, 1440))  # 限制在1-1440分钟之间
        self.ma_window = max(5, min(self.ma_window, 200))  # 限制在5-200之间
        self.stop_loss_pct = max(0.001, min(self.stop_loss_pct, 0.10))  # 限制在1%-10%之间
        self.trailing_pct = max(0.001, min(self.trailing_pct, 0.20))  # 限制在1%-20%之间
        self.fixed_size = max(1, self.fixed_size)  # 至少为1

        # 根据时间周期选择不同的BarGenerator配置和参数
        if self.timeframe >= 1440:
            # 日线配置，需要指定收盘时间14:59
            self.bg: BarGenerator = BarGenerator(
                on_bar=self.on_bar,
                window=1,
                on_window_bar=self.on_xmin_bar,
                interval=Interval.DAILY,
                daily_end=time(14, 59)  # 日线收盘时间14:59
            )
            # 日线需要更多数据，使用更大的ArrayManager
            array_manager_size = 50
            load_days = 200  # 日线需要更多历史数据
        elif self.timeframe >= 60:
            # 小时级别配置：使用Interval.HOUR来处理>=60分钟的情况
            # window参数表示多少小时生成一个K线
            hour_window = self.timeframe // 60
            self.bg: BarGenerator = BarGenerator(
                on_bar=self.on_bar,
                window=hour_window,
                on_window_bar=self.on_xmin_bar,
                interval=Interval.HOUR
            )
            # 小时级别：需要考虑转换为小时后的数据量
            # 每天约6.5小时交易时间，所以每小时K线约6.5根
            hours_per_day = 6.5
            bars_per_day = hours_per_day / hour_window
            min_bars_needed = 50  # 至少需要50根K线
            array_manager_size = 25
            load_days = max(int(min_bars_needed / bars_per_day), 100)
        else:
            # 分钟线配置：使用Interval.MINUTE处理<60分钟的情况
            self.bg: BarGenerator = BarGenerator(self.on_bar, self.timeframe, self.on_xmin_bar)
            # 根据时间周期动态调整ArrayManager大小和数据加载量
            # 对于高时间周期，需要更多数据来满足初始化要求
            if self.timeframe >= 240:
                array_manager_size = 20  # 超高时间周期使用更小的size要求
                load_days = max(200, self.timeframe * 3)  # 需要更多历史数据
            elif self.timeframe >= 120:
                array_manager_size = 25  # 高时间周期使用更小的size要求
                load_days = max(150, self.timeframe * 2)  # 确保有足够数据
            elif self.timeframe >= 60:
                array_manager_size = 30  # 中等时间周期使用中等size要求
                load_days = max(120, self.timeframe * 2)  # 确保有足够数据
            else:
                array_manager_size = 100
                load_days = 100

        self.am: ArrayManager = ArrayManager(array_manager_size)

        # 加载足够的历史数据
        self.load_bar(load_days)

    def on_start(self) -> None:
        """
        Callback when strategy is started.
        """
        self.write_log("日线均线突破策略启动")
        self.put_event()

    def on_stop(self) -> None:
        """
        Callback when strategy is stopped.
        """
        self.write_log("日线均线突破策略停止")
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
        Callback of new x-minute/daily bar data update.
        """
        try:
            self.cancel_all()

            am: ArrayManager = self.am
            am.update_bar(bar)
            if not am.inited:
                return

            # 计算技术指标
            self.ma_value = am.sma(self.ma_window)

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
        检查入场信号：价格突破N周期均线开仓，支持反手操作
        """
        try:
            # 检查是否有足够的数据计算均线
            if len(self.am.close_array) < self.ma_window:
                return

            # 获取前一根K线的收盘价和均线值
            prev_close = self.am.close_array[-2] if len(self.am.close_array) >= 2 else bar.close_price
            prev_ma = self.am.sma(self.ma_window, array=True)[-2] if len(self.am.close_array) >= self.ma_window + 1 else self.ma_value

            # 多头突破信号：价格从均线下方向上突破均线
            if prev_close <= prev_ma and bar.close_price > self.ma_value:
                # 如果当前持有空单，先平仓再开多单（反手）
                if self.pos < 0:
                    self.cover(bar.close_price + 10, abs(self.pos))
                    self.write_log(f"空单反手：先平空单，价格={bar.close_price:.2f}")

                # 开多单
                self.buy(bar.close_price + 10, self.fixed_size)
                self.entry_price = bar.close_price
                self.intra_trade_high = bar.high_price
                self.intra_trade_low = bar.low_price
                self.write_log(f"多头突破入场：价格={bar.close_price:.2f}, 均线={prev_ma:.2f}")
                return

            # 空头突破信号：价格从均线上方向下突破均线
            if prev_close >= prev_ma and bar.close_price < self.ma_value:
                # 如果当前持有多单，先平仓再开空单（反手）
                if self.pos > 0:
                    self.sell(bar.close_price - 10, abs(self.pos))
                    self.write_log(f"多单反手：先平多单，价格={bar.close_price:.2f}")

                # 开空单
                self.short(bar.close_price - 10, self.fixed_size)
                self.entry_price = bar.close_price
                self.intra_trade_high = bar.high_price
                self.intra_trade_low = bar.low_price
                self.write_log(f"空头突破入场：价格={bar.close_price:.2f}, 均线={prev_ma:.2f}")
                return

        except Exception as e:
            # 在多进程环境中记录错误但不中断优化过程
            self.write_log(f"入场信号检查出错: {e}")
            return

    def check_exit_signals(self, bar: BarData) -> None:
        """
        检查出场信号：反向突破均线平仓，或止损止盈
        """
        try:
            # 检查是否有足够的数据计算均线
            if len(self.am.close_array) < self.ma_window:
                return

            # 获取前一根K线的收盘价和均线值
            prev_close = self.am.close_array[-2] if len(self.am.close_array) >= 2 else bar.close_price
            prev_ma = self.am.sma(self.ma_window, array=True)[-2] if len(self.am.close_array) >= self.ma_window + 1 else self.ma_value

            # 多头出场条件
            if self.pos > 0:
                # 反向突破：价格从均线上方向下突破均线
                if prev_close >= prev_ma and bar.close_price < self.ma_value:
                    self.sell(bar.close_price - 10, abs(self.pos))
                    self.write_log(f"空头反向突破出场：价格={bar.close_price:.2f}, 均线={prev_ma:.2f}")
                    return

                # 固定止损
                if self.use_fixed_stop and self.stop_loss_pct > 0:
                    stop_loss_price = self.entry_price * (1 - self.stop_loss_pct)
                    if bar.low_price <= stop_loss_price:
                        self.sell(stop_loss_price - 10, abs(self.pos))
                        self.write_log(f"多头固定止损：价格={stop_loss_price:.2f}")
                        return

                # 跟踪止盈
                if self.use_trailing_stop and self.trailing_pct > 0:
                    trailing_stop_price = self.intra_trade_high * (1 - self.trailing_pct)
                    if bar.low_price <= trailing_stop_price:
                        self.sell(trailing_stop_price - 10, abs(self.pos))
                        self.write_log(f"多头跟踪止盈：价格={trailing_stop_price:.2f}")
                        return

            # 空头出场条件
            elif self.pos < 0:
                # 反向突破：价格从均线下方向上突破均线
                if prev_close <= prev_ma and bar.close_price > self.ma_value:
                    self.cover(bar.close_price + 10, abs(self.pos))
                    self.write_log(f"空头反向突破出场：价格={bar.close_price:.2f}, 均线={prev_ma:.2f}")
                    return

                # 固定止损
                if self.use_fixed_stop and self.stop_loss_pct > 0:
                    stop_loss_price = self.entry_price * (1 + self.stop_loss_pct)
                    if bar.high_price >= stop_loss_price:
                        self.cover(stop_loss_price + 10, abs(self.pos))
                        self.write_log(f"空头固定止损：价格={stop_loss_price:.2f}")
                        return

                # 跟踪止盈
                if self.use_trailing_stop and self.trailing_pct > 0:
                    trailing_stop_price = self.intra_trade_low * (1 + self.trailing_pct)
                    if bar.high_price >= trailing_stop_price:
                        self.cover(trailing_stop_price + 10, abs(self.pos))
                        self.write_log(f"空头跟踪止盈：价格={trailing_stop_price:.2f}")
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
