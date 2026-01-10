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


class BreakoutStrategy(CtaTemplate):
    """"""
    author = "用Python的交易员"

    # 参数
    timeframe: int = 60  # 交易周期（分钟）
    breakout_period: int = 20  # 突破周期N
    stop_loss_pct: float = 0.05  # 固定止损比例 5%
    profit_take_pct: float = 0.08  # 利润保护百分比 8%（达到此利润后，如果回到成本价则平仓）
    time_exit_period: int = 10  # 时间出场周期M（开仓后M个周期平仓）
    fixed_size: int = 1  # 固定手数

    # 变量
    highest_value: float = 0.0  # N周期最高价
    lowest_value: float = 0.0  # N周期最低价
    entry_price: float = 0.0  # 开仓价格
    entry_bar_count: int = 0  # 开仓后的K线计数
    profit_target_reached: bool = False  # 是否达到过利润目标

    parameters = [
        "timeframe",
        "breakout_period",
        "stop_loss_pct",
        "profit_take_pct",
        "time_exit_period",
        "fixed_size"
    ]

    variables = [
        "highest_value",
        "lowest_value",
        "entry_price",
        "entry_bar_count",
        "profit_target_reached"
    ]

    def on_init(self) -> None:
        """
        Callback when strategy is inited.
        """
        self.write_log("突破策略初始化")

        # 参数验证和修正
        self.timeframe = max(1, min(self.timeframe, 1440))  # 限制在1-1440分钟之间
        self.breakout_period = max(5, min(self.breakout_period, 200))  # 限制在5-200之间
        self.stop_loss_pct = max(0.001, min(self.stop_loss_pct, 0.10))  # 限制在1%-10%之间
        self.profit_take_pct = max(0.001, min(self.profit_take_pct, 0.20))  # 限制在1%-20%之间
        self.time_exit_period = max(1, min(self.time_exit_period, 100))  # 限制在1-100之间
        self.fixed_size = max(1, self.fixed_size)  # 至少为1

        # 根据时间周期选择不同的BarGenerator配置
        if self.timeframe >= 1440:
            # 日线配置
            self.bg: BarGenerator = BarGenerator(
                on_bar=self.on_bar,
                window=1,
                on_window_bar=self.on_xmin_bar,
                interval=Interval.DAILY,
                daily_end=time(14, 59)  # 日线收盘时间14:59
            )
            array_manager_size = 50
            load_days = 200
        elif self.timeframe >= 60:
            # 小时级别配置
            hour_window = self.timeframe // 60
            self.bg: BarGenerator = BarGenerator(
                on_bar=self.on_bar,
                window=hour_window,
                on_window_bar=self.on_xmin_bar,
                interval=Interval.HOUR
            )
            hours_per_day = 6.5
            bars_per_day = hours_per_day / hour_window
            min_bars_needed = 50
            array_manager_size = 25
            load_days = max(int(min_bars_needed / bars_per_day), 100)
        else:
            # 分钟线配置
            self.bg: BarGenerator = BarGenerator(self.on_bar, self.timeframe, self.on_xmin_bar)
            if self.timeframe >= 240:
                array_manager_size = 20
                load_days = max(200, self.timeframe * 3)
            elif self.timeframe >= 120:
                array_manager_size = 25
                load_days = max(150, self.timeframe * 2)
            elif self.timeframe >= 60:
                array_manager_size = 30
                load_days = max(120, self.timeframe * 2)
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
        self.write_log("突破策略启动")
        self.put_event()

    def on_stop(self) -> None:
        """
        Callback when strategy is stopped.
        """
        self.write_log("突破策略停止")
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

            # 计算技术指标：使用前N根bar（不包含当前bar）计算突破基准
            if len(am.close_array) >= self.breakout_period + 1:
                # 使用前N根bar的最高价/最低价作为突破基准
                self.highest_value = max(am.high_array[-self.breakout_period-1:-1])
                self.lowest_value = min(am.low_array[-self.breakout_period-1:-1])
            else:
                # 数据不足时使用当前计算结果
                self.highest_value = am.max(self.breakout_period)
                self.lowest_value = am.min(self.breakout_period)

            # 更新开仓后的K线计数
            if self.pos != 0:
                self.entry_bar_count += 1

            # 交易逻辑
            if self.pos == 0:
                self.check_entry_signals(bar)
            else:
                self.check_exit_signals(bar)

            self.put_event()
        except Exception as e:
            self.write_log(f"on_{self.timeframe}min_bar处理出错: {e}")
            return

    def check_entry_signals(self, bar: BarData) -> None:
        """
        检查入场信号：价格突破前N周期最高价开多，或突破前N周期最低价开空
        """
        try:
            # 检查是否有足够的数据计算指标
            if len(self.am.close_array) < self.breakout_period:
                return

            # 多头突破信号：价格突破N周期最高价
            if bar.close_price > self.highest_value:
                # 开多单
                self.buy(bar.close_price + 10, self.fixed_size)
                self.entry_price = bar.close_price
                self.entry_bar_count = 0
                self.write_log(f"多头突破入场：价格={bar.close_price:.2f}, 前{self.breakout_period}周期最高价={self.highest_value:.2f}")
                return

            # 空头突破信号：价格突破前N周期最低价
            if bar.close_price < self.lowest_value:
                # 开空单
                self.short(bar.close_price - 10, self.fixed_size)
                self.entry_price = bar.close_price
                self.entry_bar_count = 0
                self.write_log(f"空头突破入场：价格={bar.close_price:.2f}, 前{self.breakout_period}周期最低价={self.lowest_value:.2f}")
                return

        except Exception as e:
            self.write_log(f"入场信号检查出错: {e}")
            return

    def check_exit_signals(self, bar: BarData) -> None:
        """
        检查出场信号：固定比例止损、利润保护或时间出场
        """
        try:
            # 多头出场条件
            if self.pos > 0:
                # 计算当前盈利百分比
                current_profit_pct = (bar.close_price - self.entry_price) / self.entry_price

                # 利润保护逻辑
                if self.profit_take_pct > 0:
                    # 如果还未达到利润目标，检查是否达到
                    if not self.profit_target_reached and current_profit_pct >= self.profit_take_pct:
                        self.profit_target_reached = True
                        self.write_log(f"多头达到利润目标：盈利{current_profit_pct:.1%}")

                    # 如果已经达到利润目标，检查是否回到成本价附近
                    elif self.profit_target_reached and bar.close_price <= self.entry_price * 1.002:  # 允许0.2%的误差
                        self.sell(bar.close_price - 10, abs(self.pos))
                        self.write_log(f"多头利润保护出场：回到成本价附近，价格={bar.close_price:.2f}")
                        return

                # 固定比例止损
                if self.stop_loss_pct > 0:
                    stop_loss_price = self.entry_price * (1 - self.stop_loss_pct)
                    if bar.low_price <= stop_loss_price:
                        self.sell(stop_loss_price - 10, abs(self.pos))
                        self.write_log(f"多头固定止损出场：价格={stop_loss_price:.2f}")
                        return

                # 时间出场：开仓后M个周期平仓
                if self.entry_bar_count >= self.time_exit_period:
                    self.sell(bar.close_price - 10, abs(self.pos))
                    self.write_log(f"多头时间出场：开仓后{self.time_exit_period}个周期，价格={bar.close_price:.2f}")
                    return

            # 空头出场条件
            elif self.pos < 0:
                # 计算当前盈利百分比（空头盈利 = 成本价 - 当前价）
                current_profit_pct = (self.entry_price - bar.close_price) / self.entry_price

                # 利润保护逻辑
                if self.profit_take_pct > 0:
                    # 如果还未达到利润目标，检查是否达到
                    if not self.profit_target_reached and current_profit_pct >= self.profit_take_pct:
                        self.profit_target_reached = True
                        self.write_log(f"空头达到利润目标：盈利{current_profit_pct:.1%}")

                    # 如果已经达到利润目标，检查是否回到成本价附近
                    elif self.profit_target_reached and bar.close_price >= self.entry_price * 0.998:  # 允许0.2%的误差
                        self.cover(bar.close_price + 10, abs(self.pos))
                        self.write_log(f"空头利润保护出场：回到成本价附近，价格={bar.close_price:.2f}")
                        return

                # 固定比例止损
                if self.stop_loss_pct > 0:
                    stop_loss_price = self.entry_price * (1 + self.stop_loss_pct)
                    if bar.high_price >= stop_loss_price:
                        self.cover(stop_loss_price + 10, abs(self.pos))
                        self.write_log(f"空头固定止损出场：价格={stop_loss_price:.2f}")
                        return

                # 时间出场：开仓后M个周期平仓
                if self.entry_bar_count >= self.time_exit_period:
                    self.cover(bar.close_price + 10, abs(self.pos))
                    self.write_log(f"空头时间出场：开仓后{self.time_exit_period}个周期，价格={bar.close_price:.2f}")
                    return

        except Exception as e:
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
        if trade.offset.name == "OPEN":
            self.entry_price = trade.price
            self.entry_bar_count = 0
            self.profit_target_reached = False  # 重置利润目标标志
            direction = "多头" if trade.direction.name == "LONG" else "空头"
            self.write_log(f"{direction}开仓成交：价格={trade.price:.2f}, 手数={trade.volume}")

        self.put_event()

    def on_stop_order(self, stop_order: StopOrder) -> None:
        """
        Callback of stop order update.
        """
        pass
