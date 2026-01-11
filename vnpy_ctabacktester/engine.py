import importlib
import traceback
import csv
import os
import json
from datetime import datetime
from threading import Thread
from pathlib import Path
from inspect import getfile
from glob import glob
from types import ModuleType
from pandas import DataFrame
import psutil

from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.constant import Interval
from vnpy.trader.utility import save_json
from vnpy.trader.utility import extract_vt_symbol
from vnpy.trader.object import HistoryRequest, TickData, BarData, ContractData
from vnpy.trader.datafeed import BaseDatafeed, get_datafeed
from vnpy.trader.database import BaseDatabase, get_database

import vnpy_ctastrategy
from vnpy_ctastrategy import CtaTemplate, TargetPosTemplate
from vnpy_ctastrategy.backtesting import (
    BacktestingEngine,
    OptimizationSetting,
    BacktestingMode
)
from .locale import _

APP_NAME = "CtaBacktester"

EVENT_BACKTESTER_LOG = "eBacktesterLog"
EVENT_BACKTESTER_BACKTESTING_FINISHED = "eBacktesterBacktestingFinished"
EVENT_BACKTESTER_OPTIMIZATION_FINISHED = "eBacktesterOptimizationFinished"


class BacktesterEngine(BaseEngine):
    """
    For running CTA strategy backtesting.
    """

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.classes: dict = {}
        self.backtesting_engine: BacktestingEngine = None
        self.thread: Thread | None = None

        self.datafeed: BaseDatafeed = get_datafeed()
        self.database: BaseDatabase = get_database()

        # Backtesting reuslt
        self.result_df: DataFrame | None = None
        self.result_statistics: dict | None = None

        # Optimization result
        self.result_values: list | None = None

        # Trade save option
        self.save_trades_separately: bool = False
        self.current_class_name: str = ""
        self.current_vt_symbol: str = ""

        # Data source settings
        self.data_source: str = "database"  # "database" or "csv"
        self.csv_path: str = r"C:\new_tdxqh\vipdoc\ds\minline\csv"

        # CSV data cache
        self.csv_data_cache: dict = {}  # Cache for loaded CSV data

        # CSV interval mapping: UI interval -> CSV filename suffix
        self.csv_interval_mapping = {
            "1m": "1m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1h",
            "4h": "4h",
            "d": "d"
        }

        # Optimization control
        self.optimization_running: bool = False

    def init_engine(self) -> None:
        """"""
        self.write_log(_("初始化CTA回测引擎"))

        # Create backtestresult directory
        backtestresult_dir = Path.home() / ".vntrader" / "backtestresult"
        backtestresult_dir.mkdir(parents=True, exist_ok=True)
        self.write_log(_("回测结果目录已创建"))

        self.backtesting_engine = BacktestingEngine()
        # Redirect log from backtesting engine outside.
        self.backtesting_engine.output = self.write_log

        self.load_strategy_class()
        self.write_log(_("策略文件加载完成"))

        self.init_datafeed()

    def init_datafeed(self) -> None:
        """
        Init datafeed client.
        """
        result: bool = self.datafeed.init(self.write_log)
        if result:
            self.write_log(_("数据服务初始化成功"))

    def write_log(self, msg: str) -> None:
        """"""
        event: Event = Event(EVENT_BACKTESTER_LOG)
        event.data = msg
        self.event_engine.put(event)

    def load_strategy_class(self) -> None:
        """
        Load strategy class from source code.
        """
        app_path: Path = Path(vnpy_ctastrategy.__file__).parent
        path1: Path = app_path.joinpath("strategies")
        self.load_strategy_class_from_folder(path1, "vnpy_ctastrategy.strategies")

        path2: Path = Path.cwd().joinpath("strategies")
        self.load_strategy_class_from_folder(path2, "strategies")

    def load_strategy_class_from_folder(self, path: Path, module_name: str = "") -> None:
        """
        Load strategy class from certain folder.
        """
        for suffix in ["py", "pyd", "so"]:
            pathname: str = str(path.joinpath(f"*.{suffix}"))
            for filepath in glob(pathname):
                filename: str = Path(filepath).stem
                name: str = f"{module_name}.{filename}"
                self.load_strategy_class_from_module(name)

    def load_strategy_class_from_module(self, module_name: str) -> None:
        """
        Load strategy class from module file.
        """
        try:
            module: ModuleType = importlib.import_module(module_name)

            # 重载模块，确保如果策略文件中有任何修改，能够立即生效。
            importlib.reload(module)

            for name in dir(module):
                value = getattr(module, name)
                if (
                    isinstance(value, type)
                    and issubclass(value, CtaTemplate)
                    and value not in {CtaTemplate, TargetPosTemplate}
                ):
                    self.classes[value.__name__] = value
        except:  # noqa
            msg: str = _("策略文件{}加载失败，触发异常：\n{}").format(
                module_name, traceback.format_exc()
            )
            self.write_log(msg)

    def reload_strategy_class(self) -> None:
        """"""
        self.classes.clear()
        self.load_strategy_class()
        self.write_log(_("策略文件重载刷新完成"))

    def get_strategy_class_names(self) -> list:
        """"""
        return list(self.classes.keys())

    def run_backtesting(
        self,
        class_name: str,
        vt_symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        rate: float,
        slippage: float,
        size: int,
        pricetick: float,
        capital: int,
        setting: dict
    ) -> None:
        """"""
        self.result_df = None
        self.result_statistics = None

        engine: BacktestingEngine = self.backtesting_engine
        engine.clear_data()

        if interval == Interval.TICK.value:
            mode: BacktestingMode = BacktestingMode.TICK
            vnpy_interval = interval
        else:
            mode = BacktestingMode.BAR
            # Map UI interval to vn.py Interval enum
            if interval in ["1m", "5m", "15m", "30m"]:
                vnpy_interval = Interval.MINUTE.value
            elif interval in ["1h", "4h"]:
                vnpy_interval = Interval.HOUR.value
            elif interval == "d":
                vnpy_interval = Interval.DAILY.value
            else:
                # Fallback to MINUTE for unknown intervals
                vnpy_interval = Interval.MINUTE.value

        engine.set_parameters(
            vt_symbol=vt_symbol,
            interval=vnpy_interval,
            start=start,
            end=end,
            rate=rate,
            slippage=slippage,
            size=size,
            pricetick=pricetick,
            capital=capital,
            mode=mode
        )

        strategy_class: type[CtaTemplate] = self.classes[class_name]
        engine.add_strategy(
            strategy_class,
            setting
        )

        # Load data based on data source selection
        if self.data_source == "csv":
            success = self.load_csv_data(engine, vt_symbol, interval, start, end)
            if not success or not engine.history_data:
                self.write_log(_("策略回测失败，CSV数据加载失败或数据为空"))
                self.thread = None
                return
        else:
            # Default database loading
            engine.load_data()
            if not engine.history_data:
                self.write_log(_("策略回测失败，历史数据为空"))
                self.thread = None
                return

        try:
            engine.run_backtesting()
        except Exception:
            msg: str = _("策略回测失败，触发异常：\n{}").format(traceback.format_exc())
            self.write_log(msg)

            self.thread = None
            return

        self.result_df = engine.calculate_result()
        self.result_statistics = engine.calculate_statistics(output=False)

        # Clear thread object handler.
        self.thread = None

        # Save trade records
        self.save_trade_records(vt_symbol)

        # Put backtesting done event
        event: Event = Event(EVENT_BACKTESTER_BACKTESTING_FINISHED)
        self.event_engine.put(event)

        # Reset trade save options after backtesting is done
        self.save_trades_separately = False
        self.current_class_name = ""
        self.current_vt_symbol = ""

    def load_csv_data(self, engine, vt_symbol: str, interval: str, start: datetime, end: datetime) -> bool:
        """
        Load data from CSV files for backtesting with caching support.

        Args:
            engine: BacktestingEngine instance
            vt_symbol: Symbol like "rb8888.SHFE"
            interval: Interval string
            start: Start datetime
            end: End datetime

        Returns:
            bool: True if data loaded successfully
        """
        try:
            # Extract symbol from vt_symbol (remove exchange part)
            symbol = vt_symbol.split('.')[0] if '.' in vt_symbol else vt_symbol

            # Create cache key
            cache_key = f"{symbol}_{interval}_{start.strftime('%Y%m%d_%H%M%S')}_{end.strftime('%Y%m%d_%H%M%S')}"

            # Check if data is already cached
            if cache_key in self.csv_data_cache:
                self.write_log(_("从缓存加载数据: {} (缓存键: {})").format(symbol, cache_key))
                engine.history_data = self.csv_data_cache[cache_key]
                self.write_log(_("成功加载 {} 条缓存数据记录").format(len(engine.history_data)))
                return True

            # Construct CSV filename with new format: symbol_exchange_interval.csv
            # Extract exchange from vt_symbol (format: symbol.exchange)
            exchange = ""
            if '.' in vt_symbol:
                parts = vt_symbol.split('.')
                if len(parts) >= 2:
                    exchange = parts[1]

            # Get CSV interval suffix from mapping
            csv_interval = self.csv_interval_mapping.get(interval, interval)

            # Construct filename: symbol_exchange_interval.csv
            csv_filename = f"{symbol}_{exchange}_{csv_interval}.csv"
            csv_filepath = os.path.join(self.csv_path, csv_filename)

            if not os.path.exists(csv_filepath):
                self.write_log(_("CSV文件不存在: {}").format(csv_filepath))
                return False

            self.write_log(_("从CSV文件加载数据: {}").format(csv_filepath))

            # Load data from CSV
            history_data = self._load_history_from_csv(csv_filepath, vt_symbol, interval, start, end)

            if not history_data:
                self.write_log(_("CSV文件中没有找到有效数据"))
                return False

            # Cache the loaded data
            self.csv_data_cache[cache_key] = history_data
            self.write_log(_("数据已缓存 (缓存键: {})").format(cache_key))

            # Set the loaded data to the engine
            engine.history_data = history_data
            self.write_log(_("成功加载 {} 条数据记录").format(len(history_data)))

            return True

        except Exception as e:
            self.write_log(_("CSV数据加载失败: {}").format(str(e)))
            import traceback
            traceback.print_exc()
            return False

    def _load_history_from_csv(self, csv_filepath: str, vt_symbol: str, interval: str, start: datetime, end: datetime):
        """
        Load history data from CSV file.

        Expected CSV format:
        datetime,open,high,low,close,volume,turnover,open_interest
        """
        from vnpy.trader.object import BarData
        from vnpy.trader.constant import Interval, Exchange
        import json

        history_data = []

        try:
            with open(csv_filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                # Load contract attribute mapping from JSON file
                contract_attributes = self._load_contract_attributes()

                for row in reader:
                    try:
                        # Parse datetime
                        dt_str = row['datetime']
                        dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')

                        # Filter by date range
                        if dt < start or dt > end:
                            continue

                        # Get exchange enum from contract attributes or fallback
                        exchange = self._get_exchange_from_contract_attributes(vt_symbol, contract_attributes)

                        # Get interval enum - map UI intervals to vn.py Interval enums
                        if interval in ["1m", "5m", "15m", "30m"]:
                            interval_enum = Interval.MINUTE
                        elif interval in ["1h", "4h"]:
                            interval_enum = Interval.HOUR
                        elif interval == "d":
                            interval_enum = Interval.DAILY
                        elif interval == "w":
                            interval_enum = Interval.WEEKLY
                        elif interval == "tick":
                            interval_enum = Interval.TICK
                        else:
                            interval_enum = Interval.MINUTE  # Default to MINUTE

                        # Create BarData object
                        bar = BarData(
                            symbol=vt_symbol.split('.')[0],
                            exchange=exchange,
                            datetime=dt,
                            interval=interval_enum,
                            volume=float(row['volume']),
                            turnover=float(row.get('turnover', 0)),
                            open_interest=float(row.get('open_interest', 0)),
                            open_price=float(row['open']),
                            high_price=float(row['high']),
                            low_price=float(row['low']),
                            close_price=float(row['close']),
                            gateway_name="CSV"
                        )

                        history_data.append(bar)

                    except (ValueError, KeyError) as e:
                        # Skip invalid rows but continue processing
                        continue

            # Sort by datetime
            history_data.sort(key=lambda x: x.datetime)

            return history_data

        except Exception as e:
            self.write_log(_("CSV文件解析失败: {}").format(str(e)))
            return []

    def _load_contract_attributes(self):
        """
        Load contract attributes from JSON file.
        """
        contract_json_path = os.path.join(self.csv_path, "contract_attribute.json")

        try:
            with open(contract_json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.write_log(f"无法加载合约属性文件 {contract_json_path}: {e}")
            return {}

    def _get_exchange_from_contract_attributes(self, vt_symbol: str, contract_attributes: dict):
        """
        Get exchange enum from contract attributes.

        Args:
            vt_symbol: Symbol like "rb8888.SHFE"
            contract_attributes: Contract attributes dictionary from JSON

        Returns:
            Exchange enum
        """
        from vnpy.trader.constant import Exchange

        # Extract symbol (remove exchange part if present)
        symbol = vt_symbol.split('.')[0] if '.' in vt_symbol else vt_symbol

        # Try to find contract in attributes
        if symbol in contract_attributes:
            contract_info = contract_attributes[symbol]
            exchange_str = contract_info.get('exchange')

            if exchange_str:
                try:
                    return Exchange[exchange_str]
                except KeyError:
                    self.write_log(f"未知交易所代码: {exchange_str}，使用默认交易所SHFE")
                    return Exchange.SHFE

        # Fallback: try to extract from vt_symbol
        if '.' in vt_symbol:
            exchange_str = vt_symbol.split('.')[1]
            try:
                return Exchange[exchange_str]
            except KeyError:
                self.write_log(f"无法识别交易所: {vt_symbol}，使用默认交易所SHFE")
                return Exchange.SHFE
        else:
            # If no exchange in vt_symbol and not found in attributes, use SHFE as default
            self.write_log(f"合约 {symbol} 未找到交易所信息，使用默认交易所SHFE")
            return Exchange.SHFE

    def clear_csv_cache(self):
        """
        Clear the CSV data cache.
        This can be useful if CSV files have been updated and you want to reload them.
        """
        cache_size = len(self.csv_data_cache)
        self.csv_data_cache.clear()
        self.write_log(_("已清除CSV数据缓存，共 {} 条记录").format(cache_size))

    def get_csv_cache_info(self):
        """
        Get information about the current CSV cache.

        Returns:
            dict: Cache statistics
        """
        total_records = sum(len(data) for data in self.csv_data_cache.values())
        return {
            "cached_keys": len(self.csv_data_cache),
            "total_records": total_records,
            "cache_keys": list(self.csv_data_cache.keys())
        }

    def stop_optimization(self) -> bool:
        """
        Force stop the current optimization task.

        Returns:
            bool: True if successfully stopped, False otherwise
        """
        if not self.thread or not self.optimization_running:
            self.write_log(_("当前没有正在运行的优化任务"))
            return False

        try:
            # Try to terminate the thread
            if self.thread.is_alive():
                # Note: Thread termination is not clean in Python
                # We just set the flag and let the thread finish gracefully
                self.optimization_running = False
                self.write_log(_("正在停止优化任务..."))

                # Wait a short time for graceful shutdown
                self.thread.join(timeout=2.0)

                if self.thread.is_alive():
                    self.write_log(_("优化任务未能正常停止，可能仍在后台运行"))
                else:
                    self.write_log(_("优化任务已停止"))
            else:
                self.write_log(_("优化任务已完成"))

            # Clean up thread reference
            self.thread = None

            # Reset optimization state
            self.result_values = None

            return True

        except Exception as e:
            self.write_log(_("停止优化任务时出错: {}").format(str(e)))
            return False

    def start_backtesting(
        self,
        class_name: str,
        vt_symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        rate: float,
        slippage: float,
        size: float,
        pricetick: float,
        capital: float,
        setting: dict
    ) -> bool:
        if self.thread:
            self.write_log(_("已有任务在运行中，请等待完成"))
            return False

        self.write_log("-" * 40)
        self.thread = Thread(
            target=self.run_backtesting,
            args=(
                class_name,
                vt_symbol,
                interval,
                start,
                end,
                rate,
                slippage,
                size,
                pricetick,
                capital,
                setting
            )
        )
        self.thread.start()

        return True

    def get_result_df(self) -> DataFrame | None:
        """"""
        return self.result_df

    def get_result_statistics(self) -> dict | None:
        """"""
        return self.result_statistics

    def get_result_values(self) -> list | None:
        """"""
        return self.result_values

    def get_default_setting(self, class_name: str) -> dict:
        """"""
        strategy_class: type[CtaTemplate] = self.classes[class_name]
        setting: dict = strategy_class.get_class_parameters()
        return setting

    def run_optimization(
        self,
        class_name: str,
        vt_symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        rate: float,
        slippage: float,
        size: int,
        pricetick: float,
        capital: int,
        optimization_setting: OptimizationSetting,
        use_ga: bool,
        max_workers: int | None = None
    ) -> None:
        """"""
        self.result_values = None

        engine: BacktestingEngine = self.backtesting_engine
        engine.clear_data()

        if interval == Interval.TICK.value:
            mode: BacktestingMode = BacktestingMode.TICK
            vnpy_interval = interval
        else:
            mode = BacktestingMode.BAR
            # Map UI interval to vn.py Interval enum
            if interval in ["1m", "5m", "15m", "30m"]:
                vnpy_interval = Interval.MINUTE.value
            elif interval in ["1h", "4h"]:
                vnpy_interval = Interval.HOUR.value
            elif interval == "d":
                vnpy_interval = Interval.DAILY.value
            else:
                # Fallback to MINUTE for unknown intervals
                vnpy_interval = Interval.MINUTE.value

        engine.set_parameters(
            vt_symbol=vt_symbol,
            interval=vnpy_interval,
            start=start,
            end=end,
            rate=rate,
            slippage=slippage,
            size=size,
            pricetick=pricetick,
            capital=capital,
            mode=mode
        )

        strategy_class: type[CtaTemplate] = self.classes[class_name]
        engine.add_strategy(
            strategy_class,
            {}
        )

        # 预加载数据，避免在优化过程中重复加载
        self.write_log(_("预加载历史数据..."))
        try:
            # Load data based on data source selection
            if self.data_source == "csv":
                success = self.load_csv_data(engine, vt_symbol, interval, start, end)
                if not success or not engine.history_data:
                    self.write_log(_("策略参数优化失败，CSV数据加载失败或数据为空"))
                    return
            else:
                # Default database loading
                engine.load_data()
            if not engine.history_data:
                self.write_log(_("策略回测失败，历史数据为空"))
                self.thread = None
                return
        except Exception as e:
            self.write_log(_("数据加载失败: {}").format(str(e)))
            self.thread = None
            self.optimization_running = False
            return

        # 0则代表不限制，但限制最大进程数避免死机
        if max_workers == 0:
            max_workers = min(4, max(1, int(os.cpu_count() * 0.5)))  # 限制为CPU核心数的一半，最多4个
        elif max_workers is None:
            max_workers = min(2, os.cpu_count())  # 默认使用较少的进程

        # 检查参数优化空间大小
        settings_count = len(optimization_setting.generate_settings())
        if settings_count > 1000:
            self.write_log(_("警告：参数优化空间过大({})，建议减少参数范围或使用遗传算法").format(settings_count))
            max_workers = min(max_workers, 2)  # 大空间时减少进程数

        # 检查系统资源
        resources = self.check_system_resources()
        if resources:
            if resources.get("memory_percent", 0) > 85:
                self.write_log(_("警告：系统内存使用率过高({:.1f}%)，可能导致死机").format(resources["memory_percent"]))
                max_workers = 1  # 内存不足时只用单进程
            if resources.get("cpu_percent", 0) > 90:
                self.write_log(_("警告：CPU使用率过高({:.1f}%)，建议等待系统负载降低").format(resources["cpu_percent"]))

            self.write_log(_("系统资源状态 - 内存:{:.1f}%, CPU:{:.1f}%, 可用内存:{:.1f}GB").format(
                resources.get("memory_percent", 0),
                resources.get("cpu_percent", 0),
                resources.get("memory_available_gb", 0)
            ))

        if use_ga:
            self.result_values = engine.run_ga_optimization(
                optimization_setting,
                output=False,
                max_workers=max_workers
            )
        else:
            self.result_values = engine.run_bf_optimization(
                optimization_setting,
                output=False,
                max_workers=max_workers
            )

        # Clear thread object handler.
        self.thread = None
        self.optimization_running = False
        self.write_log(_("多进程参数优化完成"))

        # Put optimization done event
        event: Event = Event(EVENT_BACKTESTER_OPTIMIZATION_FINISHED)
        self.event_engine.put(event)

    def start_optimization(
        self,
        class_name: str,
        vt_symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        rate: float,
        slippage: float,
        size: float,
        pricetick: float,
        capital: float,
        optimization_setting: OptimizationSetting,
        use_ga: bool,
        max_workers: int
    ) -> bool:
        if self.thread:
            self.write_log(_("已有任务在运行中，请等待完成"))
            return False

        self.write_log("-" * 40)
        self.optimization_running = True
        self.thread = Thread(
            target=self.run_optimization,
            args=(
                class_name,
                vt_symbol,
                interval,
                start,
                end,
                rate,
                slippage,
                size,
                pricetick,
                capital,
                optimization_setting,
                use_ga,
                max_workers
            )
        )
        self.thread.start()

        return True

    def run_downloading(
        self,
        vt_symbol: str,
        interval: str,
        start: datetime,
        end: datetime
    ) -> None:
        """
        执行下载任务
        """
        self.write_log(_("{}-{}开始下载历史数据").format(vt_symbol, interval))

        try:
            symbol, exchange = extract_vt_symbol(vt_symbol)
        except ValueError:
            self.write_log(_("{}解析失败，请检查交易所后缀").format(vt_symbol))
            self.thread = None
            return

        req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            interval=Interval(interval),
            start=start,
            end=end
        )

        try:
            if interval == "tick":
                tick_data: list[TickData] = self.datafeed.query_tick_history(req, self.write_log)
                if tick_data:
                    self.database.save_tick_data(tick_data)
                    self.write_log(_("{}-{}历史数据下载完成").format(vt_symbol, interval))
                else:
                    self.write_log(_("数据下载失败，无法获取{}的历史数据").format(vt_symbol))
            else:
                contract: ContractData | None = self.main_engine.get_contract(vt_symbol)

                # If history data provided in gateway, then query
                if contract and contract.history_data:
                    bar_data: list[BarData] = self.main_engine.query_history(
                        req, contract.gateway_name
                    )
                # Otherwise use RQData to query data
                else:
                    bar_data = self.datafeed.query_bar_history(req, self.write_log)

                if bar_data:
                    self.database.save_bar_data(bar_data)
                    self.write_log(_("{}-{}历史数据下载完成").format(vt_symbol, interval))
                else:
                    self.write_log(_("数据下载失败，无法获取{}的历史数据").format(vt_symbol))
        except Exception:
            msg: str = _("数据下载失败，触发异常：\n{}").format(traceback.format_exc())
            self.write_log(msg)

        # Clear thread object handler.
        self.thread = None

    def start_downloading(
        self,
        vt_symbol: str,
        interval: str,
        start: datetime,
        end: datetime
    ) -> bool:
        if self.thread:
            self.write_log(_("已有任务在运行中，请等待完成"))
            return False

        self.write_log("-" * 40)
        self.thread = Thread(
            target=self.run_downloading,
            args=(
                vt_symbol,
                interval,
                start,
                end
            )
        )
        self.thread.start()

        return True

    def get_all_trades(self) -> list:
        """"""
        trades: list = self.backtesting_engine.get_all_trades()
        return trades

    def get_all_orders(self) -> list:
        """"""
        orders: list = self.backtesting_engine.get_all_orders()
        return orders

    def get_all_daily_results(self) -> list:
        """"""
        results: list = self.backtesting_engine.get_all_daily_results()
        return results

    def get_history_data(self) -> list:
        """"""
        history_data: list = self.backtesting_engine.history_data
        return history_data

    def get_strategy_class_file(self, class_name: str) -> str:
        """"""
        strategy_class: type[CtaTemplate] = self.classes[class_name]
        file_path: str = getfile(strategy_class)
        return file_path

    def check_system_resources(self) -> dict:
        """
        检查系统资源状态，避免优化时死机
        """
        try:
            memory = psutil.virtual_memory()
            cpu_percent = psutil.cpu_percent(interval=1)

            return {
                "memory_percent": memory.percent,
                "memory_available_gb": memory.available / (1024**3),
                "cpu_percent": cpu_percent,
                "cpu_count": psutil.cpu_count(),
                "cpu_logical_count": psutil.cpu_count(logical=True)
            }
        except Exception as e:
            self.write_log(_("系统资源检查失败: {}").format(str(e)))
            return {}

    def save_trade_records(self, vt_symbol: str) -> None:
        """
        Save trade records to .vntrader folder in CSV format.
        """
        # Debug logging
        self.write_log(f"保存成交记录 - 单独保存: {self.save_trades_separately}, 策略名: {self.current_class_name}, 合约: {vt_symbol}")

        # Create .vntrader/backtestresult directory if not exists
        result_dir = Path.home() / ".vntrader" / "backtestresult"
        result_dir.mkdir(parents=True, exist_ok=True)

        # If save separately option is enabled, create a subdirectory
        if self.save_trades_separately and self.current_class_name:
            # Create subdirectory name with strategy name, symbol and timestamp
            symbol_part = self.current_vt_symbol.split('.')[0] if '.' in self.current_vt_symbol else self.current_vt_symbol
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            subdir_name = f"{self.current_class_name}_{symbol_part}_{timestamp}"
            result_dir = result_dir / subdir_name
            result_dir.mkdir(exist_ok=True)
            self.write_log(f"创建单独文件夹: {subdir_name}")

            # Save strategy parameters to the same directory
            self.save_strategy_parameters_to_dir(result_dir)

            # Save backtesting results to the same directory
            self.save_backtesting_results_to_dir(result_dir)
        else:
            self.write_log("保存到默认位置")

        # Get all trades
        trades = self.get_all_trades()
        if not trades:
            return

        # Prepare CSV file path
        csv_file = result_dir / "trades.csv"

        # Write trades to CSV
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # Write header
            header = [
                'dt', 'symbol', 'exchange', 'vtSymbol', 'tradeID', 'vtTradeID',
                'orderID', 'vtOrderID', 'direction', 'offset', 'price', 'volume',
                'tradeTime', 'gatewayName', 'rawData', 'profit', 'margin'
            ]
            writer.writerow(header)

            # Write trade data
            for trade in trades:
                # Format datetime
                dt_str = trade.datetime.strftime('%Y-%m-%d %H:%M:%S')

                # Extract symbol and exchange from vt_symbol if needed
                symbol, exchange = extract_vt_symbol(vt_symbol)

                # Format trade time (HH:MM:SS)
                trade_time = trade.datetime.strftime('%H:%M:%S')

                row = [
                    dt_str,  # dt
                    trade.symbol,  # symbol
                    trade.exchange.value if hasattr(trade.exchange, 'value') else str(trade.exchange),  # exchange
                    vt_symbol,  # vtSymbol
                    trade.tradeid,  # tradeID
                    '',  # vtTradeID (empty)
                    trade.orderid,  # orderID
                    '',  # vtOrderID (empty)
                    trade.direction.value if hasattr(trade.direction, 'value') else str(trade.direction),  # direction
                    trade.offset.value if hasattr(trade.offset, 'value') else str(trade.offset),  # offset
                    trade.price,  # price
                    trade.volume,  # volume
                    trade_time,  # tradeTime
                    trade.gateway_name,  # gatewayName
                    '',  # rawData (empty)
                    '',  # profit (empty)
                    ''   # margin (empty)
                ]
                writer.writerow(row)

        self.write_log(_("成交记录已保存到: {}").format(csv_file))

    def save_strategy_parameters_to_dir(self, target_dir: Path) -> None:
        """
        Save current strategy parameters to the specified directory.
        """
        try:
            # Get current strategy settings from backtesting engine
            if hasattr(self.backtesting_engine, 'strategy') and self.backtesting_engine.strategy:
                strategy = self.backtesting_engine.strategy
                parameters = strategy.get_parameters()

                # Save parameters to JSON file
                param_file = target_dir / "strategy_parameters.json"
                param_data = {
                    "strategy_name": self.current_class_name,
                    "symbol": self.current_vt_symbol,
                    "parameters": parameters,
                    "timestamp": datetime.now().isoformat()
                }

                with open(param_file, 'w', encoding='utf-8') as f:
                    import json
                    json.dump(param_data, f, indent=2, ensure_ascii=False)

                self.write_log(f"策略参数已保存到: {param_file}")
        except Exception as e:
            self.write_log(f"保存策略参数失败: {str(e)}")

    def save_backtesting_results_to_dir(self, target_dir: Path) -> None:
        """
        Save backtesting results (statistics and curves) to the specified directory.
        """
        try:
            # Save result statistics as JSON
            if self.result_statistics:
                stats_file = target_dir / "backtest_statistics.json"
                with open(stats_file, 'w', encoding='utf-8') as f:
                    import json
                    json.dump(self.result_statistics, f, indent=2, ensure_ascii=False, default=str)
                self.write_log(f"回测统计已保存到: {stats_file}")

            # Save result DataFrame as CSV (contains net value, drawdown curves, etc.)
            if self.result_df is not None and not self.result_df.empty:
                df_file = target_dir / "backtest_curves.csv"
                self.result_df.to_csv(df_file, encoding='utf-8-sig')
                self.write_log(f"回测曲线已保存到: {df_file}")

        except Exception as e:
            self.write_log(f"保存回测结果失败: {str(e)}")
