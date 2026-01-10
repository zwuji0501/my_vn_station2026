import csv
import os
import json
import sqlite3
from datetime import datetime, timedelta
from collections.abc import Callable
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from vnpy.trader.engine import BaseEngine, MainEngine, EventEngine
from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.object import BarData, TickData, ContractData, HistoryRequest
from vnpy.trader.database import BaseDatabase, get_database, BarOverview, DB_TZ
from vnpy.trader.datafeed import BaseDatafeed, get_datafeed
from vnpy.trader.utility import ZoneInfo

try:
    from .translate_tdx_kline_data import conver_all_with_vnpy_format
except ImportError:
    from translate_tdx_kline_data import conver_all_with_vnpy_format

APP_NAME = "DataManager"


class DataUpdateScheduler:
    """数据更新调度器，统一管理数据导入和聚合流程"""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or self._get_default_db_path()
        self.status_file = os.path.join(os.path.dirname(self.db_path), "data_update_status.json")
        self._ensure_status_file()

    def _get_default_db_path(self) -> str:
        """获取默认数据库路径"""
        current_dir = os.getcwd()
        vntrader_dir = os.path.join(current_dir, ".vntrader")
        if os.path.exists(vntrader_dir):
            return os.path.join(vntrader_dir, "database.db")

        home_dir = os.path.expanduser("~")
        vntrader_dir = os.path.join(home_dir, ".vntrader")
        return os.path.join(vntrader_dir, "database.db")

    def _ensure_status_file(self):
        """确保状态文件存在"""
        if not os.path.exists(self.status_file):
            default_status = {
                "last_update": None,
                "contracts": {},
                "processed_files": []
            }
            with open(self.status_file, 'w', encoding='utf-8') as f:
                json.dump(default_status, f, indent=2, ensure_ascii=False)

    def get_status(self) -> Dict:
        """获取当前状态"""
        try:
            with open(self.status_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self._ensure_status_file()
            return self.get_status()

    def update_status(self, contract: str = None, file_path: str = None,
                     last_update: datetime = None):
        """更新状态"""
        status = self.get_status()

        if last_update:
            status["last_update"] = last_update.isoformat()

        if contract:
            if contract not in status["contracts"]:
                status["contracts"][contract] = {}
            status["contracts"][contract]["last_update"] = datetime.now().isoformat()

        if file_path and file_path not in status["processed_files"]:
            status["processed_files"].append(file_path)

        with open(self.status_file, 'w', encoding='utf-8') as f:
            json.dump(status, f, indent=2, ensure_ascii=False)

    def is_file_processed(self, file_path: str) -> bool:
        """检查文件是否已处理"""
        status = self.get_status()
        return file_path in status["processed_files"]

    def get_contract_last_update(self, contract: str) -> Optional[datetime]:
        """获取合约最后更新时间"""
        status = self.get_status()
        contract_info = status["contracts"].get(contract, {})
        last_update_str = contract_info.get("last_update")
        return datetime.fromisoformat(last_update_str) if last_update_str else None

    def get_pending_files(self, source_dir: str) -> List[str]:
        """获取待处理的文件列表"""
        if not os.path.exists(source_dir):
            return []

        all_files = []
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                if file.endswith('.lc1'):
                    full_path = os.path.join(root, file)
                    if not self.is_file_processed(full_path):
                        all_files.append(full_path)

        return sorted(all_files)


class ManagerEngine(BaseEngine):
    """"""

    def __init__(
        self,
        main_engine: MainEngine,
        event_engine: EventEngine,
    ) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.database: BaseDatabase = get_database()
        self.datafeed: BaseDatafeed = get_datafeed()
        self.scheduler = DataUpdateScheduler()

    def import_data_from_csv(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        tz_name: str,
        datetime_head: str,
        open_head: str,
        high_head: str,
        low_head: str,
        close_head: str,
        volume_head: str,
        turnover_head: str,
        open_interest_head: str,
        datetime_format: str
    ) -> tuple:
        """"""
        with open(file_path) as f:
            buf: list = [line.replace("\0", "") for line in f]

        reader: csv.DictReader = csv.DictReader(buf, delimiter=",")

        bars: list[BarData] = []
        start: datetime | None = None
        count: int = 0
        tz: ZoneInfo = ZoneInfo(tz_name)

        for item in reader:
            if datetime_format:
                dt: datetime = datetime.strptime(item[datetime_head], datetime_format)
            else:
                dt = datetime.fromisoformat(item[datetime_head])
            dt = dt.replace(tzinfo=tz)

            turnover = item.get(turnover_head, 0)
            open_interest = item.get(open_interest_head, 0)

            bar: BarData = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                interval=interval,
                volume=float(item[volume_head]),
                open_price=float(item[open_head]),
                high_price=float(item[high_head]),
                low_price=float(item[low_head]),
                close_price=float(item[close_head]),
                turnover=float(turnover),
                open_interest=float(open_interest),
                gateway_name="DB",
            )

            bars.append(bar)

            # do some statistics
            count += 1
            if not start:
                start = bar.datetime

        end: datetime = bar.datetime

        # insert into database
        self.database.save_bar_data(bars)

        return start, end, count

    def output_data_to_csv(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> bool:
        """"""
        bars: list[BarData] = self.load_bar_data(symbol, exchange, interval, start, end)

        fieldnames: list = [
            "symbol",
            "exchange",
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
            "open_interest"
        ]

        try:
            with open(file_path, "w") as f:
                writer: csv.DictWriter = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
                writer.writeheader()

                for bar in bars:
                    d: dict = {
                        "symbol": bar.symbol,
                        "exchange": bar.exchange.value,
                        "datetime": bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                        "open": bar.open_price,
                        "high": bar.high_price,
                        "low": bar.low_price,
                        "close": bar.close_price,
                        "turnover": bar.turnover,
                        "volume": bar.volume,
                        "open_interest": bar.open_interest,
                    }
                    writer.writerow(d)

            return True
        except PermissionError:
            return False

    def get_bar_overview(self) -> list[BarOverview]:
        """"""
        overview: list[BarOverview] = self.database.get_bar_overview()
        return overview

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """"""
        bars: list[BarData] = self.database.load_bar_data(
            symbol,
            exchange,
            interval,
            start,
            end
        )

        return bars

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """"""
        count: int = self.database.delete_bar_data(
            symbol,
            exchange,
            interval
        )

        return count






    def download_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: str,
        start: datetime,
        output: Callable
    ) -> int:
        """
        Query bar data from datafeed.
        """
        req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            interval=Interval(interval),
            start=start,
            end=datetime.now(DB_TZ)
        )

        vt_symbol: str = f"{symbol}.{exchange.value}"
        contract: ContractData | None = self.main_engine.get_contract(vt_symbol)

        # If history data provided in gateway, then query
        if contract and contract.history_data:
            data: list[BarData] = self.main_engine.query_history(
                req, contract.gateway_name
            )
        # Otherwise use datafeed to query data
        else:
            data = self.datafeed.query_bar_history(req, output)

        if data:
            self.database.save_bar_data(data)
            return (len(data))

        return 0

    def download_bar_data_batch(
        self,
        source_dir: str = None,
        target_dir: str = None,
        convert_to_vnpy_format: bool = True,
        auto_import: bool = True
    ) -> int:
        """
        从TDX K线数据文件转换为CSV格式的K线数据，可选转换为vnpy格式并自动导入

        Args:
            source_dir: TDX数据源目录路径，如果为None则使用默认路径
            target_dir: CSV文件目标目录路径，如果为None则使用默认路径
            convert_to_vnpy_format: 是否同时转换为vnpy可导入格式
            auto_import: 是否自动导入转换后的数据到vnpy数据库

        Returns:
            int: 成功转换的文件数量
        """
        try:
            self.main_engine.write_log("=== 开始批量导入 TDX 数据 ===")
            count = conver_all_with_vnpy_format(source_dir, target_dir, convert_to_vnpy_format, self.main_engine.write_log)

            if auto_import and convert_to_vnpy_format and count > 0:
                # 确保target_dir不为None，使用默认值
                if target_dir is None:
                    target_dir = r'C:\new_tdxqh\vipdoc\ds\minline\csv'
                self.main_engine.write_log("=== 开始导入数据到 vnpy 数据库 ===")
                imported_count = self._auto_import_converted_data(target_dir, force_update=False)
                self.main_engine.write_log(f"=== 批量导入完成 === 转换文件: {count} 个, 导入合约: {imported_count} 个")
            else:
                self.main_engine.write_log(f"TDX数据转换完成，成功转换 {count} 个合约文件")

            return count
        except Exception as e:
            self.main_engine.write_log(f"转换TDX数据时出错: {str(e)}")
            return 0

    def _auto_import_converted_data(self, target_dir: str, force_update: bool = False) -> int:
        """
        自动导入转换后的vnpy格式CSV数据到数据库

        Args:
            target_dir: CSV文件所在目录
            force_update: 是否强制更新（删除现有数据后重新导入），默认为增量导入

        Returns:
            int: 成功导入的合约数量
        """
        import os
        import json

        imported_count = 0

        # 品种代码到交易所的映射（基于常见期货合约）
        commodity_to_exchange = {
            # 上海期货交易所 (SHFE)
            'cu': 'SHFE', 'al': 'SHFE', 'zn': 'SHFE', 'pb': 'SHFE', 'ni': 'SHFE', 'sn': 'SHFE',
            'au': 'SHFE', 'ag': 'SHFE', 'rb': 'SHFE', 'hc': 'SHFE', 'ss': 'SHFE',
            'bu': 'SHFE', 'fu': 'SHFE', 'sp': 'SHFE', 'wr': 'SHFE',
            # 大连商品交易所 (DCE)
            'm': 'DCE', 'y': 'DCE', 'p': 'DCE', 'l': 'DCE', 'v': 'DCE', 'c': 'DCE',
            'a': 'DCE', 'b': 'DCE', 'j': 'DCE', 'jm': 'DCE', 'i': 'DCE',
            'jd': 'DCE', 'fb': 'DCE', 'bb': 'DCE', 'pp': 'DCE', 'cs': 'DCE',
            # 郑州商品交易所 (CZCE)
            'CF': 'CZCE', 'SR': 'CZCE', 'TA': 'CZCE', 'OI': 'CZCE', 'MA': 'CZCE',
            'FG': 'CZCE', 'RM': 'CZCE', 'ZC': 'CZCE', 'CY': 'CZCE', 'AP': 'CZCE',
            'UR': 'CZCE', 'SA': 'CZCE', 'PF': 'CZCE', 'PK': 'CZCE', 'CJ': 'CZCE',
            'RS': 'CZCE', 'RR': 'CZCE', 'LR': 'CZCE', 'WH': 'CZCE', 'PM': 'CZCE',
            'RI': 'CZCE', 'JR': 'CZCE', 'SM': 'CZCE', 'SF': 'CZCE', 'LH': 'CZCE',
            # 中国金融期货交易所 (CFFEX)
            'IF': 'CFFEX', 'IC': 'CFFEX', 'IH': 'CFFEX', 'IM': 'CFFEX',
            'TS': 'CFFEX', 'TF': 'CFFEX', 'T': 'CFFEX'
        }

        # 读取合约属性文件作为备用方案
        contract_dic = {}
        try:
            with open('C:\\vnpy-1.9.2-LTS\\vnpy-1.9.2-LTS\\examples\\DataRecording\\contract_attribute.json', 'r', encoding='utf-8') as f:
                contract_dic = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            pass  # 合约文件不存在时使用内置映射

        # 扫描目标目录中的vnpy格式CSV文件
        if not os.path.exists(target_dir):
            self.main_engine.write_log(f"目标目录不存在: {target_dir}")
            return 0

        # 统计需要导入的文件总数
        vnpy_files = [f for f in os.listdir(target_dir) if f.endswith('_vnpy_import.csv')]
        total_files = len(vnpy_files)
        self.main_engine.write_log(f"开始导入数据，共发现 {total_files} 个 vnpy 格式文件")

        processed_count = 0
        for filename in vnpy_files:
            processed_count += 1
            # 从文件名解析symbol，如 rb8888_vnpy_import.csv -> rb8888
            symbol = filename.replace('_vnpy_import.csv', '')

            self.main_engine.write_log(f"[{processed_count}/{total_files}] 正在导入合约: {symbol}")

            # 获取交易所信息
            # 从品种代码映射获取交易所
            symbol_code = ''.join([char for char in symbol if char.isalpha()])
            exchange_str = commodity_to_exchange.get(symbol_code)

            # 如果内置映射中没有找到，尝试从合约属性文件中查找
            if not exchange_str:
                if symbol_code in contract_dic:
                    exchange_str = contract_dic[symbol_code].get("exchange")

            if not exchange_str:
                self.main_engine.write_log(f"合约 {symbol} (品种代码: {symbol_code}) 无法获取交易所信息，跳过导入")
                continue

            try:
                exchange = Exchange(exchange_str)

                file_path = os.path.join(target_dir, filename)

                # 根据force_update决定是否删除现有数据
                if force_update:
                    deleted_count = self.delete_bar_data(symbol, exchange, Interval.MINUTE)
                    if deleted_count > 0:
                        self.main_engine.write_log(f"删除了合约 {symbol}.{exchange.value} 的 {deleted_count} 条原有数据")
                else:
                    self.main_engine.write_log(f"增量导入合约 {symbol}.{exchange.value}")

                # 导入新数据
                count = self.import_data_from_csv(
                        file_path=file_path,
                        symbol=symbol,
                        exchange=exchange,
                        interval=Interval.MINUTE,
                        tz_name="Asia/Shanghai",
                        datetime_head="datetime",
                        open_head="open",
                        high_head="high",
                        low_head="low",
                        close_head="close",
                        volume_head="volume",
                        turnover_head="turnover",
                        open_interest_head="open_interest",
                        datetime_format="%Y-%m-%d %H:%M:%S"
                    )

                self.main_engine.write_log(f"成功导入合约 {symbol}.{exchange.value}，数据条数: {count}")
                imported_count += 1

            except Exception as e:
                self.main_engine.write_log(f"导入合约 {symbol} 时出错: {str(e)}")
                continue

        return imported_count

    def download_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        output: Callable
    ) -> int:
        """
        Query tick data from datafeed.
        """
        req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            start=start,
            end=datetime.now(DB_TZ)
        )

        data: list[TickData] = self.datafeed.query_tick_history(req, output)

        if data:
            self.database.save_tick_data(data)
            return (len(data))

        return 0

    def run_data_update_pipeline(
        self,
        source_dir: str = None,
        target_dir: str = None,
        auto_aggregate: bool = True,
        force_update: bool = False
    ) -> Dict[str, int]:
        """
        运行完整的数据更新流水线：检测新文件 -> 转换导入 -> 聚合生成高周期数据

        Args:
            source_dir: .lc1文件源目录
            target_dir: CSV文件目标目录
            auto_aggregate: 是否自动聚合生成高周期数据
            force_update: 是否强制更新所有数据

        Returns:
            Dict[str, int]: 处理统计信息
        """
        stats = {
            "converted_files": 0,
            "imported_contracts": 0,
            "aggregated_hourly": 0,
            "aggregated_daily": 0,
            "errors": 0
        }

        try:
            self.main_engine.write_log("=== 开始数据更新流水线 ===")

            # 1. 检测并转换新文件
            if source_dir:
                self.main_engine.write_log("步骤1: 检测和转换新文件...")
                converted = self._process_new_files(source_dir, target_dir, force_update)
                stats["converted_files"] = converted

            # 2. 自动导入转换后的数据
            if target_dir and os.path.exists(target_dir):
                self.main_engine.write_log("步骤2: 导入转换后的数据...")
                imported = self._auto_import_converted_data(target_dir, force_update)
                stats["imported_contracts"] = imported

            # 3. 聚合生成高周期数据
            if auto_aggregate:
                self.main_engine.write_log("步骤3: 聚合生成高周期数据...")
                hourly_count, daily_count = self._auto_aggregate_data(force_update)
                stats["aggregated_hourly"] = hourly_count
                stats["aggregated_daily"] = daily_count

            self.main_engine.write_log(
                f"=== 数据更新流水线完成 ===\n"
                f"转换文件: {stats['converted_files']} 个\n"
                f"导入合约: {stats['imported_contracts']} 个\n"
                f"聚合小时线: {stats['aggregated_hourly']} 条\n"
                f"聚合日线: {stats['aggregated_daily']} 条"
            )

        except Exception as e:
            self.main_engine.write_log(f"数据更新流水线出错: {str(e)}")
            stats["errors"] += 1

        return stats

    def _process_new_files(self, source_dir: str, target_dir: str = None, force_update: bool = False) -> int:
        """处理新的.lc1文件"""
        pending_files = self.scheduler.get_pending_files(source_dir)
        if not pending_files:
            self.main_engine.write_log("没有发现新的.lc1文件需要处理")
            return 0

        self.main_engine.write_log(f"发现 {len(pending_files)} 个新文件待处理")

        # 使用现有的批量转换功能
        converted_count = self.download_bar_data_batch(
            source_dir=source_dir,
            target_dir=target_dir,
            convert_to_vnpy_format=True,
            auto_import=False  # 不自动导入，我们手动控制
        )

        # 标记已处理的文件
        for file_path in pending_files:
            if os.path.exists(file_path):  # 确保文件仍然存在
                self.scheduler.update_status(file_path=file_path)

        return converted_count

    def _auto_aggregate_data(self, force_update: bool = False) -> Tuple[int, int]:
        """自动聚合所有合约的高周期数据"""
        try:
            from vnpy_chartwizard.preprocess_daily_data import BarDataPreprocessor
        except ImportError:
            self.main_engine.write_log("无法导入BarDataPreprocessor，跳过聚合步骤")
            return 0, 0

        preprocessor = BarDataPreprocessor(self.scheduler.db_path)

        # 获取所有有1分钟数据的合约
        symbols = preprocessor.get_available_symbols()
        if not symbols:
            self.main_engine.write_log("没有找到有1分钟数据的合约")
            return 0, 0

        self.main_engine.write_log(f"开始聚合 {len(symbols)} 个合约的高周期数据")

        total_hourly = 0
        total_daily = 0

        for symbol, exchange, count in symbols:
            try:
                self.main_engine.write_log(f"正在聚合 {symbol} ({exchange})...")

                # 总是执行聚合，只要有数据就处理
                # 去掉时间检查逻辑，强制执行聚合

                # 聚合小时线数据
                hourly_count = preprocessor.aggregate_hourly_data(symbol, exchange, force_update)
                total_hourly += hourly_count

                # 聚合日线数据
                daily_count = preprocessor.aggregate_daily_data(symbol, exchange, force_update)
                total_daily += daily_count

                # 更新状态
                self.scheduler.update_status(contract=f"{symbol}_{exchange}")

                if hourly_count > 0 or daily_count > 0:
                    self.main_engine.write_log(f"  {symbol} 完成 - 小时线: {hourly_count}, 日线: {daily_count}")

            except Exception as e:
                self.main_engine.write_log(f"聚合 {symbol} ({exchange}) 时出错: {str(e)}")
                continue

        return total_hourly, total_daily

    def get_data_update_status(self) -> Dict:
        """获取数据更新状态"""
        return self.scheduler.get_status()

    def check_for_updates(self, source_dir: str) -> Dict[str, List[str]]:
        """检查数据更新情况"""
        pending_files = self.scheduler.get_pending_files(source_dir)

        # 检查需要更新的合约（基于时间戳）
        status = self.scheduler.get_status()
        contracts_needing_update = []

        try:
            conn = sqlite3.connect(self.scheduler.db_path)
            cursor = conn.cursor()

            # 获取所有有数据的合约
            cursor.execute("""
                SELECT DISTINCT symbol, exchange, MAX(datetime) as last_data_time
                FROM dbbardata
                WHERE interval = '1m'
                GROUP BY symbol, exchange
            """)

            for row in cursor.fetchall():
                symbol, exchange, last_data_time = row
                contract_key = f"{symbol}_{exchange}"

                # 检查是否需要更新（如果最后数据时间超过24小时）
                try:
                    last_data_dt = datetime.fromisoformat(last_data_time.replace(' ', 'T'))
                    if datetime.now() - last_data_dt > timedelta(hours=24):
                        contracts_needing_update.append(f"{symbol}.{exchange}")
                except (ValueError, AttributeError):
                    continue

        except sqlite3.Error as e:
            self.main_engine.write_log(f"检查合约更新状态时出错: {str(e)}")
        finally:
            if 'conn' in locals():
                conn.close()

        return {
            "pending_files": pending_files,
            "contracts_needing_update": contracts_needing_update
        }
