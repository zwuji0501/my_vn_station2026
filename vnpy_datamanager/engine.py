import csv
import os
import json
import sqlite3
from datetime import datetime, timedelta
from collections.abc import Callable
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import pandas as pd
except ImportError:
    pd = None

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
        # 获取数据库路径
        db_path = self._get_db_path()
        if not db_path:
            self.main_engine.write_log("无法获取数据库路径，跳过聚合步骤")
            return 0, 0

        # 获取所有有1分钟数据的合约
        symbols = self._get_available_symbols(db_path)
        if not symbols:
            self.main_engine.write_log("没有找到有1分钟数据的合约")
            return 0, 0

        self.main_engine.write_log(f"开始聚合 {len(symbols)} 个合约的高周期数据")

        total_hourly = 0
        total_daily = 0

        for symbol, exchange, count in symbols:
            try:
                self.main_engine.write_log(f"正在聚合 {symbol} ({exchange})...")

                # 聚合小时线数据
                hourly_count = self._aggregate_hourly_data(db_path, symbol, exchange, force_update)
                total_hourly += hourly_count

                # 聚合日线数据
                daily_count = self._aggregate_daily_data(db_path, symbol, exchange, force_update)
                total_daily += daily_count

                # 更新状态
                self.scheduler.update_status(contract=f"{symbol}_{exchange}")

                if hourly_count > 0 or daily_count > 0:
                    self.main_engine.write_log(f"  {symbol} 完成 - 小时线: {hourly_count}, 日线: {daily_count}")

            except Exception as e:
                self.main_engine.write_log(f"聚合 {symbol} ({exchange}) 时出错: {str(e)}")
                continue

        return total_hourly, total_daily

    def _get_db_path(self) -> str:
        """获取数据库路径"""
        current_dir = os.getcwd()
        vntrader_dir = os.path.join(current_dir, ".vntrader")
        if os.path.exists(vntrader_dir):
            return os.path.join(vntrader_dir, "database.db")

        home_dir = os.path.expanduser("~")
        vntrader_dir = os.path.join(home_dir, ".vntrader")
        return os.path.join(vntrader_dir, "database.db")

    def _get_available_symbols(self, db_path: str) -> list:
        """获取数据库中所有有1分钟数据的合约"""
        import sqlite3

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 查询有1分钟数据的所有合约
        cursor.execute("""
            SELECT DISTINCT symbol, exchange, COUNT(*) as count
            FROM dbbardata
            WHERE interval = '1m'
            GROUP BY symbol, exchange
            ORDER BY symbol
        """)

        symbols = cursor.fetchall()
        conn.close()

        return symbols

    def _aggregate_hourly_data(self, db_path: str, symbol: str, exchange: str, force_update: bool = False) -> int:
        """
        将指定合约的1分钟数据聚合为小时线数据

        Args:
            db_path: 数据库路径
            symbol: 合约代码
            exchange: 交易所
            force_update: 是否强制更新已存在的小时线数据

        Returns:
            int: 新增的小时线数据条数
        """
        import sqlite3

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        try:
            # 检查是否已有小时线数据，并决定聚合策略
            cursor.execute("""
                SELECT COUNT(*) FROM dbbardata
                WHERE symbol = ? AND exchange = ? AND interval = '1h'
            """, (symbol, exchange))

            existing_count = cursor.fetchone()[0]

            if existing_count > 0 and force_update:
                self.main_engine.write_log(f"  {symbol} 已存在 {existing_count} 条小时线数据，将重新生成")
                # 删除现有的小时线数据
                cursor.execute("""
                    DELETE FROM dbbardata
                    WHERE symbol = ? AND exchange = ? AND interval = '1h'
                """, (symbol, exchange))
                conn.commit()
                existing_count = 0

            if existing_count == 0:
                self.main_engine.write_log(f"  {symbol} 首次生成小时线数据")
            else:
                self.main_engine.write_log(f"  {symbol} 跳过小时线聚合（已存在数据）")
                conn.close()
                return 0

            # 查询该合约的1分钟数据
            cursor.execute("""
                SELECT datetime, open_price, high_price, low_price, close_price, volume
                FROM dbbardata
                WHERE symbol = ? AND exchange = ? AND interval = '1m'
                ORDER BY datetime
            """, (symbol, exchange))

            rows = cursor.fetchall()

            if not rows:
                self.main_engine.write_log(f"  {symbol} 没有找到1分钟数据")
                conn.close()
                return 0

            self.main_engine.write_log(f"  找到 {len(rows)} 条1分钟数据，开始聚合小时线...")

            # 聚合小时线数据
            hourly_data = []
            hourly_high = 0
            hourly_low = 999999999999999
            hourly_volume = 0
            index = 0
            current_hour = None

            for row in rows:
                datetime_str, open_price, high_price, low_price, close_price, volume = row

                # 解析日期和时间
                dt = datetime.fromisoformat(datetime_str.replace(' ', 'T'))
                hour_key = dt.strftime('%Y%m%d %H')  # 按小时分组

                # 如果是新的小时，重置计数器
                if current_hour != hour_key:
                    if index > 0:  # 保存上一小时的数据
                        hourly_datetime = datetime.strptime(f"{current_hour}:00:00", '%Y%m%d %H:%M:%S')
                        hourly_data.append({
                            'symbol': symbol,
                            'exchange': exchange,
                            'datetime': hourly_datetime.isoformat(' ', 'seconds'),
                            'interval': '1h',
                            'volume': hourly_volume,
                            'turnover': 0.0,
                            'open_interest': 0.0,
                            'open_price': hourly_open,
                            'high_price': hourly_high,
                            'low_price': hourly_low,
                            'close_price': hourly_close
                        })

                    # 初始化新小时
                    current_hour = hour_key
                    hourly_open = float(open_price)
                    hourly_high = float(high_price)
                    hourly_low = float(low_price)
                    hourly_close = float(close_price)
                    hourly_volume = float(volume)
                    index = 1
                else:
                    # 更新当前小时的数据
                    hourly_high = max(float(high_price), hourly_high)
                    hourly_low = min(float(low_price), hourly_low)
                    hourly_close = float(close_price)
                    hourly_volume += float(volume)
                    index += 1

            # 保存最后一个小时的数据
            if index > 0 and current_hour:
                hourly_datetime = datetime.strptime(f"{current_hour}:00:00", '%Y%m%d %H:%M:%S')
                hourly_data.append({
                    'symbol': symbol,
                    'exchange': exchange,
                    'datetime': hourly_datetime.isoformat(' ', 'seconds'),
                    'interval': '1h',
                    'volume': hourly_volume,
                    'turnover': 0.0,
                    'open_interest': 0.0,
                    'open_price': hourly_open,
                    'high_price': hourly_high,
                    'low_price': hourly_low,
                    'close_price': hourly_close
                })

            # 批量插入小时线数据
            if hourly_data:
                cursor.executemany("""
                    INSERT INTO dbbardata
                    (symbol, exchange, datetime, interval, volume, turnover, open_interest,
                     open_price, high_price, low_price, close_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [(d['symbol'], d['exchange'], d['datetime'], d['interval'],
                       d['volume'], d['turnover'], d['open_interest'],
                       d['open_price'], d['high_price'], d['low_price'], d['close_price'])
                      for d in hourly_data])

                conn.commit()
                self.main_engine.write_log(f"  成功插入 {len(hourly_data)} 条小时线数据")

                # 更新dbbaroverview表
                cursor.execute("""
                    SELECT COUNT(*), MIN(datetime), MAX(datetime)
                    FROM dbbardata
                    WHERE symbol = ? AND exchange = ? AND interval = '1h'
                """, (symbol, exchange))

                count, start_date, end_date = cursor.fetchone()

                # 检查是否已存在概览记录
                cursor.execute("""
                    SELECT id FROM dbbaroverview
                    WHERE symbol = ? AND exchange = ? AND interval = '1h'
                """, (symbol, exchange))

                existing = cursor.fetchone()

                if existing:
                    # 更新现有记录
                    cursor.execute("""
                        UPDATE dbbaroverview
                        SET count = ?, start = ?, end = ?
                        WHERE symbol = ? AND exchange = ? AND interval = '1h'
                    """, (count, start_date, end_date, symbol, exchange))
                else:
                    # 插入新记录
                    cursor.execute("""
                        INSERT INTO dbbaroverview (symbol, exchange, interval, count, start, end)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (symbol, exchange, '1h', count, start_date, end_date))

                conn.commit()
                self.main_engine.write_log(f"  更新了dbbaroverview表")

            conn.close()
            return len(hourly_data)

        except Exception as e:
            self.main_engine.write_log(f"小时线聚合出错: {str(e)}")
            conn.close()
            return 0

    def _aggregate_daily_data(self, db_path: str, symbol: str, exchange: str, force_update: bool = False) -> int:
        """
        将指定合约的1分钟数据聚合为日线数据

        Args:
            db_path: 数据库路径
            symbol: 合约代码
            exchange: 交易所
            force_update: 是否强制更新已存在的日线数据

        Returns:
            int: 新增的日线数据条数
        """
        import sqlite3

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        try:
            # 检查是否已有日线数据，并决定聚合策略
            cursor.execute("""
                SELECT COUNT(*) FROM dbbardata
                WHERE symbol = ? AND exchange = ? AND interval = 'd'
            """, (symbol, exchange))

            existing_count = cursor.fetchone()[0]

            if existing_count > 0 and force_update:
                self.main_engine.write_log(f"  {symbol} 已存在 {existing_count} 条日线数据，将重新生成")
                # 删除现有的日线数据
                cursor.execute("""
                    DELETE FROM dbbardata
                    WHERE symbol = ? AND exchange = ? AND interval = 'd'
                """, (symbol, exchange))
                conn.commit()
                existing_count = 0

            if existing_count == 0:
                self.main_engine.write_log(f"  {symbol} 首次生成日线数据")
            else:
                self.main_engine.write_log(f"  {symbol} 跳过日线聚合（已存在数据）")
                conn.close()
                return 0

            # 查询该合约的1分钟数据
            cursor.execute("""
                SELECT datetime, open_price, high_price, low_price, close_price, volume
                FROM dbbardata
                WHERE symbol = ? AND exchange = ? AND interval = '1m'
                ORDER BY datetime
            """, (symbol, exchange))

            rows = cursor.fetchall()

            if not rows:
                self.main_engine.write_log(f"  {symbol} 没有找到1分钟数据")
                conn.close()
                return 0

            self.main_engine.write_log(f"  找到 {len(rows)} 条1分钟数据，开始聚合日线...")

            # 聚合日线数据
            daily_data = []
            daily_high = 0
            daily_low = 999999999999999
            daily_volume = 0
            index = 0
            last_data = None

            for row in rows:
                datetime_str, open_price, high_price, low_price, close_price, volume = row

                # 解析日期和时间
                dt = datetime.fromisoformat(datetime_str.replace(' ', 'T'))
                date_str = dt.strftime('%Y%m%d')
                time_str = dt.strftime('%H:%M:%S')

                d = [date_str, time_str, str(int(dt.timestamp())),
                     open_price, high_price, low_price, close_price, volume]

                if index == 0:
                    daily_open = float(d[3])
                daily_high = max(float(d[4]), daily_high)
                daily_low = min(float(d[5]), daily_low)
                daily_close = float(d[6])
                daily_volume += float(d[7])
                daily_date = d[0] + ' 15:00:00'
                index += 1

                # 关键逻辑：当时间为14:59:00时，认为这是日线的收盘时刻
                if d[1] == '14:59:00':
                    daily_close = float(d[6])
                    daily_date = d[0] + ' 15:00:00'
                    daily_datetime = datetime.strptime(daily_date, '%Y%m%d %H:%M:%S')

                    daily_data.append({
                        'symbol': symbol,
                        'exchange': exchange,
                        'datetime': daily_datetime.isoformat(' ', 'seconds'),
                        'interval': 'd',
                        'volume': daily_volume,
                        'turnover': 0.0,  # 日线数据通常没有turnover
                        'open_interest': 0.0,  # 可以后续更新
                        'open_price': daily_open,
                        'high_price': daily_high,
                        'low_price': daily_low,
                        'close_price': daily_close
                    })

                    # 重置计数器
                    daily_high = 0
                    daily_low = 999999999999999
                    daily_volume = 0
                    index = 0

                last_data = d

            # 处理最后一天的数据（如果没有在14:59:00结束）
            if index != 0 and (not last_data or last_data[1] != '14:59:00'):
                daily_close = float(last_data[6])
                daily_date = last_data[0] + ' ' + last_data[1]
                daily_datetime = datetime.strptime(daily_date, '%Y%m%d %H:%M:%S')

                daily_data.append({
                    'symbol': symbol,
                    'exchange': exchange,
                    'datetime': daily_datetime.isoformat(' ', 'seconds'),
                    'interval': 'd',
                    'volume': daily_volume,
                    'turnover': 0.0,
                    'open_interest': 0.0,
                    'open_price': daily_open,
                    'high_price': daily_high,
                    'low_price': daily_low,
                    'close_price': daily_close
                })

            # 批量插入日线数据
            if daily_data:
                cursor.executemany("""
                    INSERT INTO dbbardata
                    (symbol, exchange, datetime, interval, volume, turnover, open_interest,
                     open_price, high_price, low_price, close_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [(d['symbol'], d['exchange'], d['datetime'], d['interval'],
                       d['volume'], d['turnover'], d['open_interest'],
                       d['open_price'], d['high_price'], d['low_price'], d['close_price'])
                      for d in daily_data])

                conn.commit()
                self.main_engine.write_log(f"  成功插入 {len(daily_data)} 条日线数据")

                # 更新dbbaroverview表
                cursor.execute("""
                    SELECT COUNT(*), MIN(datetime), MAX(datetime)
                    FROM dbbardata
                    WHERE symbol = ? AND exchange = ? AND interval = 'd'
                """, (symbol, exchange))

                count, start_date, end_date = cursor.fetchone()

                # 检查是否已存在概览记录
                cursor.execute("""
                    SELECT id FROM dbbaroverview
                    WHERE symbol = ? AND exchange = ? AND interval = 'd'
                """, (symbol, exchange))

                existing = cursor.fetchone()

                if existing:
                    # 更新现有记录
                    cursor.execute("""
                        UPDATE dbbaroverview
                        SET count = ?, start = ?, end = ?
                        WHERE symbol = ? AND exchange = ? AND interval = 'd'
                    """, (count, start_date, end_date, symbol, exchange))
                else:
                    # 插入新记录
                    cursor.execute("""
                        INSERT INTO dbbaroverview (symbol, exchange, interval, count, start, end)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (symbol, exchange, 'd', count, start_date, end_date))

                conn.commit()
                self.main_engine.write_log(f"  更新了dbbaroverview表")

            conn.close()
            return len(daily_data)

        except Exception as e:
            self.main_engine.write_log(f"日线聚合出错: {str(e)}")
            conn.close()
            return 0

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

    def _get_contract_exchange(self, contract_name: str) -> str:
        """从contract_attribute.json文件中获取合约的交易所信息"""
        try:
            # 合约属性文件路径
            contract_file = r'C:\new_tdxqh\vipdoc\ds\minline\csv\contract_attribute.json'

            if not os.path.exists(contract_file):
                self.main_engine.write_log(f"合约属性文件不存在: {contract_file}")
                return "UNKNOWN"

            with open(contract_file, 'r', encoding='utf-8') as f:
                contract_data = json.load(f)

            # 尝试多种方式查找合约
            # 1. 直接用合约名查找
            if contract_name in contract_data:
                return contract_data[contract_name].get("exchange", "UNKNOWN")

            # 2. 尝试提取品种代码（字母部分）
            commodity_code = ''.join([char for char in contract_name if char.isalpha()])
            if commodity_code and commodity_code in contract_data:
                return contract_data[commodity_code].get("exchange", "UNKNOWN")

            # 3. 对于一些特殊的合约格式，尝试不同的匹配方式
            # 例如：a8888 -> a, rb8888 -> rb
            if contract_name.endswith('8888'):
                base_code = contract_name[:-4]  # 移除末尾的8888
                if base_code in contract_data:
                    return contract_data[base_code].get("exchange", "UNKNOWN")

            self.main_engine.write_log(f"未找到合约 {contract_name} 的交易所信息")
            return "UNKNOWN"

        except Exception as e:
            self.main_engine.write_log(f"读取合约属性文件时出错: {str(e)}")
            return "UNKNOWN"

    def batch_process_csv_data(self, source_dir: str = None, target_dir: str = None) -> Dict[str, int]:
        """
        批量处理CSV数据，生成多周期文件（不导入数据库）

        Args:
            source_dir: .lc1文件源目录
            target_dir: CSV文件目标目录

        Returns:
            Dict[str, int]: 处理统计信息
        """
        if pd is None:
            self.main_engine.write_log("错误：pandas不可用，无法进行数据聚合")
            return {"processed_contracts": 0, "generated_files": 0, "errors": 1}

        import os

        stats = {
            "processed_contracts": 0,
            "generated_files": 0,
            "errors": 0
        }

        try:
            self.main_engine.write_log("=== 开始批量处理CSV数据 ===")

            # 设置默认路径
            if source_dir is None:
                source_dir = r'C:\new_tdxqh\vipdoc\ds\minline'
            if target_dir is None:
                target_dir = r'C:\new_tdxqh\vipdoc\ds\minline\csv'

            # 确保目标目录存在
            os.makedirs(target_dir, exist_ok=True)

            # 定义需要生成的周期
            intervals = {
                '1m': 1,
                '5m': 5,
                '15m': 15,
                '30m': 30,
                '1h': 60,  # 60分钟
                '4h': 240,  # 4小时 = 240分钟
                'd': 1440   # 日线 = 1440分钟
            }

            # 获取所有vnpy格式的CSV文件
            csv_files = []
            if os.path.exists(target_dir):
                for file in os.listdir(target_dir):
                    if file.endswith('_vnpy_import.csv'):
                        csv_files.append(os.path.join(target_dir, file))

            self.main_engine.write_log(f"找到 {len(csv_files)} 个待处理的CSV文件")

            for csv_file in csv_files:
                try:
                    contract_name = os.path.basename(csv_file).replace('_vnpy_import.csv', '')

                    # 获取交易所信息
                    exchange = self._get_contract_exchange(contract_name)
                    self.main_engine.write_log(f"正在处理合约: {contract_name} ({exchange})")

                    # 读取原始数据
                    df = pd.read_csv(csv_file)

                    # 检查必要的列
                    required_cols = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'open_interest']
                    if not all(col in df.columns for col in required_cols):
                        self.main_engine.write_log(f"跳过 {contract_name}：缺少必要列")
                        continue

                    # 转换时间列
                    df['datetime'] = pd.to_datetime(df['datetime'])
                    df = df.sort_values('datetime').reset_index(drop=True)

                    # 检查输入数据的最大时间，用于判断是否需要接续
                    input_max_time = df['datetime'].max()
                    self.main_engine.write_log(f"输入数据时间范围: {df['datetime'].min()} 到 {input_max_time}")

                    # 检查是否已有部分周期文件，准备接续数据
                    existing_data = {}
                    has_existing_data = False

                    for interval_name in intervals.keys():
                        output_file = os.path.join(target_dir, f"{contract_name}_{exchange}_{interval_name}.csv")
                        if os.path.exists(output_file):
                            try:
                                existing_df = pd.read_csv(output_file)
                                if not existing_df.empty:
                                    existing_df['datetime'] = pd.to_datetime(existing_df['datetime'])
                                    existing_data[interval_name] = existing_df
                                    has_existing_data = True
                                    self.main_engine.write_log(f"发现现有数据: {contract_name}_{exchange}_{interval_name}.csv ({len(existing_df)} 条)")
                            except Exception as e:
                                self.main_engine.write_log(f"读取现有文件 {output_file} 失败: {str(e)}")

                    if has_existing_data:
                        self.main_engine.write_log(f"合约 {contract_name} 将接续现有数据进行处理")

                    # 为每个周期生成数据
                    for interval_name, minutes in intervals.items():
                        output_file = os.path.join(target_dir, f"{contract_name}_{exchange}_{interval_name}.csv")

                        # 生成对应周期的数据
                        if interval_name == '1m':
                            # 1分钟数据直接使用
                            interval_df = df.copy()
                        elif interval_name == 'd':
                            # 日线数据
                            interval_df = self._aggregate_to_daily(df)
                        else:
                            # 其他分钟线数据
                            interval_df = self._resample_data(df, minutes)

                        if interval_df is not None and not interval_df.empty:
                            # 如果有现有数据，用新数据替换重叠部分
                            if interval_name in existing_data:
                                existing_df = existing_data[interval_name]
                                # 使用_merge_dataframes方法，用新数据替换重叠部分
                                combined_df = self._merge_dataframes(existing_df, interval_df)
                                if combined_df is not None:
                                    interval_df = combined_df
                                    self.main_engine.write_log(f"合并数据: {contract_name}_{exchange}_{interval_name} (原有: {len(existing_df)}, 新数据: {len(interval_df) - len(existing_df) + len(existing_df)})")
                                else:
                                    self.main_engine.write_log(f"数据合并失败，使用新数据: {contract_name}_{exchange}_{interval_name}")
                            else:
                                self.main_engine.write_log(f"生成新文件: {contract_name}_{exchange}_{interval_name}")

                            # 保存到CSV
                            interval_df.to_csv(output_file, index=False, encoding='utf-8')
                            stats["generated_files"] += 1
                            self.main_engine.write_log(f"保存文件: {os.path.basename(output_file)} ({len(interval_df)} 条数据)")

                    stats["processed_contracts"] += 1

                except Exception as e:
                    self.main_engine.write_log(f"处理 {os.path.basename(csv_file)} 时出错: {str(e)}")
                    stats["errors"] += 1
                    continue

            self.main_engine.write_log(
                f"=== 批量处理CSV完成 ===\n"
                f"处理合约: {stats['processed_contracts']} 个\n"
                f"生成文件: {stats['generated_files']} 个\n"
                f"错误数量: {stats['errors']} 个"
            )

        except Exception as e:
            self.main_engine.write_log(f"批量处理CSV时出错: {str(e)}")
            stats["errors"] += 1

        return stats

    def _merge_dataframes(self, existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        """合并现有数据和新数据，用新数据替换重叠部分"""
        try:
            if existing_df.empty:
                return new_df
            if new_df.empty:
                return existing_df

            # 确保datetime列格式一致
            existing_df['datetime'] = pd.to_datetime(existing_df['datetime'])
            new_df['datetime'] = pd.to_datetime(new_df['datetime'])

            # 获取新数据的开始和结束时间
            new_start_time = new_df['datetime'].min()
            new_end_time = new_df['datetime'].max()

            # 从现有数据中保留早于新数据开始时间的数据
            existing_before_new = existing_df[existing_df['datetime'] < new_start_time]

            # 从现有数据中保留晚于新数据结束时间的数据（如果有的话）
            existing_after_new = existing_df[existing_df['datetime'] > new_end_time]

            # 合并数据：原有早期数据 + 新数据 + 原有晚期数据
            combined_parts = []
            if not existing_before_new.empty:
                combined_parts.append(existing_before_new)
            combined_parts.append(new_df)
            if not existing_after_new.empty:
                combined_parts.append(existing_after_new)

            if combined_parts:
                combined_df = pd.concat(combined_parts, ignore_index=True)
                # 按时间排序并重置索引
                combined_df = combined_df.sort_values('datetime').reset_index(drop=True)

                self.main_engine.write_log(f"数据合并完成: 保留早期数据 {len(existing_before_new)} 条, 新数据 {len(new_df)} 条, 保留晚期数据 {len(existing_after_new)} 条")
                return combined_df
            else:
                return new_df

        except Exception as e:
            self.main_engine.write_log(f"数据合并失败: {str(e)}")
            return None

    def cleanup_intermediate_files(self, target_dir: str) -> int:
        """清理中间文件"""
        import os
        import glob

        cleanup_count = 0

        try:
            self.main_engine.write_log("=== 开始清理中间文件 ===")

            # 清理 _1min_1.csv 文件
            pattern1 = os.path.join(target_dir, "*_1min_1.csv")
            for file_path in glob.glob(pattern1):
                try:
                    os.remove(file_path)
                    self.main_engine.write_log(f"删除中间文件: {os.path.basename(file_path)}")
                    cleanup_count += 1
                except Exception as e:
                    self.main_engine.write_log(f"删除文件失败 {file_path}: {str(e)}")

            # 清理 _vnpy_import.csv 文件
            pattern2 = os.path.join(target_dir, "*_vnpy_import.csv")
            for file_path in glob.glob(pattern2):
                try:
                    os.remove(file_path)
                    self.main_engine.write_log(f"删除中间文件: {os.path.basename(file_path)}")
                    cleanup_count += 1
                except Exception as e:
                    self.main_engine.write_log(f"删除文件失败 {file_path}: {str(e)}")

            self.main_engine.write_log(f"=== 清理完成 === 删除 {cleanup_count} 个中间文件")

        except Exception as e:
            self.main_engine.write_log(f"清理中间文件时出错: {str(e)}")

        return cleanup_count

    def _resample_data(self, df: pd.DataFrame, minutes: int) -> pd.DataFrame:
        """使用BarGenerator重采样数据到指定分钟周期"""
        try:
            from vnpy.trader.utility import BarGenerator
            from vnpy.trader.object import BarData
            from vnpy.trader.constant import Interval

            # 存储生成的bar数据
            generated_bars = []

            def on_bar(bar: BarData) -> None:
                """BarGenerator回调函数"""
                bar_dict = {
                    'datetime': bar.datetime,
                    'open': bar.open_price,
                    'high': bar.high_price,
                    'low': bar.low_price,
                    'close': bar.close_price,
                    'volume': bar.volume,
                    'turnover': bar.turnover,
                    'open_interest': bar.open_interest
                }
                generated_bars.append(bar_dict)

            # 根据分钟数选择不同的BarGenerator配置
            if minutes >= 60:  # 小时线及以上
                # 对于小时线，使用HOUR interval和相应的window
                if minutes == 60:  # 1小时
                    bg = BarGenerator(
                        on_bar=lambda x: None,  # 小时线不需要分钟级回调
                        window=1,  # 1小时
                        on_window_bar=on_bar,
                        interval=Interval.HOUR
                    )
                elif minutes == 240:  # 4小时
                    bg = BarGenerator(
                        on_bar=lambda x: None,
                        window=4,  # 4小时
                        on_window_bar=on_bar,
                        interval=Interval.HOUR
                    )
                else:
                    # 其他小时数，直接使用分钟线方式
                    bg = BarGenerator(
                        on_bar=on_bar,
                        window=minutes,
                        on_window_bar=on_bar,
                        interval=Interval.MINUTE
                    )
            else:  # 分钟线
                # 对于分钟线聚合，window参数表示多少分钟合成一个bar
                bg = BarGenerator(
                    on_bar=on_bar,
                    window=minutes,
                    on_window_bar=on_bar,
                    interval=Interval.MINUTE
                )

            # 将DataFrame数据转换为BarData对象并传入BarGenerator
            for _, row in df.iterrows():
                # 创建BarData对象（分钟线数据）
                bar = BarData(
                    symbol="temp",  # 临时symbol
                    exchange=Exchange.SHFE,  # 使用SHFE作为临时exchange  # 临时exchange
                    datetime=row['datetime'],
                    interval=Interval.MINUTE,
                    volume=row['volume'],
                    open_price=row['open'],
                    high_price=row['high'],
                    low_price=row['low'],
                    close_price=row['close'],
                    turnover=row.get('turnover', 0.0),
                    open_interest=row.get('open_interest', 0.0),
                    gateway_name="CSV"
                )

                # 传入BarGenerator
                bg.update_bar(bar)

            # 生成最后的bar（如果有未完成的）
            bg.generate()

            if generated_bars:
                result_df = pd.DataFrame(generated_bars)
                # 确保datetime列是datetime类型
                result_df['datetime'] = pd.to_datetime(result_df['datetime'])
                return result_df
            else:
                return pd.DataFrame()

        except Exception as e:
            self.main_engine.write_log(f"使用BarGenerator重采样数据时出错: {str(e)}")
            return None

    def _aggregate_to_daily(self, df: pd.DataFrame) -> pd.DataFrame:
        """使用BarGenerator聚合为日线数据"""
        try:
            from vnpy.trader.utility import BarGenerator
            from vnpy.trader.object import BarData
            from vnpy.trader.constant import Interval
            from datetime import time

            # 存储生成的日线数据
            generated_bars = []

            def on_daily_bar(bar: BarData) -> None:
                """日线BarGenerator回调函数"""
                # 将日线时间设置为15:00:00（按照用户要求）
                daily_datetime = bar.datetime.replace(hour=15, minute=0, second=0, microsecond=0)
                bar_dict = {
                    'datetime': daily_datetime,
                    'open': bar.open_price,
                    'high': bar.high_price,
                    'low': bar.low_price,
                    'close': bar.close_price,
                    'volume': bar.volume,
                    'turnover': bar.turnover,
                    'open_interest': bar.open_interest
                }
                generated_bars.append(bar_dict)

            # 创建日线BarGenerator
            # 对于日线，需要设置daily_end为14:59:00（或15:00:00）
            bg = BarGenerator(
                on_bar=lambda x: None,  # 日线不需要分钟级回调
                window=0,  # 日线不需要窗口
                on_window_bar=on_daily_bar,
                interval=Interval.DAILY,
                daily_end=time(14, 59, 0)  # 14:59:00作为日线收盘时间
            )

            # 将DataFrame数据转换为BarData对象并传入BarGenerator
            for _, row in df.iterrows():
                # 创建BarData对象（分钟线数据）
                bar = BarData(
                    symbol="temp",  # 临时symbol
                    exchange=Exchange.SHFE,  # 使用SHFE作为临时exchange  # 临时exchange
                    datetime=row['datetime'],
                    interval=Interval.MINUTE,
                    volume=row['volume'],
                    open_price=row['open'],
                    high_price=row['high'],
                    low_price=row['low'],
                    close_price=row['close'],
                    turnover=row.get('turnover', 0.0),
                    open_interest=row.get('open_interest', 0.0),
                    gateway_name="CSV"
                )

                # 传入BarGenerator
                bg.update_bar(bar)

            if generated_bars:
                result_df = pd.DataFrame(generated_bars)
                # 确保datetime列是datetime类型
                result_df['datetime'] = pd.to_datetime(result_df['datetime'])
                return result_df
            else:
                return pd.DataFrame()

        except Exception as e:
            self.main_engine.write_log(f"使用BarGenerator聚合日线数据时出错: {str(e)}")
            return None
