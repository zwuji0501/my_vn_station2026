from datetime import datetime
from threading import Thread

from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.constant import Interval
from vnpy.trader.object import BarData, HistoryRequest, ContractData
from vnpy.trader.utility import extract_vt_symbol
from vnpy.trader.database import get_database, BaseDatabase
from vnpy.trader.datafeed import get_datafeed, BaseDatafeed


APP_NAME = "ChartWizard"

EVENT_CHART_HISTORY = "eChartHistory"


class ChartWizardEngine(BaseEngine):
    """
    For running chartWizard.
    """

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.datafeed: BaseDatafeed = get_datafeed()
        self.database: BaseDatabase = get_database()

    def query_history(
        self,
        vt_symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> None:
        """"""
        thread: Thread = Thread(
            target=self._query_history,
            args=[vt_symbol, interval, start, end]
        )
        thread.start()

    def _query_history(
        self,
        vt_symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> None:
        """"""
        symbol, exchange = extract_vt_symbol(vt_symbol)

        req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start=start,
            end=end
        )

        contract: ContractData | None = self.main_engine.get_contract(vt_symbol)
        if contract:
            if contract.history_data:
                self.main_engine.write_log(f"ChartWizard: 从网关 {contract.gateway_name} 查询 {vt_symbol} 历史数据")
                data: list[BarData] | None = self.main_engine.query_history(req, contract.gateway_name)
            else:
                self.main_engine.write_log(f"ChartWizard: 从数据源查询 {vt_symbol} 历史数据")
                data = self.datafeed.query_bar_history(req)
        else:
            self.main_engine.write_log(f"ChartWizard: 从数据库加载 {vt_symbol} ({symbol}.{exchange.value}) 历史数据")
            data = self.database.load_bar_data(
                symbol,
                exchange,
                interval,
                start,
                end
            )

        if data:
            self.main_engine.write_log(f"ChartWizard: 成功获取 {vt_symbol} 数据，共 {len(data)} 条")
        else:
            self.main_engine.write_log(f"ChartWizard: 未获取到 {vt_symbol} 数据")

        event: Event = Event(EVENT_CHART_HISTORY, data)
        self.event_engine.put(event)
