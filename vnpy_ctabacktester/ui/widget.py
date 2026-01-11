import platform
import csv
import os
import json
import shutil
import subprocess
from datetime import datetime, timedelta
from copy import copy
from pathlib import Path
from typing import Any, cast

import numpy as np
import pyqtgraph as pg
from pandas import DataFrame

from vnpy.trader.constant import Interval, Direction, Exchange
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import QtCore, QtWidgets, QtGui
from vnpy.trader.ui.widget import BaseMonitor, BaseCell, DirectionCell, EnumCell
from vnpy.event import Event, EventEngine
from vnpy.chart import ChartWidget, CandleItem, VolumeItem
from vnpy.trader.utility import load_json, save_json
from vnpy.trader.object import BarData, TradeData, OrderData
from vnpy.trader.database import DB_TZ
from vnpy_ctastrategy.backtesting import DailyResult

from ..locale import _
from ..engine import (
    APP_NAME,
    EVENT_BACKTESTER_LOG,
    EVENT_BACKTESTER_BACKTESTING_FINISHED,
    EVENT_BACKTESTER_OPTIMIZATION_FINISHED,
    OptimizationSetting,
    BacktesterEngine
)


class BacktesterManager(QtWidgets.QWidget):
    """"""

    setting_filename: str = "cta_backtester_setting.json"

    signal_log: QtCore.Signal = QtCore.Signal(Event)
    signal_backtesting_finished: QtCore.Signal = QtCore.Signal(Event)
    signal_optimization_finished: QtCore.Signal = QtCore.Signal(Event)

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__()

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine

        self.backtester_engine: BacktesterEngine = main_engine.get_engine(APP_NAME)     # type: ignore
        self.class_names: list = []
        self.settings: dict = {}

        self.target_display: str = ""

        # Save current backtesting options for event handling
        self.current_save_separately: bool = False
        self.current_class_name: str = ""
        self.current_vt_symbol: str = ""

        self.init_ui()
        self.register_event()
        self.backtester_engine.init_engine()
        self.init_strategy_settings()

        # Initialize symbol auto-completion before loading auto settings
        self.init_symbol_completion()

        # Load auto setting after strategy settings are initialized
        self.load_auto_setting()

    def init_symbol_completion(self) -> None:
        """Initialize auto-completion for symbol input."""
        self.symbol_completer = QtWidgets.QCompleter()
        self.symbol_line.setCompleter(self.symbol_completer)

        # Update completion when data source or interval changes
        self.data_source_combo.currentIndexChanged.connect(self.update_symbol_completion)
        self.interval_combo.currentIndexChanged.connect(self.update_symbol_completion)

        # Update contract attributes when symbol changes
        self.symbol_line.editingFinished.connect(self.update_contract_attributes)

        # Initial update
        self.update_symbol_completion()

    def update_symbol_completion(self) -> None:
        """Update symbol completion list based on current data source."""
        data_source = self.data_source_combo.currentData()

        if data_source == "csv":
            # Get symbols from CSV files filtered by current interval
            current_interval = self.interval_combo.currentText()
            symbols = self.get_csv_symbols_for_interval(current_interval)
        else:
            # For database mode, use some common symbols or empty list
            symbols = self.get_common_symbols()

        # Create completer model
        model = QtCore.QStringListModel(symbols)
        self.symbol_completer.setModel(model)
        self.symbol_completer.setCaseSensitivity(QtCore.Qt.CaseSensitivity.CaseInsensitive)

    def get_csv_symbols(self) -> list:
        """Get symbol list from CSV files in the CSV directory with exchange suffixes."""
        symbols = []
        csv_path = self.csv_path_line.text()

        if not csv_path or not os.path.exists(csv_path):
            return symbols

        try:
            # Load contract attributes to get exchange information
            contract_attributes = self._load_contract_attributes_for_completion()

            # Scan for CSV files with new naming format: symbol_exchange_interval.csv
            for filename in os.listdir(csv_path):
                if filename.endswith('.csv') and not filename.startswith('contract_attribute'):
                    # Parse new filename format: symbol_exchange_interval.csv
                    parts = filename[:-4].split('_')  # Remove .csv and split by _
                    if len(parts) >= 3:
                        symbol = parts[0]
                        exchange = parts[1]
                        interval_suffix = parts[2]

                        # Construct full vt_symbol
                        full_symbol = f"{symbol}.{exchange}"
                        if full_symbol not in symbols:
                            symbols.append(full_symbol)

            # Sort symbols for better UX
            symbols.sort()

        except Exception as e:
            print(f"Error scanning CSV files: {e}")

        return symbols

    def get_csv_symbols_for_interval(self, interval: str) -> list:
        """Get symbol list from CSV files for specific interval."""
        symbols = []
        csv_path = self.csv_path_line.text()

        if not csv_path or not os.path.exists(csv_path):
            return symbols

        try:
            # Load contract attributes to get exchange information
            contract_attributes = self._load_contract_attributes_for_completion()

            # Scan for CSV files with specific interval
            for filename in os.listdir(csv_path):
                if filename.endswith('.csv') and not filename.startswith('contract_attribute'):
                    # Parse new filename format: symbol_exchange_interval.csv
                    parts = filename[:-4].split('_')  # Remove .csv and split by _
                    if len(parts) >= 3:
                        symbol = parts[0]
                        exchange = parts[1]
                        file_interval = parts[2]

                        # Check if this file matches the requested interval
                        if file_interval == interval:
                            # Construct full vt_symbol
                            full_symbol = f"{symbol}.{exchange}"
                            if full_symbol not in symbols:
                                symbols.append(full_symbol)

            # Sort symbols for better UX
            symbols.sort()

        except Exception as e:
            print(f"Error scanning CSV files for interval {interval}: {e}")

        return symbols

    def update_contract_attributes(self) -> None:
        """Update contract attributes (size, price tick, slippage) based on selected symbol."""
        vt_symbol = self.symbol_line.text().strip()
        if not vt_symbol:
            return

        try:
            # Load contract attributes from JSON file
            contract_attributes = self._load_contract_attributes_for_completion()

            # Extract symbol code (remove exchange suffix and keep only letters)
            # For example: "rb8888.SHFE" -> "rb", "c8888.DCE" -> "c", "IF8888.CFFEX" -> "IF"
            symbol_code = ""
            if '.' in vt_symbol:
                base_symbol = vt_symbol.split('.')[0]

                # Extract only alphabetic characters from the beginning
                import re
                match = re.match(r'^([a-zA-Z]+)', base_symbol)
                if match:
                    symbol_code = match.group(1).lower()

                    # Special handling for certain futures that need specific casing
                    if symbol_code in ['if', 'ih', 'ic', 'im']:
                        symbol_code = symbol_code.upper()  # Index futures: IF, IH, IC, IM
                    elif symbol_code in ['t', 'tf', 'tl', 'ts']:
                        symbol_code = symbol_code.upper()  # Treasury futures: T, TF, TL, TS

            # Look up contract attributes
            if symbol_code in contract_attributes:
                contract_info = contract_attributes[symbol_code]
                size = contract_info.get('size', 1)
                price_tick = contract_info.get('priceTick', 0.01)

                # Update UI fields
                self.size_line.setText(str(size))
                self.pricetick_line.setText(str(price_tick))
                self.slippage_line.setText(str(price_tick))  # Default slippage to 1 price tick

                self.write_log(_("更新合约属性: {} - 乘数:{}, 跳动:{}, 滑点:{}").format(
                    vt_symbol, size, price_tick, price_tick))
            else:
                self.write_log(_("未找到合约属性: {} (尝试查找: {})").format(vt_symbol, symbol_code))

        except Exception as e:
            self.write_log(_("更新合约属性失败: {}").format(str(e)))

    def _load_contract_attributes_for_completion(self):
        """Load contract attributes for auto-completion."""
        contract_json_path = os.path.join(self.csv_path_line.text(), "contract_attribute.json")

        try:
            with open(contract_json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _get_full_symbol_with_exchange(self, symbol: str, contract_attributes: dict) -> str:
        """Get full symbol with exchange suffix."""
        if symbol in contract_attributes:
            contract_info = contract_attributes[symbol]
            exchange = contract_info.get('exchange')
            if exchange:
                return f"{symbol}.{exchange}"

        # Fallback: try to guess exchange based on common patterns
        return self._guess_exchange_for_symbol(symbol)

    def _guess_exchange_for_symbol(self, symbol: str) -> str:
        """Guess exchange for symbol based on common patterns."""
        # Common exchange patterns for Chinese futures
        if symbol.startswith(('IF', 'IH', 'IC', 'IM', 'T', 'TF', 'TL', 'TS')):
            return f"{symbol}.CFFEX"  # China Financial Futures Exchange
        elif symbol.startswith(('cu', 'al', 'zn', 'pb', 'ni', 'sn', 'au', 'ag')):
            return f"{symbol}.SHFE"   # Shanghai Futures Exchange
        elif symbol.startswith(('rb', 'hc', 'sp', 'wr', 'fu', 'bu', 'ao')):
            return f"{symbol}.SHFE"   # Shanghai Futures Exchange
        elif symbol.startswith(('c', 'cs', 'a', 'b', 'm', 'y', 'p', 'fb', 'bb', 'jd', 'lh', 'v', 'pp', 'l', 'i', 'j', 'jm')):
            return f"{symbol}.DCE"    # Dalian Commodity Exchange
        elif symbol.startswith(('SR', 'OI', 'RM', 'TA', 'FG', 'MA', 'CF', 'CY', 'PF', 'PK', 'PR', 'PX', 'SH', 'SF', 'SM', 'RS', 'OI', 'UR', 'SA', 'CJ', 'AP')):
            return f"{symbol}.CZCE"   # Zhengzhou Commodity Exchange
        elif symbol.startswith(('pt', 'pd', 'lc', 'ps', 'si')):
            return f"{symbol}.GFEX"   # Guangzhou Futures Exchange
        elif symbol.startswith(('sc', 'nr', 'lu', 'ec', 'bc')):
            return f"{symbol}.INE"    # Shanghai International Energy Exchange
        else:
            # Default fallback
            return f"{symbol}.SHFE"

    def get_common_symbols(self) -> list:
        """Get common symbol list for database mode."""
        # Return some common futures symbols
        return [
            "IF8888.CFFEX", "IH8888.CFFEX", "IC8888.CFFEX",
            "rb8888.SHFE", "hc8888.SHFE", "i8888.DCE",
            "j8888.DCE", "jm8888.DCE", "p8888.DCE",
            "y8888.DCE", "m8888.DCE", "a8888.DCE",
            "c8888.DCE", "cs8888.DCE", "jd8888.DCE"
        ]

    def init_strategy_settings(self) -> None:
        """"""
        self.class_names = self.backtester_engine.get_strategy_class_names()
        self.class_names.sort()

        for class_name in self.class_names:
            setting: dict = self.backtester_engine.get_default_setting(class_name)
            self.settings[class_name] = setting

        self.class_combo.addItems(self.class_names)

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle(_("CTA回测"))

        # Setting Part
        self.class_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()

        self.symbol_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit("IF88.CFFEX")

        # Data source selection
        self.data_source_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        self.data_source_combo.addItem(_("数据库"), "database")
        self.data_source_combo.addItem(_("本地CSV"), "csv")
        self.data_source_combo.currentIndexChanged.connect(self.on_data_source_changed)

        # CSV file path selection
        self.csv_path_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit(r"C:\new_tdxqh\vipdoc\ds\minline\csv")
        self.csv_path_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("选择路径"))
        self.csv_path_button.clicked.connect(self.select_csv_path)
        self.csv_path_line.setEnabled(False)
        self.csv_path_button.setEnabled(False)

        self.interval_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        # Add supported intervals including custom ones for CSV files
        supported_intervals = ["1m", "5m", "15m", "30m", "1h", "4h", "d"]
        for interval in supported_intervals:
            self.interval_combo.addItem(interval)

        end_dt: datetime = datetime.now()
        start_dt: datetime = end_dt - timedelta(days=3 * 365)

        self.start_date_edit: QtWidgets.QDateEdit = QtWidgets.QDateEdit(
            QtCore.QDate(
                start_dt.year,
                start_dt.month,
                start_dt.day
            )
        )
        self.end_date_edit: QtWidgets.QDateEdit = QtWidgets.QDateEdit(
            QtCore.QDate.currentDate()
        )

        self.rate_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit("0.000025")
        self.slippage_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit("0.2")
        self.size_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit("300")
        self.pricetick_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit("0.2")
        self.capital_line: QtWidgets.QLineEdit = QtWidgets.QLineEdit("1000000")

        # Save trades option
        self.save_trades_separately_checkbox: QtWidgets.QCheckBox = QtWidgets.QCheckBox(_("另存为其他交易记录"))
        self.save_trades_separately_checkbox.setToolTip(_("勾选后，回测结果将保存到以策略名和参数命名的单独文件夹中"))

        backtesting_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("开始回测"))
        backtesting_button.clicked.connect(self.start_backtesting)

        optimization_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("参数优化"))
        optimization_button.clicked.connect(self.start_optimization)

        self.result_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("优化结果"))
        self.result_button.clicked.connect(self.show_optimization_result)
        self.result_button.setEnabled(False)

        downloading_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("下载数据"))
        downloading_button.clicked.connect(self.start_downloading)

        clear_cache_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("清除缓存"))
        clear_cache_button.clicked.connect(self.clear_csv_cache)

        cache_info_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("缓存情况"))
        cache_info_button.clicked.connect(self.show_cache_info)

        stop_optimization_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("结束优化"))
        stop_optimization_button.clicked.connect(self.stop_optimization)

        self.order_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("委托记录"))
        self.order_button.clicked.connect(self.show_backtesting_orders)
        self.order_button.setEnabled(False)

        self.trade_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("成交记录"))
        self.trade_button.clicked.connect(self.show_backtesting_trades)
        self.trade_button.setEnabled(False)

        self.daily_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("每日盈亏"))
        self.daily_button.clicked.connect(self.show_daily_results)
        self.daily_button.setEnabled(False)

        self.candle_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("K线图表"))
        self.candle_button.clicked.connect(self.show_candle_chart)
        self.candle_button.setEnabled(False)

        edit_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("代码编辑"))
        edit_button.clicked.connect(self.edit_strategy_code)

        reload_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("策略重载"))
        reload_button.clicked.connect(self.reload_strategy_class)

        save_setting_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("保存条件"))
        save_setting_button.clicked.connect(self.save_backtesting_setting)

        load_setting_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("读取条件"))
        load_setting_button.clicked.connect(self.load_backtesting_setting)

        open_vntrader_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("打开.vntrader文件夹"))
        open_vntrader_button.clicked.connect(self.open_vntrader_folder)

        for button in [
            backtesting_button,
            optimization_button,
            stop_optimization_button,
            downloading_button,
            clear_cache_button,
            cache_info_button,
            self.result_button,
            self.order_button,
            self.trade_button,
            self.daily_button,
            self.candle_button,
            edit_button,
            reload_button,
            save_setting_button,
            load_setting_button,
            open_vntrader_button
        ]:
            button.setFixedHeight(button.sizeHint().height() * 2)

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form.addRow(_("交易策略"), self.class_combo)
        form.addRow(_("本地代码"), self.symbol_line)
        form.addRow(_("K线周期"), self.interval_combo)
        form.addRow(_("数据源"), self.data_source_combo)

        # CSV path row with button
        csv_path_layout = QtWidgets.QHBoxLayout()
        csv_path_layout.addWidget(self.csv_path_line)
        csv_path_layout.addWidget(self.csv_path_button)
        form.addRow(_("CSV路径"), csv_path_layout)

        form.addRow(_("开始日期"), self.start_date_edit)
        form.addRow(_("结束日期"), self.end_date_edit)
        form.addRow(_("手续费率"), self.rate_line)
        form.addRow(_("交易滑点"), self.slippage_line)
        form.addRow(_("合约乘数"), self.size_line)
        form.addRow(_("价格跳动"), self.pricetick_line)
        form.addRow(_("回测资金"), self.capital_line)

        result_grid: QtWidgets.QGridLayout = QtWidgets.QGridLayout()
        result_grid.addWidget(self.trade_button, 0, 0)
        result_grid.addWidget(self.order_button, 0, 1)
        result_grid.addWidget(self.daily_button, 1, 0)
        result_grid.addWidget(self.candle_button, 1, 1)

        # Setting buttons layout
        setting_buttons_layout: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        setting_buttons_layout.addWidget(save_setting_button)
        setting_buttons_layout.addWidget(load_setting_button)

        left_vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        left_vbox.addLayout(form)
        left_vbox.addLayout(setting_buttons_layout)
        left_vbox.addWidget(self.save_trades_separately_checkbox)
        left_vbox.addWidget(backtesting_button)
        left_vbox.addWidget(downloading_button)
        left_vbox.addWidget(clear_cache_button)
        left_vbox.addWidget(cache_info_button)
        left_vbox.addStretch()
        left_vbox.addWidget(optimization_button)
        left_vbox.addWidget(stop_optimization_button)
        left_vbox.addWidget(self.result_button)
        left_vbox.addStretch()
        left_vbox.addLayout(result_grid)
        left_vbox.addStretch()
        left_vbox.addWidget(optimization_button)
        left_vbox.addWidget(self.result_button)
        left_vbox.addStretch()
        left_vbox.addWidget(edit_button)
        left_vbox.addWidget(reload_button)
        left_vbox.addWidget(open_vntrader_button)

        # Result part
        self.statistics_monitor: StatisticsMonitor = StatisticsMonitor()

        self.log_monitor: QtWidgets.QTextEdit = QtWidgets.QTextEdit()

        self.chart: BacktesterChart = BacktesterChart()
        chart: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        chart.addWidget(self.chart)

        self.trade_dialog: BacktestingResultDialog = BacktestingResultDialog(
            self.main_engine,
            self.event_engine,
            _("回测成交记录"),
            BacktestingTradeMonitor
        )
        self.order_dialog: BacktestingResultDialog = BacktestingResultDialog(
            self.main_engine,
            self.event_engine,
            _("回测委托记录"),
            BacktestingOrderMonitor
        )
        self.daily_dialog: BacktestingResultDialog = BacktestingResultDialog(
            self.main_engine,
            self.event_engine,
            _("回测每日盈亏"),
            DailyResultMonitor
        )

        # Candle Chart
        self.candle_dialog: CandleChartDialog = CandleChartDialog()

        # Layout
        middle_vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        middle_vbox.addWidget(self.statistics_monitor)
        middle_vbox.addWidget(self.log_monitor)

        left_hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        left_hbox.addLayout(left_vbox)
        left_hbox.addLayout(middle_vbox)

        left_widget: QtWidgets.QWidget = QtWidgets.QWidget()
        left_widget.setLayout(left_hbox)

        right_vbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        right_vbox.addWidget(self.chart)

        right_widget: QtWidgets.QWidget = QtWidgets.QWidget()
        right_widget.setLayout(right_vbox)

        hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        hbox.addWidget(left_widget)
        hbox.addWidget(right_widget)
        self.setLayout(hbox)

    def on_data_source_changed(self) -> None:
        """Handle data source selection change"""
        data_source = self.data_source_combo.currentData()
        is_csv = data_source == "csv"

        # Enable/disable CSV path controls based on selection
        self.csv_path_line.setEnabled(is_csv)
        self.csv_path_button.setEnabled(is_csv)

    def select_csv_path(self) -> None:
        """Select CSV directory path"""
        current_path = self.csv_path_line.text()
        if not current_path:
            current_path = r"C:\new_tdxqh\vipdoc\ds\minline\csv"

        path = QtWidgets.QFileDialog.getExistingDirectory(
            self, _("选择CSV数据目录"), current_path
        )

        if path:
            self.csv_path_line.setText(path)
            # Update symbol completion when CSV path changes
            self.update_symbol_completion()

    def save_backtesting_setting(self) -> None:
        """
        Save current backtesting parameters to a user-selected JSON file.
        """
        # Get current values from UI controls
        class_name = self.class_combo.currentText()
        vt_symbol = self.symbol_line.text()
        interval = self.interval_combo.currentText()
        data_source = self.data_source_combo.currentData()
        csv_path = self.csv_path_line.text()
        start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
        end_date = self.end_date_edit.date().toString("yyyy-MM-dd")
        rate = float(self.rate_line.text())
        slippage = float(self.slippage_line.text())
        size = float(self.size_line.text())
        pricetick = float(self.pricetick_line.text())
        capital = float(self.capital_line.text())

        # Create settings dictionary
        setting = {
            "class_name": class_name,
            "vt_symbol": vt_symbol,
            "interval": interval,
            "data_source": data_source,
            "csv_path": csv_path,
            "start": start_date,
            "end": end_date,
            "rate": rate,
            "slippage": slippage,
            "size": size,
            "pricetick": pricetick,
            "capital": capital
        }

        # Let user choose save location
        path, _filter = QtWidgets.QFileDialog.getSaveFileName(
            self, _("保存回测参数"), "", "JSON(*.json)"
        )

        if not path:
            return

        # Save to file
        save_json(path, setting)
        QtWidgets.QMessageBox.information(
            self, _("成功"), _("回测参数已保存到: {}").format(path)
        )

    def load_backtesting_setting(self) -> None:
        """
        Load backtesting parameters from a user-selected JSON file.
        """
        # Let user choose file to load
        path, _filter = QtWidgets.QFileDialog.getOpenFileName(
            self, _("读取回测参数"), "", "JSON(*.json)"
        )

        if not path:
            return

        # Load from file
        setting: dict = load_json(path)
        if not setting:
            QtWidgets.QMessageBox.warning(
                self, _("错误"), _("无法读取参数文件")
            )
            return

        # Set values to UI controls
        if "class_name" in setting:
            self.class_combo.setCurrentIndex(
                self.class_combo.findText(setting["class_name"])
            )

        if "vt_symbol" in setting:
            self.symbol_line.setText(setting["vt_symbol"])

        if "interval" in setting:
            self.interval_combo.setCurrentIndex(
                self.interval_combo.findText(setting["interval"])
            )

        if "data_source" in setting:
            index = self.data_source_combo.findData(setting["data_source"])
            if index >= 0:
                self.data_source_combo.setCurrentIndex(index)
                self.on_data_source_changed()  # Update UI state

        if "csv_path" in setting:
            self.csv_path_line.setText(setting["csv_path"])
            # Update symbol completion after loading CSV path
            self.update_symbol_completion()

        if "start" in setting:
            start_dt: QtCore.QDate = QtCore.QDate.fromString(setting["start"], "yyyy-MM-dd")
            self.start_date_edit.setDate(start_dt)

        if "end" in setting:
            end_dt: QtCore.QDate = QtCore.QDate.fromString(setting["end"], "yyyy-MM-dd")
            self.end_date_edit.setDate(end_dt)

        if "rate" in setting:
            self.rate_line.setText(str(setting["rate"]))

        if "slippage" in setting:
            self.slippage_line.setText(str(setting["slippage"]))

        if "size" in setting:
            self.size_line.setText(str(setting["size"]))

        if "pricetick" in setting:
            self.pricetick_line.setText(str(setting["pricetick"]))

        if "capital" in setting:
            self.capital_line.setText(str(setting["capital"]))

        QtWidgets.QMessageBox.information(
            self, _("成功"), _("回测参数已从文件加载")
        )

    def load_auto_setting(self) -> None:
        """
        Load backtesting parameters from the auto-save JSON file.
        """
        setting: dict = load_json(self.setting_filename)
        if not setting:
            return

        # Set strategy class if it exists in the combo box
        if "class_name" in setting:
            index = self.class_combo.findText(setting["class_name"])
            if index >= 0:
                self.class_combo.setCurrentIndex(index)

        # Set symbol
        if "vt_symbol" in setting:
            self.symbol_line.setText(setting["vt_symbol"])

        # Set interval if it exists in the combo box
        if "interval" in setting:
            index = self.interval_combo.findText(setting["interval"])
            if index >= 0:
                self.interval_combo.setCurrentIndex(index)

        # Set data source
        if "data_source" in setting:
            index = self.data_source_combo.findData(setting["data_source"])
            if index >= 0:
                self.data_source_combo.setCurrentIndex(index)
                self.on_data_source_changed()  # Update UI state

        # Set CSV path
        if "csv_path" in setting:
            self.csv_path_line.setText(setting["csv_path"])
            # Update symbol completion after loading CSV path
            self.update_symbol_completion()

        # Set start date
        start_str: str = setting.get("start", "")
        if start_str:
            start_dt: QtCore.QDate = QtCore.QDate.fromString(start_str, "yyyy-MM-dd")
            if start_dt.isValid():
                self.start_date_edit.setDate(start_dt)

        # Set end date
        end_str: str = setting.get("end", "")
        if end_str:
            end_dt: QtCore.QDate = QtCore.QDate.fromString(end_str, "yyyy-MM-dd")
            if end_dt.isValid():
                self.end_date_edit.setDate(end_dt)

        # Set other parameters with safe fallbacks
        if "rate" in setting:
            self.rate_line.setText(str(setting["rate"]))
        if "slippage" in setting:
            self.slippage_line.setText(str(setting["slippage"]))
        if "size" in setting:
            self.size_line.setText(str(setting["size"]))
        if "pricetick" in setting:
            self.pricetick_line.setText(str(setting["pricetick"]))
        if "capital" in setting:
            self.capital_line.setText(str(setting["capital"]))

    def register_event(self) -> None:
        """"""
        self.signal_log.connect(self.process_log_event)
        self.signal_backtesting_finished.connect(
            self.process_backtesting_finished_event)
        self.signal_optimization_finished.connect(
            self.process_optimization_finished_event)

        self.event_engine.register(EVENT_BACKTESTER_LOG, self.signal_log.emit)
        self.event_engine.register(EVENT_BACKTESTER_BACKTESTING_FINISHED, self.signal_backtesting_finished.emit)
        self.event_engine.register(EVENT_BACKTESTER_OPTIMIZATION_FINISHED, self.signal_optimization_finished.emit)

    def process_log_event(self, event: Event) -> None:
        """"""
        msg = event.data
        self.write_log(msg)

    def write_log(self, msg: str) -> None:
        """"""
        timestamp: str = datetime.now().strftime("%H:%M:%S")
        msg = f"{timestamp}\t{msg}"
        self.log_monitor.append(msg)

    def process_backtesting_finished_event(self, event: Event) -> None:
        """"""
        statistics: dict | None = self.backtester_engine.get_result_statistics()
        if statistics:
            self.statistics_monitor.set_data(statistics)

        df: DataFrame | None = self.backtester_engine.get_result_df()
        if df is not None:
            self.chart.set_data(df)

            # Save chart image if save separately option was enabled
            if self.current_save_separately:
                try:
                    # Create the result directory path (same as in engine)
                    from pathlib import Path
                    result_dir = Path.home() / ".vntrader" / "backtestresult"
                    if self.current_class_name:
                        # Create subdirectory name with strategy name and symbol
                        symbol_part = self.current_vt_symbol.split('.')[0] if '.' in self.current_vt_symbol else self.current_vt_symbol
                        from datetime import datetime
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        subdir_name = f"{self.current_class_name}_{symbol_part}_{timestamp}"
                        result_dir = result_dir / subdir_name
                        result_dir.mkdir(parents=True, exist_ok=True)

                        # Save chart - pass the data directly to avoid serialization issues
                        chart_file = result_dir / "backtest_chart.png"

                        # Get data from engine instead of stored in chart
                        df = self.backtester_engine.get_result_df()
                        if df is not None:
                            # Temporarily set data in chart for saving
                            original_data = getattr(self.chart, '_data', None)
                            self.chart.set_data(df)
                            success = self.chart.save_chart_image(str(chart_file))
                            # Restore original data if it existed
                            if original_data is not None:
                                self.chart._data = original_data
                        else:
                            success = self.chart.save_chart_image(str(chart_file))

                        if success:
                            self.write_log(f"回测图表已保存到: {chart_file}")
                        else:
                            self.write_log("保存回测图表失败")
                except Exception as e:
                    self.write_log(f"保存回测图表失败: {str(e)}")

        self.trade_button.setEnabled(True)
        self.order_button.setEnabled(True)
        self.daily_button.setEnabled(True)

        # Reset saved options after processing (keep vt_symbol for chart viewing)
        self.current_save_separately = False
        self.current_class_name = ""
        # Keep current_vt_symbol for chart viewing after backtest completion

        # Tick data can not be displayed using candle chart
        interval: str = self.interval_combo.currentText()
        if interval != Interval.TICK.value:
            self.candle_button.setEnabled(True)

    def process_optimization_finished_event(self, event: Event) -> None:
        """"""
        self.write_log(_("请点击[优化结果]按钮查看"))
        self.result_button.setEnabled(True)

    def start_backtesting(self) -> None:
        """"""
        class_name: str = self.class_combo.currentText()
        if not class_name:
            self.write_log(_("请选择要回测的策略"))
            return

        vt_symbol: str = self.symbol_line.text()
        interval: str = self.interval_combo.currentText()
        data_source: str = self.data_source_combo.currentData()
        csv_path: str = self.csv_path_line.text()
        start: datetime = cast(datetime, self.start_date_edit.dateTime().toPython())
        end: datetime = cast(datetime, self.end_date_edit.dateTime().toPython())
        rate: float = float(self.rate_line.text())
        slippage: float = float(self.slippage_line.text())
        size: float = float(self.size_line.text())
        pricetick: float = float(self.pricetick_line.text())
        capital: float = float(self.capital_line.text())

        # Check validity of vt_symbol
        if "." not in vt_symbol:
            self.write_log(_("本地代码缺失交易所后缀，请检查"))
            return

        __, exchange_str = vt_symbol.split(".")
        if exchange_str not in Exchange.__members__:
            self.write_log(_("本地代码的交易所后缀不正确，请检查"))
            return

        # Save backtesting parameters
        backtesting_setting: dict = {
            "class_name": class_name,
            "vt_symbol": vt_symbol,
            "interval": interval,
            "data_source": data_source,
            "csv_path": csv_path,
            "start": start.strftime("%Y-%m-%d"),
            "rate": rate,
            "slippage": slippage,
            "size": size,
            "pricetick": pricetick,
            "capital": capital
        }
        save_json(self.setting_filename, backtesting_setting)

        # Get strategy setting
        old_setting: dict = self.settings[class_name]
        dialog: BacktestingSettingEditor = BacktestingSettingEditor(class_name, old_setting)
        i: int = dialog.exec()
        if i != dialog.DialogCode.Accepted:
            return

        new_setting: dict = dialog.get_setting()
        self.settings[class_name] = new_setting

        # Set trade save options
        save_separately = self.save_trades_separately_checkbox.isChecked()
        self.backtester_engine.save_trades_separately = save_separately
        self.backtester_engine.current_class_name = class_name
        self.backtester_engine.current_vt_symbol = vt_symbol

        # Save options for event handling (before they get reset)
        self.current_save_separately = save_separately
        self.current_class_name = class_name
        self.current_vt_symbol = vt_symbol
        self.backtester_engine.chart = self.chart

        # Set data source information
        self.backtester_engine.data_source = data_source
        self.backtester_engine.csv_path = csv_path

        result: bool = self.backtester_engine.start_backtesting(
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
            new_setting
        )

        if result:
            self.statistics_monitor.clear_data()
            self.chart.clear_data()

            self.trade_button.setEnabled(False)
            self.order_button.setEnabled(False)
            self.daily_button.setEnabled(False)
            self.candle_button.setEnabled(False)

            self.trade_dialog.clear_data()
            self.order_dialog.clear_data()
            self.daily_dialog.clear_data()
            self.candle_dialog.clear_data()

    def start_optimization(self) -> None:
        """"""
        class_name: str = self.class_combo.currentText()
        vt_symbol: str = self.symbol_line.text()
        interval: str = self.interval_combo.currentText()
        data_source: str = self.data_source_combo.currentData()
        csv_path: str = self.csv_path_line.text()
        start: datetime = cast(datetime, self.start_date_edit.dateTime().toPython())
        end: datetime = cast(datetime, self.end_date_edit.dateTime().toPython())
        rate: float = float(self.rate_line.text())
        slippage: float = float(self.slippage_line.text())
        size: float = float(self.size_line.text())
        pricetick: float = float(self.pricetick_line.text())
        capital: float = float(self.capital_line.text())

        parameters: dict = self.settings[class_name]
        dialog: OptimizationSettingEditor = OptimizationSettingEditor(class_name, parameters)
        i: int = dialog.exec()
        if i != dialog.DialogCode.Accepted:
            return

        optimization_setting, use_ga, max_workers = dialog.get_setting()
        self.target_display = dialog.target_display

        # Set data source information for optimization
        self.backtester_engine.data_source = data_source
        self.backtester_engine.csv_path = csv_path

        self.backtester_engine.start_optimization(
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

        self.result_button.setEnabled(False)

    def start_downloading(self) -> None:
        """"""
        vt_symbol: str = self.symbol_line.text()
        interval: str = self.interval_combo.currentText()
        start_date: QtCore.QDate = self.start_date_edit.date()
        end_date: QtCore.QDate = self.end_date_edit.date()

        start: datetime = datetime(
            start_date.year(),
            start_date.month(),
            start_date.day(),
        )
        start= start.replace(tzinfo=DB_TZ)

        end: datetime = datetime(
            end_date.year(),
            end_date.month(),
            end_date.day(),
            23,
            59,
            59,
        )
        end = end.replace(tzinfo=DB_TZ)

        self.backtester_engine.start_downloading(
            vt_symbol,
            interval,
            start,
            end
        )

    def clear_csv_cache(self) -> None:
        """
        Clear the CSV data cache.
        """
        cache_info = self.backtester_engine.get_csv_cache_info()
        cache_size = cache_info.get("cached_keys", 0)

        if cache_size == 0:
            QtWidgets.QMessageBox.information(
                self, _("提示"), _("CSV缓存已为空")
            )
            return

        reply = QtWidgets.QMessageBox.question(
            self,
            _("确认清除缓存"),
            _("确定要清除CSV数据缓存吗？\n\n当前缓存了 {} 个数据集合，共 {} 条记录。".format(
                cache_info.get("cached_keys", 0),
                cache_info.get("total_records", 0)
            )),
            QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No
        )

        if reply == QtWidgets.QMessageBox.StandardButton.Yes:
            self.backtester_engine.clear_csv_cache()
            QtWidgets.QMessageBox.information(
                self, _("成功"), _("CSV数据缓存已清除")
            )

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        """
        Handle UI close event - automatically clear CSV cache when closing.
        """
        try:
            # Clear CSV cache automatically when closing the UI
            self.backtester_engine.clear_csv_cache()
            self.write_log(_("UI关闭时自动清除CSV数据缓存"))
        except Exception as e:
            self.write_log(_("清除缓存时出错: {}").format(str(e)))

        # Accept the close event
        event.accept()

    def show_cache_info(self) -> None:
        """
        Show CSV cache information in a dialog.
        """
        cache_info = self.backtester_engine.get_csv_cache_info()

        if not cache_info.get("cached_keys", 0):
            QtWidgets.QMessageBox.information(
                self, _("缓存情况"), _("当前没有缓存的CSV数据")
            )
            return

        # Create dialog to show cache information
        dialog = CacheInfoDialog(cache_info, self)
        dialog.exec_()

    def stop_optimization(self) -> None:
        """
        Stop the current optimization task.
        """
        success = self.backtester_engine.stop_optimization()
        if success:
            QtWidgets.QMessageBox.information(
                self, _("成功"), _("优化任务已停止")
            )
        else:
            QtWidgets.QMessageBox.warning(
                self, _("警告"), _("当前没有正在运行的优化任务")
            )

    def show_optimization_result(self) -> None:
        """"""
        result_values: list | None = self.backtester_engine.get_result_values()
        if result_values is None:
            return

        # Get current optimization parameters for filename generation
        class_name: str = self.class_combo.currentText()
        vt_symbol: str = self.symbol_line.text()
        start_date: str = self.start_date_edit.date().toString("yyyy-MM-dd")
        end_date: str = self.end_date_edit.date().toString("yyyy-MM-dd")

        dialog: OptimizationResultMonitor = OptimizationResultMonitor(
            result_values,
            self.target_display,
            class_name,
            vt_symbol,
            start_date,
            end_date
        )
        dialog.exec_()

    def show_backtesting_trades(self) -> None:
        """"""
        if not self.trade_dialog.is_updated():
            trades: list[TradeData] = self.backtester_engine.get_all_trades()
            self.trade_dialog.update_data(trades)

        self.trade_dialog.exec_()

    def show_backtesting_orders(self) -> None:
        """"""
        if not self.order_dialog.is_updated():
            orders: list[OrderData] = self.backtester_engine.get_all_orders()
            self.order_dialog.update_data(orders)

        self.order_dialog.exec_()

    def show_daily_results(self) -> None:
        """"""
        if not self.daily_dialog.is_updated():
            results: list[DailyResult] = self.backtester_engine.get_all_daily_results()
            self.daily_dialog.update_data(results)

        self.daily_dialog.exec_()

    def show_candle_chart(self) -> None:
        """显示K线图表 - 使用vnpy_chartwizard的show_chart.py"""
        try:
            # 获取当前回测的合约信息
            vt_symbol = self.current_vt_symbol

            # 如果current_vt_symbol为空，尝试从UI控件获取
            if not vt_symbol:
                vt_symbol = self.symbol_line.text()
                if not vt_symbol:
                    QtWidgets.QMessageBox.warning(
                        self,
                        "警告",
                        "请先运行一次回测或在合约输入框中输入合约代码"
                    )
                    return

            # 验证合约格式
            if "." not in vt_symbol:
                QtWidgets.QMessageBox.warning(
                    self,
                    "警告",
                    "合约代码格式不正确，请使用格式：合约.交易所（如：rb8888.SHFE）"
                )
                return

            # 解析合约代码
            symbol = vt_symbol.split('.')[0] if '.' in vt_symbol else vt_symbol

            # 查找trades.csv文件
            trades_csv_path = self._find_trades_csv()
            if not trades_csv_path:
                QtWidgets.QMessageBox.warning(
                    self,
                    "警告",
                    "未找到trades.csv文件，请确保回测已保存交易记录"
                )
                return

            # 获取回测的时间范围
            start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
            end_date = self.end_date_edit.date().toString("yyyy-MM-dd")

            # 直接在后台线程中启动图表，避免阻塞UI
            from vnpy_chartwizard.backtest_chart_viewer import run_backtest_chart

            # 在后台线程中运行，避免阻塞UI
            import threading

            def run_chart_in_thread():
                try:
                    run_backtest_chart(symbol, start_date, end_date, trades_csv_path)
                except Exception as e:
                    print(f"图表运行出错: {e}")
                    import traceback
                    traceback.print_exc()

            thread = threading.Thread(target=run_chart_in_thread, daemon=True)
            thread.start()

            QtWidgets.QMessageBox.information(
                self,
                "图表启动",
                "图表查看器正在启动，请稍候查看新弹出的窗口。"
            )

        except Exception as e:
            QtWidgets.QMessageBox.critical(
                self,
                "错误",
                f"启动图表查看器失败: {str(e)}"
            )
            import traceback
            traceback.print_exc()

    def _find_trades_csv(self) -> str | None:
        """查找trades.csv文件"""
        # 首先尝试在.vntrader目录下查找
        vntrader_dir = Path.home() / ".vntrader"

        # 查找最新的trades.csv文件
        trades_files = list(vntrader_dir.glob("**/trades.csv"))
        if trades_files:
            # 返回最新的文件
            return str(max(trades_files, key=lambda x: x.stat().st_mtime))

        # 如果找不到，尝试在当前工作目录查找
        current_dir = Path.cwd()
        trades_file = current_dir / "trades.csv"
        if trades_file.exists():
            return str(trades_file)

        return None


    def edit_strategy_code(self) -> None:
        """"""
        class_name: str = self.class_combo.currentText()
        if not class_name:
            return

        file_path: str = self.backtester_engine.get_strategy_class_file(class_name)

        # 按优先级排序的常用代码编辑器命令列表
        editor_cmds: list[str] = [
            "code",         # VS Code
            "cursor",       # Cursor
            "pycharm64",    # PyCharm (Windows)
            "charm",        # PyCharm (命令行启动器)
        ]

        # 查找可用的编辑器
        editor_cmd: str = ""
        for cmd in editor_cmds:
            if shutil.which(cmd):
                editor_cmd = cmd
                break

        if editor_cmd:
            if platform.system() == "Windows":
                subprocess.run([editor_cmd, file_path], shell=True)
            else:
                subprocess.run([editor_cmd, file_path])
        else:
            QtWidgets.QMessageBox.warning(
                self,
                _("启动代码编辑器失败"),
                _("未检测到可用的代码编辑器，请安装以下任一编辑器并添加到系统PATH：\n"
                  "Cursor、VS Code、PyCharm")
            )

    def reload_strategy_class(self) -> None:
        """"""
        self.backtester_engine.reload_strategy_class()

        current_strategy_name: str = self.class_combo.currentText()

        self.class_combo.clear()
        self.init_strategy_settings()

        ix: int = self.class_combo.findText(current_strategy_name)
        self.class_combo.setCurrentIndex(ix)

    def open_vntrader_folder(self) -> None:
        """
        Open the .vntrader folder in file explorer.
        """
        vntrader_path = Path.home() / ".vntrader"

        # Create directory if it doesn't exist
        vntrader_path.mkdir(exist_ok=True)

        try:
            if platform.system() == "Windows":
                os.startfile(vntrader_path)
            elif platform.system() == "Darwin":  # macOS
                subprocess.run(["open", vntrader_path])
            else:  # Linux and other Unix-like systems
                subprocess.run(["xdg-open", vntrader_path])
        except Exception as e:
            QtWidgets.QMessageBox.warning(
                self, _("错误"), _("无法打开.vntrader文件夹: {}").format(str(e))
            )

    def show(self) -> None:
        """"""
        self.showMaximized()


class StatisticsMonitor(QtWidgets.QTableWidget):
    """"""
    KEY_NAME_MAP: dict = {
        "start_date": _("首个交易日"),
        "end_date": _("最后交易日"),

        "total_days": _("总交易日"),
        "profit_days": _("盈利交易日"),
        "loss_days": _("亏损交易日"),

        "capital": _("起始资金"),
        "end_balance": _("结束资金"),

        "total_return": _("总收益率"),
        "annual_return": _("年化收益"),
        "max_drawdown": _("最大回撤"),
        "max_ddpercent": _("百分比最大回撤"),
        "max_drawdown_duration": _("最大回撤天数"),

        "total_net_pnl": _("总盈亏"),
        "total_commission": _("总手续费"),
        "total_slippage": _("总滑点"),
        "total_turnover": _("总成交额"),
        "total_trade_count": _("总成交笔数"),

        "daily_net_pnl": _("日均盈亏"),
        "daily_commission": _("日均手续费"),
        "daily_slippage": _("日均滑点"),
        "daily_turnover": _("日均成交额"),
        "daily_trade_count": _("日均成交笔数"),

        "daily_return": _("日均收益率"),
        "return_std": _("收益标准差"),
        "sharpe_ratio": _("夏普比率"),
        "ewm_sharpe": _("EWM夏普"),
        "return_drawdown_ratio": _("收益回撤比")
    }

    def __init__(self) -> None:
        """"""
        super().__init__()

        self.cells: dict = {}

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setRowCount(len(self.KEY_NAME_MAP))
        self.setVerticalHeaderLabels(list(self.KEY_NAME_MAP.values()))

        self.setColumnCount(1)
        self.horizontalHeader().setVisible(False)
        self.horizontalHeader().setSectionResizeMode(
            QtWidgets.QHeaderView.ResizeMode.Stretch
        )
        self.setEditTriggers(self.EditTrigger.NoEditTriggers)

        for row, key in enumerate(self.KEY_NAME_MAP.keys()):
            cell: QtWidgets.QTableWidgetItem = QtWidgets.QTableWidgetItem()
            self.setItem(row, 0, cell)
            self.cells[key] = cell

    def clear_data(self) -> None:
        """"""
        for cell in self.cells.values():
            cell.setText("")

    def set_data(self, data: dict) -> None:
        """"""
        data["capital"] = f"{data['capital']:,.2f}"
        data["end_balance"] = f"{data['end_balance']:,.2f}"
        data["total_return"] = f"{data['total_return']:,.2f}%"
        data["annual_return"] = f"{data['annual_return']:,.2f}%"
        data["max_drawdown"] = f"{data['max_drawdown']:,.2f}"
        data["max_ddpercent"] = f"{data['max_ddpercent']:,.2f}%"
        data["total_net_pnl"] = f"{data['total_net_pnl']:,.2f}"
        data["total_commission"] = f"{data['total_commission']:,.2f}"
        data["total_slippage"] = f"{data['total_slippage']:,.2f}"
        data["total_turnover"] = f"{data['total_turnover']:,.2f}"
        data["daily_net_pnl"] = f"{data['daily_net_pnl']:,.2f}"
        data["daily_commission"] = f"{data['daily_commission']:,.2f}"
        data["daily_slippage"] = f"{data['daily_slippage']:,.2f}"
        data["daily_turnover"] = f"{data['daily_turnover']:,.2f}"
        data["daily_trade_count"] = f"{data['daily_trade_count']:,.2f}"
        data["daily_return"] = f"{data['daily_return']:,.2f}%"
        data["return_std"] = f"{data['return_std']:,.2f}%"
        data["sharpe_ratio"] = f"{data['sharpe_ratio']:,.2f}"
        data["ewm_sharpe"] = f"{data['ewm_sharpe']:,.2f}"
        data["return_drawdown_ratio"] = f"{data['return_drawdown_ratio']:,.2f}"

        for key, cell in self.cells.items():
            value = data.get(key, "")
            cell.setText(str(value))


class BacktestingSettingEditor(QtWidgets.QDialog):
    """
    For creating new strategy and editing strategy parameters.
    """

    def __init__(
        self, class_name: str, parameters: dict
    ) -> None:
        """"""
        super().__init__()

        self.class_name: str = class_name
        self.parameters: dict = parameters
        self.edits: dict = {}

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()

        # Add vt_symbol and name edit if add new strategy
        self.setWindowTitle(_("策略参数配置：{}").format(self.class_name))
        button_text: str = _("确定")
        parameters: dict = self.parameters

        for name, value in parameters.items():
            type_ = type(value)

            edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit(str(value))
            if type_ is int:
                int_validator: QtGui.QIntValidator = QtGui.QIntValidator()
                edit.setValidator(int_validator)
            elif type_ is float:
                double_validator: QtGui.QDoubleValidator = QtGui.QDoubleValidator()
                edit.setValidator(double_validator)

            form.addRow(f"{name} {type_}", edit)

            self.edits[name] = (edit, type_)

        # Create buttons layout
        buttons_layout: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()

        save_param_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("保存参数"))
        save_param_button.clicked.connect(self.save_strategy_parameters)

        load_param_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("读取参数"))
        load_param_button.clicked.connect(self.load_strategy_parameters)

        buttons_layout.addWidget(save_param_button)
        buttons_layout.addWidget(load_param_button)
        buttons_layout.addStretch()

        button: QtWidgets.QPushButton = QtWidgets.QPushButton(button_text)
        button.clicked.connect(self.accept)
        buttons_layout.addWidget(button)

        form.addRow(buttons_layout)

        widget: QtWidgets.QWidget = QtWidgets.QWidget()
        widget.setLayout(form)

        scroll: QtWidgets.QScrollArea = QtWidgets.QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(widget)

        vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        vbox.addWidget(scroll)
        self.setLayout(vbox)

    def get_setting(self) -> dict:
        """"""
        setting: dict = {}

        for name, tp in self.edits.items():
            edit, type_ = tp
            value_text = edit.text()

            if type_ is bool:
                if value_text == "True":
                    value = True
                else:
                    value = False
            else:
                value = type_(value_text)

            setting[name] = value

        return setting

    def save_strategy_parameters(self) -> None:
        """
        Save current strategy parameters to a user-selected JSON file.
        """
        # Get current parameter values from UI
        current_params = self.get_setting()

        # Create default directory
        default_dir = Path.cwd() / ".vntrader" / "backtestparam"
        default_dir.mkdir(parents=True, exist_ok=True)

        # Generate default filename with strategy name
        default_filename = f"{self.class_name}_parameters.json"
        default_path = default_dir / default_filename

        # Let user choose save location with suggested path
        path, _filter = QtWidgets.QFileDialog.getSaveFileName(
            self, _("保存策略参数"), str(default_path), "JSON(*.json)"
        )

        if not path:
            return

        # Save to file
        save_json(path, {
            "strategy_name": self.class_name,
            "parameters": current_params
        })

        QtWidgets.QMessageBox.information(
            self, _("成功"), _("策略参数已保存到: {}").format(path)
        )

    def load_strategy_parameters(self) -> None:
        """
        Load strategy parameters from a user-selected JSON file.
        """
        # Create default directory
        default_dir = Path.cwd() / ".vntrader" / "backtestparam"
        default_dir.mkdir(parents=True, exist_ok=True)

        # Generate suggested filename with strategy name
        suggested_filename = f"{self.class_name}_parameters.json"
        suggested_path = default_dir / suggested_filename

        # Let user choose file to load
        path, _filter = QtWidgets.QFileDialog.getOpenFileName(
            self, _("读取策略参数"), str(suggested_path), "JSON(*.json)"
        )

        if not path:
            return

        # Load from file
        data: dict = load_json(path)
        if not data:
            QtWidgets.QMessageBox.warning(
                self, _("错误"), _("无法读取参数文件")
            )
            return

        # Check if the file is for the correct strategy
        if data.get("strategy_name") != self.class_name:
            QtWidgets.QMessageBox.warning(
                self, _("错误"), _("参数文件不匹配当前策略")
            )
            return

        parameters = data.get("parameters", {})
        if not parameters:
            QtWidgets.QMessageBox.warning(
                self, _("错误"), _("参数文件格式错误")
            )
            return

        # Set parameter values to UI controls
        for name, value in parameters.items():
            if name in self.edits:
                edit, type_ = self.edits[name]
                edit.setText(str(value))

        QtWidgets.QMessageBox.information(
            self, _("成功"), _("策略参数已从文件加载")
        )


class BacktesterChart(pg.GraphicsLayoutWidget):
    """"""

    def __init__(self) -> None:
        """"""
        super().__init__(title="Backtester Chart")

        self.dates: dict = {}
        self._data: DataFrame | None = None

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        pg.setConfigOptions(antialias=True)

        # Create plot widgets
        self.balance_plot = self.addPlot(
            title=_("账户净值"),
            axisItems={"bottom": DateAxis(self.dates, orientation="bottom")}
        )
        self.nextRow()

        self.drawdown_plot = self.addPlot(
            title=_("净值回撤"),
            axisItems={"bottom": DateAxis(self.dates, orientation="bottom")}
        )
        self.nextRow()

        self.pnl_plot = self.addPlot(
            title=_("每日盈亏"),
            axisItems={"bottom": DateAxis(self.dates, orientation="bottom")}
        )
        self.nextRow()

        self.distribution_plot = self.addPlot(title=_("盈亏分布"))

        # Add curves and bars on plot widgets
        self.balance_curve = self.balance_plot.plot(
            pen=pg.mkPen("#ffc107", width=3)
        )

        dd_color: str = "#303f9f"
        self.drawdown_curve = self.drawdown_plot.plot(
            fillLevel=-0.3, brush=dd_color, pen=dd_color
        )

        profit_color: str = 'r'
        loss_color: str = 'g'
        self.profit_pnl_bar = pg.BarGraphItem(
            x=[], height=[], width=0.3, brush=profit_color, pen=profit_color
        )
        self.loss_pnl_bar = pg.BarGraphItem(
            x=[], height=[], width=0.3, brush=loss_color, pen=loss_color
        )
        self.pnl_plot.addItem(self.profit_pnl_bar)
        self.pnl_plot.addItem(self.loss_pnl_bar)

        distribution_color: str = "#6d4c41"
        self.distribution_curve = self.distribution_plot.plot(
            fillLevel=-0.3, brush=distribution_color, pen=distribution_color
        )

    def clear_data(self) -> None:
        """"""
        self.balance_curve.setData([], [])
        self.drawdown_curve.setData([], [])
        self.profit_pnl_bar.setOpts(x=[], height=[])
        self.loss_pnl_bar.setOpts(x=[], height=[])
        self.distribution_curve.setData([], [])

    def set_data(self, df: DataFrame) -> None:
        """"""
        if df is None:
            return

        # Store data for later use (e.g., matplotlib export)
        self._data = df.copy()

        count: int = len(df)

        self.dates.clear()
        for n, date in enumerate(df.index):
            self.dates[n] = date

        # Set data for curve of balance and drawdown
        self.balance_curve.setData(df["balance"])
        self.drawdown_curve.setData(df["drawdown"])

        # Set data for daily pnl bar
        profit_pnl_x: list = []
        profit_pnl_height: list = []
        loss_pnl_x: list = []
        loss_pnl_height: list = []

        for count, pnl in enumerate(df["net_pnl"]):
            if pnl >= 0:
                profit_pnl_height.append(pnl)
                profit_pnl_x.append(count)
            else:
                loss_pnl_height.append(pnl)
                loss_pnl_x.append(count)

        self.profit_pnl_bar.setOpts(x=profit_pnl_x, height=profit_pnl_height)
        self.loss_pnl_bar.setOpts(x=loss_pnl_x, height=loss_pnl_height)

        # Set data for pnl distribution
        hist, x = np.histogram(df["net_pnl"], bins="auto")
        x = x[:-1]
        self.distribution_curve.setData(x, hist)

    def save_chart_image(self, file_path: str) -> bool:
        """
        Save the chart as an image file.

        Args:
            file_path: Path to save the image (should end with .png, .jpg, etc.)

        Returns:
            bool: True if saved successfully, False otherwise
        """
        try:
            # Force update of the widget to ensure all elements are rendered
            self.update()

            # Try pyqtgraph's export method first (most reliable)
            try:
                # Check if export method exists
                if hasattr(self, 'export'):
                    self.export(file_path)
                    return True
                else:
                    raise AttributeError("export method not available")
            except Exception as e:
                print(f"pyqtgraph export failed: {e}")

            # Fallback to matplotlib-based chart recreation
            try:
                self._save_chart_with_matplotlib(file_path)
                return True
            except Exception as e:
                print(f"matplotlib export failed: {e}")

            # Final fallback to Qt rendering method
            try:
                # Try different Qt imports for compatibility
                QtModules = None
                try:
                    from PyQt5.QtGui import QPixmap, QPainter
                    from PyQt5.QtCore import QPoint
                    QtModules = "PyQt5"
                except ImportError:
                    try:
                        from PyQt6.QtGui import QPixmap, QPainter
                        from PyQt6.QtCore import QPoint
                        QtModules = "PyQt6"
                    except ImportError:
                        try:
                            from PySide2.QtGui import QPixmap, QPainter
                            from PySide2.QtCore import QPoint
                            QtModules = "PySide2"
                        except ImportError:
                            QtModules = None

                if QtModules is None:
                    print("Failed to save chart image: No compatible Qt bindings found")
                    return False

                # Get the size of the widget
                size = self.size()
                pixmap = QPixmap(size)

                # Create painter and render the widget
                painter = QPainter(pixmap)
                # Use render method to draw the widget onto the pixmap
                target_rect = pixmap.rect()
                self.render(painter, targetOffset=QPoint(0, 0), sourceRegion=target_rect)
                painter.end()

                # Save the pixmap
                success = pixmap.save(file_path)

                return success
            except Exception as e:
                print(f"Qt rendering failed: {e}")
                return False

        except Exception as e:
            print(f"Failed to save chart image: {e}")
            return False

    def _save_chart_with_matplotlib(self, file_path: str) -> None:
        """
        Save chart using matplotlib as a fallback method.
        """
        try:
            import matplotlib
            matplotlib.use('Agg')  # Use non-interactive backend
            import matplotlib.pyplot as plt

            # Check if we have data
            if not hasattr(self, '_data') or self._data is None or self._data.empty:
                raise Exception("No chart data available")

            df = self._data

            # Create simple figure with 2x2 subplots
            fig, axes = plt.subplots(2, 2, figsize=(10, 6))
            fig.suptitle('Backtest Results', fontsize=12)

            # Flatten axes for easier indexing
            axes = axes.flatten()

            # Plot 1: Account Balance (use index as x-axis to avoid date issues)
            if 'balance' in df.columns:
                axes[0].plot(range(len(df)), df['balance'].values, 'b-', linewidth=1.5)
                axes[0].set_title('Account Balance', fontsize=10)
                axes[0].set_ylabel('Balance', fontsize=8)
                axes[0].grid(True, alpha=0.3)

            # Plot 2: Net Value
            if 'net_value' in df.columns:
                axes[1].plot(range(len(df)), df['net_value'].values, 'g-', linewidth=1.5)
                axes[1].set_title('Net Value', fontsize=10)
                axes[1].set_ylabel('Net Value', fontsize=8)
                axes[1].grid(True, alpha=0.3)

            # Plot 3: Drawdown
            if 'drawdown' in df.columns:
                axes[2].fill_between(range(len(df)), df['drawdown'].values, alpha=0.3, color='red')
                axes[2].set_title('Drawdown', fontsize=10)
                axes[2].set_ylabel('Drawdown', fontsize=8)
                axes[2].grid(True, alpha=0.3)

            # Plot 4: P&L Distribution
            if 'net_pnl' in df.columns:
                try:
                    axes[3].hist(df['net_pnl'].values, bins=20, alpha=0.7, color='blue', edgecolor='black')
                    axes[3].set_title('P&L Distribution', fontsize=10)
                    axes[3].set_xlabel('P&L', fontsize=8)
                    axes[3].set_ylabel('Frequency', fontsize=8)
                    axes[3].grid(True, alpha=0.3)
                except Exception:
                    # If histogram fails, just plot a simple line
                    axes[3].plot(range(len(df)), df['net_pnl'].values, 'purple', linewidth=1)
                    axes[3].set_title('Daily P&L', fontsize=10)
                    axes[3].set_ylabel('P&L', fontsize=8)
                    axes[3].grid(True, alpha=0.3)

            # Adjust layout and save
            plt.tight_layout()
            plt.savefig(file_path, dpi=100, bbox_inches='tight')
            plt.close(fig)

            print(f"Chart saved using matplotlib: {file_path}")

        except ImportError:
            raise Exception("matplotlib not available")
        except Exception as e:
            raise Exception(f"matplotlib chart creation failed: {e}")


class DateAxis(pg.AxisItem):
    """Axis for showing date data"""

    def __init__(self, dates: dict, *args: Any, **kwargs: Any) -> None:
        """"""
        super().__init__(*args, **kwargs)
        self.dates: dict = dates

    def tickStrings(self, values: list, scale: float, spacing: float) -> list:
        """"""
        strings: list = []
        for v in values:
            dt = self.dates.get(v, "")
            strings.append(str(dt))
        return strings


class OptimizationSettingEditor(QtWidgets.QDialog):
    """
    For setting up parameters for optimization.
    """
    DISPLAY_NAME_MAP: dict = {
        _("总收益率"): "total_return",
        _("夏普比率"): "sharpe_ratio",
        _("EWM夏普"): "ewm_sharpe",
        _("收益回撤比"): "return_drawdown_ratio",
        _("日均盈亏"): "daily_net_pnl"
    }

    def __init__(
        self, class_name: str, parameters: dict
    ) -> None:
        """"""
        super().__init__()

        self.class_name: str = class_name
        self.parameters: dict = parameters
        self.edits: dict = {}

        self.optimization_setting: OptimizationSetting = None
        self.use_ga: bool = False

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.target_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        self.target_combo.addItems(list(self.DISPLAY_NAME_MAP.keys()))

        self.worker_spin: QtWidgets.QSpinBox = QtWidgets.QSpinBox()
        self.worker_spin.setRange(0, 10000)
        self.worker_spin.setValue(0)
        self.worker_spin.setToolTip(_("设为0则自动根据CPU核心数启动对应数量的进程"))

        grid: QtWidgets.QGridLayout = QtWidgets.QGridLayout()
        grid.addWidget(QtWidgets.QLabel(_("优化目标")), 0, 0)
        grid.addWidget(self.target_combo, 0, 1, 1, 3)
        grid.addWidget(QtWidgets.QLabel(_("进程上限")), 1, 0)
        grid.addWidget(self.worker_spin, 1, 1, 1, 3)
        grid.addWidget(QtWidgets.QLabel(_("参数")), 2, 0)
        grid.addWidget(QtWidgets.QLabel(_("开始")), 2, 1)
        grid.addWidget(QtWidgets.QLabel(_("步进")), 2, 2)
        grid.addWidget(QtWidgets.QLabel(_("结束")), 2, 3)

        # Add vt_symbol and name edit if add new strategy
        self.setWindowTitle(_("优化参数配置：{}").format(self.class_name))

        validator: QtGui.QDoubleValidator = QtGui.QDoubleValidator()
        row: int = 3

        for name, value in self.parameters.items():
            type_ = type(value)
            if type_ not in [int, float]:
                continue

            start_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit(str(value))
            step_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit(str(1))
            end_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit(str(value))

            for edit in [start_edit, step_edit, end_edit]:
                edit.setValidator(validator)

            grid.addWidget(QtWidgets.QLabel(name), row, 0)
            grid.addWidget(start_edit, row, 1)
            grid.addWidget(step_edit, row, 2)
            grid.addWidget(end_edit, row, 3)

            self.edits[name] = {
                "type": type_,
                "start": start_edit,
                "step": step_edit,
                "end": end_edit
            }

            row += 1

        parallel_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("多进程优化"))
        parallel_button.clicked.connect(self.generate_parallel_setting)
        grid.addWidget(parallel_button, row, 0, 1, 4)

        row += 1
        ga_button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("遗传算法优化"))
        ga_button.clicked.connect(self.generate_ga_setting)
        grid.addWidget(ga_button, row, 0, 1, 4)

        widget: QtWidgets.QWidget = QtWidgets.QWidget()
        widget.setLayout(grid)

        scroll: QtWidgets.QScrollArea = QtWidgets.QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(widget)

        vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        vbox.addWidget(scroll)
        self.setLayout(vbox)

    def generate_ga_setting(self) -> None:
        """"""
        self.use_ga = True
        self.generate_setting()

    def generate_parallel_setting(self) -> None:
        """"""
        self.use_ga = False
        self.generate_setting()

    def generate_setting(self) -> None:
        """"""
        self.optimization_setting = OptimizationSetting()

        self.target_display: str = self.target_combo.currentText()
        target_name: str = self.DISPLAY_NAME_MAP[self.target_display]
        self.optimization_setting.set_target(target_name)

        for name, d in self.edits.items():
            type_ = d["type"]
            start_value = type_(d["start"].text())
            step_value = type_(d["step"].text())
            end_value = type_(d["end"].text())

            if start_value == end_value:
                self.optimization_setting.add_parameter(name, start_value)
            else:
                self.optimization_setting.add_parameter(
                    name,
                    start_value,
                    end_value,
                    step_value
                )

        self.accept()

    def get_setting(self) -> tuple[OptimizationSetting, bool, int]:
        """"""
        return self.optimization_setting, self.use_ga, self.worker_spin.value()


class OptimizationResultMonitor(QtWidgets.QDialog):
    """
    For viewing optimization result.
    """

    def __init__(
        self, result_values: list, target_display: str,
        class_name: str = "", vt_symbol: str = "", start_date: str = "", end_date: str = ""
    ) -> None:
        """"""
        super().__init__()

        self.result_values: list = result_values
        self.target_display: str = target_display
        self.show_detailed_stats: bool = False
        self.class_name: str = class_name
        self.vt_symbol: str = vt_symbol
        self.start_date: str = start_date
        self.end_date: str = end_date

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle(_("参数优化结果"))
        self.resize(1800, 600)

        # Create controls layout
        controls_layout = QtWidgets.QHBoxLayout()

        # Checkbox to toggle detailed statistics view
        self.detailed_checkbox = QtWidgets.QCheckBox(_("显示详细统计"))
        self.detailed_checkbox.stateChanged.connect(self.toggle_detailed_view)
        # Also connect clicked signal as backup
        self.detailed_checkbox.clicked.connect(self.toggle_detailed_view_clicked)
        controls_layout.addWidget(self.detailed_checkbox)

        controls_layout.addStretch()

        # Create table to show result
        self.table: QtWidgets.QTableWidget = QtWidgets.QTableWidget()
        self.update_table_content()

        # Create buttons
        button: QtWidgets.QPushButton = QtWidgets.QPushButton(_("保存"))
        button.clicked.connect(self.save_csv)

        button_layout = QtWidgets.QHBoxLayout()
        button_layout.addStretch()
        button_layout.addWidget(button)

        # Create main layout
        layout = QtWidgets.QVBoxLayout()
        layout.addLayout(controls_layout)
        layout.addWidget(self.table)
        layout.addLayout(button_layout)

        self.setLayout(layout)

    def update_table_content(self) -> None:
        """Update table content based on current view mode."""
        print(f"DEBUG: update_table_content called, show_detailed_stats: {self.show_detailed_stats}")

        # Clear the entire table and rebuild it
        self.table.clear()

        if self.show_detailed_stats:
            print("DEBUG: Setting up detailed table")
            self._setup_detailed_table()
        else:
            print("DEBUG: Setting up simple table")
            self._setup_simple_table()

    def _setup_simple_table(self) -> None:
        """Setup table for simple view (parameters + target value)."""
        self.table.setColumnCount(2)
        self.table.setRowCount(len(self.result_values))
        self.table.setHorizontalHeaderLabels([_("参数"), self.target_display])

        self.table.horizontalHeader().setSectionResizeMode(
            0, QtWidgets.QHeaderView.ResizeMode.ResizeToContents
        )
        self.table.horizontalHeader().setSectionResizeMode(
            1, QtWidgets.QHeaderView.ResizeMode.Stretch
        )

        for n, tp in enumerate(self.result_values):
            setting, target_value, __ = tp
            setting_cell: QtWidgets.QTableWidgetItem = QtWidgets.QTableWidgetItem(str(setting))
            target_cell: QtWidgets.QTableWidgetItem = QtWidgets.QTableWidgetItem(f"{target_value:.4f}")

            setting_cell.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            target_cell.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

            self.table.setItem(n, 0, setting_cell)
            self.table.setItem(n, 1, target_cell)

    def _setup_detailed_table(self) -> None:
        """Setup table for detailed statistics view."""
        # Define the statistics columns we want to show - match backtest statistics exactly
        stat_columns = [
            ("参数", "setting"),
            ("目标值", "target_value"),
            ("首个交易日", "start_date"),
            ("最后交易日", "end_date"),
            ("总交易日", "total_days"),
            ("盈利交易日", "profit_days"),
            ("亏损交易日", "loss_days"),
            ("起始资金", "capital"),
            ("结束资金", "end_balance"),
            ("总收益率", "total_return"),
            ("年化收益", "annual_return"),
            ("最大回撤", "max_drawdown"),
            ("百分比最大回撤", "max_ddpercent"),
            ("最大回撤天数", "max_drawdown_duration"),
            ("总盈亏", "total_net_pnl"),
            ("总手续费", "total_commission"),
            ("总滑点", "total_slippage"),
            ("总成交额", "total_turnover"),
            ("总成交笔数", "total_trade_count"),
            ("日均盈亏", "daily_net_pnl"),
            ("日均手续费", "daily_commission"),
            ("日均滑点", "daily_slippage"),
            ("日均成交额", "daily_turnover"),
            ("日均成交笔数", "daily_trade_count"),
            ("日均收益率", "daily_return"),
            ("收益标准差", "return_std"),
            ("夏普比率", "sharpe_ratio"),
            ("EWM夏普", "ewm_sharpe"),
            ("收益回撤比", "return_drawdown_ratio")
        ]

        self.table.setColumnCount(len(stat_columns))
        self.table.setRowCount(len(self.result_values))
        self.table.setHorizontalHeaderLabels([col[0] for col in stat_columns])

        # Set column resize modes - allow all columns to be manually resized
        for i, (header, _) in enumerate(stat_columns):
            self.table.horizontalHeader().setSectionResizeMode(
                i, QtWidgets.QHeaderView.ResizeMode.Interactive
            )

            # Set reasonable default column widths
            if header in ["参数", "目标值"]:
                self.table.setColumnWidth(i, 120)
            elif header in ["首个交易日", "最后交易日"]:
                self.table.setColumnWidth(i, 100)
            elif header in ["总交易日", "盈利交易日", "亏损交易日", "最大回撤天数", "总成交笔数"]:
                self.table.setColumnWidth(i, 90)
            elif header in ["起始资金", "结束资金", "总收益率", "年化收益", "最大回撤", "百分比最大回撤"]:
                self.table.setColumnWidth(i, 110)
            elif header in ["总盈亏", "总手续费", "总滑点", "总成交额"]:
                self.table.setColumnWidth(i, 100)
            elif header in ["日均盈亏", "日均手续费", "日均滑点", "日均成交额", "日均成交笔数"]:
                self.table.setColumnWidth(i, 100)
            elif header in ["日均收益率", "收益标准差"]:
                self.table.setColumnWidth(i, 100)
            elif header in ["夏普比率", "EWM夏普", "收益回撤比"]:
                self.table.setColumnWidth(i, 90)

        for n, tp in enumerate(self.result_values):
            setting, target_value, statistics = tp

            for col_idx, (header, stat_key) in enumerate(stat_columns):
                if stat_key == "setting":
                    value = str(setting)
                elif stat_key == "target_value":
                    value = f"{target_value:.4f}"
                else:
                    # Get value from statistics dict and format like backtest statistics
                    stat_value = statistics.get(stat_key, 0)
                    if isinstance(stat_value, float):
                        if stat_key in ["start_date", "end_date"]:
                            # Date formatting
                            value = str(stat_value) if stat_value else ""
                        elif stat_key in ["capital", "end_balance"]:
                            # Currency formatting with commas
                            value = f"{stat_value:,.2f}"
                        elif stat_key in ["total_return", "annual_return", "max_ddpercent", "daily_return", "return_std"]:
                            # Percentage formatting
                            value = f"{stat_value:.2f}%" if stat_value != 0 else "0.00%"
                        elif stat_key in ["max_drawdown", "total_net_pnl", "total_commission", "total_slippage", "total_turnover",
                                        "daily_net_pnl", "daily_commission", "daily_slippage", "daily_turnover"]:
                            # Currency formatting
                            value = f"{stat_value:.2f}"
                        elif stat_key in ["sharpe_ratio", "ewm_sharpe", "return_drawdown_ratio"]:
                            # Ratio formatting with 4 decimal places
                            value = f"{stat_value:.4f}"
                        elif stat_key in ["daily_trade_count"]:
                            # Count formatting
                            value = f"{stat_value:.2f}"
                        else:
                            # Integer values (days, counts)
                            value = str(int(stat_value)) if stat_value == int(stat_value) else f"{stat_value:.2f}"
                    else:
                        value = str(stat_value)

                cell = QtWidgets.QTableWidgetItem(value)
                cell.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
                self.table.setItem(n, col_idx, cell)

    def toggle_detailed_view(self, state: int) -> None:
        """Toggle between simple and detailed statistics view."""
        # Debug: Print state value
        print(f"DEBUG: toggle_detailed_view called with state: {state}")

        # Simple check: state == 2 means checked, state == 0 means unchecked
        self.show_detailed_stats = (state == 2)

        print(f"DEBUG: show_detailed_stats set to: {self.show_detailed_stats}")
        self.update_table_content()

    def toggle_detailed_view_clicked(self) -> None:
        """Toggle detailed view when checkbox is clicked."""
        self.show_detailed_stats = self.detailed_checkbox.isChecked()
        print(f"DEBUG: toggle_detailed_view_clicked - show_detailed_stats: {self.show_detailed_stats}")
        self.update_table_content()

    def save_csv(self) -> None:
        """
        Save table data into a csv file
        """
        # Generate default path and filename
        default_dir = r"C:\Users\Administrator\.vntrader\backtestresult\optimize"

        # Ensure directory exists
        os.makedirs(default_dir, exist_ok=True)

        # Generate filename: strategy_name + start_date + to + end_date + symbol + current_time
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Clean dates to remove hyphens for filename
        start_date_clean = self.start_date.replace("-", "") if self.start_date else ""
        end_date_clean = self.end_date.replace("-", "") if self.end_date else ""

        # Clean symbol to remove exchange suffix for filename
        symbol_clean = self.vt_symbol.split('.')[0] if self.vt_symbol else ""

        # Build filename
        filename_parts = []
        if self.class_name:
            filename_parts.append(self.class_name)
        if start_date_clean and end_date_clean:
            filename_parts.append(f"{start_date_clean}to{end_date_clean}")
        if symbol_clean:
            filename_parts.append(symbol_clean)
        filename_parts.append(current_time)

        default_filename = "_".join(filename_parts) + ".csv"
        default_path = os.path.join(default_dir, default_filename)

        path, __ = QtWidgets.QFileDialog.getSaveFileName(
            self, _("保存数据"), default_path, "CSV(*.csv)")

        if not path:
            return

        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)

            if self.show_detailed_stats:
                # Save detailed statistics - match backtest statistics exactly
                headers = [
                    "参数", "目标值", "首个交易日", "最后交易日", "总交易日", "盈利交易日", "亏损交易日",
                    "起始资金", "结束资金", "总收益率", "年化收益", "最大回撤", "百分比最大回撤", "最大回撤天数",
                    "总盈亏", "总手续费", "总滑点", "总成交额", "总成交笔数",
                    "日均盈亏", "日均手续费", "日均滑点", "日均成交额", "日均成交笔数",
                    "日均收益率", "收益标准差", "夏普比率", "EWM夏普", "收益回撤比"
                ]
                writer.writerow(headers)

                for tp in self.result_values:
                    setting, target_value, statistics = tp

                    # Format data to match backtest statistics display
                    row_data = [
                        str(setting),
                        f"{target_value:.4f}",
                        str(statistics.get('start_date', '')),
                        str(statistics.get('end_date', '')),
                        statistics.get('total_days', 0),
                        statistics.get('profit_days', 0),
                        statistics.get('loss_days', 0),
                        f"{statistics.get('capital', 0):,.2f}",
                        f"{statistics.get('end_balance', 0):,.2f}",
                        f"{statistics.get('total_return', 0):.2f}%",
                        f"{statistics.get('annual_return', 0):.2f}%",
                        f"{statistics.get('max_drawdown', 0):.2f}",
                        f"{statistics.get('max_ddpercent', 0):.2f}%",
                        statistics.get('max_drawdown_duration', 0),
                        f"{statistics.get('total_net_pnl', 0):.2f}",
                        f"{statistics.get('total_commission', 0):.2f}",
                        f"{statistics.get('total_slippage', 0):.2f}",
                        f"{statistics.get('total_turnover', 0):.2f}",
                        statistics.get('total_trade_count', 0),
                        f"{statistics.get('daily_net_pnl', 0):.2f}",
                        f"{statistics.get('daily_commission', 0):.2f}",
                        f"{statistics.get('daily_slippage', 0):.2f}",
                        f"{statistics.get('daily_turnover', 0):.2f}",
                        f"{statistics.get('daily_trade_count', 0):.2f}",
                        f"{statistics.get('daily_return', 0):.2f}%",
                        f"{statistics.get('return_std', 0):.2f}%",
                        f"{statistics.get('sharpe_ratio', 0):.4f}",
                        f"{statistics.get('ewm_sharpe', 0):.4f}",
                        f"{statistics.get('return_drawdown_ratio', 0):.4f}"
                    ]
                    writer.writerow(row_data)
            else:
                # Save simple view (original format)
                writer.writerow([_("参数"), self.target_display])

                for tp in self.result_values:
                    setting, target_value, __ = tp
                    row_data: list = [str(setting), f"{target_value:.4f}"]
                    writer.writerow(row_data)


class BacktestingTradeMonitor(BaseMonitor):
    """
    Monitor for backtesting trade data.
    """

    headers: dict = {
        "tradeid": {"display": _("成交号 "), "cell": BaseCell, "update": False},
        "orderid": {"display": _("委托号"), "cell": BaseCell, "update": False},
        "symbol": {"display": _("代码"), "cell": BaseCell, "update": False},
        "exchange": {"display": _("交易所"), "cell": EnumCell, "update": False},
        "direction": {"display": _("方向"), "cell": DirectionCell, "update": False},
        "offset": {"display": _("开平"), "cell": EnumCell, "update": False},
        "price": {"display": _("价格"), "cell": BaseCell, "update": False},
        "volume": {"display": _("数量"), "cell": BaseCell, "update": False},
        "datetime": {"display": _("时间"), "cell": BaseCell, "update": False},
        "gateway_name": {"display": _("接口"), "cell": BaseCell, "update": False},
    }


class BacktestingOrderMonitor(BaseMonitor):
    """
    Monitor for backtesting order data.
    """

    headers: dict = {
        "orderid": {"display": _("委托号"), "cell": BaseCell, "update": False},
        "symbol": {"display": _("代码"), "cell": BaseCell, "update": False},
        "exchange": {"display": _("交易所"), "cell": EnumCell, "update": False},
        "type": {"display": _("类型"), "cell": EnumCell, "update": False},
        "direction": {"display": _("方向"), "cell": DirectionCell, "update": False},
        "offset": {"display": _("开平"), "cell": EnumCell, "update": False},
        "price": {"display": _("价格"), "cell": BaseCell, "update": False},
        "volume": {"display": _("总数量"), "cell": BaseCell, "update": False},
        "traded": {"display": _("已成交"), "cell": BaseCell, "update": False},
        "status": {"display": _("状态"), "cell": EnumCell, "update": False},
        "datetime": {"display": _("时间"), "cell": BaseCell, "update": False},
        "gateway_name": {"display": _("接口"), "cell": BaseCell, "update": False},
    }


class FloatCell(BaseCell):
    """
    Cell used for showing pnl data.
    """

    def __init__(self, content: Any, data: Any) -> None:
        """"""
        content = f"{content:.2f}"
        super().__init__(content, data)


class DailyResultMonitor(BaseMonitor):
    """
    Monitor for backtesting daily result.
    """

    headers: dict = {
        "date": {"display": _("日期"), "cell": BaseCell, "update": False},
        "trade_count": {"display": _("成交笔数"), "cell": BaseCell, "update": False},
        "start_pos": {"display": _("开盘持仓"), "cell": BaseCell, "update": False},
        "end_pos": {"display": _("收盘持仓"), "cell": BaseCell, "update": False},
        "turnover": {"display": _("成交额"), "cell": FloatCell, "update": False},
        "commission": {"display": _("手续费"), "cell": FloatCell, "update": False},
        "slippage": {"display": _("滑点"), "cell": FloatCell, "update": False},
        "trading_pnl": {"display": _("交易盈亏"), "cell": FloatCell, "update": False},
        "holding_pnl": {"display": _("持仓盈亏"), "cell": FloatCell, "update": False},
        "total_pnl": {"display": _("总盈亏"), "cell": FloatCell, "update": False},
        "net_pnl": {"display": _("净盈亏"), "cell": FloatCell, "update": False},
    }


class BacktestingResultDialog(QtWidgets.QDialog):
    """"""

    def __init__(
        self,
        main_engine: MainEngine,
        event_engine: EventEngine,
        title: str,
        table_class: type[BaseMonitor]
    ) -> None:
        """"""
        super().__init__()

        self.main_engine: MainEngine = main_engine
        self.event_engine: EventEngine = event_engine
        self.title: str = title
        self.table_class: type[BaseMonitor] = table_class

        self.updated: bool = False

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle(self.title)
        self.resize(1100, 600)

        self.table: BaseMonitor = self.table_class(self.main_engine, self.event_engine)

        vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        vbox.addWidget(self.table)

        self.setLayout(vbox)

    def clear_data(self) -> None:
        """"""
        self.updated = False
        self.table.setRowCount(0)

    def update_data(self, data: list) -> None:
        """"""
        self.updated = True

        data.reverse()
        for obj in data:
            self.table.insert_new_row(obj)

    def is_updated(self) -> bool:
        """"""
        return self.updated


class CandleChartDialog(QtWidgets.QDialog):
    """"""

    def __init__(self) -> None:
        """"""
        super().__init__()

        self.updated: bool = False

        self.dt_ix_map: dict = {}
        self.ix_bar_map: dict = {}

        self.high_price = 0
        self.low_price = 0
        self.price_range = 0

        self.items: list = []

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle(_("回测K线图表"))
        self.resize(1400, 800)

        # Create chart widget
        self.chart: ChartWidget = ChartWidget()
        self.chart.add_plot("candle", hide_x_axis=True)
        self.chart.add_plot("volume", maximum_height=200)
        self.chart.add_item(CandleItem, "candle", "candle")
        self.chart.add_item(VolumeItem, "volume", "volume")
        self.chart.add_cursor()

        # Create help widget
        text1: str = _("红色虚线 —— 盈利交易")
        label1: QtWidgets.QLabel = QtWidgets.QLabel(text1)
        label1.setStyleSheet("color:red")

        text2: str = _("绿色虚线 —— 亏损交易")
        label2: QtWidgets.QLabel = QtWidgets.QLabel(text2)
        label2.setStyleSheet("color:#00FF00")

        text3: str = _("黄色向上箭头 —— 买入开仓 Buy")
        label3: QtWidgets.QLabel = QtWidgets.QLabel(text3)
        label3.setStyleSheet("color:yellow")

        text4: str = _("黄色向下箭头 —— 卖出平仓 Sell")
        label4: QtWidgets.QLabel = QtWidgets.QLabel(text4)
        label4.setStyleSheet("color:yellow")

        text5: str = _("紫红向下箭头 —— 卖出开仓 Short")
        label5: QtWidgets.QLabel = QtWidgets.QLabel(text5)
        label5.setStyleSheet("color:magenta")

        text6: str = _("紫红向上箭头 —— 买入平仓 Cover")
        label6: QtWidgets.QLabel = QtWidgets.QLabel(text6)
        label6.setStyleSheet("color:magenta")

        hbox1: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        hbox1.addStretch()
        hbox1.addWidget(label1)
        hbox1.addStretch()
        hbox1.addWidget(label2)
        hbox1.addStretch()

        hbox2: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        hbox2.addStretch()
        hbox2.addWidget(label3)
        hbox2.addStretch()
        hbox2.addWidget(label4)
        hbox2.addStretch()

        hbox3: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        hbox3.addStretch()
        hbox3.addWidget(label5)
        hbox3.addStretch()
        hbox3.addWidget(label6)
        hbox3.addStretch()

        # Set layout
        vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        vbox.addWidget(self.chart)
        vbox.addLayout(hbox1)
        vbox.addLayout(hbox2)
        vbox.addLayout(hbox3)
        self.setLayout(vbox)

    def update_history(self, history: list) -> None:
        """"""
        self.updated = True
        self.chart.update_history(history)

        for ix, bar in enumerate(history):
            self.ix_bar_map[ix] = bar
            self.dt_ix_map[bar.datetime] = ix

            if not self.high_price:
                self.high_price = bar.high_price
                self.low_price = bar.low_price
            else:
                self.high_price = max(self.high_price, bar.high_price)
                self.low_price = min(self.low_price, bar.low_price)

        self.price_range = self.high_price - self.low_price

    def update_trades(self, trades: list) -> None:
        """"""
        trade_pairs: list = generate_trade_pairs(trades)

        candle_plot: pg.PlotItem = self.chart.get_plot("candle")

        scatter_data: list = []

        y_adjustment: float = self.price_range * 0.001

        for d in trade_pairs:
            open_ix = self.dt_ix_map[d["open_dt"]]
            close_ix = self.dt_ix_map[d["close_dt"]]
            open_price = d["open_price"]
            close_price = d["close_price"]

            # Trade Line
            x: list = [open_ix, close_ix]
            y: list = [open_price, close_price]

            if d["direction"] == Direction.LONG and close_price >= open_price:
                color: str = "r"
            elif d["direction"] == Direction.SHORT and close_price <= open_price:
                color = "r"
            else:
                color = "g"

            pen: QtGui.QPen = pg.mkPen(color, width=1.5, style=QtCore.Qt.PenStyle.DashLine)
            item: pg.PlotCurveItem = pg.PlotCurveItem(x, y, pen=pen)

            self.items.append(item)
            candle_plot.addItem(item)

            # Trade Scatter
            open_bar: BarData = self.ix_bar_map[open_ix]
            close_bar: BarData = self.ix_bar_map[close_ix]

            if d["direction"] == Direction.LONG:
                scatter_color: str = "yellow"
                open_symbol: str = "t1"
                close_symbol: str = "t"
                open_side: int = 1
                close_side: int = -1
                open_y: float = open_bar.low_price
                close_y: float = close_bar.high_price
            else:
                scatter_color = "magenta"
                open_symbol = "t"
                close_symbol = "t1"
                open_side = -1
                close_side = 1
                open_y = open_bar.high_price
                close_y = close_bar.low_price

            pen = pg.mkPen(QtGui.QColor(scatter_color))
            brush: QtGui.QBrush = pg.mkBrush(QtGui.QColor(scatter_color))
            size: int = 10

            open_scatter: dict = {
                "pos": (open_ix, open_y - open_side * y_adjustment),
                "size": size,
                "pen": pen,
                "brush": brush,
                "symbol": open_symbol
            }

            close_scatter: dict = {
                "pos": (close_ix, close_y - close_side * y_adjustment),
                "size": size,
                "pen": pen,
                "brush": brush,
                "symbol": close_symbol
            }

            scatter_data.append(open_scatter)
            scatter_data.append(close_scatter)

            # Trade text
            volume = d["volume"]
            text_color: QtGui.QColor = QtGui.QColor(scatter_color)
            open_text: pg.TextItem = pg.TextItem(f"[{volume}]", color=text_color, anchor=(0.5, 0.5))
            close_text: pg.TextItem = pg.TextItem(f"[{volume}]", color=text_color, anchor=(0.5, 0.5))

            open_text.setPos(open_ix, open_y - open_side * y_adjustment * 3)
            close_text.setPos(close_ix, close_y - close_side * y_adjustment * 3)

            self.items.append(open_text)
            self.items.append(close_text)

            candle_plot.addItem(open_text)
            candle_plot.addItem(close_text)

        trade_scatter: pg.ScatterPlotItem = pg.ScatterPlotItem(scatter_data)
        self.items.append(trade_scatter)
        candle_plot.addItem(trade_scatter)

    def clear_data(self) -> None:
        """"""
        self.updated = False

        candle_plot: pg.PlotItem = self.chart.get_plot("candle")
        for item in self.items:
            candle_plot.removeItem(item)
        self.items.clear()

        self.chart.clear_all()

        self.dt_ix_map.clear()
        self.ix_bar_map.clear()

    def is_updated(self) -> bool:
        """"""
        return self.updated


def generate_trade_pairs(trades: list) -> list:
    """"""
    long_trades: list = []
    short_trades: list = []
    trade_pairs: list = []

    for trade in trades:
        trade = copy(trade)

        if trade.direction == Direction.LONG:
            same_direction: list = long_trades
            opposite_direction: list = short_trades
        else:
            same_direction = short_trades
            opposite_direction = long_trades

        while trade.volume and opposite_direction:
            open_trade: TradeData = opposite_direction[0]

            close_volume = min(open_trade.volume, trade.volume)
            d: dict = {
                "open_dt": open_trade.datetime,
                "open_price": open_trade.price,
                "close_dt": trade.datetime,
                "close_price": trade.price,
                "direction": open_trade.direction,
                "volume": close_volume,
            }
            trade_pairs.append(d)

            open_trade.volume -= close_volume
            if not open_trade.volume:
                opposite_direction.pop(0)

            trade.volume -= close_volume

        if trade.volume:
            same_direction.append(trade)

    return trade_pairs


class CacheInfoDialog(QtWidgets.QDialog):
    """
    Dialog for displaying CSV cache information.
    """

    def __init__(self, cache_info: dict, parent: QtWidgets.QWidget = None) -> None:
        """"""
        super().__init__(parent)

        self.cache_info = cache_info
        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle(_("CSV缓存情况"))
        self.resize(800, 600)

        # Debug: Print cache info
        print(f"DEBUG: CacheInfoDialog received cache_info: {self.cache_info}")

        # Create main layout
        layout = QtWidgets.QVBoxLayout()

        # Summary information
        summary_group = QtWidgets.QGroupBox(_("缓存概览"))
        summary_layout = QtWidgets.QFormLayout()

        cached_keys = self.cache_info.get("cached_keys", 0)
        total_records = self.cache_info.get("total_records", 0)

        summary_layout.addRow(_("缓存条目数:"), QtWidgets.QLabel(str(cached_keys)))
        summary_layout.addRow(_("总记录数:"), QtWidgets.QLabel(str(total_records)))
        summary_layout.addRow(_("内存占用:"), QtWidgets.QLabel(self._estimate_memory_usage()))

        summary_group.setLayout(summary_layout)
        layout.addWidget(summary_group)

        # Cache details table
        details_group = QtWidgets.QGroupBox(_("缓存详情"))
        details_layout = QtWidgets.QVBoxLayout()

        # Create table
        table = QtWidgets.QTableWidget()
        table.setColumnCount(4)
        table.setHorizontalHeaderLabels([_("品种"), _("周期"), _("时间范围"), _("记录数")])
        table.setAlternatingRowColors(True)
        table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)

        # Set column widths
        header = table.horizontalHeader()
        header.setSectionResizeMode(0, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QtWidgets.QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(3, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)

        # Populate table
        cache_keys = self.cache_info.get("cache_keys", [])
        print(f"DEBUG: Found {len(cache_keys)} cache keys to display")
        table.setRowCount(len(cache_keys))

        for row, cache_key in enumerate(cache_keys):
            print(f"DEBUG: Processing row {row}, cache_key: {cache_key}")
            # Parse cache key: {symbol}_{interval}_{start_time}_{end_time}
            # Format: rb8888_MINUTE_20230101_090000_20230101_091000
            parts = cache_key.split('_')
            if len(parts) >= 4:
                symbol = parts[0]
                interval = parts[1]
                start_time_str = parts[2] + "_" + parts[3]  # 20230101_090000
                end_time_str = parts[4] + "_" + parts[5]    # 20230101_091000

                # Format time range
                try:
                    start_formatted = f"{start_time_str[:8]} {start_time_str[9:11]}:{start_time_str[11:13]}:{start_time_str[13:]}"
                    end_formatted = f"{end_time_str[:8]} {end_time_str[9:11]}:{end_time_str[11:13]}:{end_time_str[13:]}"
                    time_range = f"{start_formatted} - {end_formatted}"
                except:
                    time_range = f"{start_time_str} - {end_time_str}"
            else:
                symbol = cache_key
                interval = "Unknown"
                time_range = "Unknown"

            # Get record count for this cache entry
            # Note: We don't have per-entry count in current implementation
            # This would require modifying the cache structure
            record_count = "N/A"

            table.setItem(row, 0, QtWidgets.QTableWidgetItem(symbol))
            table.setItem(row, 1, QtWidgets.QTableWidgetItem(interval))
            table.setItem(row, 2, QtWidgets.QTableWidgetItem(time_range))
            table.setItem(row, 3, QtWidgets.QTableWidgetItem(record_count))

            print(f"DEBUG: Set row {row}: {symbol}, {interval}, {time_range}, {record_count}")

        details_layout.addWidget(table)
        details_group.setLayout(details_layout)
        layout.addWidget(details_group)

        # Buttons
        buttons_layout = QtWidgets.QHBoxLayout()
        buttons_layout.addStretch()

        refresh_button = QtWidgets.QPushButton(_("刷新"))
        refresh_button.clicked.connect(self.refresh_cache_info)
        buttons_layout.addWidget(refresh_button)

        close_button = QtWidgets.QPushButton(_("关闭"))
        close_button.clicked.connect(self.accept)
        buttons_layout.addWidget(close_button)

        layout.addLayout(buttons_layout)

        self.setLayout(layout)

    def _estimate_memory_usage(self) -> str:
        """Estimate memory usage of cache."""
        total_records = self.cache_info.get("total_records", 0)

        # Rough estimate: each record ~1KB
        estimated_bytes = total_records * 1024

        if estimated_bytes < 1024:
            return f"{estimated_bytes} bytes"
        elif estimated_bytes < 1024 * 1024:
            return f"{estimated_bytes / 1024:.1f} KB"
        else:
            return f"{estimated_bytes / (1024 * 1024):.1f} MB"

    def refresh_cache_info(self) -> None:
        """Refresh cache information."""
        # Get updated cache info
        cache_info = self.parent().backtester_engine.get_csv_cache_info()

        # Update summary
        cached_keys = cache_info.get("cached_keys", 0)
        total_records = cache_info.get("total_records", 0)

        # Find and update summary labels
        summary_group = self.findChild(QtWidgets.QGroupBox, _("缓存概览"))
        if summary_group:
            layout = summary_group.layout()
            if layout and layout.rowCount() >= 3:
                # Update cached keys
                label = layout.itemAt(1, QtWidgets.QFormLayout.ItemRole.FieldRole).widget()
                if label:
                    label.setText(str(cached_keys))

                # Update total records
                label = layout.itemAt(3, QtWidgets.QFormLayout.ItemRole.FieldRole).widget()
                if label:
                    label.setText(str(total_records))

                # Update memory usage
                label = layout.itemAt(5, QtWidgets.QFormLayout.ItemRole.FieldRole).widget()
                if label:
                    label.setText(self._estimate_memory_usage())

        # Update table
        details_group = self.findChild(QtWidgets.QGroupBox, _("缓存详情"))
        if details_group:
            table = details_group.findChild(QtWidgets.QTableWidget)
            if table:
                cache_keys = cache_info.get("cache_keys", [])
                table.setRowCount(len(cache_keys))

                for row, cache_key in enumerate(cache_keys):
                    # Same parsing logic as in init_ui
                    # Format: rb8888_MINUTE_20230101_090000_20230101_091000
                    parts = cache_key.split('_')
                    if len(parts) >= 6:
                        symbol = parts[0]
                        interval = parts[1]
                        start_time_str = parts[2] + "_" + parts[3]  # 20230101_090000
                        end_time_str = parts[4] + "_" + parts[5]    # 20230101_091000

                        try:
                            start_formatted = f"{start_time_str[:8]} {start_time_str[9:11]}:{start_time_str[11:13]}:{start_time_str[13:]}"
                            end_formatted = f"{end_time_str[:8]} {end_time_str[9:11]}:{end_time_str[11:13]}:{end_time_str[13:]}"
                            time_range = f"{start_formatted} - {end_formatted}"
                        except:
                            time_range = f"{start_time_str} - {end_time_str}"
                    else:
                        symbol = cache_key
                        interval = "Unknown"
                        time_range = "Unknown"

                    record_count = "N/A"

                    table.setItem(row, 0, QtWidgets.QTableWidgetItem(symbol))
                    table.setItem(row, 1, QtWidgets.QTableWidgetItem(interval))
                    table.setItem(row, 2, QtWidgets.QTableWidgetItem(time_range))
                    table.setItem(row, 3, QtWidgets.QTableWidgetItem(record_count))

        QtWidgets.QMessageBox.information(self, _("成功"), _("缓存信息已刷新"))
