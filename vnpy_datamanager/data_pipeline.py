#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
VNpy 数据管理流水线
统一管理 .lc1文件导入、数据聚合和更新的完整流程
"""

import argparse
import os
import sys
from datetime import datetime


class DataPipelineManager:
    """数据流水线管理器"""

    def __init__(self):
        self.vnpy_path = self._find_vnpy_path()
        if not self.vnpy_path:
            raise RuntimeError("无法找到vnpy安装路径")

        # 添加路径以便导入vnpy模块
        sys.path.insert(0, self.vnpy_path)

        try:
            from vnpy.trader.engine import MainEngine, EventEngine
            from .engine import ManagerEngine
        except ImportError as e:
            raise RuntimeError(f"无法导入vnpy模块: {e}")

        # 创建引擎实例
        self.main_engine = MainEngine()
        self.event_engine = EventEngine()
        self.data_manager = ManagerEngine(self.main_engine, self.event_engine)

    def _find_vnpy_path(self):
        """查找vnpy安装路径"""
        # 首先尝试从当前脚本位置查找
        current_dir = os.path.dirname(os.path.abspath(__file__))
        vnpy_path = os.path.dirname(current_dir)  # 上级目录

        # 检查是否包含vnpy模块
        if os.path.exists(os.path.join(vnpy_path, "vnpy")):
            return vnpy_path

        # 尝试其他常见位置
        for path in sys.path:
            if os.path.exists(os.path.join(path, "vnpy")):
                return path

        return None

    def run_pipeline(self, source_dir=None, target_dir=None, auto_aggregate=True, force_update=False):
        """运行完整的数据更新流水线"""
        print("=== VNpy 数据管理流水线 ===")
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # 设置默认路径
        if source_dir is None:
            source_dir = r'C:\new_tdxqh\vipdoc\ds\minline'

        if target_dir is None:
            target_dir = r'C:\new_tdxqh\vipdoc\ds\minline\csv'

        print(f"源目录: {source_dir}")
        print(f"目标目录: {target_dir}")
        print(f"自动聚合: {auto_aggregate}")
        print(f"强制更新: {force_update}")
        print()

        # 运行流水线
        result = self.data_manager.run_data_update_pipeline(
            source_dir=source_dir,
            target_dir=target_dir,
            auto_aggregate=auto_aggregate,
            force_update=force_update
        )

        print()
        print("=== 处理完成 ===")
        for key, value in result.items():
            print(f"{key}: {value}")

        return result

    def check_status(self):
        """检查数据更新状态"""
        print("=== 数据更新状态检查 ===")

        status = self.data_manager.get_data_update_status()
        print(f"最后更新时间: {status.get('last_update', '从未更新')}")

        contracts = status.get('contracts', {})
        print(f"已跟踪合约数量: {len(contracts)}")

        if contracts:
            print("最近更新的合约:")
            # 按更新时间排序
            sorted_contracts = sorted(
                contracts.items(),
                key=lambda x: x[1].get('last_update', ''),
                reverse=True
            )[:10]  # 只显示最近10个

            for contract, info in sorted_contracts:
                last_update = info.get('last_update', '未知')
                print(f"  {contract}: {last_update}")

        processed_files = status.get('processed_files', [])
        print(f"已处理文件数量: {len(processed_files)}")

        return status

    def check_updates(self, source_dir=None):
        """检查可用的更新"""
        print("=== 检查数据更新 ===")

        if source_dir is None:
            source_dir = r'C:\new_tdxqh\vipdoc\ds\minline'

        print(f"检查目录: {source_dir}")

        update_info = self.data_manager.check_for_updates(source_dir)

        pending_files = update_info.get('pending_files', [])
        contracts_needing_update = update_info.get('contracts_needing_update', [])

        print(f"待处理文件数量: {len(pending_files)}")
        if pending_files:
            print("待处理文件:")
            for file_path in pending_files[:5]:  # 只显示前5个
                print(f"  {os.path.basename(file_path)}")
            if len(pending_files) > 5:
                print(f"  ... 还有 {len(pending_files) - 5} 个文件")

        print(f"需要更新的合约数量: {len(contracts_needing_update)}")
        if contracts_needing_update:
            print("需要更新的合约:")
            for contract in contracts_needing_update[:5]:  # 只显示前5个
                print(f"  {contract}")
            if len(contracts_needing_update) > 5:
                print(f"  ... 还有 {len(contracts_needing_update) - 5} 个合约")

        return update_info


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='VNpy 数据管理流水线')
    parser.add_argument('action', choices=['run', 'status', 'check'],
                       help='执行操作: run(运行流水线), status(查看状态), check(检查更新)')

    parser.add_argument('--source-dir', help='lc1文件源目录路径')
    parser.add_argument('--target-dir', help='CSV文件目标目录路径')
    parser.add_argument('--no-aggregate', action='store_true', help='不自动聚合高周期数据')
    parser.add_argument('--force-update', action='store_true', help='强制更新所有数据')

    args = parser.parse_args()

    try:
        # 创建流水线管理器
        pipeline = DataPipelineManager()

        if args.action == 'run':
            # 运行数据更新流水线
            pipeline.run_pipeline(
                source_dir=args.source_dir,
                target_dir=args.target_dir,
                auto_aggregate=not args.no_aggregate,
                force_update=args.force_update
            )

        elif args.action == 'status':
            # 查看状态
            pipeline.check_status()

        elif args.action == 'check':
            # 检查更新
            pipeline.check_updates(args.source_dir)

    except Exception as e:
        print(f"程序执行出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
