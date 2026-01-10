#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
VNpy 数据管理流水线使用示例
演示如何使用统一的数据管理流程
"""

import os
import sys
from datetime import datetime


def example_basic_usage():
    """基本使用示例"""
    print("=== VNpy 数据管理流水线 - 基本使用示例 ===\n")

    try:
        from vnpy_datamanager.data_pipeline import DataPipelineManager

        # 创建流水线管理器
        pipeline = DataPipelineManager()

        # 检查当前状态
        print("1. 检查当前数据状态:")
        status = pipeline.check_status()
        print(f"   最后更新: {status.get('last_update', '从未更新')}")
        print(f"   已跟踪合约: {len(status.get('contracts', {}))} 个\n")

        # 检查可用更新
        print("2. 检查可用更新:")
        updates = pipeline.check_updates()
        print(f"   待处理文件: {len(updates['pending_files'])} 个")
        print(f"   需要更新的合约: {len(updates['contracts_needing_update'])} 个\n")

        if updates['pending_files']:
            print("   待处理文件列表:")
            for i, file_path in enumerate(updates['pending_files'][:5], 1):
                print(f"     {i}. {os.path.basename(file_path)}")
            if len(updates['pending_files']) > 5:
                print(f"     ... 还有 {len(updates['pending_files']) - 5} 个文件\n")

        # 运行数据更新流水线
        if updates['pending_files'] or updates['contracts_needing_update']:
            print("3. 运行数据更新流水线:")
            result = pipeline.run_pipeline(
                auto_aggregate=True,
                force_update=False
            )

            print("   处理结果:")
            print(f"     转换文件: {result['converted_files']} 个")
            print(f"     导入合约: {result['imported_contracts']} 个")
            print(f"     聚合小时线: {result['aggregated_hourly']} 条")
            print(f"     聚合日线: {result['aggregated_daily']} 条")
            if result['errors'] > 0:
                print(f"     错误数量: {result['errors']} 个")
        else:
            print("3. 没有需要更新的数据，跳过流水线执行")

    except ImportError as e:
        print(f"导入错误: {e}")
        print("请确保vnpy_datamanager已正确安装")
    except Exception as e:
        print(f"执行出错: {e}")
        import traceback
        traceback.print_exc()


def example_custom_paths():
    """自定义路径使用示例"""
    print("\n=== 自定义路径使用示例 ===\n")

    try:
        from vnpy_datamanager.data_pipeline import DataPipelineManager

        # 指定自定义路径
        custom_source = r"C:\your\custom\lcd\path"
        custom_target = r"C:\your\custom\csv\path"

        print(f"自定义源目录: {custom_source}")
        print(f"自定义目标目录: {custom_target}")

        pipeline = DataPipelineManager()

        # 检查自定义路径
        updates = pipeline.check_updates(custom_source)
        print(f"自定义路径下待处理文件: {len(updates['pending_files'])} 个")

        if updates['pending_files']:
            # 运行流水线
            result = pipeline.run_pipeline(
                source_dir=custom_source,
                target_dir=custom_target,
                auto_aggregate=True,
                force_update=False
            )
            print(f"处理完成: {result}")

    except Exception as e:
        print(f"执行出错: {e}")


def example_force_update():
    """强制更新示例"""
    print("\n=== 强制更新示例 ===\n")

    try:
        from vnpy_datamanager.data_pipeline import DataPipelineManager

        pipeline = DataPipelineManager()

        print("执行强制更新（将重新处理所有数据）...")
        result = pipeline.run_pipeline(
            auto_aggregate=True,
            force_update=True  # 强制更新所有数据
        )

        print("强制更新结果:")
        for key, value in result.items():
            print(f"  {key}: {value}")

    except Exception as e:
        print(f"执行出错: {e}")


def example_monitoring():
    """监控和状态检查示例"""
    print("\n=== 监控和状态检查示例 ===\n")

    try:
        from vnpy_datamanager.data_pipeline import DataPipelineManager

        pipeline = DataPipelineManager()

        # 详细的状态检查
        print("当前系统状态:")
        status = pipeline.get_data_update_status()

        print(f"最后更新时间: {status.get('last_update', '无')}")

        contracts = status.get('contracts', {})
        print(f"\n已跟踪合约数量: {len(contracts)}")

        if contracts:
            print("\n最近更新的合约:")
            # 按更新时间排序
            sorted_contracts = sorted(
                contracts.items(),
                key=lambda x: x[1].get('last_update', ''),
                reverse=True
            )

            for contract, info in sorted_contracts[:10]:  # 显示前10个
                last_update = info.get('last_update', '未知')
                print(f"  {contract}: {last_update}")

        processed_files = status.get('processed_files', [])
        print(f"\n已处理文件数量: {len(processed_files)}")

        if processed_files:
            print("\n最近处理的文件:")
            for file_path in processed_files[-5:]:  # 显示最后5个
                print(f"  {os.path.basename(file_path)}")

    except Exception as e:
        print(f"执行出错: {e}")


def main():
    """主函数"""
    print("VNpy 数据管理流水线使用示例")
    print("=" * 50)

    # 运行各种示例
    example_basic_usage()
    example_custom_paths()
    example_force_update()
    example_monitoring()

    print("\n" + "=" * 50)
    print("示例执行完成")
    print("\n更多信息请查看 README.md 文件")


if __name__ == '__main__':
    main()
