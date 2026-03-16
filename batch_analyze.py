#!/usr/bin/env python3
"""
批量分析SQL血缘关系
处理优化后目录下的所有SQL任务
"""

import sys
import os
import glob
import traceback
from pathlib import Path

# 添加项目根目录到路径
project_root = os.path.join(os.path.dirname(__file__), '.')
sys.path.insert(0, project_root)

from src.analyzers.enhanced_field_lineage import EnhancedFieldLineageAnalyzer
from trace_field_lineage import FieldLineageTracker
from extract_table_joins import TableJoinExtractor


def analyze_single_sql(sql_file, output_dir, metadata_files):
    """
    分析单个SQL文件

    Args:
        sql_file: SQL文件路径
        output_dir: 输出目录
        metadata_files: 元数据文件列表

    Returns:
        分析结果字典
    """
    try:
        # 读取SQL
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # 获取任务名称（SQL文件的父目录名）
        task_name = Path(sql_file).parent.name

        # 输出目录就是任务文件夹本身（输入和输出在同一文件夹）
        task_output_dir = str(Path(sql_file).parent)

        # 创建分析器
        analyzer = EnhancedFieldLineageAnalyzer(
            metadata_files=metadata_files,
            use_metadata=True
        )

        # 分析SQL
        result = analyzer.analyze_sql(sql_content, dialect='oracle')

        # 字段血缘追踪
        field_tracker = FieldLineageTracker(result['parser'])

        # 导出字段血缘CSV
        lineage_csv_file = os.path.join(task_output_dir, '字段血缘.csv')
        field_tracker.export_to_csv(lineage_csv_file)

        # 表关联关系提取
        join_extractor = TableJoinExtractor(result['parser'])

        # 导出表关联关系CSV
        joins_csv_file = os.path.join(task_output_dir, '表关联关系.csv')
        joins = join_extractor.extract_joins_from_sql(sql_content)
        join_extractor.export_joins_to_csv(joins, joins_csv_file)

        # 统计
        lineage_mapping = field_tracker.build_lineage_mapping()
        total_fields = len(lineage_mapping)
        traced_fields = sum(1 for m in lineage_mapping if m['source_fields'] and
                           not all(sf.get('metadata', {}).get('untraced') for sf in m['source_fields']))

        return {
            'task_name': task_name,
            'sql_file': sql_file,
            'success': True,
            'total_fields': total_fields,
            'traced_fields': traced_fields,
            'traceability': f"{traced_fields/total_fields*100:.1f}%" if total_fields > 0 else "0%",
            'total_joins': len(joins),
            'main_tables': sum(1 for j in joins if j.get('join_type') == 'FROM'),
            'join_tables': sum(1 for j in joins if j.get('join_type') == 'JOIN'),
            'lineage_csv': lineage_csv_file,
            'joins_csv': joins_csv_file
        }

    except Exception as e:
        return {
            'task_name': Path(sql_file).parent.name,
            'sql_file': sql_file,
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }


def batch_analyze(sql_files_pattern, output_dir, metadata_files):
    """
    批量分析SQL文件

    Args:
        sql_files_pattern: SQL文件模式
        output_dir: 输出目录
        metadata_files: 元数据文件列表
    """
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    # 查找所有SQL文件
    sql_files = glob.glob(sql_files_pattern)
    sql_files.sort()

    print("="*100)
    print("批量SQL血缘分析")
    print("="*100)
    print(f"找到 {len(sql_files)} 个SQL文件")
    print(f"输出目录: {output_dir}")
    print()

    # 批量分析
    results = []
    success_count = 0
    fail_count = 0

    for i, sql_file in enumerate(sql_files, 1):
        task_name = Path(sql_file).parent.name
        print(f"[{i}/{len(sql_files)}] 正在分析: {task_name}")

        result = analyze_single_sql(sql_file, output_dir, metadata_files)
        results.append(result)

        if result['success']:
            success_count += 1
            print(f"  ✓ 字段数: {result['total_fields']}, 追踪率: {result['traceability']}, 表关联: {result['total_joins']}")
        else:
            fail_count += 1
            print(f"  ✗ 分析失败: {result.get('error', 'Unknown error')}")

    # 输出汇总报告
    print("\n" + "="*100)
    print("分析完成")
    print("="*100)
    print(f"总任务数: {len(sql_files)}")
    print(f"成功: {success_count}")
    print(f"失败: {fail_count}")

    if success_count > 0:
        # 统计成功率
        success_results = [r for r in results if r['success']]
        avg_traceability = sum(
            float(r['traceability'].rstrip('%')) for r in success_results
        ) / len(success_results)

        print(f"\n平均字段追踪率: {avg_traceability:.1f}%")

        # 统计完美追踪的任务
        perfect_tasks = [r for r in success_results if r['traceability'] == '100.0%']
        print(f"完美追踪任务 (100%): {len(perfect_tasks)}/{success_count}")

    # 导出详细报告
    export_summary_report(results, output_dir)

    return results


def export_summary_report(results, output_dir):
    """
    导出汇总报告

    Args:
        results: 分析结果列表
        output_dir: 输出目录
    """
    import csv

    # 导出汇总CSV（保存到根目录，而不是tasks子目录）
    summary_file = '汇总报告.csv'

    with open(summary_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)

        # 写入表头
        writer.writerow([
            '序号',
            '任务名称',
            'SQL文件',
            '状态',
            '字段总数',
            '已追踪字段',
            '追踪率',
            '表关联数',
            '主表数',
            '关联表数',
            '错误信息'
        ])

        # 写入数据
        for i, result in enumerate(results, 1):
            if result['success']:
                writer.writerow([
                    i,
                    result['task_name'],
                    result['sql_file'],
                    '成功',
                    result['total_fields'],
                    result['traced_fields'],
                    result['traceability'],
                    result['total_joins'],
                    result['main_tables'],
                    result['join_tables'],
                    ''
                ])
            else:
                writer.writerow([
                    i,
                    result['task_name'],
                    result['sql_file'],
                    '失败',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    result.get('error', 'Unknown error')
                ])

    print(f"\n汇总报告已导出到: {summary_file}")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='批量SQL血缘分析工具')
    parser.add_argument('--sql-dir', default='tasks', help='任务文件夹目录')
    parser.add_argument('--output-dir', default='tasks', help='任务文件夹目录（输入和输出在同一文件夹）')
    parser.add_argument('--metadata', nargs='+',
                       default=['metadata/大数据ods的实例库表字段.csv',
                                'metadata/大数据dw和dm的实例库表字段.csv'],
                       help='元数据CSV文件')

    args = parser.parse_args()

    # 构建SQL文件模式
    sql_files_pattern = os.path.join(args.sql_dir, '*/原始.sql')

    # 批量分析
    batch_analyze(sql_files_pattern, args.output_dir, args.metadata)


if __name__ == '__main__':
    main()
