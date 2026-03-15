#!/usr/bin/env python3
"""
字段血缘分析系统 - 统一入口
Version 3.1 (元数据增强版)

使用方法:
    python main.py <sql_file> [选项]

示例:
    python main.py 原始.sql
    python main.py 原始.sql --output result.json
    python main.py 原始.sql --export-neo4j
    python main.py 原始.sql --no-metadata
"""

import sys
import os
import argparse
import time

# 添加src目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from parsers.sql_node_parser_v2 import SQLNodeParser
from analyzers.enhanced_field_lineage import EnhancedFieldLineageAnalyzer
from exporters.import_to_neo4j import Neo4jImporter


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='字段血缘分析系统 v3.1 - 元数据增强版',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  %(prog)s 原始.sql                    # 分析SQL文件
  %(prog)s 原始.sql --output result.json  # 导出JSON
  %(prog)s 原始.sql --export-neo4j       # 导出到Neo4j
  %(prog)s 原始.sql --no-metadata        # 禁用元数据增强
        '''
    )

    parser.add_argument('sql_file', help='SQL文件路径')
    parser.add_argument('--dialect', default='mysql', help='SQL方言（默认: mysql）')
    parser.add_argument('--output', help='输出JSON文件路径')
    parser.add_argument('--export-neo4j', action='store_true', help='导出到Neo4j')
    parser.add_argument('--no-metadata', action='store_true', help='禁用元数据增强')
    parser.add_argument('--no-scope', action='store_true', help='禁用作用域系统')
    parser.add_argument('--uri', default='bolt://localhost:7687', help='Neo4j URI')
    parser.add_argument('--user', default='neo4j', help='Neo4j用户名')
    parser.add_argument('--password', default='password', help='Neo4j密码')

    args = parser.parse_args()

    # 检查SQL文件是否存在
    if not os.path.exists(args.sql_file):
        print(f"错误: SQL文件不存在 - {args.sql_file}")
        sys.exit(1)

    # 显示横幅
    print("="*80)
    print("字段血缘分析系统 v3.1 - 元数据增强版")
    print("="*80)
    print(f"\nSQL文件: {args.sql_file}")
    print(f"SQL方言: {args.dialect}")

    # 读取SQL文件
    try:
        with open(args.sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
    except Exception as e:
        print(f"错误: 读取SQL文件失败 - {e}")
        sys.exit(1)

    # 配置元数据文件
    metadata_files = [
        "metadata/大数据ods的实例库表字段.csv",
        "metadata/大数据dw和dm的实例库表字段.csv"
    ]

    # 检查元数据文件是否存在
    if not args.no_metadata:
        metadata_exist = all(os.path.exists(f) for f in metadata_files)
        if not metadata_exist:
            print("警告: 元数据文件不存在，将禁用元数据增强")
            args.no_metadata = True

    # 创建分析器
    print("\n正在创建分析器...")
    use_metadata = not args.no_metadata

    if use_metadata:
        analyzer = EnhancedFieldLineageAnalyzer(
            metadata_files=metadata_files,
            use_metadata=True
        )
    else:
        # 使用基础解析器
        analyzer = None

    # 分析SQL
    print("\n正在分析SQL...")

    start_time = time.time()

    if use_metadata and analyzer:
        result = analyzer.analyze_sql(
            sql_content,
            dialect=args.dialect,
            use_scope_system=not args.no_scope
        )
    else:
        parser = SQLNodeParser(
            sql_content,
            dialect=args.dialect,
            use_scope_system=not args.no_scope
        )
        nodes, relationships = parser.parse()

        if not args.no_scope:
            parser._build_cross_node_field_mappings()
        else:
            parser._build_field_dependencies()

        result = {
            'parser': parser,
            'nodes': nodes,
            'relationships': relationships,
            'fields': parser.fields,
            'field_relationships': parser.field_relationships
        }

    parse_time = time.time() - start_time

    # 显示解析结果
    parser_obj = result['parser']
    fields = result['fields']

    fields_with_table = sum(1 for f in fields.values() if f.table_name)
    accuracy = (fields_with_table / len(fields)) * 100 if fields else 0

    print(f"\n✓ 分析完成")
    print(f"  解析时间: {parse_time:.3f}秒")
    print(f"  节点数: {len(result['nodes'])}")
    print(f"  字段数: {len(fields)}")
    print(f"  字段关系数: {len(result['field_relationships'])}")
    print(f"  字段推断准确率: {accuracy:.1f}%")

    # 导出JSON
    if args.output:
        print(f"\n正在导出到: {args.output}")
        parser_obj.export_json(args.output)
        print(f"✓ 导出完成")

    # 导出到Neo4j
    if args.export_neo4j:
        print("\n正在导出到Neo4j...")

        try:
            importer = Neo4jImporter(
                uri=args.uri,
                user=args.user,
                password=args.password
            )

            importer.import_sql_parser_results(
                result['nodes'],
                result['relationships'],
                list(result['fields'].values()),
                result['field_relationships']
            )

            importer.close()
            print("✓ Neo4j导出完成")
            print(f"\n提示: 打开 Neo4j Browser (http://localhost:7474) 查看数据")

        except Exception as e:
            print(f"✗ Neo4j导出失败: {e}")
            print("提示: 请确保Neo4j正在运行")

    print("\n" + "="*80)
    print("分析完成！")
    print("="*80)

    # 显示快速提示
    print("\n💡 提示:")
    print("  • 使用 --output 导出JSON文件")
    print("  • 使用 --export-neo4j 导出到Neo4j图数据库")
    print("  • 使用 --no-metadata 禁用元数据增强（更快但准确率较低）")
    print("  • 查看文档: docs/README.md")


if __name__ == "__main__":
    main()
