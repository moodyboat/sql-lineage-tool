#!/usr/bin/env python3
"""
表关联关系提取工具
提取SQL中物理表之间的JOIN条件
"""

import sys
import os
import csv
import re
from collections import defaultdict

# 添加项目根目录到路径
project_root = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, project_root)

from src.analyzers.enhanced_field_lineage import EnhancedFieldLineageAnalyzer


import json


class TableJoinExtractor:
    """表关联关系提取器"""

    def __init__(self, parser):
        """
        初始化提取器

        Args:
            parser: SQLNodeParser 实例
        """
        self.parser = parser
        self.nodes = parser.nodes
        self.relationships = parser.relationships

        # 构建表别名映射
        self.table_alias_map = {}
        self._build_table_alias_map()

    def _build_table_alias_map(self):
        """构建表别名到物理表的映射"""
        for rel in self.relationships:
            if rel.type == 'REFERENCES':
                alias = rel.metadata.get('alias', '')
                if alias:
                    target_node = self.nodes.get(rel.target_id)
                    if target_node and target_node.type in ['TB', 'VW']:
                        self.table_alias_map[alias.upper()] = {
                            'table_name': target_node.name,
                            'node_id': target_node.id,
                            'alias': alias
                        }

    def extract_joins_from_sql(self, sql):
        """
        从SQL中提取JOIN条件

        Args:
            sql: SQL语句

        Returns:
            JOIN关系列表
        """
        joins = []

        # 找最外层查询的FROM...WHERE
        # 通常最外层的FROM...WHERE块不以括号开头
        from_where_pattern = r'\bFROM\s+(.*?)\s+WHERE\b'

        all_matches = list(re.finditer(from_where_pattern, sql, re.IGNORECASE | re.DOTALL))

        if not all_matches:
            return joins

        # 过滤掉子查询（以括号开头的）
        main_query_matches = [m for m in all_matches if not m.group(1).strip().startswith('(')]

        if not main_query_matches:
            # 如果所有匹配都是子查询，选择最长的
            longest_match = max(all_matches, key=lambda m: len(m.group(1)))
        else:
            # 选择最长的主查询匹配
            longest_match = max(main_query_matches, key=lambda m: len(m.group(1)))

        from_where_block = longest_match.group(1)

        # 提取主表（from_where_block 已经是 FROM 之后的内容）
        from_match = re.match(r'(\S+)(?:\s+(?:AS\s+)?(\w+))?', from_where_block.strip(), re.IGNORECASE)
        if from_match:
            main_table = from_match.group(1)
            main_alias = from_match.group(2) or ''

            # 跳过子查询（以括号开头的）
            if main_table != '(':
                # 映射到物理表
                if main_alias:
                    physical_info = self.table_alias_map.get(main_alias.upper())
                    physical_table = physical_info['table_name'] if physical_info else main_table
                else:
                    physical_table = main_table

                joins.append({
                    'table_name': physical_table,
                    'alias': main_alias,
                    'original_table': main_table,
                    'join_condition': '',
                    'join_type': 'FROM'
                })

        # 提取JOIN
        join_pattern = r'(?:INNER|LEFT|RIGHT|FULL)\s+JOIN\s+(\S+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+([^()]+?)(?=(?:INNER|LEFT|RIGHT|FULL|JOIN|WHERE|$))'

        for join_match in re.finditer(join_pattern, from_where_block, re.IGNORECASE):
            table = join_match.group(1)
            alias = join_match.group(2) or ''
            on_condition = join_match.group(3).strip()

            # 清理ON条件
            on_condition = re.sub(r'\s+', ' ', on_condition).strip()

            # 映射到物理表
            if alias:
                physical_info = self.table_alias_map.get(alias.upper())
                physical_table = physical_info['table_name'] if physical_info else table
            else:
                physical_table = table

            joins.append({
                'table_name': physical_table,
                'alias': alias,
                'original_table': table,
                'join_condition': on_condition,
                'join_type': 'JOIN'
            })

        return joins

    def extract_all_table_references(self):
        """
        提取所有表引用及其关系

        Returns:
            表引用列表
        """
        table_refs = []

        for rel in self.relationships:
            if rel.type == 'REFERENCES':
                target_node = self.nodes.get(rel.target_id)
                if target_node and target_node.type in ['TB', 'VW']:
                    source_node = self.nodes.get(rel.source_id)

                    table_refs.append({
                        'table_name': target_node.name,
                        'alias': rel.metadata.get('alias', ''),
                        'ref_type': rel.metadata.get('ref_type', 'TABLE'),
                        'referenced_by': source_node.type if source_node else '',
                        'referenced_by_name': source_node.name if source_node else ''
                    })

        return table_refs

    def export_joins_to_csv(self, joins, output_file):
        """
        导出JOIN关系到CSV

        Args:
            joins: JOIN关系列表
            output_file: 输出文件路径
        """
        with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)

            # 写入表头
            writer.writerow([
                '序号',
                '关联类型',
                '表名',
                '表别名',
                '关联条件'
            ])

            # 写入数据
            for i, join in enumerate(joins, 1):
                writer.writerow([
                    i,
                    join['join_type'],
                    join['table_name'],
                    join['alias'],
                    join['join_condition']
                ])

    def print_joins_summary(self, joins):
        """打印JOIN关系摘要"""
        print("\n" + "="*100)
        print("物理表关联关系")
        print("="*100)


        # 分组显示：主表和关联表
        main_tables = [j for j in joins if j.get('join_type') == 'FROM']
        join_tables_list = [j for j in joins if j.get('join_type') == 'JOIN']

        if main_tables:
            print("\n【主表】")
            for join in main_tables:
                table = join['table_name']
                alias = join.get('alias', '')
                print(f"  {table}")
                if alias:
                    print(f"  别名: {alias}")

        if join_tables_list:
            print("\n【关联表】")
            for i, join in enumerate(join_tables_list, 1):
                table = join['table_name']
                alias = join.get('alias', '')
                condition = join.get('join_condition', '')

                print(f"\n  {i}. {table}")
                if alias:
                    print(f"   别名: {alias}")
                print(f"   关联条件: {condition}")

        print("\n" + "="*100)
        main_count = len(main_tables)
        join_count = len(join_tables_list)
        total_count = len(joins)
        print(f"总计: {total_count} 个表引用")
        print(f"  - 主表: {main_count} 个")
        print(f"  - 关联表: {join_count} 个")
        print("="*100)


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='表关联关系提取工具')
    parser.add_argument('sql_file', help='SQL文件路径')
    parser.add_argument('--output', '-o', help='输出CSV文件路径', default=None)
    parser.add_argument('--dialect', default='mysql', help='SQL方言（默认mysql）')
    parser.add_argument('--metadata', nargs='+', help='元数据CSV文件')

    args = parser.parse_args()

    # 读取 SQL 文件
    print("="*100)
    print("表关联关系提取工具")
    print("="*100)
    print(f"SQL文件: {args.sql_file}")

    with open(args.sql_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    # 创建分析器
    print("\n正在分析SQL...")
    analyzer = EnhancedFieldLineageAnalyzer(
        metadata_files=args.metadata if args.metadata else None,
        use_metadata=bool(args.metadata)
    )

    result = analyzer.analyze_sql(sql_content, dialect=args.dialect)

    # 创建提取器
    extractor = TableJoinExtractor(result['parser'])

    # 提取JOIN关系
    joins = extractor.extract_joins_from_sql(sql_content)

    # 打印摘要
    extractor.print_joins_summary(joins)

    # 导出CSV
    if args.output:
        print(f"\n正在导出到: {args.output}")
        extractor.export_joins_to_csv(joins, args.output)
        print("✓ 导出完成")

    print("\n" + "="*100)
    print("完成")
    print("="*100)


if __name__ == '__main__':
    main()
