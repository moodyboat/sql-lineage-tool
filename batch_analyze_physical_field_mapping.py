#!/usr/bin/env python3
"""
批量SQL分析 - 字段到物理表映射关系追踪
分析优化后目录下的所有SQL文件，输出字段到最底层物理表的映射关系
"""

import sys
import os
import json
import time
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.analyzers.enhanced_field_lineage import EnhancedFieldLineageAnalyzer
from src.parsers.sql_node_parser_v2 import SQLNodeParser


class FieldToPhysicalTableMapper:
    """字段到物理表映射关系追踪器"""

    def __init__(self, metadata_manager=None):
        """初始化映射器"""
        # 字段到物理表的映射
        # {field_id: physical_table_name}
        self.field_to_physical_table: dict = {}

        # 字段到物理字段的映射
        # {field_id: physical_column_name}
        self.field_to_physical_column: dict = {}

        # SQL文件的分析结果
        self.sql_analyses: dict = {}

        # 元数据管理器（用于查找物理列名）
        self.metadata_manager = metadata_manager

    def analyze_sql_file(self, sql_file_path: str, analyzer: EnhancedFieldLineageAnalyzer):
        """
        分析单个SQL文件，建立字段映射关系

        Args:
            sql_file_path: SQL文件路径
            analyzer: 增强分析器实例
        """
        print(f"\n分析文件: {sql_file_path}")

        try:
            # 读取SQL文件
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            # 分析SQL
            result = analyzer.analyze_sql(sql_content, dialect="mysql", use_scope_system=True)

            parser = result['parser']
            fields = result['fields']
            nodes = result['nodes']

            # 为每个字段追溯物理表和物理列
            for field_id, field in fields.items():
                physical_table, physical_column = self._trace_to_physical_table_and_column(
                    field, nodes, parser
                )

                self.field_to_physical_table[field_id] = physical_table
                self.field_to_physical_column[field_id] = physical_column

            # 存储分析结果（使用唯一键：目录名_文件名）
            sql_dir_name = Path(sql_file_path).parent.name
            sql_file_name = Path(sql_file_path).name
            sql_key = f"{sql_dir_name}_{sql_file_name}"

            self.sql_analyses[sql_key] = {
                'file_path': sql_file_path,
                'sql_dir': sql_dir_name,
                'sql_name': sql_file_name,
                'parser': parser,
                'result': result,
                'field_count': len(fields),
                'physical_table_count': sum(1 for t in self.field_to_physical_table.values() if t),
                'success': True
            }

            # 显示简要统计
            fields_with_physical = sum(1 for t in self.field_to_physical_table.values() if t)
            print(f"  ✓ 字段总数: {len(fields)}")
            print(f"  ✓ 追溯到物理表: {fields_with_physical}")
            print(f"  ✓ 追溯成功率: {(fields_with_physical/len(fields)*100):.1f}%")

            return True

        except Exception as e:
            print(f"  ✗ 分析失败: {e}")
            sql_dir_name = Path(sql_file_path).parent.name
            sql_file_name = Path(sql_file_path).name
            sql_key = f"{sql_dir_name}_{sql_file_name}"

            self.sql_analyses[sql_key] = {
                'file_path': sql_file_path,
                'sql_dir': sql_dir_name,
                'sql_name': sql_file_name,
                'error': str(e),
                'success': False
            }
            return False

    def _trace_to_physical_table_and_column(self, field, nodes, parser):
        """
        追踪字段到物理表和物理列

        Args:
            field: 字段对象
            nodes: 所有节点
            parser: 解析器实例

        Returns:
            (物理表名, 物理列名)元组，如果无法追溯返回(None, None)
        """
        # 情况1：字段已经有table_name，且是物理表
        if field.table_name:
            # 检查是否是物理表（TB类型节点）
            for node_id, node in nodes.items():
                if node.type == "TB" and node.name == field.table_name:
                    # 通过元数据查找真实的物理列名
                    physical_column = self._find_physical_column(
                        field, field.table_name
                    )
                    return (field.table_name, physical_column)

            # 检查是否是CTE引用，需要继续追溯
            if field.table_name.upper() in parser.cte_names:
                return self._trace_cte_to_physical(field.table_name, field, nodes, parser)

        # 情况2：字段没有table_name，尝试通过字段关系追溯（优先级高）
        if hasattr(parser, 'field_relationships'):
            for rel in parser.field_relationships:
                if rel.target_id == field.id and rel.type == "DERIVES":
                    source_field = parser.fields.get(rel.source_id)
                    if source_field:
                        result = self._trace_to_physical_table_and_column(source_field, nodes, parser)
                        if result[0]:  # 如果找到了物理表
                            return result

        # 情况3：尝试通过元数据推断字段来源
        if self.metadata_manager and field.column_name:
            # 获取当前节点引用的所有表
            referenced_tables = self._get_referenced_tables_for_node(parser, field.parent_node_id)

            # 通过元数据解析字段来源
            suggested_table = self.metadata_manager.resolve_field_source(
                field.column_name, referenced_tables, db_name=None
            )

            if suggested_table:
                # 找到了来源表，现在查找物理列名
                physical_column = self._find_physical_column_from_metadata(
                    field.column_name, suggested_table
                )
                return (suggested_table, physical_column)

        # 情况4：处理没有column_name但有name的字段
        if self.metadata_manager and field.name and not field.column_name:
            referenced_tables = self._get_referenced_tables_for_node(parser, field.parent_node_id)
            suggested_table = self.metadata_manager.resolve_field_source(
                field.name, referenced_tables, db_name=None
            )

            if suggested_table:
                physical_column = self._find_physical_column_from_metadata(
                    field.name, suggested_table
                )
                return (suggested_table, physical_column)

        # 情况5：特殊处理常见字典表字段（标量子查询场景）
        if self.metadata_manager and field.column_name:
            dictionary_mapping = self._get_dictionary_table_mapping(field.column_name)
            if dictionary_mapping:
                table_name = dictionary_mapping['table']
                column_name = dictionary_mapping['column']
                return (table_name, column_name)

        return (None, None)

    def _get_dictionary_table_mapping(self, field_name: str) -> dict:
        """
        获取常见字典表字段的映射关系

        Args:
            field_name: 字段名

        Returns:
            {'table': 表名, 'column': 列名}，如果不是字典表字段返回None
        """
        # 常见字典表字段映射
        dictionary_mappings = {
            # 行业字典表
            'INDUSTRY_NAME': {'table': 'LC00059999.LN_INDUSTRY_DIC', 'column': 'INDUSTRY_NAME'},
            'INDUSTRY_CODE': {'table': 'LC00059999.LN_INDUSTRY_DIC', 'column': 'INDUSTRY_CODE'},

            # 币种字典表
            'F_WBMC': {'table': 'LC00059999.LSWBZD', 'column': 'F_WBMC'},
            'F_WBBH': {'table': 'LC00059999.LSWBZD', 'column': 'F_WBBH'},
            'F_WBDM': {'table': 'LC00059999.LSWBZD', 'column': 'F_WBDM'},

            # 产品业务类型表
            'TYPE_NAME': {'table': 'LC00059999.PRD_BUSINESS_TYPE', 'column': 'TYPE_NAME'},
            'ID': {'table': 'LC00059999.PRD_BUSINESS_TYPE', 'column': 'ID'},  # 特殊处理

            # 公共字典项表（元数据中可能缺失）
            'ITEM_VALUE': {'table': 'PUB_DICT_ITEM', 'column': 'ITEM_VALUE'},
            'ITEM_CODE': {'table': 'PUB_DICT_ITEM', 'column': 'ITEM_CODE'},
            'DICT_CODE': {'table': 'PUB_DICT_ITEM', 'column': 'DICT_CODE'},
        }

        return dictionary_mappings.get(field_name.upper())

    def _find_physical_column(self, field, table_name: str) -> str:
        """
        查找字段的物理列名

        Args:
            field: 字段对象
            table_name: 表名

        Returns:
            物理列名，如果找不到返回字段名本身
        """
        if not self.metadata_manager:
            return field.column_name or field.name

        # 尝试通过元数据查找物理列名
        column_name = field.column_name or field.name

        # 尝试在表中查找该列
        for db_name in [None, "LC00059999", "LC00019999", "DW", "DM"]:
            column_info = self.metadata_manager.get_column_info(
                db_name or "", table_name, column_name
            )
            if column_info:
                # 找到了列元数据，返回真实的物理列名
                return column_info.column_name

        # 如果找不到，返回字段名本身
        return column_name

    def _find_physical_column_from_metadata(self, field_name: str, table_name: str) -> str:
        """
        从元数据中查找物理列名

        Args:
            field_name: 字段名（可能是中文别名）
            table_name: 表名

        Returns:
            物理列名，如果找不到返回字段名本身
        """
        if not self.metadata_manager:
            return field_name

        # 尝试在不同数据库中查找
        for db_name in [None, "LC00059999", "LC00019999", "DW", "DM"]:
            table_info = self.metadata_manager.get_table(db_name or "", table_name)
            if table_info:
                # 尝试通过列名或中文别名查找
                column = table_info.get_column(field_name)
                if column:
                    return column.column_name

        return field_name

    def _find_table_by_column_name(self, column_name: str, candidate_tables: list) -> str:
        """
        通过列名查找表（用于没有table_name的字段）

        Args:
            column_name: 列名
            candidate_tables: 候选表列表

        Returns:
            表名，如果找不到返回None
        """
        if not self.metadata_manager or not candidate_tables:
            return None

        for table_name in candidate_tables:
            for db_name in [None, "LC00059999", "LC00019999", "DW", "DM"]:
                column_info = self.metadata_manager.get_column_info(
                    db_name or "", table_name, column_name
                )
                if column_info:
                    return table_name

        return None

    def _get_referenced_tables_for_node(self, parser, node_id: str) -> list:
        """
        获取节点引用的表列表

        Args:
            parser: SQL解析器
            node_id: 节点ID

        Returns:
            引用的表名列表
        """
        referenced_tables = []

        # 查找节点引用的关系
        for rel in parser.relationships:
            if rel.source_id == node_id and rel.type == "REFERENCES":
                target_node = parser.nodes.get(rel.target_id)
                if target_node and target_node.type in ["TB", "VW"]:
                    referenced_tables.append(target_node.name)

        return referenced_tables

    def _trace_cte_to_physical(self, cte_name: str, field, nodes, parser):
        """
        追溯CTE到物理表

        Args:
            cte_name: CTE名称
            field: 当前字段对象
            nodes: 所有节点
            parser: 解析器实例

        Returns:
            (物理表名, 物理列名)元组，如果无法追溯返回(None, None)
        """
        # 找到CTE节点
        cte_node = None
        for node in nodes.values():
            if node.type == "CT" and node.metadata.get("cte_name", "").upper() == cte_name.upper():
                cte_node = node
                break

        if not cte_node:
            return (None, None)

        # 查找CTE引用的物理表
        physical_tables = []
        for rel in parser.relationships:
            if rel.source_id == cte_node.id and rel.type == "REFERENCES":
                target_node = nodes.get(rel.target_id)
                if target_node and target_node.type == "TB":
                    physical_tables.append(target_node.name)

        if len(physical_tables) == 1:
            # 找到了唯一的物理表，查找物理列名
            physical_column = self._find_physical_column(field, physical_tables[0])
            return (physical_tables[0], physical_column)
        elif len(physical_tables) > 1:
            # 多个物理表，尝试通过字段名匹配
            for table in physical_tables:
                physical_column = self._find_physical_column(field, table)
                if physical_column:
                    return (table, physical_column)
            return (", ".join(physical_tables), None)
        else:
            return (None, None)

    def generate_mapping_report(self) -> dict:
        """
        生成映射关系报告

        Returns:
            报告字典
        """
        report = {
            'total_sql_files': len(self.sql_analyses),
            'successful_analyses': sum(1 for a in self.sql_analyses.values() if a.get('success', False)),
            'failed_analyses': sum(1 for a in self.sql_analyses.values() if not a.get('success', False)),
            'total_fields': 0,
            'fields_traced_to_physical': 0,
            'trace_success_rate': 0.0,
            'sql_details': []
        }

        for sql_key, analysis in self.sql_analyses.items():
            if not analysis.get('success', False):
                continue

            parser = analysis['parser']
            fields = parser.fields

            # 使用sql_name和sql_dir构建显示名称
            display_name = f"{analysis.get('sql_dir', '')}/{analysis.get('sql_name', '')}"

            sql_detail = {
                'sql_file': display_name,
                'sql_key': sql_key,
                'file_path': analysis['file_path'],
                'field_count': len(fields),
                'physical_table_count': 0,
                'fields': []
            }

            for field_id, field in fields.items():
                physical_table = self.field_to_physical_table.get(field_id)
                physical_column = self.field_to_physical_column.get(field_id, "")

                field_info = {
                    'field_id': field_id,
                    'field_name': field.name,
                    'column_name': field.column_name,
                    'physical_table': physical_table or "(未追溯)",
                    'physical_column': physical_column,
                    'is_traced': physical_table is not None
                }

                sql_detail['fields'].append(field_info)
                report['total_fields'] += 1

                if physical_table:
                    sql_detail['physical_table_count'] += 1
                    report['fields_traced_to_physical'] += 1

            report['sql_details'].append(sql_detail)

        # 计算成功率
        if report['total_fields'] > 0:
            report['trace_success_rate'] = (report['fields_traced_to_physical'] /
                                             report['total_fields']) * 100

        return report

    def export_physical_table_mappings(self, output_file: str):
        """
        导出到物理表字段映射关系

        Args:
            output_file: 输出文件路径
        """
        print(f"\n正在生成物理表字段映射关系...")

        # 收集所有映射关系
        mappings = defaultdict(list)  # {physical_table: [fields]}
        field_mappings = []  # 字段映射详情列表

        for sql_key, analysis in self.sql_analyses.items():
            if not analysis.get('success', False):
                continue

            for field_id, field in analysis['parser'].fields.items():
                physical_table = self.field_to_physical_table.get(field_id)

                if physical_table:
                    mapping_info = {
                        'sql_file': f"{analysis.get('sql_dir', '')}/{analysis.get('sql_name', '')}",
                        'field_id': field_id,
                        'field_name': field.name,
                        'column_name': field.column_name,
                        'physical_table': physical_table,
                        'physical_column': self.field_to_physical_column.get(field_id, ""),
                        'field_type': field.field_type,
                        'parent_node': field.parent_node_id
                    }

                    mappings[physical_table].append(mapping_info)
                    field_mappings.append(mapping_info)

        # 转换为列表并排序
        mappings_list = []
        for table, fields in sorted(mappings.items()):
            mappings_list.append({
                'physical_table': table,
                'field_count': len(fields),
                'fields': sorted(fields, key=lambda x: x['field_name'])
            })

        mappings_list.sort(key=lambda x: x['physical_table'])

        # 导出到JSON
        output_data = {
            'generated_time': datetime.now().isoformat(),
            'total_physical_tables': len(mappings_list),
            'total_field_mappings': sum(len(m['fields']) for m in mappings_list),
            'mappings': mappings_list,
            'field_mappings': field_mappings
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        print(f"  ✓ 已导出到: {output_file}")
        print(f"  ✓ 物理表数量: {len(mappings_list)}")
        print(f"  ✓ 字段映射总数: {len(field_mappings)}")

    def export_sql_level_summary(self, output_file: str):
        """
        导出SQL级别的汇总报告

        Args:
            output_file: 输出文件路径
        """
        print(f"\n正在生成SQL级别汇总报告...")

        report = self.generate_mapping_report()

        output_data = {
            'generated_time': datetime.now().isoformat(),
            'summary': {
                'total_sql_files': report['total_sql_files'],
                'successful_analyses': report['successful_analyses'],
                'failed_analyses': report['failed_analyses'],
                'total_fields': report['total_fields'],
                'fields_traced_to_physical': report['fields_traced_to_physical'],
                'trace_success_rate': f"{report['trace_success_rate']:.1f}%"
            },
            'sql_details': report['sql_details']
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        print(f"  ✓ 已导出到: {output_file}")


def batch_analyze_sql_files():
    """批量分析SQL文件"""

    print("="*80)
    print("批量SQL分析 - 字段到物理表映射关系追踪")
    print("="*80)

    # 配置
    sql_dir = "/Users/gonghang/Desktop/产品/血缘分析工具/优化后"
    metadata_files = [
        "metadata/大数据ods的实例库表字段.csv",
        "metadata/大数据dw和dm的实例库表字段.csv"
    ]

    # 查找SQL文件（递归搜索所有子目录）
    sql_files = list(Path(sql_dir).rglob("*.sql"))
    sql_files.sort()

    print(f"\n找到SQL文件: {len(sql_files)}个")
    print(f"目录: {sql_dir}")

    if not sql_files:
        print("错误: 未找到SQL文件")
        return

    # 创建分析器
    print("\n正在创建分析器...")
    analyzer = EnhancedFieldLineageAnalyzer(metadata_files=metadata_files)

    # 创建映射器（传入元数据管理器）
    mapper = FieldToPhysicalTableMapper(metadata_manager=analyzer.metadata_manager)

    # 批量分析
    print("\n开始批量分析...")
    start_time = time.time()

    success_count = 0
    for i, sql_file in enumerate(sql_files, 1):
        print(f"\n[{i}/{len(sql_files)}] {sql_file.name}")

        if mapper.analyze_sql_file(str(sql_file), analyzer):
            success_count += 1

    parse_time = time.time() - start_time

    # 生成报告
    print("\n" + "="*80)
    print("分析完成")
    print("="*80)

    # 显示总体统计
    report = mapper.generate_mapping_report()

    print(f"\n总体统计:")
    print(f"  SQL文件总数: {report['total_sql_files']}")
    print(f"  分析成功: {report['successful_analyses']}")
    print(f"  分析失败: {report['failed_analyses']}")
    print(f"  字段总数: {report['total_fields']}")
    print(f"  追溯到物理表: {report['fields_traced_to_physical']}")
    print(f"  追溯成功率: {report['trace_success_rate']:.1f}%")
    print(f"  分析耗时: {parse_time:.1f}秒")

    # 导出结果
    output_dir = "/Users/gonghang/Desktop/产品/血缘分析工具/批量分析结果"
    os.makedirs(output_dir, exist_ok=True)

    # 1. 物理表字段映射关系
    mapping_file = os.path.join(output_dir, "physical_table_mappings.json")
    mapper.export_physical_table_mappings(mapping_file)

    # 2. SQL级别汇总报告
    summary_file = os.path.join(output_dir, "sql_level_summary.json")
    mapper.export_sql_level_summary(summary_file)

    # 3. 简化版的CSV报告（便于Excel查看）
    csv_file = os.path.join(output_dir, "field_mappings.csv")
    export_csv_report(mapper, csv_file)

    print(f"\n所有结果已导出到: {output_dir}")

    # 显示前几个SQL的详细结果
    print("\n" + "="*80)
    print("SQL文件详细结果（前5个）")
    print("="*80)

    shown_count = 0
    for sql_detail in report['sql_details'][:5]:
        if shown_count >= 5:
            break

        print(f"\n{sql_detail['sql_file']}:")
        print(f"  字段总数: {sql_detail['field_count']}")
        print(f"  追溯到物理表: {sql_detail['physical_table_count']}")

        # 显示前3个字段
        fields_with_physical = [f for f in sql_detail['fields'] if f['is_traced']]
        for field in fields_with_physical[:3]:
            print(f"    • {field['field_name']} → {field['physical_table']}.{field['physical_column']}")

        shown_count += 1

    print("\n" + "="*80)
    print("✅ 批量分析完成！")
    print("="*80)


def export_csv_report(mapper: FieldToPhysicalTableMapper, output_file: str):
    """导出CSV格式的报告"""
    import csv

    print(f"  正在生成CSV报告...")

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        # 写入表头
        writer.writerow([
            'SQL文件', '字段名', '列名', '物理表', '物理列名',
            '字段类型', '父节点ID', '是否追溯成功'
        ])

        # 写入数据
        for sql_key, analysis in mapper.sql_analyses.items():
            if not analysis.get('success', False):
                continue

            display_name = f"{analysis.get('sql_dir', '')}/{analysis.get('sql_name', '')}"

            for field in analysis['parser'].fields.values():
                physical_table = mapper.field_to_physical_table.get(field.id)
                physical_column = mapper.field_to_physical_column.get(field.id, "")

                writer.writerow([
                    display_name,
                    field.name,
                    field.column_name,
                    physical_table or "(未追溯)",
                    physical_column,
                    field.field_type,
                    field.parent_node_id,
                    "是" if physical_table else "否"
                ])

    print(f"  ✓ CSV报告已导出")


def main():
    """主函数"""
    batch_analyze_sql_files()


if __name__ == "__main__":
    main()
