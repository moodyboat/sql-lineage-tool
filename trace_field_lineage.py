#!/usr/bin/env python3
"""
字段血缘追踪工具 - 直接映射版本
追踪最外层字段到底层物理表字段的映射关系，忽略中间层级
"""

import sys
import os
import json
import csv
from collections import defaultdict

# 添加项目根目录到路径
project_root = os.path.join(os.path.dirname(__file__), '.')
sys.path.insert(0, project_root)

from src.analyzers.enhanced_field_lineage import EnhancedFieldLineageAnalyzer


class FieldLineageTracker:
    """字段血缘追踪器 - 追踪最外层到底层的直接映射"""

    def __init__(self, parser):
        """
        初始化追踪器

        Args:
            parser: SQLNodeParser 实例（已完成解析）
        """
        self.parser = parser
        self.nodes = parser.nodes
        self.fields = parser.fields
        self.field_relationships = parser.field_relationships

        # 构建字段依赖图（反向：从目标到源）
        self.field_dependency_graph = defaultdict(list)
        for rel in self.field_relationships:
            if rel.type == 'DERIVES':
                self.field_dependency_graph[rel.target_id].append({
                    'source_id': rel.source_id,
                    'metadata': rel.metadata
                })

        # 构建表别名映射（从别名到物理表）
        self.table_alias_map = self._build_table_alias_map()

    def _build_table_alias_map(self):
        """构建表别名到物理表/子查询的映射（改进版 - 包含子查询别名）"""
        alias_map = {}

        # 从节点关系的 metadata 中提取别名映射
        for rel in self.parser.relationships:
            if rel.type == 'REFERENCES':
                alias = rel.metadata.get('alias', '')
                if alias:
                    target_node = self.nodes.get(rel.target_id)
                    if target_node and target_node.type in ['TB', 'VW']:
                        # 映射: 别名 -> 物理表名
                        alias_map[alias.upper()] = target_node.name

        # 【新增】从所有节点的 metadata.table_aliases 中提取别名映射
        for node in self.nodes.values():
            table_aliases = node.metadata.get('table_aliases', {})
            if table_aliases:
                for alias, alias_info in table_aliases.items():
                    if isinstance(alias_info, dict):
                        # 新格式：dict with table_name, node_id, etc.
                        if alias_info.get('is_subquery') or alias_info.get('is_cte'):
                            # 子查询或CTE：映射到节点ID
                            alias_map[alias.upper()] = {
                                'node_id': alias_info.get('node_id'),
                                'is_subquery': True,
                                'table_name': alias_info.get('table_name', alias)
                            }
                        else:
                            # 物理表：映射到表名
                            alias_map[alias.upper()] = alias_info.get('table_name', alias)
                    else:
                        # 旧格式：直接映射到表名
                        alias_map[alias.upper()] = alias_info

        return alias_map

    def find_root_fields(self):
        """找到最外层查询的字段（最终输出的字段）"""

        def is_valid_root_field(field):
            """
            判断字段是否是有效的根字段（最终输出字段）

            过滤条件：
            1. 不以 _ 开头（中间字段）
            2. 不以 field_ 开头（未命名字段）
            3. 字段名不为空
            """
            # 检查字段名
            field_name = field.name or field.column_name or ''

            # 过滤条件
            if not field_name:
                return False

            if field_name.startswith('_'):
                return False

            if field_name.startswith('field_'):
                return False

            return True

        # 优先级1：找深度最大的BLK节点（最外层的BLK）
        blk_nodes = [n for n in self.nodes.values() if n.type == 'BLK']
        if blk_nodes:
            max_depth_blk = max(blk_nodes, key=lambda n: n.depth)
            all_root_fields = [f for f in self.fields.values() if f.parent_node_id == max_depth_blk.id]
            # 过滤无效字段
            root_fields = [f for f in all_root_fields if is_valid_root_field(f)]
            if root_fields:
                return root_fields

        # 优先级2：找深度最大的非 TB/VW 节点的字段
        if not self.nodes:
            return []

        non_table_nodes = [n for n in self.nodes.values() if n.type not in ['TB', 'VW']]
        if non_table_nodes:
            max_depth = max(n.depth for n in non_table_nodes)
            outer_nodes = [n for n in non_table_nodes if n.depth == max_depth]

            root_fields = []
            for field in self.fields.values():
                if field.parent_node_id in [n.id for n in outer_nodes]:
                    if is_valid_root_field(field):
                        root_fields.append(field)

            if root_fields:
                return root_fields

        # 优先级3：返回空列表
        return []

    def trace_to_physical_table(self, field_id, visited=None):
        """
        递归追踪字段到底层物理表

        Args:
            field_id: 起始字段 ID
            visited: 已访问的字段 ID（防止循环）

        Returns:
            底层物理表字段列表
        """
        if visited is None:
            visited = set()

        if field_id in visited:
            return []

        visited.add(field_id)

        # 特殊处理：PHYSICAL_ 开头的 ID 表示直接引用物理表字段
        if field_id.startswith('PHYSICAL_'):
            # 解析 PHYSICAL_table.column 或 PHYSICAL_schema.table.column 格式
            physical_ref = field_id[9:]  # 去掉 'PHYSICAL_' 前缀
            parts = physical_ref.split('.')

            # 根据点号数量判断格式
            if len(parts) == 3:
                # 格式: PHYSICAL_schema.table.column
                schema_name, table_name, column_name = parts
                full_table_name = f"{schema_name}.{table_name}"
            elif len(parts) == 2:
                # 格式: PHYSICAL_table.column
                full_table_name, column_name = parts
            else:
                # 格式不正确，返回空
                return []

            # 查找对应的物理表字段
            physical_field = self._find_physical_table_field(full_table_name, column_name)
            if physical_field:
                return [{
                    'field_id': physical_field.id,
                    'field_name': physical_field.name,
                    'column_name': physical_field.column_name,
                    'table_name': physical_field.table_name,
                    'field_type': physical_field.field_type,
                    'metadata': {**physical_field.metadata, 'via_physical_ref': True}
                }]
            else:
                # 找不到物理表字段，返回解析的信息
                return [{
                    'field_id': field_id,
                    'field_name': column_name,
                    'column_name': column_name,
                    'table_name': full_table_name,
                    'field_type': 'COLUMN',
                    'metadata': {'note': 'Direct physical table reference', 'from_physical_ref': True}
                }]

        field = self.fields.get(field_id)
        if not field:
            return []

        # 检查字段所属节点类型
        parent_node = self.nodes.get(field.parent_node_id)

        # 如果是物理表节点（TB/VW），这是底层字段
        if parent_node and parent_node.type in ['TB', 'VW']:
            return [{
                'field_id': field_id,
                'field_name': field.name,
                'column_name': field.column_name,
                'table_name': field.table_name,
                'field_type': field.field_type,
                'metadata': field.metadata
            }]

        # 如果是 LITERAL 类型，返回空（没有数据源）
        if field.field_type == 'LITERAL':
            return [{
                'field_id': field_id,
                'field_name': field.name,
                'column_name': None,
                'table_name': None,
                'field_type': 'LITERAL',
                'metadata': {'note': 'Literal value, no data source'}
            }]

        # 查找依赖的源字段
        dependencies = self.field_dependency_graph.get(field_id, [])

        # 【新增】特殊处理：FUNCTION/AGGREGATION 类型字段没有依赖时，使用函数表达式本身作为来源
        if not dependencies and field.field_type in ['FUNCTION', 'AGGREGATION']:
            # 获取函数表达式（从 metadata 中提取）
            function_expr = field.metadata.get('expression_sql', '') or field.name or field.column_name or ''

            if function_expr:
                # 将函数表达式本身作为来源，标记为已追踪
                return [{
                    'field_id': field_id,
                    'field_name': field.name,
                    'column_name': function_expr,  # 使用函数表达式作为列名
                    'table_name': function_expr,   # 使用函数表达式作为表名
                    'field_type': field.field_type,
                    'metadata': {
                        **field.metadata,
                        'note': f'Function expression used as its own source: {function_expr}',
                        'function_as_source': True
                    }
                }]

        # 如果没有依赖关系，但有 table_name，尝试通过别名映射找到物理表
        if not dependencies and field.table_name:
            # 【改进】首先在当前字段的作用域（父节点）内查找别名映射
            parent_node = self.nodes.get(field.parent_node_id)
            found_in_scope = False

            if parent_node:
                # 从父节点的table_aliases中查找别名
                parent_table_aliases = parent_node.metadata.get('table_aliases', {})
                if field.table_name.upper() in parent_table_aliases:
                    alias_info = parent_table_aliases[field.table_name.upper()]
                    if isinstance(alias_info, dict) and alias_info.get('is_subquery'):
                        # 找到了子查询别名映射
                        subquery_node_id = alias_info.get('node_id')
                        subquery_node = self.nodes.get(subquery_node_id)

                        if subquery_node:
                            # 在子查询节点中查找匹配的字段
                            matched_field = None
                            for field_id_in_subquery in subquery_node.output_fields:
                                field_in_subquery = self.fields.get(field_id_in_subquery)
                                if field_in_subquery and field_in_subquery.column_name.upper() == field.column_name.upper():
                                    matched_field = field_in_subquery
                                    break

                            if matched_field:
                                # 递归追踪子查询字段
                                return self.trace_to_physical_table(matched_field.id, visited.copy())

                            # 找不到字段，标记为未追踪
                            return [{
                                'field_id': field_id,
                                'field_name': field.name,
                                'column_name': field.column_name,
                                'table_name': field.table_name,
                                'field_type': field.field_type,
                                'metadata': {**field.metadata, 'untraced': True, 'note': f'Subquery field not found: {field.table_name}.{field.column_name}'}
                            }]
                        found_in_scope = True

            # 如果在当前作用域没找到，尝试使用全局table_alias_map
            if not found_in_scope:
                mapped_info = self.table_alias_map.get(field.table_name.upper(), field.table_name)

                # 判断映射结果的类型
                is_subquery_mapping = False
                subquery_node_id = None
                physical_table_name = field.table_name

                if isinstance(mapped_info, dict):
                    # 新格式：dict with node_id, is_subquery, etc.
                    is_subquery_mapping = mapped_info.get('is_subquery', False)
                    subquery_node_id = mapped_info.get('node_id')
                    physical_table_name = mapped_info.get('table_name', field.table_name)
                else:
                    # 旧格式：字符串（表名）
                    physical_table_name = mapped_info
                    is_subquery_mapping = (field.table_name.upper() != physical_table_name.upper())

                # 如果是子查询映射，直接查找子查询节点
                if is_subquery_mapping or subquery_node_id:
                    # 使用node_id或通过名称查找子查询节点
                    subquery_node = None
                    if subquery_node_id:
                        subquery_node = self.nodes.get(subquery_node_id)
                    else:
                        # Fallback: 通过名称查找
                        for node in self.nodes.values():
                            if node.type in ['CT', 'SQ']:
                                # 检查name或alias是否匹配
                                if (node.name.upper() == field.table_name.upper() or
                                    (node.alias and node.alias.upper() == field.table_name.upper())):
                                    subquery_node = node
                                    break

                    if subquery_node:
                        # 在子查询节点中查找匹配的字段
                        matched_field = None
                        for field_id_in_subquery in subquery_node.output_fields:
                            field_in_subquery = self.fields.get(field_id_in_subquery)
                            if field_in_subquery and field_in_subquery.column_name.upper() == field.column_name.upper():
                                matched_field = field_in_subquery
                                break

                        if matched_field:
                            # 递归追踪子查询字段
                            return self.trace_to_physical_table(matched_field.id, visited.copy())

                    # 找不到子查询节点或字段，标记为未追踪
                    return [{
                        'field_id': field_id,
                        'field_name': field.name,
                        'column_name': field.column_name,
                        'table_name': field.table_name,
                        'field_type': field.field_type,
                        'metadata': {**field.metadata, 'untraced': True, 'note': f'Subquery node or field not found: {field.table_name}.{field.column_name}'}
                    }]
                else:
                    # 物理表映射，尝试查找物理表字段
                    physical_field = self._find_physical_table_field(physical_table_name, field.column_name)

                    if physical_field:
                        return [{
                            'field_id': physical_field.id,
                            'field_name': physical_field.name,
                            'column_name': physical_field.column_name,
                            'table_name': physical_field.table_name,
                            'field_type': physical_field.field_type,
                            'metadata': {**physical_field.metadata, 'via_alias': field.table_name}
                        }]
                    else:
                        # 找不到物理表字段，标记为未追踪
                        return [{
                            'field_id': field_id,
                            'field_name': field.name,
                            'column_name': field.column_name,
                            'table_name': physical_table_name,
                            'field_type': field.field_type,
                            'metadata': {**field.metadata, 'untraced': True, 'note': f'Physical table field not found: {physical_table_name}.{field.column_name}'}
                        }]
            else:
                # 【特殊处理】没有 table_name 但有字段名的 FUNCTION/AGGREGATION 字段
                if field.field_type in ['FUNCTION', 'AGGREGATION']:
                    function_expr = field.name or field.column_name or ''
                    if function_expr:
                        return [{
                            'field_id': field_id,
                            'field_name': field.name,
                            'column_name': function_expr,
                            'table_name': function_expr,
                            'field_type': field.field_type,
                            'metadata': {
                                **field.metadata,
                                'note': f'Function expression used as its own source: {function_expr}',
                                'function_as_source': True
                            }
                        }]

                # 无法追踪到底层
                return [{
                    'field_id': field_id,
                    'field_name': field.name,
                    'column_name': field.column_name,
                    'table_name': field.table_name if field.table_name else 'Unknown',
                    'field_type': field.field_type,
                    'metadata': {**field.metadata, 'untraced': True}
                }]

        # 递归追踪所有依赖
        result = []
        for dep in dependencies:
            source_field_id = dep['source_id']
            traced = self.trace_to_physical_table(source_field_id, visited.copy())
            result.extend(traced)

        return result

    def _find_physical_table_field(self, table_name, column_name):
        """
        在物理表中查找字段

        Args:
            table_name: 物理表名
            column_name: 字段名

        Returns:
            Field 对象或 None
        """
        if not column_name:
            return None

        # 标准化名称
        table_name_upper = table_name.upper()
        column_name_upper = column_name.upper()

        # 遍历所有字段，查找匹配的物理表字段
        for field in self.fields.values():
            node = self.nodes.get(field.parent_node_id)
            if node and node.type in ['TB', 'VW']:
                # 匹配表名和字段名（大小写不敏感）
                field_table_upper = field.table_name.upper() if field.table_name else ''
                field_column_upper = field.column_name.upper() if field.column_name else ''

                # 表名匹配：完全匹配或后缀匹配（lc00059999.st_deposit_draw 匹配 st_deposit_draw）
                table_matches = (
                    field_table_upper == table_name_upper or
                    field_table_upper.endswith('.' + table_name_upper)
                )

                if table_matches and field_column_upper == column_name_upper:
                    return field

        return None

    def build_lineage_mapping(self):
        """
        构建最外层到底层的字段映射

        Returns:
            字段映射列表
        """
        root_fields = self.find_root_fields()
        lineage_mapping = []

        for root_field in root_fields:
            # 追踪到底层物理表
            source_fields = self.trace_to_physical_table(root_field.id)

            lineage_mapping.append({
                'output_field': {
                    'field_id': root_field.id,
                    'field_name': root_field.name,
                    'column_name': root_field.column_name,
                    'table_name': root_field.table_name,
                    'field_type': root_field.field_type,
                    'metadata': root_field.metadata
                },
                'source_fields': source_fields
            })

        return lineage_mapping

    def export_to_json(self, output_file):
        """
        导出字段血缘映射到 JSON 文件

        Args:
            output_file: 输出文件路径
        """
        lineage_mapping = self.build_lineage_mapping()

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'lineage_mapping': lineage_mapping,
                'statistics': {
                    'total_output_fields': len(lineage_mapping),
                    'fields_with_sources': sum(1 for m in lineage_mapping if m['source_fields']),
                    'fields_without_sources': sum(1 for m in lineage_mapping if not m['source_fields'] or
                                                  all(sf.get('metadata', {}).get('untraced') for sf in m['source_fields']))
                }
            }, f, indent=2, ensure_ascii=False)

        return lineage_mapping

    def export_to_csv(self, output_file):
        """
        导出字段血缘映射到 CSV 文件（精简版）

        Args:
            output_file: 输出文件路径
        """
        lineage_mapping = self.build_lineage_mapping()

        with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)

            # 写入表头（精简版）
            writer.writerow([
                '输出字段名',
                '来源表名',
                '来源字段名',
                '是否可追踪'
            ])

            # 收集所有行，然后去重
            all_rows = []

            # 写入数据
            for mapping in lineage_mapping:
                output = mapping['output_field']
                sources = mapping['source_fields']

                # 输出字段信息
                output_field_name = output.get('field_name', '')

                # 如果没有来源字段，写入一行
                if not sources:
                    row = (
                        output_field_name,
                        '',
                        '',
                        '否'
                    )
                    all_rows.append(row)
                else:
                    # 为每个来源字段写入一行
                    for source in sources:
                        source_table = source.get('table_name', '')
                        source_column = source.get('column_name', '')
                        source_metadata = source.get('metadata', {})

                        # 判断是否可追踪
                        is_traced = not source_metadata.get('untraced', False)

                        row = (
                            output_field_name,
                            source_table,
                            source_column,
                            '是' if is_traced else '否'
                        )
                        all_rows.append(row)

            # 去重：只删除完全相同的行
            unique_rows = list(dict.fromkeys(all_rows))

            # 写入去重后的数据
            writer.writerows(unique_rows)

        return lineage_mapping

    def print_summary(self, lineage_mapping=None):
        """打印字段血缘映射摘要"""
        if lineage_mapping is None:
            lineage_mapping = self.build_lineage_mapping()

        print("\n" + "="*80)
        print("字段血缘映射 - 最外层到底层（忽略中间层级）")
        print("="*80)


        for mapping in lineage_mapping:
            output = mapping['output_field']
            sources = mapping['source_fields']

            print(f"\n【输出字段】{output['field_name']}")
            if output.get('column_name') and output['column_name'] != output['field_name']:
                print(f"  原字段名: {output['column_name']}")

            if sources:
                print(f"  【来源字段】({len(sources)}个)")
                for i, source in enumerate(sources, 1):
                    table = source.get('table_name', 'Unknown')
                    column = source.get('column_name', 'Unknown')
                    field_type = source.get('field_type', 'Unknown')

                    if source.get('metadata', {}).get('untraced'):
                        print(f"    {i}. ⚠️ {table}.{column} ({field_type}) [未追踪到底层]")
                    elif field_type == 'LITERAL':
                        print(f"    {i}. 📝 字面值（无数据源）")
                    else:
                        print(f"    {i}. ✅ {table}.{column} ({field_type})")
            else:
                print(f"  【来源字段】⚠️ 无法追踪到来源")

        # 统计
        print("\n" + "="*80)
        print("统计信息")
        print("="*80)
        total = len(lineage_mapping)
        with_sources = sum(1 for m in lineage_mapping if m['source_fields'] and
                          not all(sf.get('metadata', {}).get('untraced') for sf in m['source_fields']))
        without_sources = total - with_sources

        print(f"总字段数: {total}")
        print(f"有来源的字段: {with_sources} ({with_sources/total*100:.1f}%)")
        print(f"无法追踪的字段: {without_sources} ({without_sources/total*100:.1f}%)")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='字段血缘追踪工具 - 直接映射版本')
    parser.add_argument('sql_file', help='SQL文件路径')
    parser.add_argument('--output', '-o', help='输出文件路径（JSON或CSV，根据扩展名自动判断）', default=None)
    parser.add_argument('--format', '-f', choices=['json', 'csv'], default=None, help='输出格式（json或csv）')
    parser.add_argument('--dialect', default='oracle', help='SQL方言（默认oracle）')
    parser.add_argument('--metadata', nargs='+', help='元数据CSV文件')

    args = parser.parse_args()

    # 读取 SQL 文件
    print("="*80)
    print("字段血缘追踪工具")
    print("="*80)
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

    # 创建追踪器
    tracker = FieldLineageTracker(result['parser'])

    # 构建映射
    lineage_mapping = tracker.build_lineage_mapping()

    # 打印摘要
    tracker.print_summary(lineage_mapping)

    # 导出
    if args.output:
        # 判断输出格式
        output_lower = args.output.lower()

        if args.format:
            # 使用指定的格式
            output_format = args.format
        elif output_lower.endswith('.csv'):
            output_format = 'csv'
        elif output_lower.endswith('.json'):
            output_format = 'json'
        else:
            # 默认JSON
            output_format = 'json'

        print(f"\n正在导出到: {args.output} (格式: {output_format.upper()})")

        if output_format == 'csv':
            tracker.export_to_csv(args.output)
        else:
            tracker.export_to_json(args.output)

        print("✓ 导出完成")

    print("\n" + "="*80)
    print("完成")
    print("="*80)


if __name__ == '__main__':
    main()
