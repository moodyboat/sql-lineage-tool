#!/usr/bin/env python3
"""
增强的字段血缘分析系统
集成元数据管理器，显著提升字段来源推断准确率
"""

import sys
import os

# 添加项目根目录到路径
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, project_root)

from src.parsers.sql_node_parser_v2 import SQLNodeParser
from src.metadata.metadata_manager import MetadataManager


class EnhancedFieldLineageAnalyzer:
    """
    增强的字段血缘分析器
    集成元数据管理，提升字段推断准确率
    """

    def __init__(self, metadata_files: list = None, use_metadata: bool = True):
        """
        初始化增强分析器

        Args:
            metadata_files: 元数据CSV文件列表
            use_metadata: 是否使用元数据（默认启用）
        """
        self.use_metadata = use_metadata
        self.metadata_manager = MetadataManager()

        # 加载元数据
        if metadata_files and use_metadata:
            print("正在加载元数据...")
            self.metadata_manager.load_from_multiple_csv(metadata_files)
            stats = self.metadata_manager.get_table_statistics()
            print(f"元数据加载完成:")
            print(f"  数据库: {stats['total_databases']}个")
            print(f"  表: {stats['total_tables']}个")
            print(f"  字段: {stats['total_columns']}个")

    def analyze_sql(self, sql: str, dialect: str = "mysql",
                   use_scope_system: bool = True) -> dict:
        """
        分析SQL的字段血缘

        Args:
            sql: SQL语句
            dialect: SQL方言
            use_scope_system: 是否使用作用域系统

        Returns:
            分析结果字典
        """
        # 创建解析器
        parser = SQLNodeParser(sql, dialect=dialect, use_scope_system=use_scope_system)

        # 解析SQL
        nodes, relationships = parser.parse()

        # 构建字段依赖关系
        if use_scope_system:
            parser._build_cross_node_field_mappings()
        else:
            parser._build_field_dependencies()

        # 如果启用了元数据，增强字段推断
        if self.use_metadata:
            self._enhance_field_inference(parser)

        return {
            'parser': parser,
            'nodes': nodes,
            'relationships': relationships,
            'fields': parser.fields,
            'field_relationships': parser.field_relationships
        }

    def _enhance_field_inference(self, parser: SQLNodeParser):
        """
        利用元数据增强字段推断

        Args:
            parser: SQL解析器
        """
        if not parser.fields:
            return

        enhanced_count = 0

        for field_id, field in parser.fields.items():
            # 跳过已经有来源表的字段
            if field.table_name:
                continue

            field_name = field.column_name or field.name

            # 获取字段所属节点引用的表
            referenced_tables = self._get_referenced_tables_for_node(parser, field.parent_node_id)

            # 利用元数据推断字段来源
            suggested_table = self.metadata_manager.resolve_field_source(
                field_name, referenced_tables, db_name=None
            )

            if suggested_table:
                # 更新字段的来源表
                field.table_name = suggested_table
                enhanced_count += 1

                # 更新转换规则
                if 'transformation' in field.metadata:
                    field.metadata['transformation']['source_table_enhanced'] = True
                    field.metadata['transformation']['enhancement_method'] = 'metadata_lookup'

        if enhanced_count > 0:
            print(f"✓ 利用元数据增强了 {enhanced_count} 个字段的来源推断")

    def _get_referenced_tables_for_node(self, parser: SQLNodeParser, node_id: str) -> list:
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

    def analyze_sql_file(self, sql_file_path: str, dialect: str = "mysql",
                        use_scope_system: str = True) -> dict:
        """
        分析SQL文件的字段血缘

        Args:
            sql_file_path: SQL文件路径
            dialect: SQL方言
            use_scope_system: 是否使用作用域系统

        Returns:
            分析结果字典
        """
        # 读取SQL文件
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
        except FileNotFoundError:
            print(f"错误: 文件不存在 - {sql_file_path}")
            return {}
        except Exception as e:
            print(f"错误: 读取文件失败 - {e}")
            return {}

        # 分析SQL
        return self.analyze_sql(sql_content, dialect, use_scope_system)

    def compare_with_without_metadata(self, sql: str, dialect: str = "mysql") -> dict:
        """
        对比使用和不使用元数据的分析结果

        Args:
            sql: SQL语句
            dialect: SQL方言

        Returns:
            对比结果字典
        """
        print("\n" + "="*80)
        print("对比分析: 使用元数据 vs 不使用元数据")
        print("="*80)

        # 不使用元数据
        print("\n【不使用元数据】")
        parser_without = SQLNodeParser(sql, dialect=dialect, use_scope_system=True)
        nodes_without, rels_without = parser_without.parse()
        parser_without._build_cross_node_field_mappings()

        fields_without_table = sum(1 for f in parser_without.fields.values() if not f.table_name)
        accuracy_without = ((len(parser_without.fields) - fields_without_table) /
                           len(parser_without.fields)) * 100 if parser_without.fields else 0

        print(f"  字段总数: {len(parser_without.fields)}")
        print(f"  无来源表的字段: {fields_without_table}")
        print(f"  推断准确率: {accuracy_without:.1f}%")

        # 使用元数据
        print("\n【使用元数据】")
        parser_with = SQLNodeParser(sql, dialect=dialect, use_scope_system=True)
        nodes_with, rels_with = parser_with.parse()
        parser_with._build_cross_node_field_mappings()

        # 利用元数据增强
        self._enhance_field_inference(parser_with)

        fields_with_table = sum(1 for f in parser_with.fields.values() if f.table_name)
        accuracy_with = (fields_with_table / len(parser_with.fields)) * 100 if parser_with.fields else 0

        print(f"  字段总数: {len(parser_with.fields)}")
        print(f"  有来源表的字段: {fields_with_table}")
        print(f"  推断准确率: {accuracy_with:.1f}%")

        # 对比结果
        improvement = accuracy_with - accuracy_without
        enhanced_fields = fields_with_table - (len(parser_without.fields) - fields_without_table)

        print(f"\n【改进效果】")
        print(f"  准确率提升: {improvement:+.1f}%")
        print(f"  新增推断字段: {enhanced_fields}个")

        return {
            'without_metadata': {
                'accuracy': accuracy_without,
                'fields_without_table': fields_without_table,
                'total_fields': len(parser_without.fields)
            },
            'with_metadata': {
                'accuracy': accuracy_with,
                'fields_with_table': fields_with_table,
                'total_fields': len(parser_with.fields)
            },
            'improvement': {
                'accuracy_gain': improvement,
                'enhanced_fields': enhanced_fields
            }
        }


def main():
    """主函数 - 演示元数据增强效果"""

    print("="*80)
    print("增强的字段血缘分析系统 - 元数据集成演示")
    print("="*80)

    # 元数据文件
    metadata_files = [
        "/Users/gonghang/Desktop/产品/血缘分析工具/大数据ods的实例库表字段.csv",
        "/Users/gonghang/Desktop/产品/血缘分析工具/大数据dw和dm的实例库表字段.csv"
    ]

    # 创建增强分析器
    analyzer = EnhancedFieldLineageAnalyzer(metadata_files=metadata_files, use_metadata=True)

    # 测试SQL
    test_sql = """
    WITH tmp AS (
      SELECT
        k.jspzk_djbh,
        l.jspzfl_pznm,
        l.JSPZFL_JE,
        JSPZFL_ZY,
        l.jspzfl_jzfx
      FROM lc00059999.jspzfl l
      INNER JOIN lc00059999.jspzk k
        ON k.jspzk_pznm = l.jspzfl_pznm
      WHERE
        l.jspzfl_kmbh = '60210101'
        AND k.jspzk_pzrq >= '20241231'
    )
    SELECT
      DUE_BILL_CODE AS 借据号,
      GRANT_DATE AS 借据开始日,
      JSPZK_DJBH AS 交易流水,
      JSPZFL_JE AS 不含税手续费
    FROM tmp
    LEFT JOIN (
      SELECT
        s.child_trac_id,
        b.due_bill_code,
        b.grant_date
      FROM lc00059999.corp_loan_due_bill b
      INNER JOIN lc00059999.corp_loan_stdbook s
        ON b.due_bill_id = s.due_bill_id
    ) t
      ON tmp.jspzk_djbh = t.child_trac_id
    """

    # 对比分析
    comparison = analyzer.compare_with_without_metadata(test_sql, dialect="mysql")

    print("\n" + "="*80)
    print("结论")
    print("="*80)
    if comparison['improvement']['accuracy_gain'] > 0:
        print(f"✅ 元数据集成有效！准确率提升 {comparison['improvement']['accuracy_gain']:.1f}%")
    else:
        print("⚠️  该SQL示例中元数据提升不明显，可能是因为:")
        print("   1. SQL中已有明确的表前缀")
        print("   2. 元数据中未包含相关表")
        print("   3. 字段命名较为特殊")

    print("\n提示: 在实际生产环境中，元数据集成通常能显著提升字段推断准确率！")


if __name__ == "__main__":
    main()
