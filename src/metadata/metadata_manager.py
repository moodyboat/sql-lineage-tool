#!/usr/bin/env python3
"""
数据库元数据管理器
集成表和字段的元数据信息，提升字段来源推断准确率
"""

from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict
import csv


class TableMetadata:
    """表元数据"""

    def __init__(self, db_name: str, table_name: str, table_name_cn: str = ""):
        self.db_name = db_name
        self.table_name = table_name
        self.table_name_cn = table_name_cn
        self.columns: Dict[str, ColumnMetadata] = {}

    def add_column(self, column_metadata: 'ColumnMetadata'):
        """添加列元数据"""
        self.columns[column_metadata.column_name] = column_metadata

    def get_column(self, column_name: str) -> Optional['ColumnMetadata']:
        """获取列元数据"""
        return self.columns.get(column_name)

    def has_column(self, column_name: str) -> bool:
        """检查是否包含指定列"""
        return column_name in self.columns

    def get_all_column_names(self) -> List[str]:
        """获取所有列名"""
        return list(self.columns.keys())

    def __repr__(self) -> str:
        return f"TableMetadata({self.db_name}.{self.table_name}, {len(self.columns)} columns)"


class ColumnMetadata:
    """列元数据"""

    def __init__(self, db_name: str, table_name: str, column_name: str,
                 column_name_cn: str = "", data_type: str = "",
                 nullable: str = "Y", is_primary_key: str = "",
                 foreign_table: str = ""):
        self.db_name = db_name
        self.table_name = table_name
        self.column_name = column_name
        self.column_name_cn = column_name_cn
        self.data_type = data_type
        self.nullable = nullable
        self.is_primary_key = is_primary_key
        self.foreign_table = foreign_table

    def __repr__(self) -> str:
        return f"ColumnMetadata({self.column_name}, {self.data_type})"


class MetadataManager:
    """
    元数据管理器
    管理数据库表和字段的元数据信息
    """

    def __init__(self):
        """初始化元数据管理器"""
        # 表名到表元数据的映射
        # 格式: {db_name.table_name: TableMetadata}
        self.tables: Dict[str, TableMetadata] = {}

        # 字段名到表的映射（用于反向查找，大小写不敏感）
        # 格式: {column_name_upper: Set[table_name_upper]}
        self.column_to_tables: Dict[str, Set[str]] = defaultdict(set)

        # 数据库列表（大小写不敏感）
        self.databases: Set[str] = set()

        # 大小写映射（原始名称 -> 规范化名称）
        self.name_mapping: Dict[str, str] = {}

    @staticmethod
    def _normalize_name(name: str) -> str:
        """
        规范化名称（转为大写）

        Args:
            name: 原始名称

        Returns:
            规范化后的名称（大写）
        """
        return name.upper().strip() if name else ""

    def _get_full_table_key(self, db_name: str, table_name: str) -> str:
        """
        获取表的完整键（规范化）

        Args:
            db_name: 数据库名
            table_name: 表名

        Returns:
            规范化后的完整键
        """
        return f"{self._normalize_name(db_name)}.{self._normalize_name(table_name)}"

    def load_from_csv(self, csv_file_path: str, encoding: str = 'GBK'):
        """
        从CSV文件加载元数据

        Args:
            csv_file_path: CSV文件路径
            encoding: 文件编码，默认GBK
        """
        try:
            with open(csv_file_path, 'r', encoding=encoding) as f:
                reader = csv.DictReader(f)

                for row in reader:
                    db_name = row['库名'].strip()
                    table_name = row['表名'].strip()
                    table_name_cn = row['表名中文'].strip()
                    column_name = row['列名'].strip()
                    column_name_cn = row['列名中文'].strip()
                    data_type = row['数据类型'].strip()
                    nullable = row['是否允空'].strip()
                    is_primary_key = row['是否主键'].strip()
                    foreign_table = row['外键关联表名'].strip()

                    # 规范化名称（转为大写）
                    db_name_norm = self._normalize_name(db_name)
                    table_name_norm = self._normalize_name(table_name)
                    column_name_norm = self._normalize_name(column_name)

                    # 创建或获取表元数据
                    full_table_name_norm = f"{db_name_norm}.{table_name_norm}"

                    if full_table_name_norm not in self.tables:
                        table_metadata = TableMetadata(db_name, table_name, table_name_cn)
                        self.tables[full_table_name_norm] = table_metadata
                        self.databases.add(db_name_norm)

                        # 保存名称映射
                        self.name_mapping[f"{db_name}.{table_name}"] = full_table_name_norm
                    else:
                        table_metadata = self.tables[full_table_name_norm]

                    # 创建列元数据
                    column_metadata = ColumnMetadata(
                        db_name, table_name, column_name,
                        column_name_cn, data_type, nullable,
                        is_primary_key, foreign_table
                    )

                    # 添加列到表
                    table_metadata.add_column(column_metadata)

                    # 更新字段到表的映射（使用规范化名称）
                    self.column_to_tables[column_name_norm].add(full_table_name_norm)

            print(f"✓ 成功加载元数据: {csv_file_path}")
            print(f"  表数量: {len(self.tables)}")
            print(f"  数据库数量: {len(self.databases)}")

        except Exception as e:
            print(f"✗ 加载元数据失败: {csv_file_path}, 错误: {e}")

    def load_from_multiple_csv(self, csv_files: List[str]):
        """
        从多个CSV文件加载元数据

        Args:
            csv_files: CSV文件路径列表
        """
        for csv_file in csv_files:
            self.load_from_csv(csv_file)

    def get_table(self, db_name: str, table_name: str) -> Optional[TableMetadata]:
        """
        获取表元数据（大小写不敏感）

        Args:
            db_name: 数据库名
            table_name: 表名

        Returns:
            表元数据，如果不存在返回None
        """
        full_table_name = self._get_full_table_key(db_name, table_name)
        return self.tables.get(full_table_name)

    def find_table(self, full_table_name: str) -> Optional[TableMetadata]:
        """
        通过完整表名查找表元数据（支持schema.table格式）

        Args:
            full_table_name: 完整表名，可能是"table"或"schema.table"格式

        Returns:
            表元数据，如果不存在返回None
        """
        # 尝试解析schema.table格式
        if '.' in full_table_name:
            parts = full_table_name.split('.')
            if len(parts) == 2:
                schema, table = parts
                return self.get_table(schema, table)

        # 遍历所有数据库查找表
        table_name_upper = full_table_name.upper()
        for db_key, table_meta in self.tables.items():
            if table_meta.table_name.upper() == table_name_upper:
                return table_meta

        # 尝试作为完整key查找
        return self.tables.get(full_table_name.upper())

    def has_table(self, db_name: str, table_name: str) -> bool:
        """
        检查表是否存在（大小写不敏感）

        Args:
            db_name: 数据库名
            table_name: 表名

        Returns:
            表是否存在
        """
        full_table_name = self._get_full_table_key(db_name, table_name)
        return full_table_name in self.tables

    def has_column(self, db_name: str, table_name: str, column_name: str) -> bool:
        """
        检查表是否包含指定列（大小写不敏感）

        Args:
            db_name: 数据库名
            table_name: 表名
            column_name: 列名

        Returns:
            列是否存在
        """
        table = self.get_table(db_name, table_name)
        if table:
            column_name_norm = self._normalize_name(column_name)
            # 检查列名（大小写不敏感）
            for col in table.get_all_column_names():
                if self._normalize_name(col) == column_name_norm:
                    return True
        return False

    def find_tables_by_column(self, column_name: str, db_name: str = None) -> List[str]:
        """
        根据列名查找包含该列的表（大小写不敏感）

        Args:
            column_name: 列名
            db_name: 数据库名（可选，用于过滤）

        Returns:
            表名列表
        """
        column_name_norm = self._normalize_name(column_name)

        if column_name_norm not in self.column_to_tables:
            return []

        tables = list(self.column_to_tables[column_name_norm])

        if db_name:
            # 过滤指定数据库的表
            db_name_norm = self._normalize_name(db_name)
            tables = [t for t in tables if t.startswith(f"{db_name_norm}.")]

        return tables

    def get_column_info(self, db_name: str, table_name: str, column_name: str) -> Optional[ColumnMetadata]:
        """
        获取列元数据

        Args:
            db_name: 数据库名
            table_name: 表名
            column_name: 列名

        Returns:
            列元数据，如果不存在返回None
        """
        table = self.get_table(db_name, table_name)
        if table:
            return table.get_column(column_name)
        return None

    def resolve_field_source(self, field_name: str, candidate_tables: List[str],
                            db_name: str = None) -> Optional[str]:
        """
        解析字段来源表（利用元数据）

        Args:
            field_name: 字段名
            candidate_tables: 候选表名列表
            db_name: 数据库名（可选）

        Returns:
            来源表名，如果无法确定返回None
        """
        # 定义常见表的优先级（用于字段推断）
        TABLE_PRIORITY = {
            # 客户信息表（CUST_CODE, CUST_NAME, CUST_ID等字段）
            'LC00059999.CUST_CORP_INFO': 100,
            'LC00019999.CUST_CORP_INFOBK': 90,
            'LC00019999.DWD_CUST_CORP_INFO_HIS': 80,

            # 贷款相关表
            'LC00059999.CORP_LOAN_DUE_BILL': 100,
            'LC00059999.CORP_LOAN_CONT_BASE': 95,

            # 字典表
            'LC00059999.LSWBZD': 100,
            'LC00059999.LN_INDUSTRY_DIC': 100,
            'LC00059999.PRD_BUSINESS_TYPE': 100,

            # 公共字典表
            'PUB_DICT_ITEM': 100,
        }

        # 如果没有候选表，通过字段名查找
        if not candidate_tables:
            tables = self.find_tables_by_column(field_name, db_name)
            if len(tables) == 1:
                # 只找到一个表，返回它
                return tables[0].split('.', 1)[1] if '.' in tables[0] else tables[0]
            elif len(tables) > 1:
                # 找到多个表，按优先级排序
                tables_with_priority = []
                for table in tables:
                    full_table_name = table if '.' in table else f"LC00059999.{table}"
                    priority = TABLE_PRIORITY.get(full_table_name.upper(), 0)
                    tables_with_priority.append((priority, table))

                # 按优先级降序排序
                tables_with_priority.sort(key=lambda x: x[0], reverse=True)

                # 返回优先级最高的表
                if tables_with_priority[0][0] > 0:
                    best_table = tables_with_priority[0][1]
                    return best_table.split('.', 1)[1] if '.' in best_table else best_table
                else:
                    # 没有高优先级表，返回第一个
                    first_table = tables_with_priority[0][1]
                    return first_table.split('.', 1)[1] if '.' in first_table else first_table
            else:
                # 没找到
                return None

        # 如果有候选表，检查哪个表包含该字段
        matching_tables = []
        for table_name in candidate_tables:
            # 表名可能是简写或完整格式
            if '.' in table_name:
                # 完整格式: db_name.table_name
                parts = table_name.split('.')
                check_db = parts[0]
                check_table = parts[1] if len(parts) > 1 else parts[0]
            else:
                # 简写格式: table_name
                check_db = db_name
                check_table = table_name

            if self.has_column(check_db, check_table, field_name):
                matching_tables.append(table_name)

        if len(matching_tables) == 1:
            return matching_tables[0]
        elif len(matching_tables) > 1:
            # 多个表都有该字段，按优先级排序
            tables_with_priority = []
            for table in matching_tables:
                full_table_name = table if '.' in table else f"LC00059999.{table}"
                priority = TABLE_PRIORITY.get(full_table_name.upper(), 0)
                tables_with_priority.append((priority, table))

            # 按优先级降序排序
            tables_with_priority.sort(key=lambda x: x[0], reverse=True)
            return tables_with_priority[0][1]
        else:
            return None

    def get_table_statistics(self) -> Dict[str, int]:
        """
        获取统计信息

        Returns:
            统计信息字典
        """
        total_columns = sum(len(table.columns) for table in self.tables.values())

        # 按数据库统计表数量
        db_table_counts = defaultdict(int)
        for table_name in self.tables.keys():
            db_name = table_name.split('.')[0]
            db_table_counts[db_name] += 1

        return {
            'total_databases': len(self.databases),
            'total_tables': len(self.tables),
            'total_columns': total_columns,
            'db_table_counts': dict(db_table_counts)
        }

    def get_all_tables(self, db_name: str = None) -> List[str]:
        """
        获取所有表名

        Args:
            db_name: 数据库名（可选，用于过滤）

        Returns:
            表名列表
        """
        if db_name:
            return [t for t in self.tables.keys() if t.startswith(f"{db_name}.")]
        return list(self.tables.keys())

    def suggest_table_for_field(self, field_name: str, context_tables: List[str] = None,
                               db_name: str = None) -> List[Tuple[str, float]]:
        """
        为字段建议可能的来源表（带置信度）

        Args:
            field_name: 字段名
            context_tables: 上下文表列表（可选）
            db_name: 数据库名（可选）

        Returns:
            [(表名, 置信度)] 列表，按置信度降序排序
        """
        suggestions = []

        # 1. 如果有上下文表，优先检查
        if context_tables:
            for table_name in context_tables:
                # 解析表名
                if '.' in table_name:
                    parts = table_name.split('.')
                    check_db = parts[0]
                    check_table = parts[1] if len(parts) > 1 else parts[0]
                else:
                    check_db = db_name
                    check_table = table_name

                if self.has_column(check_db, check_table, field_name):
                    suggestions.append((table_name, 0.9))  # 高置信度

        # 2. 通过字段名反向查找
        tables = self.find_tables_by_column(field_name, db_name)
        for table in tables:
            # 避免重复
            table_name_only = table.split('.', 1)[1] if '.' in table else table
            if not any(table_name_only in s[0] for s in suggestions):
                confidence = 0.5 if len(tables) == 1 else 0.3  # 多个表时置信度较低
                suggestions.append((table_name_only, confidence))

        # 按置信度降序排序
        suggestions.sort(key=lambda x: x[1], reverse=True)

        return suggestions

    def __repr__(self) -> str:
        stats = self.get_table_statistics()
        return (f"MetadataManager(databases={stats['total_databases']}, "
                f"tables={stats['total_tables']}, "
                f"columns={stats['total_columns']})")
