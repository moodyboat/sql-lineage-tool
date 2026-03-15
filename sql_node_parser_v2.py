#!/usr/bin/env python3
"""
SQL节点解析器
识别SQL脚本中的所有节点及其关系
"""

import sqlglot
from sqlglot import exp, parse
from collections import defaultdict
from typing import List, Dict, Set, Tuple, Optional
from dataclasses import dataclass, field
import json
import re


# ============================================================================
# SQL预处理器
# ============================================================================

class SQLPreprocessor:
    """SQL预处理器 - 处理特殊语法和模板变量"""

    def __init__(self, sql: str):
        self.original_sql = sql
        self.preprocessed_sql = sql
        self.var_mappings = {}  # 模板变量映射
        self.comments = []     # 提取的注释

    def preprocess(self) -> Tuple[str, dict]:
        """
        预处理SQL
        返回: (处理后的SQL, 变量映射字典)
        """
        # 1. 处理Oracle特殊语法
        self._handle_oracle_syntax()

        # 2. 处理模板变量
        self._handle_template_variables()

        # 3. 处理注释
        self._handle_comments()

        # 4. 处理特殊函数
        self._handle_special_functions()

        return self.preprocessed_sql, self.var_mappings

    def _handle_oracle_syntax(self):
        """处理Oracle特殊语法"""

        # 处理 TO_CHAR 函数 - 简化策略：直接移除格式参数
        # to_char(expr, 'format') -> expr

        # 处理 to_char(round(..., digits), 'format')
        self.preprocessed_sql = re.sub(
            r'to_char\(\s*\(round\([^)]+\)\s*,\s*\d+\)\s*,\s*[\'"][^\'"]*[\'"]\s*\)',
            lambda m: m.group(0).split('),')[0].replace('to_char(', '').rstrip(),
            self.preprocessed_sql,
            flags=re.IGNORECASE
        )

        # 处理 to_char(round(..., digits), 'format') - 不带外层括号
        self.preprocessed_sql = re.sub(
            r'to_char\(\s*round\([^)]+\)\s*,\s*\d+\s*,\s*[\'"][^\'"]*[\'"]\s*\)',
            lambda m: 'round' + m.group(0).split('round')[1].rsplit(',', 1)[0] + ')',
            self.preprocessed_sql,
            flags=re.IGNORECASE
        )

        # 处理简单的 to_char(expr, 'format')
        self.preprocessed_sql = re.sub(
            r'to_char\(\s*([^,]+?)\s*,\s*[\'"][^\'"]*[\'"]\s*\)',
            r'\1',
            self.preprocessed_sql,
            flags=re.IGNORECASE
        )

        # 处理 TO_DATE
        self.preprocessed_sql = re.sub(
            r'to_date\(([^,]+),\s*[\'"][^\'"]*[\'"]\)',
            r'CAST(\1 AS DATE)',
            self.preprocessed_sql,
            flags=re.IGNORECASE
        )

        # 处理 NVL
        self.preprocessed_sql = re.sub(
            r'nvl\(([^,]+),\s*([^)]+)\)',
            r'COALESCE(\1, \2)',
            self.preprocessed_sql,
            flags=re.IGNORECASE
        )

        # 处理 DECODE - 支持多分支
        def decode_to_case(match):
            decode_content = match.group(1)
            # 按逗号分割，但要考虑括号和引号
            parts = []
            current = []
            in_parens = 0
            in_quotes = False
            quote_char = None

            for char in decode_content:
                if char in ("'", '"') and (not in_quotes or quote_char == char):
                    in_quotes = not in_quotes
                    quote_char = char if in_quotes else None
                    current.append(char)
                elif in_quotes:
                    current.append(char)
                elif char == '(':
                    in_parens += 1
                    current.append(char)
                elif char == ')':
                    in_parens -= 1
                    current.append(char)
                elif char == ',' and in_parens == 0:
                    parts.append(''.join(current).strip())
                    current = []
                else:
                    current.append(char)

            if current:
                parts.append(''.join(current).strip())

            if len(parts) < 4:
                # 参数不够，不转换
                return match.group(0)

            expr = parts[0]
            case_parts = []
            i = 1
            while i < len(parts) - 1:
                if i + 1 < len(parts):
                    search = parts[i]
                    result = parts[i + 1]
                    case_parts.append(f'WHEN {search} THEN {result}')
                    i += 2
                else:
                    # 最后一个参数作为default
                    default = parts[i]
                    case_parts.append(f'ELSE {default}')
                    break

            if i < len(parts):
                # 还有未处理的参数，最后一个作为default
                if 'ELSE' not in case_parts[-1]:
                    case_parts.append(f'ELSE {parts[-1]}')

            return f'CASE {expr} {" ".join(case_parts)} END'

        self.preprocessed_sql = re.sub(
            r'decode\((.*?)\)',
            decode_to_case,
            self.preprocessed_sql,
            flags=re.IGNORECASE | re.DOTALL
        )

        # 处理 dual 表（替换为可以解析的形式）
        self.preprocessed_sql = re.sub(
            r'\bfrom\s+dual\b',
            'FROM (SELECT 1 AS dummy)',
            self.preprocessed_sql,
            flags=re.IGNORECASE
        )

    def _handle_template_variables(self):
        """处理模板变量（如 <!JEDW!>）"""

        # 匹配 <!VAR!> 格式的变量
        pattern = r'<!([A-Z_]+)!>'

        def replace_var(match):
            var_name = match.group(1)
            # 为每个变量生成一个替换值
            replacement_map = {
                'JEDW': '10000',  # 金额单位
                'KSSJ': '20240101',  # 开始时间
                'JSSJ': '20241231',  # 结束时间
                'KHMC': '',  # 客户名称
                'HTBH': '',  # 合同编号
                'YWLX': '',  # 业务类型
            }

            value = replacement_map.get(var_name, '1')

            # 记录映射关系
            self.var_mappings[var_name] = {
                'original': match.group(0),
                'replacement': value
            }

            return value

        self.preprocessed_sql = re.sub(pattern, replace_var, self.preprocessed_sql)

    def _handle_comments(self):
        """处理SQL注释"""
        # 移除单行注释
        self.preprocessed_sql = re.sub(
            r'--[^\n]*',
            '',
            self.preprocessed_sql
        )

        # 移除多行注释
        self.preprocessed_sql = re.sub(
            r'/\*.*?\*/',
            '',
            self.preprocessed_sql,
            flags=re.DOTALL
        )

    def _handle_special_functions(self):
        """处理特殊函数"""

        # 处理 REPLACE 中的空格问题
        self.preprocessed_sql = re.sub(
            r'REPLACE\(([^,]+),\s*[\'"]\s*[\'"],\s*[\'"][^\'"]*[\'"]\)',
            r'TRIM(\1)',
            self.preprocessed_sql,
            flags=re.IGNORECASE
        )


# ============================================================================
# SQL节点解析器
# ============================================================================

@dataclass
class Node:
    """SQL节点"""
    id: str
    type: str
    name: str
    sql: str
    parent_id: Optional[str]
    depth: int
    line_start: int = 0
    line_end: int = 0
    alias: str = ""
    metadata: dict = field(default_factory=dict)

    def to_dict(self):
        return {
            "id": self.id,
            "type": self.type,
            "name": self.name,
            "sql": self.sql,
            "parent_id": self.parent_id,
            "depth": self.depth,
            "line_start": self.line_start,
            "line_end": self.line_end,
            "alias": self.alias,
            "metadata": self.metadata
        }


@dataclass
class Relationship:
    """节点关系"""
    source_id: str
    target_id: str
    type: str
    metadata: dict = field(default_factory=dict)

    def to_dict(self):
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "type": self.type,
            "metadata": self.metadata
        }


class SQLNodeParser:
    """SQL节点解析器"""

    def __init__(self, sql: str, dialect: str = "mysql"):
        self.sql = sql
        self.dialect = dialect
        self.nodes: Dict[str, Node] = {}
        self.relationships: List[Relationship] = []
        self.node_counter = defaultdict(int)
        self.cte_names: Set[str] = set()
        self.table_nodes: Dict[str, str] = {}

    def parse(self) -> Tuple[Dict[str, Node], List[Relationship]]:
        """解析SQL，返回节点和关系"""
        try:
            # 预处理SQL
            preprocessor = SQLPreprocessor(self.sql)
            preprocessed_sql, var_mappings = preprocessor.preprocess()

            # 解析SQL
            ast = parse(preprocessed_sql, dialect=self.dialect, read=self.dialect)
            if not ast:
                raise ValueError("SQL解析失败")

            root_expr = ast[0] if ast else None

            # 创建ROOT节点
            root_node = Node(
                id="ROOT",
                type="ROOT",
                name="ROOT",
                sql=self.sql.strip(),
                parent_id=None,
                depth=0,
                line_start=1,
                line_end=self.sql.count('\n') + 1
            )
            self.nodes["ROOT"] = root_node

            # 开始解析
            self._parse_expression(root_expr, root_node)

            return self.nodes, self.relationships

        except Exception as e:
            print(f"解析错误: {e}")
            raise

    def _generate_id(self, node_type: str, parent: Node, name: str = "") -> str:
        """生成全局唯一ID"""
        self.node_counter[node_type] += 1
        if node_type == "TB":
            return f"TB_{name}"
        if name:
            return f"{parent.id}_{node_type}_{name}"
        else:
            return f"{parent.id}_{node_type}_{self.node_counter[node_type]}"

    def _get_sql_from_expression(self, expr) -> str:
        """从表达式提取SQL代码"""
        try:
            return expr.sql(self.dialect)
        except:
            return str(expr)

    def _parse_expression(self, expr, parent_node: Node, context: str = "SELECT"):
        """递归解析表达式"""
        if expr is None:
            return

        # 处理WITH子句
        if isinstance(expr, exp.With):
            self._parse_with(expr, parent_node)
            return

        # 处理CTE
        if isinstance(expr, exp.CTE):
            self._parse_cte(expr, parent_node)
            return

        # 处理SELECT
        if isinstance(expr, exp.Select):
            self._parse_select(expr, parent_node, context)
            return

        # 处理UNION
        if isinstance(expr, exp.Union):
            self._parse_union(expr, parent_node)
            return

        # 处理子查询
        if isinstance(expr, exp.Subquery):
            self._parse_subquery(expr, parent_node, context)
            return

        # 处理表
        if isinstance(expr, exp.Table):
            self._parse_table(expr, parent_node)
            return

    def _parse_with(self, with_expr: exp.With, parent_node: Node):
        """解析WITH子句"""
        # 收集CTE名称
        for cte in with_expr.expressions:
            if isinstance(cte, exp.CTE):
                self.cte_names.add(cte.alias.upper())

        # 解析每个CTE
        for cte in with_expr.expressions:
            self._parse_expression(cte, parent_node, "WITH")

        # 解析主查询
        if with_expr.this:
            self._parse_expression(with_expr.this, parent_node, "SELECT")

    def _parse_cte(self, cte: exp.CTE, parent_node: Node):
        """解析CTE"""
        cte_name = cte.alias or ""
        cte_sql = self._get_sql_from_expression(cte.this)

        node_id = self._generate_id("CT", parent_node, cte_name)

        node = Node(
            id=node_id,
            type="CT",
            name=cte_name,
            sql=cte_sql,
            parent_id=parent_node.id,
            depth=parent_node.depth + 1,
            metadata={"cte_name": cte_name}
        )

        self.nodes[node_id] = node
        self._add_relationship(parent_node.id, node_id, "CONTAINS")

        # 递归解析CTE内部
        self._parse_expression(cte.this, node, "SELECT")

    def _parse_select(self, select_expr: exp.Select, parent_node: Node, context: str):
        """解析SELECT"""
        # 检查是否有WITH子句
        if hasattr(select_expr, 'args') and 'with_' in select_expr.args:
            with_clause = select_expr.args['with_']
            if isinstance(with_clause, exp.With):
                # 先处理WITH子句（CTE）
                self._parse_with(with_clause, parent_node)

        # 判断是否是UNION分支
        is_union_branch = parent_node.type in ["ROOT", "UNION"] and context == "SELECT"

        if is_union_branch:
            # 创建BLK节点
            block_id = self._generate_id("BLK", parent_node)
            block_sql = self._get_sql_from_expression(select_expr)

            block_node = Node(
                id=block_id,
                type="BLK",
                name=f"BLK_{self.node_counter['BLK']}",
                sql=block_sql,
                parent_id=parent_node.id,
                depth=parent_node.depth + 1
            )

            self.nodes[block_id] = block_node
            self._add_relationship(parent_node.id, block_id, "CONTAINS")
            current_parent = block_node
        else:
            current_parent = parent_node

        # 遍历SELECT中的所有子查询和表
        for subquery in select_expr.find_all(exp.Subquery):
            self._parse_subquery(subquery, current_parent, context)

        for table in select_expr.find_all(exp.Table):
            self._parse_table(table, current_parent)

    def _parse_union(self, union_expr: exp.Union, parent_node: Node):
        """解析UNION"""
        union_id = f"{parent_node.id}_UNION_{self.node_counter['UNION']}"
        self.node_counter["UNION"] += 1

        union_node = Node(
            id=union_id,
            type="UNION",
            name=f"UNION_{self.node_counter['UNION']}",
            sql=self._get_sql_from_expression(union_expr),
            parent_id=parent_node.id,
            depth=parent_node.depth + 1
        )

        self.nodes[union_id] = union_node
        self._add_relationship(parent_node.id, union_id, "CONTAINS")

        # 解析左右分支
        self._parse_expression(union_expr.this, union_node, "SELECT")
        self._parse_expression(union_expr.expression, union_node, "SELECT")

    def _parse_subquery(self, subquery: exp.Subquery, parent_node: Node, context: str):
        """解析子查询"""
        sub_sql = self._get_sql_from_expression(subquery.this)
        alias = subquery.alias or ""

        # 根据上下文确定节点类型
        if context == "FROM":
            node_type = "DT"
        elif context == "WHERE":
            node_type = "WQ"
        elif context in ["SELECT", "PROJECTION"]:
            node_type = "SQ"
        else:
            node_type = "SQ"

        node_id = self._generate_id(node_type, parent_node, alias)

        node = Node(
            id=node_id,
            type=node_type,
            name=alias or f"{node_type}_{self.node_counter[node_type]}",
            sql=sub_sql,
            parent_id=parent_node.id,
            depth=parent_node.depth + 1,
            alias=alias
        )

        self.nodes[node_id] = node
        self._add_relationship(parent_node.id, node_id, "CONTAINS")

        # 递归解析子查询内部
        self._parse_expression(subquery.this, node, "SELECT")

    def _parse_table(self, table: exp.Table, parent_node: Node):
        """解析表"""
        table_name = table.name or ""
        schema = table.db or ""
        alias = table.alias or ""
        full_name = f"{schema}.{table_name}" if schema else table_name

        # 检查是否是CTE引用
        if table_name.upper() in self.cte_names:
            cte_node_id = self._find_cte_node_id(table_name)
            if cte_node_id:
                self._add_relationship(parent_node.id, cte_node_id, "REFERENCES",
                                      metadata={"alias": alias, "ref_type": "CTE"})
            return

        # 物理表或视图
        table_id = f"TB_{full_name}"

        # 如果表节点不存在，创建它
        if table_id not in self.nodes:
            table_sql = f"SELECT * FROM {full_name}"
            is_view = "VIEW" in table_name.upper() or "V_" in table_name.upper()
            node_type = "VW" if is_view else "TB"

            table_node = Node(
                id=table_id,
                type=node_type,
                name=full_name,
                sql=table_sql,
                parent_id=None,
                depth=0,
                metadata={"schema": schema, "table": table_name, "alias": alias}
            )

            self.nodes[table_id] = table_node
            self.table_nodes[full_name] = table_id

        # 创建引用关系
        self._add_relationship(parent_node.id, table_id, "REFERENCES",
                              metadata={"alias": alias, "ref_type": "TABLE"})

    def _find_cte_node_id(self, cte_name: str) -> Optional[str]:
        """根据CTE名称查找CTE节点ID"""
        cte_name_upper = cte_name.upper()
        for node_id, node in self.nodes.items():
            if node.type == "CT" and node.metadata.get("cte_name", "").upper() == cte_name_upper:
                return node_id
        return None

    def _add_relationship(self, source_id: str, target_id: str, rel_type: str,
                          metadata: dict = None):
        """添加关系"""
        rel = Relationship(
            source_id=source_id,
            target_id=target_id,
            type=rel_type,
            metadata=metadata or {}
        )
        self.relationships.append(rel)

    def export_json(self, filepath: str):
        """导出为JSON文件"""
        data = {
            "nodes": [node.to_dict() for node in self.nodes.values()],
            "relationships": [rel.to_dict() for rel in self.relationships]
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def print_summary(self):
        """打印统计摘要"""
        print("\n" + "="*80)
        print("节点统计")
        print("="*80)

        type_counts = defaultdict(int)
        for node in self.nodes.values():
            type_counts[node.type] += 1

        type_names = {
            "ROOT": "根节点",
            "CT": "CTE",
            "BLK": "查询块",
            "DT": "派生表",
            "SQ": "标量子查询",
            "WQ": "WHERE子查询",
            "IQ": "IN子查询",
            "TB": "物理表",
            "VW": "视图",
            "UNION": "UNION"
        }

        print(f"\n节点类型统计:")
        for node_type, count in sorted(type_counts.items()):
            cn_name = type_names.get(node_type, node_type)
            print(f"  {cn_name}: {count}个")

        rel_counts = defaultdict(int)
        for rel in self.relationships:
            rel_counts[rel.type] += 1

        print(f"\n关系类型统计:")
        rel_names = {"CONTAINS": "包含关系", "REFERENCES": "引用关系"}
        for rel_type, count in sorted(rel_counts.items()):
            cn_name = rel_names.get(rel_type, rel_type)
            print(f"  {cn_name}: {count}个")

        max_depth = max(node.depth for node in self.nodes.values())
        print(f"\n最大嵌套深度: {max_depth}层")
        print(f"\n总节点数: {len(self.nodes)}")
        print(f"总关系数: {len(self.relationships)}")
        print("="*80)

    def get_node_by_id(self, node_id: str) -> Optional[Node]:
        """根据ID获取节点"""
        return self.nodes.get(node_id)

    def get_children(self, node_id: str) -> List[Node]:
        """获取子节点"""
        children = []
        for rel in self.relationships:
            if rel.source_id == node_id and rel.type == "CONTAINS":
                child = self.nodes.get(rel.target_id)
                if child:
                    children.append(child)
        return children

    def get_dependencies(self, node_id: str) -> List[Node]:
        """获取节点依赖"""
        deps = []
        for rel in self.relationships:
            if rel.source_id == node_id and rel.type == "REFERENCES":
                dep = self.nodes.get(rel.target_id)
                if dep:
                    deps.append(dep)
        return deps

    def get_dependents(self, node_id: str) -> List[Node]:
        """获取依赖此节点的其他节点"""
        dependents = []
        for rel in self.relationships:
            if rel.target_id == node_id and rel.type == "REFERENCES":
                dependent = self.nodes.get(rel.source_id)
                if dependent:
                    dependents.append(dependent)
        return dependents


def main():
    """主函数"""
    import sys

    if len(sys.argv) < 2:
        print("用法: python sql_node_parser_v2.py <sql文件路径> [dialect]")
        sys.exit(1)

    sql_file = sys.argv[1]
    dialect = sys.argv[2] if len(sys.argv) > 2 else "mysql"

    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
    except FileNotFoundError:
        print(f"错误: 文件不存在 - {sql_file}")
        sys.exit(1)

    print(f"正在解析SQL文件: {sql_file}")
    print(f"使用SQL方言: {dialect}")
    print("-" * 80)

    parser = SQLNodeParser(sql_content, dialect=dialect)

    try:
        nodes, relationships = parser.parse()
        parser.print_summary()

        output_file = sql_file.rsplit('.', 1)[0] + '_nodes.json'
        parser.export_json(output_file)
        print(f"\n✓ 已导出节点和关系到: {output_file}")

    except Exception as e:
        print(f"解析失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
