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
import sys
import os

# 添加项目根目录到路径以导入模块
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, project_root)

# 检查作用域系统是否可用
def check_scope_system_available():
    """运行时检查作用域系统是否可用"""
    # 确保项目根目录在sys.path中
    import sys
    import os

    # 获取当前文件所在目录
    current_file = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_file)
    project_root = os.path.join(current_dir, '..', '..')

    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    try:
        from src.core.field_scope import FieldScope, FieldInfo, ScopeManager
        from src.core.alias_manager import AliasManager
        from src.core.field_propagation import FieldPropagationEngine
        return True
    except ImportError:
        try:
            from core.field_scope import FieldScope, FieldInfo, ScopeManager
            from core.alias_manager import AliasManager
            from core.field_propagation import FieldPropagationEngine
            return True
        except ImportError:
            return False


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

        # 2. 统一大小写（标识符转大写）
        self._normalize_case()

        # 3. 处理模板变量
        self._handle_template_variables()

        return self.preprocessed_sql, self.var_mappings

    def _handle_oracle_syntax(self):
        """处理Oracle特殊语法"""

        # 处理中文标点符号 - 替换为英文标点
        # 中文逗号 → 英文逗号
        self.preprocessed_sql = self.preprocessed_sql.replace('，', ',')
        # 中文分号 → 英文分号（SQL语句分隔符）
        self.preprocessed_sql = self.preprocessed_sql.replace('；', ';')
        # 中文左括号 → 英文左括号
        self.preprocessed_sql = self.preprocessed_sql.replace('（', '(')
        # 中文右括号 → 英文右括号
        self.preprocessed_sql = self.preprocessed_sql.replace('）', ')')
        # 中文单引号 → 英文单引号（使用Unicode避免Python语法冲突）
        self.preprocessed_sql = self.preprocessed_sql.replace('\u2018', "'")  # 左单引号
        self.preprocessed_sql = self.preprocessed_sql.replace('\u2019', "'")  # 右单引号

    def _handle_template_variables(self):
        """处理模板变量（如 <!JEDW!>）"""

        # 匹配 <!VAR!> 格式的变量
        pattern = r'<!([A-Z_]+)!>'

        def replace_var(match):
            var_name = match.group(1)
            # 为每个变量生成一个替换值
            replacement_map = {
                # 金额相关
                'JEDW': '10000',  # 金额单位

                # 时间日期相关
                'KSSJ': '20240101',  # 开始时间
                'JSSJ': '20241231',  # 结束时间
                'KSRQ': '20241231',  # 开始日期
                'JSRQ': '20241231',  # 结束日期 (使用最多，145次)
                'RQ': '20241231',    # 日期

                # 业务参数（保持为空或默认值）
                'KHMC': 'KHMC',  # 客户名称
                'HTBH': 'HTBH',  # 合同编号
                'YWLX': 'YWLX',  # 业务类型
                'HTZT': '1', # 合同状态
                'ZJLY': '1', # 资金来源
                'BZ': '01',   # 币种
            }

            value = replacement_map.get(var_name, '1')

            # 记录映射关系
            self.var_mappings[var_name] = {
                'original': match.group(0),
                'replacement': value
            }

            return value

        self.preprocessed_sql = re.sub(pattern, replace_var, self.preprocessed_sql)

    def _normalize_case(self):
        """
        统一SQL大小写
        策略：将所有标识符（表名、字段名、别名）转为大写
        保留：字符串字面量、注释、数字
        """
        try:
            # 使用sqlglot解析SQL为AST（保留注释）
            parsed = sqlglot.parse_one(
                self.preprocessed_sql,
                dialect='mysql',
                comments=True  # 保留注释
            )

            # 遍历AST，转换所有标识符为大写
            def normalize_identifier(node):
                """递归遍历节点，转换标识符"""
                if isinstance(node, exp.Identifier):
                    # 转换标识符名为大写
                    if hasattr(node, 'this') and node.this:
                        node.set('this', str(node.this).upper())
                    if hasattr(node, 'alias') and node.alias:
                        node.set('alias', str(node.alias).upper())

                elif isinstance(node, exp.Column):
                    # 处理列引用
                    if hasattr(node, 'this') and node.this:
                        # 转换列名
                        if isinstance(node.this, exp.Identifier):
                            node.this.set('this', str(node.this.this).upper())
                        elif isinstance(node.this, str):
                            node.set('this', node.this.upper())
                    # 转换表别名
                    if hasattr(node, 'table') and node.table:
                        if isinstance(node.table, exp.Identifier):
                            node.table.set('this', str(node.table.this).upper())
                        elif isinstance(node.table, str):
                            node.set('table', node.table.upper())

                elif isinstance(node, exp.Table):
                    # 处理表引用
                    if hasattr(node, 'this') and node.this:
                        if isinstance(node.this, exp.Identifier):
                            node.this.set('this', str(node.this.this).upper())
                        elif isinstance(node.this, str):
                            node.set('this', node.this.upper())
                    # 转换表别名
                    if hasattr(node, 'alias') and node.alias:
                        node.set('alias', str(node.alias).upper())

                elif isinstance(node, exp.Alias):
                    # 处理别名定义
                    if hasattr(node, 'alias') and node.alias:
                        node.set('alias', str(node.alias).upper())

                # 递归处理子节点
                for child in node.iter_expressions():
                    normalize_identifier(child)

            # 执行规范化
            normalize_identifier(parsed)

            # 重新生成SQL（保留注释和格式）
            self.preprocessed_sql = parsed.sql(
                dialect='mysql',
                comments=True,
                pretty=True  # 美化格式
            )

        except Exception as e:
            # 如果解析失败，保持原SQL（降级处理）
            print(f"⚠️ 警告: 大小写规范化失败: {e}")
            print(f"   使用原始SQL继续处理")


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
    output_fields: List[str] = field(default_factory=list)  # 新增：该节点的输出字段ID列表

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
            "metadata": self.metadata,
            "output_fields": self.output_fields
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


@dataclass
class Field:
    """字段节点"""
    id: str                    # 字段唯一ID
    name: str                  # 字段名/别名
    table_name: str            # 来源表名
    column_name: str           # 基础列名
    field_type: str            # 字段类型（基础列/函数转换/CASE等）
    transformation: dict       # 转换规则（JSON格式）
    dependencies: List[str]    # 依赖的其他字段ID
    parent_node_id: str        # 所属节点ID
    metadata: dict = field(default_factory=dict)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "table_name": self.table_name,
            "column_name": self.column_name,
            "field_type": self.field_type,
            "transformation": self.transformation,
            "dependencies": self.dependencies,
            "parent_node_id": self.parent_node_id,
            "metadata": self.metadata
        }


@dataclass
class FieldRelationship:
    """字段级关系"""
    source_id: str            # 源字段ID
    target_id: str            # 目标字段ID
    type: str                 # 关系类型（DERIVES, USES, TRANSFORMS）
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

    def __init__(self, sql: str, dialect: str = "mysql", use_scope_system: bool = True):
        self.sql = sql
        self.dialect = dialect
        self.nodes: Dict[str, Node] = {}
        self.relationships: List[Relationship] = []
        self.node_counter = defaultdict(int)
        self.cte_names: Set[str] = set()
        self.table_nodes: Dict[str, str] = {}
        # 字段级血缘分析
        self.fields: Dict[str, Field] = {}
        self.field_relationships: List[FieldRelationship] = []
        self.field_counter = defaultdict(int)

        # 作用域系统（新增）
        self.use_scope_system = use_scope_system and check_scope_system_available()
        if self.use_scope_system:
            # 动态导入作用域系统组件
            try:
                from src.core.field_scope import FieldScope, FieldInfo, ScopeManager
                from src.core.alias_manager import AliasManager
                from src.core.field_propagation import FieldPropagationEngine
            except ImportError:
                from core.field_scope import FieldScope, FieldInfo, ScopeManager
                from core.alias_manager import AliasManager
                from core.field_propagation import FieldPropagationEngine

            self.scope_manager = ScopeManager()
            self.alias_manager = AliasManager(self.scope_manager)
            self.propagation_engine = FieldPropagationEngine(self.alias_manager)
        else:
            self.scope_manager = None
            self.alias_manager = None
            self.propagation_engine = None

    def parse(self) -> Tuple[Dict[str, Node], List[Relationship]]:
        """解析SQL，返回节点和关系"""
        try:
            # 预处理SQL
            preprocessor = SQLPreprocessor(self.sql)
            preprocessed_sql, var_mappings = preprocessor.preprocess()

            # 使用Oracle方言解析，然后转换为MySQL方言
            # 这样可以自动处理Oracle特定的语法，如 || 字符串连接
            try:
                # 先用Oracle方言解析
                ast = parse(preprocessed_sql, dialect='oracle', read='oracle')
                if not ast:
                    raise ValueError("SQL解析失败")

                # 转换为MySQL方言的SQL
                mysql_sql = ' '.join(expr.sql(dialect='mysql') for expr in ast)

                # 再用MySQL方言解析最终的AST
                ast = parse(mysql_sql, dialect=self.dialect, read=self.dialect)
                if not ast:
                    raise ValueError("SQL解析失败")

            except Exception as oracle_error:
                # 如果Oracle方言失败，回退到直接使用MySQL方言
                print(f"Oracle方言解析失败，回退到MySQL方言: {oracle_error}")
                ast = parse(preprocessed_sql, dialect=self.dialect, read=self.dialect)
                if not ast:
                    raise ValueError("SQL解析失败")

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

            # 如果启用作用域系统，创建ROOT作用域
            if self.use_scope_system:
                self.alias_manager.create_scope("ROOT", None)
                self.alias_manager.push_scope(self.alias_manager.get_current_scope())

            # 解析所有语句
            for i, root_expr in enumerate(ast):
                if root_expr:
                    self._parse_expression(root_expr, root_node, context=f"STATEMENT_{i+1}")

            # 如果启用作用域系统，构建字段传播关系
            if self.use_scope_system:
                self._build_scope_hierarchy()
                self._propagate_fields_all_nodes()

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
                cte_name = cte.alias.upper() if cte.alias else ""
                self.cte_names.add(cte_name.upper())

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

        # 如果启用作用域系统，注册CTE全局别名
        if self.use_scope_system:
            self.alias_manager.register_cte_alias(cte_name, node_id, parent_node.id)

        # 递归解析CTE内部
        self._parse_expression(cte.this, node, "SELECT")

    def _parse_select(self, select_expr: exp.Select, parent_node: Node, context: str):
        """解析SELECT"""
        # 检查是否有WITH子句（仅在尚未处理时处理）
        # 使用context参数来判断：如果context已经是"WITH"，说明WITH已经被处理过了
        if context != "WITH" and hasattr(select_expr, 'args') and 'with_' in select_expr.args:
            with_clause = select_expr.args['with_']
            if isinstance(with_clause, exp.With):
                # 先处理WITH子句（CTE）
                self._parse_with(with_clause, parent_node)

        # 判断是否是UNION分支或ROOT节点的直接SELECT子节点
        is_union_branch = parent_node.type in ["ROOT", "UNION"]

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

        # 解析FROM子句，建立表别名映射
        if self.use_scope_system:
            # 使用新的作用域系统
            self._parse_from_clause_with_scope(select_expr, current_parent)
            table_aliases = {}  # 作用域系统会管理别名
        else:
            # 使用旧方法
            table_aliases = self._parse_from_clause(select_expr, current_parent)
            current_parent.metadata['table_aliases'] = table_aliases

        # 提取字段信息（字段级血缘分析）
        if self.use_scope_system:
            # 使用新的作用域系统
            self._parse_select_clause_with_scope(select_expr, current_parent)
        else:
            # 使用旧方法
            self._extract_fields(select_expr, current_parent, table_aliases)

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

        # 如果启用作用域系统，为子查询创建作用域
        if self.use_scope_system:
            # 创建子查询作用域
            subquery_scope = self.alias_manager.create_scope(node_id, parent_node.id)

            # 如果子查询有别名，注册到别名管理器
            if alias:
                # 根据上下文决定注册方式
                if context == "FROM":
                    # FROM子句中的子查询
                    self.alias_manager.register_subquery_alias(alias, node_id, parent_node.id)
                elif context in ["SELECT", "PROJECTION"]:
                    # SELECT子句中的标量子查询
                    # 标量子查询通常不作为表别名，但需要记录
                    self.alias_manager.subquery_mappings[f"scalar_{alias}"] = node_id

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

    def _generate_field_id(self, parent_id: str, field_name: str, index: int) -> str:
        """生成字段唯一ID"""
        clean_name = field_name.replace('.', '_').replace('@', '_')[:50]
        return f"{parent_id}_FLD_{clean_name}_{index}"

    def _extract_fields(self, select_stmt: exp.Select, parent_node: Node, table_aliases: Dict[str, str] = None):
        """从SELECT语句提取字段信息"""
        if table_aliases is None:
            table_aliases = {}

        if not hasattr(select_stmt, 'expressions'):
            return

        for idx, projection in enumerate(select_stmt.expressions):
            # 获取字段别名
            alias = None
            if isinstance(projection, exp.Alias):
                alias = projection.alias
                actual_expr = projection.this
            else:
                actual_expr = projection
                # 如果没有显式别名，尝试从表达式推断
                if isinstance(actual_expr, exp.Column):
                    alias = actual_expr.name

            # 生成字段ID
            field_name = alias or f"field_{idx}"
            field_id = self._generate_field_id(parent_node.id, field_name, idx)

            # 分析字段表达式（第一遍：仅收集基本信息，不解析依赖）
            field_info = self._analyze_field_expression_basic(actual_expr, table_aliases)

            # 推断字段来源表
            inferred_table = self._infer_field_source(actual_expr, field_name, table_aliases, parent_node)

            # 【修改】优先使用field_info中的table_name（如ARITHMETIC类型提取的）
            final_table_name = field_info.get('table_name') or inferred_table

            # 【关键修复】column_name 设置逻辑：
            # 1. 对于计算字段（FUNCTION, CASE, AGGREGATION, ARITHMETIC），column_name 应该是字段别名（field_name）
            # 2. 对于简单列引用（COLUMN），使用 field_info 中的 column_name
            # 3. 对于其他类型，如果没有 column_name，使用 field_name

            if field_info.get('field_type') in ['FUNCTION', 'CASE', 'AGGREGATION', 'ARITHMETIC']:
                # 计算字段使用字段别名作为 column_name
                final_column_name = field_name
            else:
                # 其他类型使用 field_info 中的 column_name，如果没有则使用 field_name
                final_column_name = field_info.get('column_name') or field_name

            # 创建字段节点
            field = Field(
                id=field_id,
                name=field_name,
                table_name=final_table_name,
                column_name=final_column_name,  # 修复：使用 final_column_name
                field_type=field_info.get('field_type', 'UNKNOWN'),
                transformation=field_info.get('transformation', {}),
                dependencies=[],  # 稍后构建
                parent_node_id=parent_node.id,
                metadata={
                    'sql': self._get_sql_from_expression(projection),
                    'position': idx,
                    'raw_expression': self._get_sql_from_expression(actual_expr),
                    'is_calculated_field': field_info.get('field_type') in ['FUNCTION', 'CASE', 'AGGREGATION', 'ARITHMETIC']
                }
            )

            self.fields[field_id] = field

            # 创建字段到节点的HAS_FIELD关系
            parent_node.metadata['fields'] = parent_node.metadata.get('fields', [])
            parent_node.metadata['fields'].append(field_id)

            # 添加到节点的输出字段清单（新增）
            parent_node.output_fields.append(field_id)

            # 特殊处理：记录标量子查询字段（延迟处理）
            if field_info.get('field_type') == 'SCALAR_QUERY':
                # 记录到待处理列表，在_build_cross_node_field_mappings中处理
                if not hasattr(self, '_scalar_query_fields'):
                    self._scalar_query_fields = []

                self._scalar_query_fields.append({
                    'outer_field': field,
                    'subquery_expr': actual_expr,
                    'outer_node': parent_node
                })

    def _handle_scalar_field_field(self, outer_field: Field, subquery_expr,
                                   outer_node: Node, table_aliases: Dict[str, str]):
        """
        处理标量子查询字段，建立正确的DERIVES关系

        目标：实现3层血缘链路
        第3层（物理表）: CUST_CORP_INFO.cust_id
            ↓ PROVIDES
        第2层（子查询）: ROOT_SQ_1.cust_id
            ↓ DERIVES (别名映射)
        第1层（外层查询）: ROOT_BLK.cust_id
        """
        # 处理Subquery包装
        actual_select = subquery_expr
        if isinstance(subquery_expr, exp.Subquery):
            actual_select = subquery_expr.this

        # 1. 找到子查询节点
        # 子查询节点应该是当前节点的直接子节点
        subquery_node = None
        for node_id, node in self.nodes.items():
            if node.parent_id == outer_node.id and node.type == 'SQ':
                # 验证是否匹配（通过SQL内容）
                subquery_sql = self._get_sql_from_expression(actual_select)
                if subquery_sql in node.sql or node.sql in subquery_sql:
                    subquery_node = node
                    break

        if not subquery_node:
            return

        # 2. 找到子查询的输出字段
        # 子查询应该只有一个输出字段
        if not subquery_node.output_fields:
            return

        subquery_field_id = subquery_node.output_fields[0]
        subquery_field = self.fields.get(subquery_field_id)

        if not subquery_field:
            return

        # 3. 建立DERIVES关系：子查询输出字段 → 外层字段
        derives_rel = FieldRelationship(
            source_id=subquery_field_id,
            target_id=outer_field.id,
            type='DERIVES',
            metadata={
                'relationship_type': 'scalar_subquery',
                'subquery_node_id': subquery_node.id,
                'alias_mapping': {
                    'source_field': subquery_field.name,  # AS前（子查询的字段名）
                    'target_field': outer_field.name     # AS后（外层的字段名）
                }
            }
        )

        self.field_relationships.append(derives_rel)


    def _analyze_field_expression_basic(self, expr: exp.Expression, table_aliases: Dict[str, str] = None) -> dict:
        """分析字段表达式，仅提取基本信息（不解析依赖）"""
        if table_aliases is None:
            table_aliases = {}

        result = {
            'field_type': 'UNKNOWN',
            'transformation': {},
            'column_name': '',
            'table_name': ''  # 新增：用于存储推断的table_name
        }

        if expr is None:
            return result

        # 1. 基础列引用
        if isinstance(expr, exp.Column):
            table_name = expr.table or ''
            column_name = expr.name or ''
            result['field_type'] = 'COLUMN'
            result['column_name'] = column_name
            result['transformation'] = {
                'type': 'column_reference',
                'table': table_name,
                'column': column_name
            }

        # 2. 函数转换
        elif isinstance(expr, exp.Func):
            func_name = expr.__class__.__name__
            result['field_type'] = 'FUNCTION'
            result['transformation'] = self._get_function_transformation(expr)

            # 【新增】提取函数中的第一个列引用作为 column_name
            columns_in_func = list(expr.find_all(exp.Column))
            if columns_in_func:
                result['column_name'] = columns_in_func[0].name
                if columns_in_func[0].table:
                    result['table_name'] = columns_in_func[0].table

        # 3. CASE表达式
        elif isinstance(expr, exp.Case):
            result['field_type'] = 'CASE'
            result['transformation'] = self._get_case_transformation(expr)

            # 【新增】提取 CASE 中的第一个列引用作为 column_name
            columns_in_case = list(expr.find_all(exp.Column))
            if columns_in_case:
                result['column_name'] = columns_in_case[0].name
                if columns_in_case[0].table:
                    result['table_name'] = columns_in_case[0].table

        # 4. 算术运算
        elif isinstance(expr, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod)):
            result['field_type'] = 'ARITHMETIC'
            result['transformation'] = {
                'type': 'arithmetic',
                'operator': expr.__class__.__name__.upper()
            }

            # 【新增】提取算术表达式中的列引用，用于推断table_name和column_name
            # 查找表达式中的所有列引用
            columns_in_expr = list(expr.find_all(exp.Column))
            if columns_in_expr:
                # 使用第一个列引用的信息
                first_col = columns_in_expr[0]
                result['column_name'] = first_col.name  # 设置 column_name

                if first_col.table:
                    result['table_name'] = first_col.table
                # 如果列没有表前缀，尝试从别名中推断
                elif table_aliases and first_col.name in table_aliases:
                    result['table_name'] = table_aliases[first_col.name]

        # 5. 字面量
        elif isinstance(expr, exp.Literal):
            result['field_type'] = 'LITERAL'
            result['transformation'] = {
                'type': 'literal',
                'value': str(expr.this) if hasattr(expr, 'this') else str(expr)
            }
            # 字面量没有 column_name，会在字段创建时使用 field_name

        # 6. 聚合函数
        elif isinstance(expr, (exp.AggFunc,)):
            result['field_type'] = 'AGGREGATION'
            result['transformation'] = self._get_function_transformation(expr)

            # 【新增】提取聚合函数中的列引用作为 column_name
            columns_in_agg = list(expr.find_all(exp.Column))
            if columns_in_agg:
                result['column_name'] = columns_in_agg[0].name
                if columns_in_agg[0].table:
                    result['table_name'] = columns_in_agg[0].table

        # 7. 标量子查询（新增）
        elif isinstance(expr, (exp.Select, exp.Subquery)):
            result['field_type'] = 'SCALAR_QUERY'
            result['transformation'] = {
                'type': 'scalar_subquery',
                'sql': self._get_sql_from_expression(expr)
            }

        return result

    def _get_function_transformation(self, func_expr: exp.Func) -> dict:
        """提取函数转换规则"""
        func_name = func_expr.__class__.__name__
        args_info = []

        # 提取参数信息
        for arg_name, arg_value in func_expr.args.items():
            if arg_value is not None:
                if isinstance(arg_value, exp.Expression):
                    arg_sql = self._get_sql_from_expression(arg_value)
                    args_info.append({
                        'name': arg_name,
                        'expression': arg_sql
                    })
                else:
                    args_info.append({
                        'name': arg_name,
                        'value': str(arg_value)
                    })

        return {
            'type': 'function',
            'function': func_name,
            'arguments': args_info,
            'output_type': self._infer_output_type(func_expr)
        }

    def _get_case_transformation(self, case_expr: exp.Case) -> dict:
        """提取CASE表达式转换规则"""
        branches = []

        if hasattr(case_expr, 'args'):
            # CASE的ifs部分包含WHEN-THEN对
            ifs = case_expr.args.get('ifs', [])
            for if_expr in ifs:
                if hasattr(if_expr, 'args'):
                    condition = self._get_sql_from_expression(if_expr.args.get('this', None))
                    then_expr = self._get_sql_from_expression(if_expr.args.get('expression', None))
                    branches.append({
                        'when': condition,
                        'then': then_expr
                    })

            # ELSE部分
            default = case_expr.args.get('default', None)
            if default:
                branches.append({
                    'else': self._get_sql_from_expression(default)
                })

        return {
            'type': 'case_expression',
            'branches': branches
        }

    def _parse_from_clause(self, select_expr: exp.Select, parent_node: Node) -> Dict[str, str]:
        """解析FROM子句，建立表别名映射"""
        table_aliases = {}

        # 查找FROM子句
        from_clause = select_expr.find(exp.From)
        if not from_clause:
            return table_aliases

        # 遍历FROM子句中的表（FROM子句中的第一个表）
        for table in from_clause.find_all(exp.Table):
            table_name = table.name or ''
            alias = table.alias or table_name

            if alias and table_name:
                table_aliases[alias] = table_name

                # 检查是否是CTE引用
                if table_name.upper() in self.cte_names:
                    cte_node_id = self._find_cte_node_id(table_name)
                    if cte_node_id:
                        self._add_relationship(parent_node.id, cte_node_id, "REFERENCES",
                                              metadata={"alias": alias, "ref_type": "CTE"})
                    return

        # 处理JOIN关系（从Select.joins中获取）
        if hasattr(select_expr, 'args') and 'joins' in select_expr.args:
            joins = select_expr.args['joins']
            for join in joins:
                if isinstance(join, exp.Join) and hasattr(join, 'this'):
                    join_expr = join.this
                    # 查找JOIN中的表
                    if isinstance(join_expr, exp.Table):
                        table = join_expr
                        table_name = table.name or ''
                        alias = table.alias or table_name

                        if alias and table_name:
                            table_aliases[alias] = table_name

        return table_aliases

    def _find_subquery_node_id(self, subquery: exp.Subquery, parent_node: Node) -> Optional[str]:
        """查找子查询对应的节点ID"""
        # 这里简化处理，实际中需要更复杂的逻辑
        # 返回一个虚拟的子查询ID
        return f"{parent_node.id}_SUBQUERY_{id(subquery)}"

    def _infer_field_source(self, expr: exp.Expression, field_name: str,
                           table_aliases: Dict[str, str], parent_node: Node) -> str:
        """推断字段来源表"""
        # 1. 检查表达式是否有显式表前缀
        if isinstance(expr, exp.Column):
            table_name = expr.table or ''
            column_name = expr.name or ''

            # 如果有表前缀，直接使用
            if table_name:
                # 通过表别名映射找到实际表名
                actual_table = table_aliases.get(table_name, table_name)

                # 【新增】如果实际表是CTE，返回CTE名称
                if actual_table.upper() in self.cte_names:
                    return actual_table

                return actual_table

            # 如果没有表前缀，尝试推断（改进的推断逻辑）

            # 【新增】对于子查询，如果只有一个表，使用那个表
            if parent_node.type == "SQ":
                # 获取子查询的FROM子句中的表
                from_tables = []
                for alias, actual_table in table_aliases.items():
                    if not actual_table.startswith('SUBQUERY:'):
                        from_tables.append(actual_table)

                # 如果只有一个物理表，使用它
                if len(from_tables) == 1:
                    return from_tables[0]

            # 优先级1: 检查字段名是否存在于CTE中
            if parent_node.type == "CT":
                # CTE字段优先推断为FROM子句的表
                for alias, actual_table in table_aliases.items():
                    # 跳过子查询别名
                    if not actual_table.startswith('SUBQUERY:'):
                        return actual_table

            # 优先级2: 检查节点引用的CTE
            cte_references = [rel.target_id for rel in self.relationships
                             if rel.source_id == parent_node.id
                             and rel.metadata.get('ref_type') == 'CTE']

            if cte_references:
                # 如果节点引用了CTE，字段很可能来自CTE
                # 检查CTE中是否有这个字段
                for cte_id in cte_references:
                    cte_node = self.nodes.get(cte_id)
                    if cte_node:
                        # 检查CTE的表别名映射
                        cte_aliases = cte_node.metadata.get('table_aliases', {})
                        # 如果CTE只引用一个物理表，字段来自该表
                        if len(cte_aliases) == 1:
                            actual_table = list(cte_aliases.values())[0]
                            if not actual_table.startswith('SUBQUERY:'):
                                return actual_table

            # 新增：检查节点是否引用了CTE（用于没有表前缀的字段）
            cte_refs = []
            for rel in self.relationships:
                if rel.source_id == parent_node.id:
                    if rel.type == "REFERENCES" and rel.metadata.get('ref_type') == 'CTE':
                        cte_refs.append(rel.target_id)

            if cte_refs and len(cte_refs) == 1:
                # 如果节点只引用一个CTE，字段来自该CTE
                cte_node = self.nodes.get(cte_refs[0])
                if cte_node:
                    cte_name = cte_node.metadata.get('cte_name', '')
                    if cte_name:
                        return cte_name

            # 优先级3: 通过字段名匹配推断
            # 检查节点引用的表中是否有这个字段
            for rel in self.relationships:
                if rel.source_id == parent_node.id and rel.type == "REFERENCES":
                    target_node = self.nodes.get(rel.target_id)
                    if target_node and target_node.type in ["TB", "VW"]:
                        # 假设字段名可能匹配表名中的列
                        # 这里简化处理，返回表名
                        return target_node.name

        # 2. 对于函数或复杂表达式，查找包含的列引用
        tables_in_expr = set()
        for col in expr.find_all(exp.Column):
            if col.table:
                actual_table = table_aliases.get(col.table, col.table)
                if actual_table:
                    tables_in_expr.add(actual_table)

        # 如果只找到一个表，返回它
        if len(tables_in_expr) == 1:
            return list(tables_in_expr)[0]

        return ''

    def _get_referenced_tables(self, node: Node) -> List[str]:
        """获取节点引用的所有表名"""
        referenced_tables = []

        for rel in self.relationships:
            if rel.source_id == node.id and rel.type == "REFERENCES":
                target_node = self.nodes.get(rel.target_id)
                if target_node and target_node.type in ["TB", "VW"]:
                    referenced_tables.append(target_node.name)

        return referenced_tables

    def _extract_table_name(self, expr: exp.Expression, parent_node: Node) -> str:
        """从表达式提取来源表名"""
        if isinstance(expr, exp.Column):
            return expr.table or ''

        # 对于函数或复杂表达式，查找包含的列引用
        tables = set()
        for col in expr.find_all(exp.Column):
            if col.table:
                tables.add(col.table)

        return ', '.join(sorted(tables)) if tables else ''

    def _infer_output_type(self, func_expr: exp.Func) -> str:
        """推断函数输出类型"""
        func_name = func_expr.__class__.__name__.upper()

        # 数值函数
        if func_name in ['ABS', 'ROUND', 'CEIL', 'FLOOR', 'TRUNCATE', 'SUM', 'AVG']:
            return 'numeric'
        # 字符串函数
        elif func_name in ['CONCAT', 'SUBSTRING', 'UPPER', 'LOWER', 'TRIM']:
            return 'string'
        # 日期函数
        elif func_name in ['TO_DATE', 'TO_CHAR', 'DATE_TRUNC']:
            return 'date'
        # 聚合函数
        elif func_name in ['COUNT', 'MAX', 'MIN']:
            return 'mixed'
        else:
            return 'unknown'

    def _build_field_dependencies(self):
        """构建字段级依赖关系"""
        # 第一遍：为每个字段解析其依赖
        dependency_count = 0
        for field_id, field in self.fields.items():
            raw_expr = field.metadata.get('raw_expression', '')
            if raw_expr:
                # 解析原始表达式以获取依赖
                dependencies = self._extract_field_dependencies_from_expression(
                    raw_expr, field.parent_node_id, field_id  # Pass field_id to exclude self
                )
                field.dependencies = dependencies
                if dependencies:
                    dependency_count += len(dependencies)

        # 第二遍：创建字段关系
        rel_count = 0
        for field_id, field in self.fields.items():
            for dep_id in field.dependencies:
                if dep_id in self.fields:
                    # 创建DERIVES关系
                    field_rel = FieldRelationship(
                        source_id=dep_id,
                        target_id=field_id,
                        type="DERIVES",
                        metadata={
                            'transformation': field.transformation.get('type', 'unknown')
                        }
                    )
                    self.field_relationships.append(field_rel)

                    # 同时创建USES反向关系
                    field_rel_reverse = FieldRelationship(
                        source_id=field_id,
                        target_id=dep_id,
                        type="USES",
                        metadata={}
                    )
                    self.field_relationships.append(field_rel_reverse)
                    rel_count += 2
                elif dep_id.startswith('PHYSICAL_'):
                    # 物理表字段引用（格式: PHYSICAL_table.column）
                    # 创建一个特殊的关系，标记为物理表引用
                    parts = dep_id.split('_', 1)[1].rsplit('.', 1) if '.' in dep_id else []
                    if len(parts) == 2:
                        table_name, column_name = parts
                        field_rel = FieldRelationship(
                            source_id=dep_id,  # 使用虚拟ID
                            target_id=field_id,
                            type="DERIVES",
                            metadata={
                                'transformation': field.transformation.get('type', 'unknown'),
                                'physical_table': table_name,
                                'physical_column': column_name,
                                'is_physical_reference': True
                            }
                        )
                        self.field_relationships.append(field_rel)
                        rel_count += 1

    def _extract_field_dependencies_from_expression(self, expr_sql: str,
                                                     parent_node_id: str,
                                                     exclude_field_id: str = None) -> List[str]:
        """从表达式SQL中提取依赖的字段ID"""
        dependencies = []

        try:
            # 解析表达式
            expr = parse(expr_sql, dialect=self.dialect)
            if not expr:
                return dependencies

            expr = expr[0]

            # 获取当前节点的表别名映射
            parent_node = self.nodes.get(parent_node_id)
            table_aliases = parent_node.metadata.get('table_aliases', {}) if parent_node else {}

            # 查找所有列引用
            col_count = 0
            for col in expr.find_all(exp.Column):
                col_count += 1
                table_name = col.table or ''
                column_name = col.name or ''

                # 情况1：有表前缀的字段
                if table_name:
                    # 解析表别名
                    actual_table_name = table_name
                    if table_name in table_aliases:
                        actual_table_name = table_aliases[table_name]

                    # 尝试找匹配的字段（同节点或CTE节点）
                    matching_field_id = self._find_field_by_reference(table_name, column_name, parent_node_id)
                    if matching_field_id:
                        if matching_field_id not in dependencies and matching_field_id != exclude_field_id:
                            dependencies.append(matching_field_id)
                    else:
                        # 没有找到字段对象，创建物理表引用
                        table_node = None
                        for node in self.nodes.values():
                            if node.type == 'TB' and (node.name == actual_table_name or node.name.endswith('.' + actual_table_name)):
                                table_node = node
                                break

                        if table_node:
                            physical_ref = f"PHYSICAL_{table_node.name}.{column_name}"
                            if physical_ref not in dependencies and physical_ref != exclude_field_id:
                                dependencies.append(physical_ref)

                # 情况2：无表前缀的字段 - 需要推断来源
                else:
                    # 优先级1：查找同节点的字段（如SELECT子句中的计算字段）
                    same_node_field = self._find_field_in_node(column_name, parent_node_id)
                    if same_node_field and same_node_field != exclude_field_id:
                        if same_node_field not in dependencies:
                            dependencies.append(same_node_field)
                        continue

                    # 优先级2：如果节点引用了CTE，查找CTE中的字段
                    cte_fields = self._find_field_in_ctes(column_name, parent_node_id)
                    if cte_fields:
                        # 如果找到多个，使用第一个（实际中应该只有一个）
                        for cte_field in cte_fields:
                            if cte_field not in dependencies and cte_field != exclude_field_id:
                                dependencies.append(cte_field)
                        continue

                    # 优先级3：从FROM子句的表中推断
                    if table_aliases:
                        # 如果只有一个表，字段肯定来自这个表
                        if len(table_aliases) == 1:
                            table_name = list(table_aliases.values())[0]
                            # 创建物理表引用
                            table_node = None
                            for node in self.nodes.values():
                                if node.type == 'TB' and (node.name == table_name or node.name.endswith('.' + table_name)):
                                    table_node = node
                                    break

                            if table_node:
                                physical_ref = f"PHYSICAL_{table_node.name}.{column_name}"
                                if physical_ref not in dependencies and physical_ref != exclude_field_id:
                                    dependencies.append(physical_ref)
                        else:
                            # 多个表，尝试通过字段名推断
                            for alias, actual_table in table_aliases.items():
                                # 检查这个表是否有这个字段（通过元数据或直接查找）
                                if self._table_has_column(actual_table, column_name):
                                    table_node = None
                                    for node in self.nodes.values():
                                        if node.type == 'TB' and (node.name == actual_table or node.name.endswith('.' + actual_table)):
                                            table_node = node
                                            break

                                    if table_node:
                                        physical_ref = f"PHYSICAL_{table_node.name}.{column_name}"
                                        if physical_ref not in dependencies and physical_ref != exclude_field_id:
                                            dependencies.append(physical_ref)
                                        break

        except Exception as e:
            # 如果解析失败，返回空列表
            pass

        return dependencies

    def _find_field_in_node(self, column_name: str, node_id: str) -> Optional[str]:
        """
        在指定节点中查找字段（改进版 - 支持大小写不敏感匹配）
        """
        for field_id, field in self.fields.items():
            if field.parent_node_id == node_id and field.column_name.upper() == column_name.upper():
                return field_id
        return None

    def _find_field_in_ctes(self, column_name: str, parent_node_id: str) -> List[str]:
        """
        在节点引用的CTE中查找字段（改进版 - 支持相邻CTE查找）

        相邻引用关系：只在直接引用的CTE和子查询中查找，不跨越层级
        """
        # 找到节点引用的所有CTE和子查询
        referenced_nodes = []
        for rel in self.relationships:
            if rel.source_id == parent_node_id:
                # 包含CTE关系和子查询关系
                if rel.type == 'REFERENCES' and rel.metadata.get('ref_type') == 'CTE':
                    referenced_nodes.append(rel.target_id)
                elif rel.type == 'CONTAINS':
                    # 检查目标是否是CTE或SQ节点
                    target_node = self.nodes.get(rel.target_id)
                    if target_node and target_node.type in ['CT', 'SQ']:
                        referenced_nodes.append(rel.target_id)

        matching_fields = []
        for ref_node_id in referenced_nodes:
            ref_node = self.nodes.get(ref_node_id)
            if ref_node:
                # 在CTE/子查询节点中查找字段（支持大小写不敏感匹配）
                for field_id, field in self.fields.items():
                    if field.parent_node_id == ref_node_id:
                        # 优先级1: 精确匹配 column_name
                        if field.column_name == column_name:
                            matching_fields.append(field_id)
                        # 优先级2: 大小写不敏感匹配
                        elif field.column_name.upper() == column_name.upper():
                            # 避免重复添加
                            if field_id not in matching_fields:
                                matching_fields.append(field_id)
                        # 优先级3: 匹配字段别名（field.name）
                        elif field.name.upper() == column_name.upper():
                            if field_id not in matching_fields:
                                matching_fields.append(field_id)

        return matching_fields

    def _table_has_column(self, table_name: str, column_name: str) -> bool:
        """检查表是否有指定列（简化版）"""
        # 这里可以集成元数据管理器来精确检查
        # 目前简单返回True，让调用者继续处理
        return True

    def _find_field_by_reference(self, table_name: str, column_name: str,
                                  parent_node_id: str) -> Optional[str]:
        """
        根据表名和列名查找字段ID（改进版 - 支持大小写不敏感匹配）

        相邻引用关系：只在相邻作用域内查找
        """
        # 如果没有表名，尝试只通过列名查找
        if not table_name and column_name:
            # 在同一个节点内查找（大小写不敏感）
            for field_id, field in self.fields.items():
                if (field.column_name.upper() == column_name.upper() and
                    field.parent_node_id == parent_node_id):
                    return field_id

            # 如果没找到，尝试全局查找（大小写不敏感）
            for field_id, field in self.fields.items():
                if field.column_name.upper() == column_name.upper():
                    return field_id

        # 如果有表名和列名，尝试精确匹配（大小写不敏感）
        if table_name and column_name:
            # 优先在同一个节点内查找
            for field_id, field in self.fields.items():
                if (field.column_name.upper() == column_name.upper() and
                    field.table_name.upper() == table_name.upper() and
                    field.parent_node_id == parent_node_id):
                    return field_id

            # 如果没找到，尝试全局查找（不限制节点）
            for field_id, field in self.fields.items():
                if (field.column_name.upper() == column_name.upper() and
                    field.table_name.upper() == table_name.upper()):
                    return field_id

        return None

    def _build_scope_hierarchy(self):
        """
        构建作用域层次结构
        为所有节点创建对应的作用域并建立父子关系
        """
        if not self.use_scope_system:
            return

        # 按深度顺序处理节点（确保父节点先被处理）
        nodes_by_depth = sorted(
            [(node_id, node) for node_id, node in self.nodes.items() if node_id != "ROOT"],
            key=lambda x: x[1].depth
        )

        for node_id, node in nodes_by_depth:
            # 为每个节点创建作用域
            parent_scope_id = node.parent_id if node.parent_id else "ROOT"
            self.alias_manager.create_scope(node_id, parent_scope_id)

    def _propagate_fields_all_nodes(self):
        """
        在所有节点中传播字段
        建立完整的字段依赖链路
        """
        if not self.use_scope_system:
            return

        # 按深度顺序处理节点
        nodes_by_depth = sorted(
            [(node_id, node) for node_id, node in self.nodes.items()],
            key=lambda x: x[1].depth
        )

        for node_id, node in nodes_by_depth:
            if node.type in ["BLK", "CT", "DT", "SQ"]:
                # 为SELECT类型的节点传播字段
                self._propagate_fields_in_node(node)

    def _propagate_fields_in_node(self, node: Node):
        """
        在节点中传播字段
        解析FROM子句，建立别名映射，传播字段信息

        Args:
            node: SQL节点
        """
        if not self.use_scope_system:
            return

        scope = self.alias_manager.scope_manager.get_scope(node.id)
        if not scope:
            return

        # 解析节点的SQL以获取字段信息
        try:
            ast = parse(node.sql, dialect=self.dialect)
            if not ast:
                return

            for expr in ast:
                if isinstance(expr, exp.Select):
                    # 解析FROM子句
                    self._parse_from_clause_with_scope(expr, node)

                    # 解析SELECT子句中的字段
                    self._parse_select_clause_with_scope(expr, node)

        except Exception as e:
            # 解析失败时使用降级方案
            pass

    def _parse_from_clause_with_scope(self, select_expr: exp.Select, node: Node):
        """
        解析FROM子句（使用作用域系统）
        建立表别名映射并传播字段

        Args:
            select_expr: SELECT表达式
            node: SQL节点
        """
        if not self.use_scope_system:
            # 使用旧的逻辑
            table_aliases = self._parse_from_clause(select_expr, node)
            node.metadata['table_aliases'] = table_aliases
            return

        scope = self.alias_manager.scope_manager.get_scope(node.id)
        if not scope:
            return

        # 查找FROM子句
        from_clause = select_expr.find(exp.From)
        if not from_clause:
            return

        # 递归遍历FROM子句中的所有表和子查询
        def process_from_expression(expr, join_type="INNER"):
            """递归处理FROM表达式"""
            if isinstance(expr, exp.Table):
                table_name = expr.name or ''
                alias = expr.alias or expr.name

                if not table_name:
                    return

                # 检查是否是CTE引用
                if table_name.upper() in self.cte_names:
                    cte_node_id = self._find_cte_node_id(table_name)
                    if cte_node_id:
                        # 注册CTE别名
                        self.alias_manager.register_from_alias(
                            alias, cte_node_id, "CTE", node.id
                        )

                        # 从CTE传播字段
                        cte_scope = self.alias_manager.scope_manager.get_scope(cte_node_id)
                        if cte_scope:
                            propagated_fields = self.propagation_engine.propagate_from_cte(
                                table_name, cte_node_id, node.id, cte_scope
                            )

                            # 将传播的字段添加到当前作用域
                            for field_info in propagated_fields:
                                scope.add_field(field_info)
                else:
                    # 物理表
                    full_name = table_name
                    table_id = f"TB_{full_name}"

                    # 注册表别名
                    self.alias_manager.register_from_alias(
                        alias, full_name, "TABLE", node.id
                    )

                    # 从物理表传播字段
                    # 注意：这里需要知道表的列信息，实际应用中可以从信息库获取
                    propagated_fields = self.propagation_engine.propagate_from_physical_table(
                        full_name, table_id, node.id, columns=[]  # 空列表表示无法获取列信息
                    )

                    # 将传播的字段添加到当前作用域
                    for field_info in propagated_fields:
                        scope.add_field(field_info)

                # 处理子查询别名
                if isinstance(expr.this, exp.Subquery):
                    subquery_id = self._find_subquery_node_id(expr.this, node)
                    if subquery_id:
                        # 注册子查询别名
                        self.alias_manager.register_subquery_alias(alias, subquery_id, node.id)

                        # 从子查询传播字段
                        subquery_scope = self.alias_manager.scope_manager.get_scope(subquery_id)
                        if subquery_scope:
                            propagated_fields = self.propagation_engine.propagate_from_subquery(
                                subquery_id, alias, node.id, subquery_scope
                            )

                            # 将传播的字段添加到当前作用域
                            for field_info in propagated_fields:
                                scope.add_field(field_info)

            elif isinstance(expr, exp.Join):
                # 处理JOIN
                # 首先处理JOIN的右表
                process_from_expression(expr.this, join_type)

                # 然后处理JOIN中的其他表
                if hasattr(expr, 'args'):
                    # 处理JOIN的侧边表（side）
                    side = expr.args.get('side')
                    if side:
                        process_from_expression(side, join_type)

            elif isinstance(expr, exp.Subquery):
                # 处理子查询
                subquery_id = self._find_subquery_node_id(expr, node)
                if subquery_id:
                    alias = expr.alias or f"subq_{subquery_id}"

                    # 注册子查询别名
                    self.alias_manager.register_subquery_alias(alias, subquery_id, node.id)

                    # 从子查询传播字段
                    subquery_scope = self.alias_manager.scope_manager.get_scope(subquery_id)
                    if subquery_scope:
                        propagated_fields = self.propagation_engine.propagate_from_subquery(
                            subquery_id, alias, node.id, subquery_scope
                        )

                        # 将传播的字段添加到当前作用域
                        for field_info in propagated_fields:
                            scope.add_field(field_info)

        # 遍历FROM子句中的所有表
        for table in from_clause.find_all(exp.Table):
            process_from_expression(table)

        # 遍历FROM子句中的所有JOIN
        for join in from_clause.find_all(exp.Join):
            process_from_expression(join)

        # 遍历FROM子句中的所有子查询
        for subquery in from_clause.find_all(exp.Subquery):
            process_from_expression(subquery)

    def _parse_select_clause_with_scope(self, select_expr: exp.Select, node: Node):
        """
        解析SELECT子句（使用作用域系统）
        提取字段定义并建立字段关系

        Args:
            select_expr: SELECT表达式
            node: SQL节点
        """
        if not self.use_scope_system:
            # 使用旧的逻辑
            table_aliases = node.metadata.get('table_aliases', {})
            self._extract_fields(select_expr, node, table_aliases)
            return

        scope = self.alias_manager.scope_manager.get_scope(node.id)
        if not scope:
            return

        if not hasattr(select_expr, 'expressions'):
            return

        for idx, projection in enumerate(select_expr.expressions):
            # 获取字段别名
            alias = None
            if isinstance(projection, exp.Alias):
                alias = projection.alias
                actual_expr = projection.this
            else:
                actual_expr = projection
                if isinstance(actual_expr, exp.Column):
                    alias = actual_expr.name

            field_name = alias or f"field_{idx}"

            # 使用作用域系统推断字段来源
            table_name, column_name = self._infer_field_source_v2(
                actual_expr, field_name, scope, node
            )

            # 生成字段ID
            field_id = self._generate_field_id(node.id, field_name, idx)

            # 分析字段表达式
            field_info = self._analyze_field_expression_basic(actual_expr, {})

            # 创建Field对象
            field = Field(
                id=field_id,
                name=field_name,
                table_name=table_name or field_info.get('table_name', ''),
                column_name=column_name or field_info.get('column_name', ''),
                field_type=field_info.get('field_type', 'UNKNOWN'),
                transformation=field_info.get('transformation', {}),
                dependencies=[],  # 稍后构建
                parent_node_id=node.id,
                metadata={
                    'sql': self._get_sql_from_expression(projection),
                    'position': idx,
                    'raw_expression': self._get_sql_from_expression(actual_expr),
                    'scope_system': True  # 标记使用作用域系统
                }
            )

            self.fields[field_id] = field

            # 创建FieldInfo并添加到作用域
            field_info_scope = FieldInfo(
                field_id=field_id,
                field_name=field_name,
                source_node_id=node.id,
                source_table=table_name or '',
                column_name=column_name or field_name,
                field_type=field_info.get('field_type', 'UNKNOWN'),
                transformation=field_info.get('transformation', {}),
                propagation_path=[node.id],
                is_inherited=False,
                inheritance_depth=0
            )

            scope.add_field(field_info_scope)

    def _infer_field_source_v2(self, expr: exp.Expression, field_name: str,
                               current_scope: 'FieldScope', parent_node: Node) -> Tuple[str, str]:
        """
        改进的字段来源推断算法（使用作用域机制）

        Args:
            expr: 字段表达式
            field_name: 字段名
            current_scope: 当前作用域
            parent_node: 父节点

        Returns:
            (表名, 列名) 元组
        """
        if not self.use_scope_system:
            # 降级到旧方法
            return self._infer_field_source(expr, field_name, {}, parent_node), field_name

        # 1. 提取表前缀和列名
        table_prefix = ""
        column_name = ""

        if isinstance(expr, exp.Column):
            table_prefix = expr.table or ''
            column_name = expr.name or ''

        # 2. 如果有表前缀，使用作用域机制解析
        if table_prefix:
            # 解析表别名
            alias_info = current_scope.resolve_table_alias(table_prefix)
            if alias_info:
                if alias_info.alias_type == "TABLE":
                    # 物理表
                    return alias_info.target, column_name
                elif alias_info.alias_type == "CTE":
                    # CTE
                    return alias_info.alias, column_name
                elif alias_info.alias_type == "SUBQUERY":
                    # 子查询
                    return alias_info.alias, column_name

        # 3. 如果没有表前缀或解析失败，尝试推断
        # 在当前作用域的所有可见字段中查找匹配的字段
        all_fields = current_scope.get_all_visible_fields()

        if column_name:
            # 尝试精确匹配列名
            for visible_field_name, field_id in all_fields.items():
                if visible_field_name.endswith(f".{column_name}") or visible_field_name == column_name:
                    field_info = current_scope.get_field_info(field_id)
                    if field_info:
                        return field_info.source_table, column_name

        # 4. 如果还是找不到，尝试从表达式中提取表引用
        tables_in_expr = set()
        for col in expr.find_all(exp.Column):
            if col.table:
                tables_in_expr.add(col.table)

        if len(tables_in_expr) == 1:
            # 只找到一个表引用，使用它
            table_name = list(tables_in_expr)[0]
            alias_info = current_scope.resolve_table_alias(table_name)
            if alias_info:
                return alias_info.target if alias_info.alias_type == "TABLE" else alias_info.alias, column_name or field_name

        return '', column_name or field_name

    def _build_cross_node_field_mappings(self):
        """
        构建跨节点字段映射关系
        使用作用域信息提取字段依赖
        """
        if not self.use_scope_system:
            # 使用旧的字段依赖构建方法
            self._build_field_dependencies()

            # 新增：无论是否使用作用域系统，都处理标量子查询
            self._build_scalar_query_derivations()

            # 新增：处理CTE字段关系
            self._build_cte_field_derivations()

            # 新增：处理UNION字段关系
            self._build_union_field_derivations()
            return

        # 清空现有的字段关系
        self.field_relationships.clear()

        # 遍历所有字段，建立依赖关系
        for field_id, field in self.fields.items():
            raw_expr = field.metadata.get('raw_expression', '')
            if not raw_expr:
                continue

            # 解析表达式以获取依赖
            dependencies = self._extract_field_dependencies_from_expression_v2(
                raw_expr, field.parent_node_id, field_id
            )

            field.dependencies = dependencies

            # 创建字段关系
            for dep_id in dependencies:
                if dep_id in self.fields:
                    # 创建DERIVES关系
                    field_rel = FieldRelationship(
                        source_id=dep_id,
                        target_id=field_id,
                        type="DERIVES",
                        metadata={
                            'transformation': field.transformation.get('type', 'unknown'),
                            'cross_node': self._is_cross_node_relationship(dep_id, field_id),
                            'scope_system': True
                        }
                    )
                    self.field_relationships.append(field_rel)

                    # 创建USES反向关系
                    field_rel_reverse = FieldRelationship(
                        source_id=field_id,
                        target_id=dep_id,
                        type="USES",
                        metadata={'scope_system': True}
                    )
                    self.field_relationships.append(field_rel_reverse)

        # 新增：处理标量子查询字段的DERIVES关系
        self._build_scalar_query_derivations()

        # 新增：处理CTE字段的DERIVES关系
        self._build_cte_field_derivations()

        # 新增：处理UNION字段的DERIVES关系
        self._build_union_field_derivations()

    def _build_scalar_query_derivations(self):
        """
        为标量子查询字段建立DERIVES关系

        在所有节点都创建完成后调用，确保能找到对应的子查询节点
        """
        if not hasattr(self, '_scalar_query_fields'):
            return

        for item in self._scalar_query_fields:
            outer_field = item['outer_field']
            subquery_expr = item['subquery_expr']
            outer_node = item['outer_node']

            # 处理Subquery包装
            actual_select = subquery_expr
            if isinstance(subquery_expr, exp.Subquery):
                actual_select = subquery_expr.this

            # 1. 找到子查询节点
            subquery_node = None
            for node_id, node in self.nodes.items():
                if node.parent_id == outer_node.id and node.type == 'SQ':
                    # 验证是否匹配（通过SQL内容）
                    subquery_sql = self._get_sql_from_expression(actual_select)
                    if subquery_sql in node.sql or node.sql in subquery_sql:
                        subquery_node = node
                        break

            if not subquery_node:
                continue

            # 2. 找到子查询的输出字段
            if not subquery_node.output_fields:
                continue

            subquery_field_id = subquery_node.output_fields[0]
            subquery_field = self.fields.get(subquery_field_id)

            if not subquery_field:
                continue

            # 3. 建立DERIVES关系：子查询输出字段 → 外层字段
            derives_rel = FieldRelationship(
                source_id=subquery_field_id,
                target_id=outer_field.id,
                type='DERIVES',
                metadata={
                    'relationship_type': 'scalar_subquery',
                    'subquery_node_id': subquery_node.id,
                    'alias_mapping': {
                        'source_field': subquery_field.name,  # AS前（子查询的字段名）
                        'target_field': outer_field.name     # AS后（外层的字段名）
                    }
                }
            )

            self.field_relationships.append(derives_rel)

            # 4. 【新增】继承子查询内部字段的table_name和column_name
            # 这样在导入Neo4j时，会为子查询输出字段创建PROVIDES关系
            if subquery_field.table_name:
                outer_field.table_name = subquery_field.table_name
                outer_field.column_name = subquery_field.column_name

    def _build_cte_field_derivations(self):
        """
        为CTE字段建立DERIVES关系

        目标：CTE输出字段 → 引用位置的DERIVES关系
        示例：CTE_TMP_FK.ZY_FK -(DERIVES)-> ROOT_BLK.ZY_FK
        """

        # 构建别名到CTE名称的映射
        alias_to_cte = {}
        for node_id, node in self.nodes.items():
            if node.type == "CT":
                # 找到引用这个CTE的节点，获取别名
                for rel in self.relationships:
                    if rel.target_id == node_id and rel.metadata.get('ref_type') == 'CTE':
                        alias = rel.metadata.get('alias')
                        if alias:
                            alias_to_cte[alias.upper()] = node.metadata.get('cte_name', node.name)

        # 遍历所有字段
        for field_id, field in self.fields.items():
            # 检查字段的table_name
            if not field.table_name:
                continue

            table_name_upper = field.table_name.upper()

            # 检查是否直接是CTE引用
            cte_name = None
            if table_name_upper in self.cte_names:
                cte_name = field.table_name
            # 检查是否是CTE别名
            elif table_name_upper in alias_to_cte:
                cte_name = alias_to_cte[table_name_upper]

            if not cte_name:
                continue

            # 找到CTE节点（使用CTE名称而不是别名）
            cte_node_id = self._find_cte_node_id(cte_name)
            if not cte_node_id:
                continue

            cte_node = self.nodes.get(cte_node_id)
            if not cte_node:
                continue

            # 在CTE节点中查找对应的输出字段
            cte_field_id = None
            for fid, f in self.fields.items():
                if f.parent_node_id == cte_node_id and f.name == field.column_name:
                    cte_field_id = fid
                    break

            if not cte_field_id:
                continue

            cte_field = self.fields.get(cte_field_id)
            if not cte_field:
                continue

            # 建立DERIVES关系：CTE输出字段 → 引用字段
            derives_rel = FieldRelationship(
                source_id=cte_field_id,
                target_id=field_id,
                type='DERIVES',
                metadata={
                    'relationship_type': 'cte_reference',
                    'cte_node_id': cte_node_id,
                    'cte_name': field.table_name,
                    'alias_mapping': {
                        'source_field': cte_field.name,  # CTE中的字段名
                        'target_field': field.name      # 引用位置的字段名
                    }
                }
            )

            self.field_relationships.append(derives_rel)

    def _build_union_field_derivations(self):
        """
        为UNION查询建立字段关系

        目标：创建UNION输出字段，并建立从分支字段到UNION输出字段的DERIVES关系
        示例：
          ROOT_UNION_0_BLK_1.CUST_CODE -(DERIVES)-> ROOT_UNION_0.CUST_CODE
          ROOT_UNION_0_BLK_2.CUST_CODE -(DERIVES)-> ROOT_UNION_0.CUST_CODE
        """

        # 找到所有UNION节点
        union_nodes = []
        for node_id, node in self.nodes.items():
            if node.type == "UNION":
                union_nodes.append(node)

        if not union_nodes:
            return

        # 处理每个UNION节点
        for union_node in union_nodes:
            # 找到UNION节点的所有子节点（分支）
            branch_nodes = []
            for node_id, node in self.nodes.items():
                if node.parent_id == union_node.id and node.type in ["BLK", "SQ"]:
                    branch_nodes.append(node)

            if len(branch_nodes) < 2:
                continue

            # 合并所有分支的字段，创建UNION输出字段
            # 找出所有分支共有的字段（按位置匹配）
            union_fields = {}  # {字段名: [分支字段ID列表]}

            for branch_node in branch_nodes:
                # 获取该分支的输出字段
                branch_fields = []
                for field_id, field in self.fields.items():
                    if field.parent_node_id == branch_node.id and field_id in branch_node.output_fields:
                        branch_fields.append(field)

                # 按位置排序
                branch_fields.sort(key=lambda f: field.metadata.get('position', 0))

                # 按位置添加到union_fields
                for idx, field in enumerate(branch_fields):
                    if idx not in union_fields:
                        union_fields[idx] = []
                    union_fields[idx].append((field, branch_node))

            # 为UNION节点创建输出字段，并建立DERIVES关系
            for idx, fields_list in union_fields.items():
                if not fields_list:
                    continue

                # 使用第一个分支的字段名作为UNION输出字段名
                first_field = fields_list[0][0]
                union_field_name = first_field.name

                # 创建UNION输出字段
                union_field_id = f"{union_node.id}_FLD_{union_field_name}_{idx}"
                union_field = Field(
                    id=union_field_id,
                    name=union_field_name,
                    table_name="",  # UNION字段没有单一来源表
                    column_name=first_field.column_name,
                    field_type="UNION",
                    transformation={'type': 'union', 'branch_count': len(fields_list)},
                    dependencies=[f[0].id for f in fields_list],
                    parent_node_id=union_node.id,
                    metadata={
                        'position': idx,
                        'is_union': True,
                        'branches': len(fields_list)
                    }
                )

                self.fields[union_field_id] = union_field
                union_node.output_fields.append(union_field_id)

                # 为每个分支字段建立DERIVES关系到UNION输出字段
                for branch_field, branch_node in fields_list:
                    derives_rel = FieldRelationship(
                        source_id=branch_field.id,
                        target_id=union_field_id,
                        type='DERIVES',
                        metadata={
                            'relationship_type': 'union_branch',
                            'union_node_id': union_node.id,
                            'branch_node_id': branch_node.id,
                            'branch_position': idx
                        }
                    )

                    self.field_relationships.append(derives_rel)

    def _extract_field_dependencies_from_expression_v2(self, expr_sql: str,
                                                       parent_node_id: str,
                                                       exclude_field_id: str = None) -> List[str]:
        """
        从表达式SQL中提取依赖的字段ID（使用作用域系统）

        Args:
            expr_sql: 表达式SQL
            parent_node_id: 父节点ID
            exclude_field_id: 要排除的字段ID

        Returns:
            依赖字段ID列表
        """
        if not self.use_scope_system:
            return self._extract_field_dependencies_from_expression(
                expr_sql, parent_node_id, exclude_field_id
            )

        dependencies = []

        try:
            # 解析表达式
            expr = parse(expr_sql, dialect=self.dialect)
            if not expr:
                return dependencies

            expr = expr[0]

            # 获取当前作用域
            scope = self.alias_manager.scope_manager.get_scope(parent_node_id)
            if not scope:
                return dependencies

            # 查找所有列引用
            for col in expr.find_all(exp.Column):
                table_name = col.table or ''
                column_name = col.name or ''

                # 使用作用域系统解析字段引用
                field_info = scope.resolve_field(column_name, table_name)
                if field_info and field_info.field_id != exclude_field_id:
                    if field_info.field_id not in dependencies:
                        dependencies.append(field_info.field_id)

        except Exception as e:
            # 解析失败，返回空列表
            pass

        return dependencies

    def _is_cross_node_relationship(self, source_field_id: str, target_field_id: str) -> bool:
        """
        判断字段关系是否跨节点

        Args:
            source_field_id: 源字段ID
            target_field_id: 目标字段ID

        Returns:
            是否跨节点
        """
        # 提取节点ID（假设字段ID格式为 node_id_FLD_...）
        source_node = '_'.join(source_field_id.split('_')[:-2])
        target_node = '_'.join(target_field_id.split('_')[:-2])

        return source_node != target_node

    def export_json(self, filepath: str):
        """导出为JSON文件"""
        # 构建字段依赖关系（如果还没有构建）
        if self.fields and not self.field_relationships:
            self._build_cross_node_field_mappings()  # 修改：统一调用这个方法

        data = {
            "nodes": [node.to_dict() for node in self.nodes.values()],
            "relationships": [rel.to_dict() for rel in self.relationships],
            "fields": [field.to_dict() for field in self.fields.values()],
            "field_relationships": [rel.to_dict() for rel in self.field_relationships]
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def print_summary(self):
        """打印统计摘要"""
        # 确保字段依赖关系已构建
        if self.fields and not self.field_relationships:
            self._build_cross_node_field_mappings()  # 修改：统一调用这个方法

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

        # 字段级统计
        if self.fields:
            print("\n" + "="*80)
            print("字段级血缘统计")
            print("="*80)

            field_type_counts = defaultdict(int)
            for field in self.fields.values():
                field_type_counts[field.field_type] += 1

            print(f"\n字段类型统计:")
            for field_type, count in sorted(field_type_counts.items()):
                print(f"  {field_type}: {count}个")

            print(f"\n总字段数: {len(self.fields)}")
            print(f"字段关系数: {len(self.field_relationships)}")

            # 转换规则统计
            transformation_types = defaultdict(int)
            for field in self.fields.values():
                trans_type = field.transformation.get('type', 'unknown')
                transformation_types[trans_type] += 1

            if transformation_types:
                print(f"\n转换规则统计:")
                for trans_type, count in sorted(transformation_types.items()):
                    print(f"  {trans_type}: {count}个")

        # 作用域系统统计
        if self.use_scope_system:
            print("\n" + "="*80)
            print("作用域系统统计")
            print("="*80)

            print(f"\n作用域总数: {len(self.scope_manager.scopes)}")
            print(f"全局别名数: {len(self.alias_manager.global_aliases)}")
            print(f"表别名映射数: {len(self.alias_manager.table_mappings)}")
            print(f"子查询映射数: {len(self.alias_manager.subquery_mappings)}")

            prop_stats = self.propagation_engine.get_statistics()
            print(f"\n传播引擎统计:")
            print(f"  缓存传播数: {prop_stats['cached_propagations']}")
            print(f"  字段映射数: {prop_stats['field_mappings']}")
            print(f"  总映射数: {prop_stats['total_mappings']}")

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
