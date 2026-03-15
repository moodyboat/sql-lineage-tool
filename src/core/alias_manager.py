#!/usr/bin/env python3
"""
别名管理器
管理SQL解析过程中的表别名和CTE别名
"""

from typing import Dict, List, Set, Optional, Tuple
from field_scope import FieldScope, AliasInfo, ScopeManager
from collections import defaultdict


class AliasManager:
    """
    别名管理器
    负责管理表别名、CTE别名和子查询别名
    支持作用域链查找和别名解析
    """

    def __init__(self, scope_manager: ScopeManager):
        """
        初始化别名管理器

        Args:
            scope_manager: 作用域管理器引用
        """
        self.scope_manager = scope_manager

        # 全局别名（CTE别名在所有作用域都可见）
        self.global_aliases: Dict[str, AliasInfo] = {}

        # 子查询别名映射 (alias -> node_id)
        self.subquery_mappings: Dict[str, str] = {}

        # 物理表别名映射 (alias -> table_name)
        self.table_mappings: Dict[str, str] = {}

        # CTE名称到节点ID的映射
        self.cte_mappings: Dict[str, str] = {}

        # 作用域栈（用于解析过程跟踪）
        self.scope_stack: List[FieldScope] = []

    def create_scope(self, scope_id: str, parent_scope_id: Optional[str] = None) -> FieldScope:
        """
        创建新作用域

        Args:
            scope_id: 作用域ID
            parent_scope_id: 父作用域ID

        Returns:
            创建的作用域对象
        """
        scope = self.scope_manager.create_scope(scope_id, parent_scope_id)
        return scope

    def push_scope(self, scope: FieldScope):
        """
        将作用域压入栈

        Args:
            scope: 作用域对象
        """
        self.scope_stack.append(scope)

    def pop_scope(self) -> Optional[FieldScope]:
        """
        从栈中弹出作用域

        Returns:
            弹出的作用域对象，如果栈为空返回None
        """
        if self.scope_stack:
            return self.scope_stack.pop()
        return None

    def get_current_scope(self) -> Optional[FieldScope]:
        """
        获取当前作用域（栈顶）

        Returns:
            当前作用域对象
        """
        if self.scope_stack:
            return self.scope_stack[-1]
        return None

    def register_cte_alias(self, cte_name: str, cte_node_id: str, scope_id: str):
        """
        注册CTE全局别名

        Args:
            cte_name: CTE名称
            cte_node_id: CTE节点ID
            scope_id: 作用域ID
        """
        # 创建CTE别名信息
        alias_info = AliasInfo(
            alias=cte_name.upper(),
            target=cte_node_id,
            alias_type="CTE",
            scope_id=scope_id
        )

        # 添加到全局别名
        self.global_aliases[cte_name.upper()] = alias_info

        # 添加到CTE映射
        self.cte_mappings[cte_name.upper()] = cte_node_id

    def register_from_alias(self, alias: str, target: str, alias_type: str,
                           scope_id: str, propagation_source: Optional[str] = None):
        """
        注册FROM子句中的表别名

        Args:
            alias: 表别名
            target: 实际表名/CTE名/子查询ID
            alias_type: 别名类型（TABLE/CTE/SUBQUERY）
            scope_id: 作用域ID
            propagation_source: 别名传播来源（可选）
        """
        # 创建别名信息
        alias_info = AliasInfo(
            alias=alias,
            target=target,
            alias_type=alias_type,
            propagation_source=propagation_source,
            scope_id=scope_id
        )

        # 添加到对应的作用域
        scope = self.scope_manager.get_scope(scope_id)
        if scope:
            scope.add_table_alias(alias_info)

        # 如果是物理表，添加到表映射
        if alias_type == "TABLE":
            self.table_mappings[alias] = target

        # 如果是子查询，添加到子查询映射
        if alias_type == "SUBQUERY":
            self.subquery_mappings[alias] = target

    def register_subquery_alias(self, alias: str, subquery_node_id: str, scope_id: str):
        """
        注册子查询别名映射

        Args:
            alias: 子查询别名
            subquery_node_id: 子查询节点ID
            scope_id: 作用域ID
        """
        self.register_from_alias(alias, subquery_node_id, "SUBQUERY", scope_id)
        self.subquery_mappings[alias] = subquery_node_id

    def resolve_table_reference(self, table_name: str, scope_id: str) -> Optional[AliasInfo]:
        """
        解析表引用（支持作用域链查找）

        Args:
            table_name: 表名或别名
            scope_id: 作用域ID

        Returns:
            别名信息，如果找不到返回None
        """
        # 1. 首先在全局别名中查找（CTE）
        if table_name.upper() in self.global_aliases:
            return self.global_aliases[table_name.upper()]

        # 2. 在指定作用域中查找
        scope = self.scope_manager.get_scope(scope_id)
        if scope:
            return scope.resolve_table_alias(table_name)

        return None

    def resolve_table_reference_with_scope_chain(self, table_name: str,
                                                 scope_id: str) -> Optional[AliasInfo]:
        """
        解析表引用（支持完整的作用域链查找）

        Args:
            table_name: 表名或别名
            scope_id: 起始作用域ID

        Returns:
            别名信息，如果找不到返回None
        """
        # 1. 首先在全局别名中查找（CTE）
        if table_name.upper() in self.global_aliases:
            return self.global_aliases[table_name.upper()]

        # 2. 在作用域链中查找
        scope = self.scope_manager.get_scope(scope_id)
        if scope:
            return scope.resolve_table_alias(table_name)

        return None

    def get_actual_table_name(self, alias: str, scope_id: str) -> Optional[str]:
        """
        获取别名对应的实际表名

        Args:
            alias: 表别名
            scope_id: 作用域ID

        Returns:
            实际表名，如果找不到返回None
        """
        alias_info = self.resolve_table_reference(alias, scope_id)
        if alias_info:
            if alias_info.alias_type == "TABLE":
                return alias_info.target
            elif alias_info.alias_type == "CTE":
                # CTE类型，返回CTE名称
                return alias_info.alias

        return None

    def get_subquery_node_id(self, alias: str, scope_id: str) -> Optional[str]:
        """
        获取子查询别名对应的节点ID

        Args:
            alias: 子查询别名
            scope_id: 作用域ID

        Returns:
            子查询节点ID，如果找不到返回None
        """
        alias_info = self.resolve_table_reference(alias, scope_id)
        if alias_info and alias_info.alias_type == "SUBQUERY":
            return alias_info.target

        return None

    def is_cte_reference(self, table_name: str) -> bool:
        """
        判断表名是否是CTE引用

        Args:
            table_name: 表名

        Returns:
            是否为CTE引用
        """
        return table_name.upper() in self.global_aliases

    def get_cte_node_id(self, cte_name: str) -> Optional[str]:
        """
        获取CTE名称对应的节点ID

        Args:
            cte_name: CTE名称

        Returns:
            CTE节点ID，如果找不到返回None
        """
        return self.cte_mappings.get(cte_name.upper())

    def get_all_aliases_in_scope(self, scope_id: str) -> Dict[str, AliasInfo]:
        """
        获取作用域中的所有别名（包括继承的）

        Args:
            scope_id: 作用域ID

        Returns:
            别名到别名信息的映射
        """
        scope = self.scope_manager.get_scope(scope_id)
        if scope:
            return scope.get_all_aliases()
        return {}

    def get_field_visibility_in_scope(self, scope_id: str) -> Dict[str, str]:
        """
        获取作用域中的所有可见字段

        Args:
            scope_id: 作用域ID

        Returns:
            字段名到字段ID的映射
        """
        scope = self.scope_manager.get_scope(scope_id)
        if scope:
            return scope.get_all_visible_fields()
        return {}

    def propagate_alias_to_child_scope(self, alias: str, child_scope_id: str):
        """
        将别名传播到子作用域

        Args:
            alias: 别名
            child_scope_id: 子作用域ID
        """
        parent_scope = self.scope_manager.get_scope(child_scope_id)
        if parent_scope and parent_scope.parent_scope:
            # 从父作用域获取别名信息
            alias_info = parent_scope.parent_scope.resolve_table_alias(alias)
            if alias_info:
                # 在子作用域中注册这个别名
                child_scope = self.scope_manager.get_scope(child_scope_id)
                if child_scope:
                    child_scope.add_table_alias(alias_info)

    def build_alias_propagation_graph(self) -> Dict[str, List[str]]:
        """
        构建别名传播图

        Returns:
            别名到依赖列表的映射
        """
        graph = defaultdict(list)

        for scope in self.scope_manager.get_all_scopes():
            for alias_info in scope.table_aliases.values():
                if alias_info.propagation_source:
                    graph[alias_info.propagation_source].append(alias_info.alias)

        return dict(graph)

    def get_alias_statistics(self) -> Dict[str, int]:
        """
        获取别名统计信息

        Returns:
            统计信息字典
        """
        stats = {
            'global_aliases': len(self.global_aliases),
            'table_mappings': len(self.table_mappings),
            'subquery_mappings': len(self.subquery_mappings),
            'cte_mappings': len(self.cte_mappings),
            'total_scopes': len(self.scope_manager.scopes)
        }

        return stats

    def clear(self):
        """清空所有别名信息"""
        self.global_aliases.clear()
        self.subquery_mappings.clear()
        self.table_mappings.clear()
        self.cte_mappings.clear()
        self.scope_stack.clear()
        self.scope_manager.clear()

    def __repr__(self) -> str:
        """字符串表示"""
        return (f"AliasManager(global_aliases={len(self.global_aliases)}, "
                f"table_mappings={len(self.table_mappings)}, "
                f"subquery_mappings={len(self.subquery_mappings)})")
