#!/usr/bin/env python3
"""
字段传播引擎
实现字段在不同SQL节点间的传播机制
"""

from typing import Dict, List, Set, Optional, Tuple
from field_scope import FieldScope, FieldInfo, AliasInfo
from alias_manager import AliasManager
from collections import defaultdict
import sqlglot
from sqlglot import exp


class FieldPropagationEngine:
    """
    字段传播引擎
    负责在不同SQL节点间传播字段信息
    实现封装和继承机制的核心逻辑
    """

    def __init__(self, alias_manager: AliasManager):
        """
        初始化字段传播引擎

        Args:
            alias_manager: 别名管理器引用
        """
        self.alias_manager = alias_manager
        self.propagation_cache: Dict[str, List[FieldInfo]] = {}  # 传播缓存
        self.field_mappings: Dict[str, List[str]] = {}  # 字段映射关系

    def propagate_from_physical_table(self, table_name: str, table_node_id: str,
                                      scope_id: str, columns: List[str] = None) -> List[FieldInfo]:
        """
        从物理表传播字段

        Args:
            table_name: 表名
            table_node_id: 表节点ID
            scope_id: 目标作用域ID
            columns: 列名列表（如果为None，尝试从表中获取）

        Returns:
            传播的字段信息列表
        """
        cache_key = f"{table_node_id}_{scope_id}"
        if cache_key in self.propagation_cache:
            return self.propagation_cache[cache_key]

        propagated_fields = []

        # 如果没有提供列名，创建默认列
        if columns is None:
            columns = []  # 实际应用中可以从信息库中获取

        # 为每列创建字段信息
        for idx, column_name in enumerate(columns):
            field_id = f"{table_node_id}_FLD_{column_name}_{idx}"

            field_info = FieldInfo(
                field_id=field_id,
                field_name=column_name,
                source_node_id=table_node_id,
                source_table=table_name,
                column_name=column_name,
                field_type="COLUMN",
                transformation={
                    'type': 'column_reference',
                    'table': table_name,
                    'column': column_name
                },
                propagation_path=[table_node_id],
                is_inherited=False,
                inheritance_depth=0
            )

            propagated_fields.append(field_info)

        # 缓存结果
        self.propagation_cache[cache_key] = propagated_fields

        return propagated_fields

    def propagate_from_cte(self, cte_name: str, cte_node_id: str,
                          target_scope_id: str, source_scope: FieldScope) -> List[FieldInfo]:
        """
        从CTE传播字段（通过CTE别名）

        Args:
            cte_name: CTE名称
            cte_node_id: CTE节点ID
            target_scope_id: 目标作用域ID
            source_scope: CTE源作用域

        Returns:
            传播的字段信息列表
        """
        cache_key = f"CTE_{cte_node_id}_{target_scope_id}"
        if cache_key in self.propagation_cache:
            return self.propagation_cache[cache_key]

        propagated_fields = []

        # 从CTE作用域中获取所有字段
        cte_fields = source_scope.get_all_visible_fields()

        # 为CTE的每个字段创建封装后的字段信息
        for field_name, field_id in cte_fields.items():
            field_info = source_scope.get_field_info(field_id)
            if field_info:
                # 创建传播后的字段信息（封装机制）
                propagated_field = FieldInfo(
                    field_id=f"{cte_node_id}_PROP_{field_info.field_id}",
                    field_name=field_info.field_name,
                    source_node_id=cte_node_id,
                    source_table=cte_name,
                    column_name=field_info.column_name,
                    field_type=field_info.field_type,
                    transformation=field_info.transformation,
                    propagation_path=field_info.propagation_path + [cte_node_id],
                    is_inherited=True,
                    inheritance_depth=field_info.inheritance_depth + 1,
                    dependencies=field_info.dependencies
                )

                propagated_fields.append(propagated_field)

        # 缓存结果
        self.propagation_cache[cache_key] = propagated_fields

        return propagated_fields

    def propagate_from_subquery(self, subquery_node_id: str, subquery_alias: str,
                               target_scope_id: str, source_scope: FieldScope) -> List[FieldInfo]:
        """
        从子查询传播字段（封装机制核心）

        Args:
            subquery_node_id: 子查询节点ID
            subquery_alias: 子查询别名
            target_scope_id: 目标作用域ID
            source_scope: 子查询源作用域

        Returns:
            传播的字段信息列表
        """
        cache_key = f"SUBQ_{subquery_node_id}_{target_scope_id}"
        if cache_key in self.propagation_cache:
            return self.propagation_cache[cache_key]

        propagated_fields = []

        # 从子查询作用域中获取所有可见字段
        subquery_fields = source_scope.get_all_visible_fields()

        # 为子查询的每个字段创建封装后的字段信息
        for field_name, field_id in subquery_fields.items():
            field_info = source_scope.get_field_info(field_id)
            if field_info:
                # 通过别名封装字段（封装机制的关键）
                # 外部访问需要通过 subquery_alias.field_name 的形式
                propagated_field = FieldInfo(
                    field_id=f"{subquery_node_id}_ENC_{field_info.field_id}",
                    field_name=f"{subquery_alias}.{field_info.field_name}",
                    source_node_id=subquery_node_id,
                    source_table=subquery_alias,
                    column_name=field_info.field_name,
                    field_type=field_info.field_type,
                    transformation={
                        'type': 'encapsulated_field',
                        'source_field': field_info.field_id,
                        'encapsulation_alias': subquery_alias
                    },
                    propagation_path=field_info.propagation_path + [subquery_node_id],
                    is_inherited=True,
                    inheritance_depth=field_info.inheritance_depth + 1,
                    dependencies=field_info.dependencies
                )

                propagated_fields.append(propagated_field)

        # 缓存结果
        self.propagation_cache[cache_key] = propagated_fields

        return propagated_fields

    def propagate_from_join(self, join_type: str, left_scope_id: str,
                           right_scope_id: str, target_scope_id: str) -> List[FieldInfo]:
        """
        从JOIN传播字段（处理LEFT/INNER JOIN）

        Args:
            join_type: JOIN类型（LEFT/INNER/FULL等）
            left_scope_id: 左表作用域ID
            right_scope_id: 右表作用域ID
            target_scope_id: 目标作用域ID

        Returns:
            传播的字段信息列表
        """
        cache_key = f"JOIN_{join_type}_{left_scope_id}_{right_scope_id}_{target_scope_id}"
        if cache_key in self.propagation_cache:
            return self.propagation_cache[cache_key]

        propagated_fields = []

        left_scope = self.alias_manager.scope_manager.get_scope(left_scope_id)
        right_scope = self.alias_manager.scope_manager.get_scope(right_scope_id)

        if not left_scope or not right_scope:
            return propagated_fields

        # 获取左右作用域的所有字段
        left_fields = left_scope.get_all_visible_fields()
        right_fields = right_scope.get_all_visible_fields()

        # INNER JOIN: 合并两个字段集
        # LEFT JOIN: 左表字段为主，右表字段为辅（可能为NULL）

        if join_type.upper() in ["INNER", "JOIN"]:
            # INNER JOIN: 合并所有字段
            for field_name, field_id in left_fields.items():
                field_info = left_scope.get_field_info(field_id)
                if field_info:
                    propagated_fields.append(field_info)

            for field_name, field_id in right_fields.items():
                field_info = right_scope.get_field_info(field_id)
                if field_info:
                    propagated_fields.append(field_info)

        elif join_type.upper() == "LEFT":
            # LEFT JOIN: 左表字段优先
            for field_name, field_id in left_fields.items():
                field_info = left_scope.get_field_info(field_id)
                if field_info:
                    propagated_fields.append(field_info)

            # 右表字段标记为可能为NULL
            for field_name, field_id in right_fields.items():
                field_info = right_scope.get_field_info(field_id)
                if field_info:
                    # 创建可能为NULL的字段信息
                    nullable_field = FieldInfo(
                        field_id=f"{field_info.field_id}_NULLABLE",
                        field_name=field_info.field_name,
                        source_node_id=field_info.source_node_id,
                        source_table=field_info.source_table,
                        column_name=field_info.column_name,
                        field_type=field_info.field_type,
                        transformation=field_info.transformation,
                        propagation_path=field_info.propagation_path,
                        is_inherited=True,
                        inheritance_depth=field_info.inheritance_depth + 1,
                        dependencies=field_info.dependencies
                    )
                    # 标记为可能为NULL
                    nullable_field.transformation['nullable'] = True
                    propagated_fields.append(nullable_field)

        # 缓存结果
        self.propagation_cache[cache_key] = propagated_fields

        return propagated_fields

    def propagate_from_union(self, union_node_id: str, branch_scopes: List[str],
                            target_scope_id: str) -> List[FieldInfo]:
        """
        从UNION传播字段（合并多个分支的字段）

        Args:
            union_node_id: UNION节点ID
            branch_scopes: 分支作用域ID列表
            target_scope_id: 目标作用域ID

        Returns:
            传播的字段信息列表
        """
        cache_key = f"UNION_{union_node_id}_{target_scope_id}"
        if cache_key in self.propagation_cache:
            return self.propagation_cache[cache_key]

        propagated_fields = []

        if not branch_scopes:
            return propagated_fields

        # 获取第一个分支的字段作为基础
        first_scope = self.alias_manager.scope_manager.get_scope(branch_scopes[0])
        if not first_scope:
            return propagated_fields

        base_fields = first_scope.get_all_visible_fields()

        # 对于每个字段，检查是否在所有分支中都存在
        for field_name in base_fields.keys():
            field_exists_in_all = True
            field_sources = []

            for scope_id in branch_scopes:
                scope = self.alias_manager.scope_manager.get_scope(scope_id)
                if scope:
                    field_info = scope.resolve_field(field_name)
                    if field_info:
                        field_sources.append(field_info.field_id)
                    else:
                        field_exists_in_all = False
                        break

            if field_exists_in_all:
                # 创建UNION字段信息
                union_field = FieldInfo(
                    field_id=f"{union_node_id}_UNION_{field_name}",
                    field_name=field_name,
                    source_node_id=union_node_id,
                    source_table=f"UNION_OF_{len(branch_scopes)}",
                    column_name=field_name,
                    field_type="UNION_FIELD",
                    transformation={
                        'type': 'union_field',
                        'sources': field_sources
                    },
                    propagation_path=[union_node_id],
                    is_inherited=False,
                    inheritance_depth=0,
                    dependencies=field_sources
                )

                propagated_fields.append(union_field)

        # 缓存结果
        self.propagation_cache[cache_key] = propagated_fields

        return propagated_fields

    def build_field_mapping(self, source_field_id: str, target_field_id: str,
                           mapping_type: str = "DIRECT"):
        """
        构建字段映射关系

        Args:
            source_field_id: 源字段ID
            target_field_id: 目标字段ID
            mapping_type: 映射类型（DIRECT/TRANSFORMED/AGGREGATED）
        """
        if source_field_id not in self.field_mappings:
            self.field_mappings[source_field_id] = []

        self.field_mappings[source_field_id].append({
            'target': target_field_id,
            'type': mapping_type
        })

    def get_field_mapping_chain(self, field_id: str) -> List[str]:
        """
        获取字段的映射链路

        Args:
            field_id: 字段ID

        Returns:
            字段ID链路（从源头到当前字段）
        """
        chain = []

        # 递归查找源字段
        current_id = field_id
        while current_id:
            chain.append(current_id)

            # 查找当前字段的源
            found_source = False
            for source_id, mappings in self.field_mappings.items():
                for mapping in mappings:
                    if mapping['target'] == current_id:
                        current_id = source_id
                        found_source = True
                        break
                if found_source:
                    break

            if not found_source:
                break

        return chain[::-1]  # 反转，从源头到当前

    def get_cross_node_field_dependencies(self) -> Dict[str, List[str]]:
        """
        获取跨节点字段依赖关系

        Returns:
            节点ID到依赖字段ID列表的映射
        """
        dependencies = defaultdict(list)

        for source_field_id, mappings in self.field_mappings.items():
            for mapping in mappings:
                target_field_id = mapping['target']

                # 提取节点ID（假设字段ID格式为 node_id_FLD_...）
                source_node = '_'.join(source_field_id.split('_')[:-2])
                target_node = '_'.join(target_field_id.split('_')[:-2])

                if source_node != target_node:
                    dependencies[target_node].append(source_field_id)

        return dict(dependencies)

    def clear_cache(self):
        """清空传播缓存"""
        self.propagation_cache.clear()

    def get_statistics(self) -> Dict[str, int]:
        """
        获取传播引擎统计信息

        Returns:
            统计信息字典
        """
        return {
            'cached_propagations': len(self.propagation_cache),
            'field_mappings': len(self.field_mappings),
            'total_mappings': sum(len(mappings) for mappings in self.field_mappings.values())
        }

    def __repr__(self) -> str:
        """字符串表示"""
        return (f"FieldPropagationEngine(cached={len(self.propagation_cache)}, "
                f"mappings={len(self.field_mappings)})")
