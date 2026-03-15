#!/usr/bin/env python3
"""
字段作用域管理系统
实现面向对象的字段作用域层次结构
"""

from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class FieldInfo:
    """字段信息类"""
    field_id: str                          # 字段唯一ID
    field_name: str                        # 字段名/别名
    source_node_id: str                    # 来源节点ID
    source_table: str                      # 来源表名
    column_name: str                       # 基础列名
    field_type: str                        # 字段类型（COLUMN/FUNCTION/CASE等）
    transformation: dict = field(default_factory=dict)  # 转换规则
    propagation_path: List[str] = field(default_factory=list)  # 字段传播路径
    is_inherited: bool = False             # 是否为继承字段
    inheritance_depth: int = 0             # 继承深度
    dependencies: List[str] = field(default_factory=list)  # 依赖的其他字段ID

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'field_id': self.field_id,
            'field_name': self.field_name,
            'source_node_id': self.source_node_id,
            'source_table': self.source_table,
            'column_name': self.column_name,
            'field_type': self.field_type,
            'transformation': self.transformation,
            'propagation_path': self.propagation_path,
            'is_inherited': self.is_inherited,
            'inheritance_depth': self.inheritance_depth,
            'dependencies': self.dependencies
        }


@dataclass
class AliasInfo:
    """别名信息类"""
    alias: str                             # 别名
    target: str                            # 实际表名/CTE名/子查询ID
    alias_type: str                        # 别名类型（TABLE/CTE/SUBQUERY）
    propagation_source: Optional[str] = None  # 别名传播来源
    scope_id: str = ""                     # 作用域ID

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'alias': self.alias,
            'target': self.target,
            'alias_type': self.alias_type,
            'propagation_source': self.propagation_source,
            'scope_id': self.scope_id
        }


class FieldScope:
    """
    字段作用域类
    管理SQL节点内定义的字段、表别名和可见性
    """

    def __init__(self, scope_id: str, parent_scope: Optional['FieldScope'] = None):
        """
        初始化字段作用域

        Args:
            scope_id: 作用域ID（对应节点ID）
            parent_scope: 父作用域引用
        """
        self.scope_id = scope_id
        self.parent_scope = parent_scope

        # 本作用域定义的字段 (field_name -> FieldInfo)
        self.fields: Dict[str, FieldInfo] = {}

        # 表别名映射 (alias -> AliasInfo)
        self.table_aliases: Dict[str, AliasInfo] = {}

        # 可见字段名到字段ID的映射 (包括继承的)
        self.visible_fields: Dict[str, str] = {}

        # 子作用域列表
        self.child_scopes: List['FieldScope'] = []

        # 如果有父作用域，将自己添加为子作用域
        if parent_scope:
            parent_scope.add_child_scope(self)

    def add_field(self, field_info: FieldInfo):
        """
        添加本作用域定义的字段

        Args:
            field_info: 字段信息
        """
        self.fields[field_info.field_name] = field_info
        self.visible_fields[field_info.field_name] = field_info.field_id

    def add_qualified_field(self, table_name: str, field_info: FieldInfo):
        """
        添加带表前缀的字段

        Args:
            table_name: 表名
            field_info: 字段信息
        """
        qualified_name = f"{table_name}.{field_info.field_name}"
        self.fields[qualified_name] = field_info
        self.visible_fields[qualified_name] = field_info.field_id

    def add_table_alias(self, alias_info: AliasInfo):
        """
        添加表别名

        Args:
            alias_info: 别名信息
        """
        self.table_aliases[alias_info.alias] = alias_info
        alias_info.scope_id = self.scope_id

    def add_child_scope(self, child_scope: 'FieldScope'):
        """
        添加子作用域

        Args:
            child_scope: 子作用域
        """
        if child_scope not in self.child_scopes:
            self.child_scopes.append(child_scope)

    def resolve_field(self, field_name: str, table_prefix: str = "") -> Optional[FieldInfo]:
        """
        解析字段引用（支持继承链查找）

        Args:
            field_name: 字段名
            table_prefix: 表前缀

        Returns:
            字段信息，如果找不到返回None
        """
        # 构建完整的字段名
        full_name = f"{table_prefix}.{field_name}" if table_prefix else field_name

        # 1. 首先在本作用域中查找
        if full_name in self.fields:
            return self.fields[full_name]

        if field_name in self.fields:
            return self.fields[field_name]

        # 2. 如果本作用域找不到，向父作用域查找（继承机制）
        if self.parent_scope:
            parent_field = self.parent_scope.resolve_field(field_name, table_prefix)
            if parent_field:
                # 标记为继承字段
                inherited_field = FieldInfo(
                    field_id=parent_field.field_id,
                    field_name=parent_field.field_name,
                    source_node_id=parent_field.source_node_id,
                    source_table=parent_field.source_table,
                    column_name=parent_field.column_name,
                    field_type=parent_field.field_type,
                    transformation=parent_field.transformation,
                    propagation_path=parent_field.propagation_path.copy(),
                    is_inherited=True,
                    inheritance_depth=parent_field.inheritance_depth + 1,
                    dependencies=parent_field.dependencies.copy()
                )
                return inherited_field

        return None

    def resolve_table_alias(self, alias: str) -> Optional[AliasInfo]:
        """
        解析表别名（支持继承链查找）

        Args:
            alias: 表别名

        Returns:
            别名信息，如果找不到返回None
        """
        # 1. 首先在本作用域中查找
        if alias in self.table_aliases:
            return self.table_aliases[alias]

        # 2. 如果本作用域找不到，向父作用域查找
        if self.parent_scope:
            return self.parent_scope.resolve_table_alias(alias)

        return None

    def get_all_visible_fields(self) -> Dict[str, str]:
        """
        获取所有可见字段（包括继承的）

        Returns:
            字段名到字段ID的映射
        """
        visible = {}

        # 添加父作用域的字段（继承的字段）
        if self.parent_scope:
            visible.update(self.parent_scope.get_all_visible_fields())

        # 添加本作用域的字段（覆盖父作用域的同名字段）
        visible.update(self.visible_fields)

        return visible

    def get_all_aliases(self) -> Dict[str, AliasInfo]:
        """
        获取所有可见的表别名（包括继承的）

        Returns:
            别名到别名信息的映射
        """
        aliases = {}

        # 添加父作用域的别名
        if self.parent_scope:
            aliases.update(self.parent_scope.get_all_aliases())

        # 添加本作用域的别名（覆盖父作用域的同名别名）
        aliases.update(self.table_aliases)

        return aliases

    def get_field_info(self, field_id: str) -> Optional[FieldInfo]:
        """
        根据字段ID获取字段信息（在作用域链中查找）

        Args:
            field_id: 字段ID

        Returns:
            字段信息，如果找不到返回None
        """
        # 在本作用域中查找
        for field_info in self.fields.values():
            if field_info.field_id == field_id:
                return field_info

        # 在子作用域中查找
        for child_scope in self.child_scopes:
            field_info = child_scope.get_field_info(field_id)
            if field_info:
                return field_info

        return None

    def get_scope_chain(self) -> List[str]:
        """
        获取作用域链（从根到当前作用域）

        Returns:
            作用域ID列表
        """
        chain = []

        current = self
        while current:
            chain.append(current.scope_id)
            current = current.parent_scope

        return chain[::-1]  # 反转，从根到当前

    def get_depth(self) -> int:
        """
        获取作用域深度

        Returns:
            深度值（根作用域为0）
        """
        depth = 0
        current = self.parent_scope
        while current:
            depth += 1
            current = current.parent_scope
        return depth

    def __repr__(self) -> str:
        """字符串表示"""
        return f"FieldScope(id={self.scope_id}, fields={len(self.fields)}, aliases={len(self.table_aliases)})"


class ScopeManager:
    """
    作用域管理器
    管理整个SQL解析过程中的所有作用域
    """

    def __init__(self):
        """初始化作用域管理器"""
        self.scopes: Dict[str, FieldScope] = {}  # 作用域ID到作用域的映射
        self.root_scope: Optional[FieldScope] = None

    def create_scope(self, scope_id: str, parent_scope_id: Optional[str] = None) -> FieldScope:
        """
        创建新作用域

        Args:
            scope_id: 作用域ID
            parent_scope_id: 父作用域ID

        Returns:
            创建的作用域对象
        """
        parent_scope = self.scopes.get(parent_scope_id) if parent_scope_id else None

        new_scope = FieldScope(scope_id, parent_scope)
        self.scopes[scope_id] = new_scope

        # 如果没有父作用域，设置为根作用域
        if not parent_scope and not self.root_scope:
            self.root_scope = new_scope

        return new_scope

    def get_scope(self, scope_id: str) -> Optional[FieldScope]:
        """
        获取作用域

        Args:
            scope_id: 作用域ID

        Returns:
            作用域对象，如果不存在返回None
        """
        return self.scopes.get(scope_id)

    def get_root_scope(self) -> Optional[FieldScope]:
        """
        获取根作用域

        Returns:
            根作用域对象
        """
        return self.root_scope

    def get_all_scopes(self) -> List[FieldScope]:
        """
        获取所有作用域

        Returns:
            作用域列表
        """
        return list(self.scopes.values())

    def get_scope_hierarchy(self) -> Dict[str, List[str]]:
        """
        获取作用域层次结构

        Returns:
            父作用域ID到子作用域ID列表的映射
        """
        hierarchy = defaultdict(list)

        for scope_id, scope in self.scopes.items():
            if scope.parent_scope:
                hierarchy[scope.parent_scope.scope_id].append(scope_id)

        return dict(hierarchy)

    def clear(self):
        """清空所有作用域"""
        self.scopes.clear()
        self.root_scope = None
