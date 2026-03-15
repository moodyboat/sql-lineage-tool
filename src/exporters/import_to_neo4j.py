#!/usr/bin/env python3
"""
SQL节点导入Neo4j工具
将SQL解析结果导入到Neo4j图数据库中
"""

from neo4j import GraphDatabase
import json
from typing import Dict, List
import sys
import os

# 添加项目根目录到路径
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, project_root)

from src.parsers.sql_node_parser_v2 import SQLNodeParser


class Neo4jImporter:
    """Neo4j导入器"""

    def __init__(self, uri: str = "bolt://localhost:7687",
                 user: str = "neo4j",
                 password: str = "password"):
        """
        初始化Neo4j连接

        Args:
            uri: Neo4j连接URI，默认 bolt://localhost:7687
            user: 用户名，默认 neo4j
            password: 密码，默认 password
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.node_count = 0
        self.rel_count = 0

    def close(self):
        """关闭连接"""
        self.driver.close()

    def clear_database(self):
        """清空数据库（谨慎使用！）"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("✓ 数据库已清空")

    def create_constraints(self):
        """创建索引和约束"""
        with self.driver.session() as session:
            # 创建唯一性约束
            constraints = [
                "CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT table_name_unique IF NOT EXISTS FOR (n:Table) REQUIRE n.name IS UNIQUE",
                "CREATE CONSTRAINT field_id_unique IF NOT EXISTS FOR (f:Field) REQUIRE f.id IS UNIQUE",
            ]

            for constraint in constraints:
                try:
                    session.run(constraint)
                    print(f"✓ 创建约束: {constraint}")
                except Exception as e:
                    print(f"⚠ 约束已存在或创建失败: {e}")

    def import_sql_parser_results(self, nodes: Dict, relationships: List,
                                   fields: Dict = None, field_relationships: List = None):
        """
        导入SQL解析结果到Neo4j

        Args:
            nodes: 节点字典
            relationships: 关系列表
            fields: 字段字典（可选）
            field_relationships: 字段关系列表（可选）
        """
        print(f"\n开始导入...")

        with self.driver.session() as session:
            # 导入节点
            self._import_nodes(session, nodes)

            # 导入字段
            if fields:
                self._import_fields(session, fields)

            # 导入关系
            self._import_relationships(session, relationships)

            # 导入字段关系
            if field_relationships:
                self._import_field_relationships(session, field_relationships)

        print(f"\n✓ 导入完成！")
        print(f"  节点数: {self.node_count}")
        print(f"  关系数: {self.rel_count}")
        if hasattr(self, 'field_count'):
            print(f"  字段数: {self.field_count}")
        if hasattr(self, 'field_rel_count'):
            print(f"  字段关系数: {self.field_rel_count}")

    def _import_nodes(self, session, nodes: Dict):
        """导入节点"""
        print("\n导入节点...")

        # 节点类型映射
        type_labels = {
            "ROOT": "RootNode",
            "CT": "CTE",
            "BLK": "QueryBlock",
            "DT": "DerivedTable",
            "SQ": "ScalarQuery",
            "WQ": "WhereQuery",
            "IQ": "InQuery",
            "TB": "Table",
            "VW": "View",
            "UNION": "UnionNode"
        }

        for node_id, node in nodes.items():
            label = type_labels.get(node.type, "Node")

            # 构建Cypher查询
            cypher = """
            CREATE (n:{label} {{
                id: $id,
                type: $type,
                name: $name,
                sql: $sql,
                parent_id: $parent_id,
                depth: $depth,
                alias: $alias
            }})
            """.format(label=label)

            try:
                # 截断过长的SQL（Neo4j有限制）
                sql = node.sql
                if len(sql) > 10000:
                    sql = sql[:10000] + "..."

                session.run(cypher, {
                    "id": node.id,
                    "type": node.type,
                    "name": node.name,
                    "sql": sql,
                    "parent_id": node.parent_id,
                    "depth": node.depth,
                    "alias": node.alias
                })
                self.node_count += 1

                if self.node_count % 10 == 0:
                    print(f"  已导入 {self.node_count}/{len(nodes)} 个节点")

            except Exception as e:
                print(f"✗ 导入节点失败 {node.id}: {e}")

    def _import_relationships(self, session, relationships: List):
        """导入关系"""
        print("\n导入关系...")

        # 关系类型映射
        rel_types = {
            "CONTAINS": "CONTAINS",
            "REFERENCES": "REFERENCES"
        }

        for rel in relationships:
            rel_type = rel_types.get(rel.type, rel.type)

            cypher = f"""
            MATCH (source {{id: $source_id}})
            MATCH (target {{id: $target_id}})
            CREATE (source)-[r:{rel_type}]->(target)
            SET r.type = $rel_type
            """

            try:
                session.run(cypher, {
                    "source_id": rel.source_id,
                    "target_id": rel.target_id,
                    "rel_type": rel.type
                })
                self.rel_count += 1

                if self.rel_count % 10 == 0:
                    print(f"  已导入 {self.rel_count}/{len(relationships)} 个关系")

            except Exception as e:
                print(f"✗ 导入关系失败 {rel.source_id}->{rel.target_id}: {e}")

    def _import_fields(self, session, fields: List):
        """导入字段（增强版）"""
        print("\n导入字段...")
        self.field_count = 0

        for field in fields:
            # 处理Field对象或字典
            if hasattr(field, 'to_dict'):
                field_data = field.to_dict()
            else:
                field_data = field

            # 将复杂对象转换为JSON字符串
            import json
            transformation = field_data.get('transformation', {})
            if isinstance(transformation, dict):
                transformation_str = json.dumps(transformation, ensure_ascii=False)
            else:
                transformation_str = str(transformation)

            dependencies = field_data.get('dependencies', [])
            if isinstance(dependencies, list):
                dependencies_str = json.dumps(dependencies, ensure_ascii=False)
            else:
                dependencies_str = str(dependencies)

            parent_node_id = field_data.get('parent_node_id', '')

            # 新增：增强的元数据字段
            metadata = field_data.get('metadata', {})

            # 提取作用域系统相关信息
            propagation_path = metadata.get('propagation_path', [])
            if isinstance(propagation_path, list):
                propagation_path_str = json.dumps(propagation_path, ensure_ascii=False)
            else:
                propagation_path_str = str(propagation_path)

            is_inherited = metadata.get('is_inherited', False)
            inheritance_depth = metadata.get('inheritance_depth', 0)

            # 构建增强的Cypher查询
            cypher = """
            CREATE (f:Field {
                id: $id,
                name: $name,
                table_name: $table_name,
                column_name: $column_name,
                field_type: $field_type,
                transformation: $transformation,
                dependencies: $dependencies,
                parent_node_id: $parent_node_id,
                propagation_path: $propagation_path,
                is_inherited: $is_inherited,
                inheritance_depth: $inheritance_depth,
                scope_system: $scope_system
            })
            """

            try:
                session.run(cypher, {
                    "id": field_data.get('id'),
                    "name": field_data.get('name'),
                    "table_name": field_data.get('table_name', ''),
                    "column_name": field_data.get('column_name', ''),
                    "field_type": field_data.get('field_type', 'UNKNOWN'),
                    "transformation": transformation_str,
                    "dependencies": dependencies_str,
                    "parent_node_id": parent_node_id,
                    "propagation_path": propagation_path_str,
                    "is_inherited": is_inherited,
                    "inheritance_depth": inheritance_depth,
                    "scope_system": metadata.get('scope_system', False)
                })
                self.field_count += 1

                # 创建字段与所属节点的HAS_FIELD关系
                if parent_node_id:
                    self._create_has_field_relationship(session, field_data.get('id'), parent_node_id)

                if self.field_count % 50 == 0:
                    print(f"  已导入 {self.field_count}/{len(fields)} 个字段")

            except Exception as e:
                print(f"✗ 导入字段失败 {field_data.get('id', 'unknown')}: {e}")

    def _create_has_field_relationship(self, session, field_id: str, parent_node_id: str):
        """创建字段与所属节点的HAS_FIELD关系"""
        cypher = """
        MATCH (f:Field {id: $field_id})
        MATCH (n {id: $parent_node_id})
        CREATE (n)-[:HAS_FIELD]->(f)
        """

        try:
            session.run(cypher, {
                "field_id": field_id,
                "parent_node_id": parent_node_id
            })
        except Exception as e:
            # 静默失败，避免大量错误信息
            pass

    def _import_field_relationships(self, session, field_relationships: List):
        """导入字段关系（增强版）"""
        print("\n导入字段关系...")
        self.field_rel_count = 0

        for field_rel in field_relationships:
            # 处理FieldRelationship对象或字典
            if hasattr(field_rel, 'to_dict'):
                rel_data = field_rel.to_dict()
            else:
                rel_data = field_rel

            rel_type = rel_data['type']
            metadata = rel_data.get('metadata', {})

            # 新增：增强的关系元数据
            cross_node = metadata.get('cross_node', False)
            scope_system = metadata.get('scope_system', False)
            propagation_type = metadata.get('propagation_type', 'unknown')

            cypher = f"""
            MATCH (source:Field {{id: $source_id}})
            MATCH (target:Field {{id: $target_id}})
            CREATE (source)-[r:{rel_type}]->(target)
            SET r.type = $rel_type,
                r.cross_node = $cross_node,
                r.scope_system = $scope_system,
                r.propagation_type = $propagation_type
            """

            try:
                session.run(cypher, {
                    "source_id": rel_data['source_id'],
                    "target_id": rel_data['target_id'],
                    "rel_type": rel_type,
                    "cross_node": cross_node,
                    "scope_system": scope_system,
                    "propagation_type": propagation_type
                })
                self.field_rel_count += 1

                if self.field_rel_count % 50 == 0:
                    print(f"  已导入 {self.field_rel_count}/{len(field_relationships)} 个字段关系")

            except Exception as e:
                print(f"✗ 导入字段关系失败 {rel_data['source_id']}->{rel_data['target_id']}: {e}")

    def create_sample_queries(self):
        """创建示例查询"""
        queries = {
            "查看所有节点": """
MATCH (n:Node)
RETURN n
LIMIT 25
            """,

            "查看节点树结构": """
MATCH path = (root:RootNode)-[:CONTAINS*]->(leaf:Node)
WHERE root.id = 'ROOT'
RETURN path
LIMIT 10
            """,

            "查找所有CTE": """
MATCH (cte:CTE)
RETURN cte.id, cte.name, cte.sql
            """,

            "查找所有标量子查询": """
MATCH (sq:ScalarQuery)
RETURN sq.id, sq.sql
ORDER BY sq.depth DESC
            """,

            "查找所有物理表": """
MATCH (tb:Table)
RETURN tb.name, tb.sql
ORDER BY tb.name
            """,

            "查看表的引用关系": """
MATCH (t:Table)<-[:REFERENCES]-(n)
RETURN t.name as 表名,
       collect(DISTINCT n.type) as 被引用类型,
       count(n) as 引用次数
ORDER BY 引用次数 DESC
            """,

            "查看深度嵌套": """
MATCH (n:Node)
WHERE n.depth > 2
RETURN n.id, n.type, n.name, n.depth
ORDER BY n.depth DESC
            """,

            "查看节点依赖": """
MATCH (n:Node)-[:REFERENCES]->(dep:Node)
RETURN n.id as 节点,
       collect(DISTINCT dep.type + ':' + dep.name) as 依赖
ORDER BY size(依赖) DESC
            """,

            "查找孤立节点": """
MATCH (n:Node)
WHERE NOT (n)-[:CONTAINS]-()
  AND NOT (n)-[:REFERENCES]-()
  AND n.type <> 'RootNode'
RETURN n.id, n.type, n.name
            """,

            "统计节点类型": """
MATCH (n:Node)
RETURN n.type as 节点类型, count(*) as 数量
ORDER BY 数量 DESC
            """,

            "最长依赖链": """
MATCH path = (start:Node)-[:REFERENCES*]->(end:Node)
RETURN path, length(path) as 链长度
ORDER BY 链长度 DESC
LIMIT 1
            """,

            # 字段级血缘查询
            "查看所有字段": """
MATCH (f:Field)
RETURN f.id, f.name, f.field_type, f.column_name
LIMIT 25
            """,

            "查看字段类型分布": """
MATCH (f:Field)
RETURN f.field_type as 字段类型, count(*) as 数量
ORDER BY 数量 DESC
            """,

            "查看字段转换链路": """
MATCH path = (source:Field)-[:DERIVES*]->(target:Field)
WHERE source.name = 'ZYZJ'
RETURN path
LIMIT 10
            """,

            "查看字段的完整血缘": """
MATCH (f:Field)
WHERE f.name = 'ZYZJ'
OPTIONAL MATCH (f)-[:DERIVES]->(dep:Field)
OPTIONAL MATCH (f)<-[:DERIVES]-(derived:Field)
RETURN f.id as 字段,
       collect(DISTINCT dep.id) as 依赖字段,
       collect(DISTINCT derived.id) as 衍生字段
            """,

            "查找函数转换字段": """
MATCH (f:Field)
WHERE f.field_type = 'FUNCTION'
RETURN f.name, f.transformation.function as 函数名
ORDER BY f.name
            """,

            "查看字段与节点的关系": """
MATCH (f:Field)
MATCH (n:Node {id: f.parent_node_id})
RETURN f.name as 字段名,
       n.type as 所属节点类型,
       n.id as 所属节点ID
ORDER BY f.name
LIMIT 25
            """,

            "查看字段与节点的关系": """
MATCH (f:Field)
MATCH (n:Node {id: f.parent_node_id})
RETURN f.name as 字段名,
       n.type as 所属节点类型,
       n.id as 所属节点ID
ORDER BY f.name
LIMIT 25
            """,

            "查看节点的字段列表": """
MATCH (n)-[:HAS_FIELD]->(f:Field)
RETURN n.id as 节点ID,
       labels(n)[0] as 节点类型,
       count(f) as 字段数量,
       collect(f.name)[0..3] as 示例字段
ORDER BY 字段数量 DESC
LIMIT 10
            """,

            "查看字段血缘链路（含节点上下文）": """
MATCH (f1:Field)-[:DERIVES]->(f2:Field)
MATCH (n1)-[:HAS_FIELD]->(f1)
MATCH (n2)-[:HAS_FIELD]->(f2)
RETURN n1.name + '的' + f1.name + ' -> ' + n2.name + '的' + f2.name as 血缘链路
LIMIT 20
            """,

            "查看完整数据结构（包含字段）": """
MATCH path = (root:RootNode)-[:CONTAINS*]->(n)-[:HAS_FIELD]->(f:Field)
WHERE root.id = 'ROOT'
RETURN path
LIMIT 25
            """,

            "查找孤立字段（无HAS_FIELD关系）": """
MATCH (f:Field)
WHERE NOT (f)-[:DERIVES]-()
RETURN f.id, f.name, f.field_type
            """,

            "查看字段血缘深度": """
MATCH path = (start:Field)-[:DERIVES*]->(end:Field)
WITH start, end, length(path) as depth
WHERE depth > 0
RETURN start.id as 起始字段,
       max(depth) as 最大血缘深度
ORDER BY 最大血缘深度 DESC
LIMIT 10
            """
        }

        return queries

    def run_sample_queries(self):
        """运行示例查询"""
        queries = self.create_sample_queries()

        print("\n" + "="*80)
        print("运行示例查询")
        print("="*80)

        with self.driver.session() as session:
            for name, cypher in queries.items():
                print(f"\n【{name}】")
                print(f"{cypher.strip()}")

                try:
                    result = session.run(cypher)
                    records = list(result)

                    if records:
                        print(f"结果: {len(records)} 条记录")
                        # 显示前3条记录
                        for record in records[:3]:
                            print(f"  {record}")
                        if len(records) > 3:
                            print(f"  ... 还有 {len(records)-3} 条")
                    else:
                        print("结果: 无数据")

                except Exception as e:
                    print(f"查询失败: {e}")

                print("-" * 80)


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='将SQL解析结果导入Neo4j')
    parser.add_argument('sql_file', help='SQL文件路径')
    parser.add_argument('--dialect', default='mysql', help='SQL方言（默认: mysql）')
    parser.add_argument('--uri', default='bolt://localhost:7687', help='Neo4j URI')
    parser.add_argument('--user', default='neo4j', help='Neo4j用户名')
    parser.add_argument('--password', default='password', help='Neo4j密码')
    parser.add_argument('--clear', action='store_true', help='导入前清空数据库')
    parser.add_argument('--no-queries', action='store_true', help='不运行示例查询')

    args = parser.parse_args()

    # 读取SQL文件
    try:
        with open(args.sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
    except FileNotFoundError:
        print(f"错误: 文件不存在 - {args.sql_file}")
        sys.exit(1)

    print("="*80)
    print("SQL节点解析器 - Neo4j导入工具")
    print("="*80)
    print(f"\nSQL文件: {args.sql_file}")
    print(f"SQL方言: {args.dialect}")
    print(f"Neo4j URI: {args.uri}")

    # 解析SQL
    print("\n" + "-"*80)
    print("步骤1: 解析SQL")
    print("-"*80)

    parser_sql = SQLNodeParser(sql_content, dialect=args.dialect)
    nodes, relationships = parser_sql.parse()

    print(f"\n✓ 解析完成")
    print(f"  节点数: {len(nodes)}")
    print(f"  关系数: {len(relationships)}")

    # 获取字段和字段关系
    if hasattr(parser_sql, 'fields'):
        # 确保字段依赖关系已构建
        if parser_sql.fields and not parser_sql.field_relationships:
            parser_sql._build_field_dependencies()

        fields = list(parser_sql.fields.values())
        field_relationships = parser_sql.field_relationships
    else:
        fields = None
        field_relationships = None

    if fields:
        print(f"  字段数: {len(fields)}")
    if field_relationships:
        print(f"  字段关系数: {len(field_relationships)}")

    # 导入Neo4j
    print("\n" + "-"*80)
    print("步骤2: 导入Neo4j")
    print("-"*80)

    try:
        importer = Neo4jImporter(
            uri=args.uri,
            user=args.user,
            password=args.password
        )

        # 清空数据库（可选）
        if args.clear:
            print("\n⚠ 警告: 即将清空数据库！")
            confirm = input("确认清空？(yes/no): ")
            if confirm.lower() == 'yes':
                importer.clear_database()
            else:
                print("已取消清空操作")

        # 创建约束
        print("\n创建索引和约束...")
        importer.create_constraints()

        # 导入数据
        importer.import_sql_parser_results(nodes, relationships, fields, field_relationships)

        # 运行示例查询
        if not args.no_queries:
            print("\n" + "-"*80)
            print("步骤3: 运行示例查询")
            print("-"*80)
            importer.run_sample_queries()

        importer.close()

        print("\n" + "="*80)
        print("导入完成！")
        print("="*80)
        print(f"\n💡 提示:")
        print(f"1. 打开 Neo4j Browser: http://localhost:7474")
        print(f"2. 运行以下查询查看数据:")
        print(f"   MATCH (n) RETURN n LIMIT 25")
        print(f"3. 查看完整树结构:")
        print(f"   MATCH path = (root:RootNode)-[:CONTAINS*]->(leaf)")
        print(f"   WHERE root.id = 'ROOT'")
        print(f"   RETURN path")

    except Exception as e:
        print(f"\n✗ 导入失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
