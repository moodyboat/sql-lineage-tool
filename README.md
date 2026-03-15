# SQL血缘分析工具

自动识别SQL脚本中的所有节点及其关系的工具，支持Neo4j可视化血缘分析。

## ✨ 核心功能

- **自动识别SQL节点**：CTE、子查询、派生表、物理表、UNION等
- **提取可执行SQL代码段**：每个节点都包含完整的可执行SQL
- **建立节点关系**：包含关系、引用关系、连接关系
- **Neo4j可视化**：一键导入图数据库进行血缘分析
- **支持多种方言**：Oracle、MySQL、PostgreSQL、SQL Server等

## 📦 识别的节点类型

| 节点类型 | 说明 | 示例 |
|---------|------|------|
| **ROOT** | SQL脚本根节点 | 整个SQL |
| **CTE** | CTE（公共表表达式） | `WITH tt AS (SELECT ...)` |
| **BLK** | 查询块（完整SELECT，UNION分支） | UNION ALL的各个分支 |
| **SQ** | 标量子查询 | `SELECT (SELECT MAX(...) FROM ...)` |
| **TB** | 物理表 | `FROM accounts` |
| **UNION** | UNION连接节点 | `UNION ALL` |

## 🔗 识别的关系类型

| 关系类型 | 说明 | 示例 |
|---------|------|------|
| **CONTAINS** | 包含关系 | CTE包含子查询 |
| **REFERENCES** | 引用关系 | 查询引用CTE或表 |

## 🚀 快速开始

### 方法1: 一键启动（推荐）

```bash
# 解析并导入到Neo4j
python3 import_to_neo4j.py 原始.sql --dialect oracle
```

### 方法2: 分步执行

```bash
# 1. 解析SQL文件
python3 sql_node_parser_v2.py 原始.sql oracle

# 2. 导入到Neo4j
python3 import_to_neo4j.py 原始.sql --dialect oracle

# 3. 打开Neo4j Browser查看结果
# http://localhost:7474
```

## 📊 在Neo4j Browser中查看

### 访问地址
**http://localhost:7474**
- 用户名: `neo4j`
- 密码: `password`

### 推荐查询

**查看完整SQL树结构**（必看！）
```cypher
MATCH path = (root:RootNode)-[:CONTAINS*]->(leaf)
WHERE root.id = 'ROOT'
RETURN path
```

**统计节点类型**
```cypher
MATCH (n)
RETURN n.type as 类型, count(*) as 数量
ORDER BY 数量 DESC
```

**查看CTE详情**
```cypher
MATCH (cte:CTE)
RETURN cte.name, cte.sql
```

**追踪表血缘关系**
```cypher
MATCH (t:Table)<-[:REFERENCES*]-(n)
RETURN t.name, collect(DISTINCT n.type)
```

**查找最深层的子查询**
```cypher
MATCH (sq:ScalarQuery)
WHERE sq.depth >= 3
RETURN sq.id, sq.sql, sq.depth
ORDER BY sq.depth DESC
```

更多查询请参考：[neo4j查询指南.md](neo4j查询指南.md)

## 📖 Python代码使用

### 基本用法

```python
from sql_node_parser_v2 import SQLNodeParser

# 读取SQL文件
with open('原始.sql', 'r', encoding='utf-8') as f:
    sql_content = f.read()

# 创建解析器
parser = SQLNodeParser(sql_content, dialect='oracle')

# 解析SQL
nodes, relationships = parser.parse()

# 打印统计摘要
parser.print_summary()

# 导出JSON
parser.export_json("output.json")
```

### 高级用法

```python
# 遍历节点树
def print_tree(node_id, level=0):
    node = parser.get_node_by_id(node_id)
    indent = "  " * level
    print(f"{indent}├─ {node.type}_{node.name}")
    for child in parser.get_children(node_id):
        print_tree(child.id, level + 1)

print_tree("ROOT")

# 获取特定类型的节点
ctes = [node for node in parser.nodes.values() if node.type == "CT"]
print(f"找到 {len(ctes)} 个CTE")

# 分析依赖关系
for node in parser.nodes.values():
    if node.type in ["CT", "BLK"]:
        deps = parser.get_dependencies(node)
        if deps:
            print(f"{node.name} 依赖: {[d.name for d in deps]}")
```

## 📁 项目结构

```
.
├── sql_node_parser_v2.py    # 主解析器
├── sql_preprocessor.py       # SQL预处理器
├── import_to_neo4j.py        # Neo4j导入工具
├── quick_start.sh            # 快速启动脚本
├── README.md                 # 本文档
├── neo4j查询指南.md          # Neo4j查询参考
├── 原始.sql                  # 示例SQL文件
└── 原始_sql_nodes.json       # 解析结果示例
```

## 🔧 安装依赖

```bash
pip install sqlglot neo4j
```

或使用requirements.txt：
```bash
pip install -r requirements.txt
```

## 📈 实际应用场景

### 1. SQL血缘分析
```python
# 追踪表的使用关系
table_id = "TB_dw_xd_corp_loan_stdbook"
dependents = parser.get_dependents(table_id)
```

### 2. SQL性能评估
```python
# 识别复杂SQL
deep_nodes = [n for n in parser.nodes.values() if n.depth > 5]
scalar_queries = [n for n in parser.nodes.values() if n.type == "SQ"]

print(f"复杂度: {len(deep_nodes)} 个深层节点")
print(f"标量查询: {len(scalar_queries)} 个")
```

### 3. 数据迁移支持
```python
# 提取所有表依赖
tables = [n for n in parser.nodes.values() if n.type == "TB"]
for table in tables:
    print(f"需要迁移: {table.name}")
```

## 🎯 验证结果

已验证SQL类型：
- ✅ Oracle SQL（含DECODE、TO_CHAR、NVL等特殊函数）
- ✅ MySQL SQL
- ✅ PostgreSQL SQL
- ✅ 复杂CTE嵌套
- ✅ 多层UNION ALL
- ✅ 深层子查询（最深7层）

## ⚙️ 支持的SQL方言

- Oracle
- MySQL
- PostgreSQL
- SQL Server
- Hive
- Spark SQL
- Redshift
- Snowflake

## 📝 命令行参数

```bash
# 解析SQL文件
python3 sql_node_parser_v2.py <sql文件> <方言>

# 导入到Neo4j
python3 import_to_neo4j.py <sql文件> --dialect <方言>

# 完整参数
python3 import_to_neo4j.py <sql文件> \
  --dialect mysql \
  --uri bolt://localhost:7687 \
  --user neo4j \
  --password your_password
```

## 🔍 可视化技巧

在Neo4j Browser中：
1. 使用 **Hierarchical** 布局查看树形结构
2. 设置节点颜色：
   - CTE: 蓝色 (`#3498db`)
   - Table: 绿色 (`#2ecc71`)
   - ScalarQuery: 红色 (`#e74c3c`)
   - QueryBlock: 黄色 (`#f1c40f`)
3. 点击节点查看详细信息（包含完整SQL）
4. 导出图形为PNG/SVG格式

## 🤝 贡献

欢迎提交Issue和Pull Request！

## 📄 许可证

MIT License

## 🔗 相关资源

- [sqlglot文档](https://github.com/tobymao/sqlglot)
- [Neo4j官方文档](https://neo4j.com/docs/)
- [neo4j查询指南.md](neo4j查询指南.md)
