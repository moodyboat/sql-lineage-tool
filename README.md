# SQL字段血缘分析系统

**版本**: 3.1 (元数据增强版)
**状态**: 生产就绪 ✅
**基于**: sqlglot AST解析

## 🎯 系统概述

SQL字段血缘分析系统，基于sqlglot AST解析技术，实现精确的字段依赖追踪。支持复杂SQL结构（CTE、子查询、JOIN、UNION、算术表达式），可导出到Neo4j图数据库进行可视化分析。

### 核心特性

- ✅ **高准确率**: 85.2%任务达到100%字段溯源率
- ✅ **复杂SQL支持**: CTE、JOIN、子查询、UNION、算术表达式
- ✅ **相邻引用关系**: 严格遵循SQL作用域规则
- ✅ **大小写不敏感**: 解决字段名大小写不一致问题
- ✅ **Neo4j集成**: 一键导出图数据库可视化
- ✅ **生产验证**: 27个真实SQL任务验证

### 性能指标

| 指标 | 数值 |
|------|------|
| 完美溯源率 | 85.2% (23/27任务) |
| 最复杂SQL | 684节点, 2520字段, 100%溯源 ✅ |
| 支持字段类型 | COLUMN, FUNCTION, CASE, AGGREGATION, ARITHMETIC, SCALAR_QUERY, UNION, LITERAL |
| 支持节点类型 | TB, VW, CT (CTE), SQ (Subquery), BLK, UNION, ROOT |

## 🚀 快速开始

### 安装依赖

```bash
pip install -r requirements.txt
```

依赖包：
- `sqlglot>=25.0.0` - SQL解析和AST
- `neo4j>=5.0.0` - Neo4j图数据库

### 基本使用

```bash
# 分析SQL文件
python main.py data/sql/01_xxx/原始.sql

# 批量分析所有SQL文件
python batch_analyze.py

# 导出JSON
python main.py data/sql/01_xxx/原始.sql --output result.json

# 导出到Neo4j
python main.py data/sql/01_xxx/原始.sql --export-neo4j

# 禁用元数据增强
python main.py data/sql/01_xxx/原始.sql --no-metadata

# 禁用作用域系统
python main.py data/sql/01_xxx/原始.sql --no-scope
```

### 查看分析结果

```bash
# 汇总报告
cat outputs/lineage/汇总报告.csv

# 某个任务的血缘分析
cat outputs/lineage/01_xxx/字段血缘.csv
```

### Python API

```python
from src.parsers.sql_node_parser_v2 import SQLNodeParser

# 解析SQL
parser = SQLNodeParser(sql_content, dialect='oracle')
nodes, relationships = parser.parse()

# 构建字段依赖
parser._build_cross_node_field_mappings()

# 查看结果
print(f"节点数: {len(nodes)}")
print(f"字段数: {len(parser.fields)}")
print(f"字段关系数: {len(parser.field_relationships)}")

# 验证字段血缘
traceability = parser.verify_field_lineage()
print(f"字段溯源率: {traceability*100:.1f}%")
```

### 增强版分析器（带元数据）

```python
from src.analyzers.enhanced_field_lineage import EnhancedFieldLineageAnalyzer

# 创建分析器
analyzer = EnhancedFieldLineageAnalyzer(
    metadata_files=[
        "metadata/大数据ods的实例库表字段.csv",
        "metadata/大数据dw和dm的实例库表字段.csv"
    ]
)

# 分析SQL
result = analyzer.analyze_sql(sql_content, dialect='oracle')

# 查看结果
parser = result['parser']
fields = result['fields']
field_relationships = result['field_relationships']
```

## 📁 项目结构

```
血缘分析工具/
├── data/                    # 数据目录
│   └── sql/                 # 原始SQL文件
│       ├── 01_xxx/原始.sql
│       └── ...
│
├── outputs/                 # 输出结果目录
│   └── lineage/            # 血缘分析结果
│       ├── 汇总报告.csv
│       ├── 01_xxx/
│       │   ├── 字段血缘.csv
│       │   └── 表关联关系.csv
│       └── ...
│
├── src/                     # 核心代码
│   ├── parsers/
│   │   └── sql_node_parser_v2.py           # SQL解析器 (基于sqlglot AST)
│   ├── analyzers/
│   │   └── enhanced_field_lineage.py       # 增强字段血缘分析器
│   ├── exporters/
│   │   └── import_to_neo4j.py              # Neo4j导出器
│   ├── metadata/
│   │   └── metadata_manager.py             # 元数据管理器
│   └── core/                               # 作用域系统 (可选)
│       ├── field_scope.py
│       ├── alias_manager.py
│       └── field_propagation.py
│
├── examples/
│   └── demo_quick_start.py                 # 快速开始示例
│
├── tests/                                   # 单元测试 & 集成测试
│   ├── unit/test_field_lineage_v3.py
│   └── integration/test_real_sql_enhanced.py
│
├── metadata/                                # 元数据文件
│   ├── 大数据ods的实例库表字段.csv
│   └── 大数据dw和dm的实例库表字段.csv
│
├── main.py                                  # 统一入口点
├── batch_analyze.py                        # 批量分析工具
├── trace_field_lineage.py                  # 字段血缘追踪
├── extract_table_joins.py                  # 表关联提取
│
├── README.md                                # 本文件
├── CLAUDE.md                                # Claude Code指南
└── DIRECTORY_STRUCTURE.md                   # 目录结构详细说明
```

## 🔧 核心架构

### 3层血缘链路

```
Physical Table (物理表)
    ↓ PROVIDES
Subquery/CTE (中间层)
    ↓ DERIVES
Outer Query (外层查询)
```

### 字段类型

| 类型 | 说明 | 示例 |
|------|------|------|
| COLUMN | 简单列引用 | `table.column` |
| FUNCTION | 函数调用 | `TO_CHAR(date, 'YYYYMM')` |
| CASE | CASE表达式 | `CASE WHEN x > 0 THEN 1 ELSE 0 END` |
| AGGREGATION | 聚合函数 | `SUM(amount), COUNT(*)` |
| ARITHMETIC | 算术表达式 | `ZY_FK * 10000` |
| SCALAR_QUERY | 标量子查询 | `(SELECT MAX(val) FROM t)` |
| UNION | UNION字段 | `UNION ALL` 结果 |
| LITERAL | 字面量 | `'担保人' AS 担保人` |

### 关系类型

- **REFERENCES** - CTE引用关系
- **CONTAINS** - 父子包含关系（子查询）
- **PROVIDES** - 表到节点的字段提供
- **DERIVES** - 字段派生关系

### 相邻引用关系原则

**核心原则**: 只追踪父子节点间的字段依赖，不跨层追溯

字段查找范围：
1. 当前节点的字段
2. 直接引用的CTE/子查询
3. 直接包含的子查询

## 📊 Neo4j集成

### 导出到Neo4j

```bash
python main.py data/sql/01_xxx/原始.sql --export-neo4j \
  --uri bolt://localhost:7687 \
  --user neo4j \
  --password password
```

### Neo4j图数据库Schema

**节点类型**:
- `:Node` - SQL语句节点
- `:Table` - 物理表节点
- `:Field` - 字段节点

**关系类型**:
- `:REFERENCES` - 引用关系
- `:CONTAINS` - 包含关系
- `:PROVIDES` - 提供关系
- `:DERIVES` - 派生关系

### Neo4j查询示例

```cypher
-- 查看字段血缘链路
MATCH (f1:Field)-[:DERIVES*]->(f2:Field)
WHERE f1.name = 'field_name'
RETURN f1, f2

-- 查看表的字段
MATCH (t:Table)<-[:REFERENCES]-(n)-[:HAS_FIELD]->(f:Field)
WHERE t.name = 'table_name'
RETURN t.name, collect(f.name)

-- 统计字段溯源率
MATCH (f:Field)
WITH count(f) as total
MATCH (f:Field)-[:DERIVES]->()
RETURN total, count(f) as traced, round(100.0*count(f)/total, 1) as traceability_rate
```

## 🔍 验证结果

### 27个SQL任务验证

| 分类 | 数量 | 百分比 |
|------|------|--------|
| 完美溯源 (100%) | 23个 | 85.2% |
| 部分溯源 (80-99%) | 2个 | 7.4% |
| 解析错误 | 2个 | 7.4% |

### 最复杂任务

**24_信贷业务办理情况表**
- 684个节点
- 2520个字段
- **100%完美溯源** ✅

### 部分溯源任务（正常情况）

1. **17_承兑手续费 (92.9%)**
   - 未溯源字段：`担保人` (LITERAL类型)
   - 原因：字面量字段 `'担保人' AS 担保人`

2. **03_内部对账余额表-票据类 (85.7%)**
   - 未溯源字段：`JSPZFL_BZ` (LITERAL类型)
   - 原因：字面量字段

### 解析错误任务

1. **05_存款计提测算-期末余额**
   - 错误：SQL语法错误 `) cd`

2. **23_贷款发生明细台账**
   - 错误：`object of type 'NoneType' has no len()`

## 🐛 常见问题

### Q: 为什么有些字段无法溯源？

**A**: 正常情况：
- LITERAL字段（字面量）没有数据源
- SQL语法错误导致解析失败

**解决方法**：
- 检查SQL语法是否正确
- 确认字段是否为字面量
- 使用 `--no-scope` 参数尝试

### Q: 如何提高字段溯源率？

**A**: 优化建议：
- 确保SQL语法正确
- 使用明确的表前缀（如 `table.column`）
- 启用元数据增强（需要元数据文件）
- 保持字段名大小写一致

### Q: 性能如何优化？

**A**: 优化方法：
- 使用 `--no-scope` 禁用作用域系统
- 使用 `--no-metadata` 禁用元数据增强
- 分批处理大型SQL文件

## 📚 技术栈

- **SQL解析**: sqlglot 25.0+
- **图数据库**: Neo4j 5.0+
- **Python**: 3.7+

## 📄 许可证

本项目用于内部数据血缘分析。

---

**版本**: 3.1 (元数据增强版)
**状态**: ✅ 生产就绪
**最后更新**: 2026年3月16日
