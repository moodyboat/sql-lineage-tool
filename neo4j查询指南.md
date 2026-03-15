# Neo4j可视化查询指南

## 访问Neo4j Browser
**地址**: http://localhost:7474
**用户名**: neo4j
**密码**: password

---

## 🎯 推荐查询

### 1. 查看完整SQL树结构（必看！）
```cypher
MATCH path = (root:RootNode)-[:CONTAINS*]->(leaf)
WHERE root.id = 'ROOT'
RETURN path
```
**效果**: 显示整个SQL的层次结构树

---

### 2. 查看所有节点
```cypher
MATCH (n)
RETURN n
LIMIT 50
```
**效果**: 显示前50个节点

---

### 3. 查看CTE及其依赖
```cypher
MATCH (cte:CTE)-[:CONTAINS*]->(descendant)
RETURN cte, descendant
```
**效果**: 显示CTE内部结构

---

### 4. 追踪表的血缘关系
```cypher
MATCH path = (t:Table {name: 'dw_xd_corp_loan_stdbook'})<-[:REFERENCES*]-(n)
RETURN path
LIMIT 25
```
**效果**: 显示哪些查询使用了该表

---

### 5. 查找最复杂的标量子查询
```cypher
MATCH (sq:SQ)
WHERE sq.depth >= 3
RETURN sq.id, sq.sql, sq.depth
ORDER BY sq.depth DESC
LIMIT 10
```
**效果**: 找出深层嵌套的子查询

---

### 6. 查看UNION操作
```cypher
MATCH (u:UNION)-[:CONTAINS*]->(branch)
RETURN u, branch
```
**效果**: 显示UNION的所有分支

---

### 7. 统计节点类型
```cypher
MATCH (n)
RETURN n.type as 类型, count(*) as 数量
ORDER BY 数量 DESC
```
**效果**: 显示各类节点的统计信息

---

### 8. 查找所有物理表
```cypher
MATCH (t:TB)
RETURN t.name, t.metadata.schema as schema
ORDER BY schema, t.name
```
**效果**: 列出所有物理表及其schema

---

### 9. 追踪单个表的完整引用链
```cypher
MATCH path = (t:TB {name: 'lc00059999.BL_BILL_CONT'})<-[:REFERENCES*]-(n)
RETURN path
```
**效果**: 显示该表被引用的完整路径

---

### 10. 查看主查询块
```cypher
MATCH (root:RootNode)-[:CONTAINS]->(blk:BLK)
RETURN root, blk
```
**效果**: 显示ROOT下的直接查询块

---

## 🎨 可视化设置

### 节点样式配置
在Neo4j Browser左侧的"Node Styles"中设置：

**CTE节点**（蓝色）:
```
type: CT
caption: name
color: #3498db
```

**物理表节点**（绿色）:
```
type: TB
caption: name
color: #2ecc71
```

**标量子查询**（红色）:
```
type: SQ
caption: id
color: #e74c3c
```

**查询块**（黄色）:
```
type: BLK
caption: name
color: #f1c40f
```

**UNION节点**（紫色）:
```
type: UNION
caption: name
color: #9b59b6
```

**根节点**（深灰色）:
```
type: ROOT
caption: name
color: #34495e
```

---

## 📊 关系样式配置

**CONTAINS关系**（蓝色虚线）:
```
type: CONTAINS
color: #3498db
dashed: true
width: 2
```

**REFERENCES关系**（橙色实线）:
```
type: REFERENCES
color: #e67e22
solid: true
width: 1
```

---

## 🔍 分析场景

### 场景1: 性能优化分析
```cypher
// 查找深层嵌套的子查询
MATCH (sq:SQ)
WHERE sq.depth >= 4
RETURN sq.id, sq.sql, sq.depth
ORDER BY sq.depth DESC
```

### 场景2: 表依赖分析
```cypher
// 查看表之间的依赖关系
MATCH (t1:TB)<-[:REFERENCES]-(n)-[:REFERENCES]->(t2:TB)
RETURN t1.name as 源表, t2.name as 目标表, count(*) as 引用次数
ORDER BY 引用次数 DESC
LIMIT 10
```

### 场景3: CTE引用分析
```cypher
// 查看CTE被哪些节点引用
MATCH (cte:CTE)<-[:REFERENCES]-(n)
RETURN cte.name as CTE名称,
       collect(DISTINCT n.type) as 引用类型,
       count(n) as 引用次数
```

### 场景4: 复杂度评估
```cypher
// 统计每个节点的子节点数量
MATCH (n)-[:CONTAINS]->(child)
WITH n, count(child) as child_count
RETURN n.id, n.type, n.name, child_count
ORDER BY child_count DESC
LIMIT 10
```

---

## 💡 使用技巧

1. **调整布局**: 使用"Hierarchical"布局查看树形结构
2. **导出图形**: 点击右上角下载图标，可导出为PNG/SVG
3. **节点详情**: 点击节点查看详细信息（包含完整SQL）
4. **搜索功能**: 在顶部搜索框输入节点ID或名称
5. **缩放**: 使用鼠标滚轮缩放图形

---

## 📝 快捷命令

```cypher
// 查看数据库统计
MATCH (n) RETURN count(n) as 节点数
MATCH ()-[r]->() RETURN count(r) as 关系数

// 查看最近创建的节点
MATCH (n)
RETURN n
ORDER BY n.id DESC
LIMIT 10
```

---

**提示**: 复制上述查询语句到Neo4j Browser的输入框中执行即可！
