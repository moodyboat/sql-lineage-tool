#!/bin/bash
#
# SQL节点解析器 - 一键导入到Neo4j
#
# 使用方法:
#   ./quick_start.sh <sql文件> [SQL方言]
#
# 示例:
#   ./quick_start.sh test_simple.sql mysql
#   ./quick_start.sh 原始.sql oracle

set -e  # 遇到错误立即退出

echo "========================================"
echo "SQL节点解析器 - 快速启动"
echo "========================================"

# 检查参数
if [ $# -lt 1 ]; then
    echo "错误: 请提供SQL文件路径"
    echo "用法: $0 <sql文件> [SQL方言]"
    echo "示例: $0 test_simple.sql mysql"
    exit 1
fi

SQL_FILE=$1
DIALECT=${2:-mysql}  # 默认使用MySQL

# 检查文件是否存在
if [ ! -f "$SQL_FILE" ]; then
    echo "错误: 文件不存在 - $SQL_FILE"
    exit 1
fi

echo ""
echo "配置信息:"
echo "  SQL文件: $SQL_FILE"
echo "  SQL方言: $DIALECT"
echo ""

# 检查Neo4j是否运行
echo "检查Neo4j状态..."
if ! python3 test_neo4j.py > /dev/null 2>&1; then
    echo "错误: 无法连接到Neo4j"
    echo "请确保Neo4j正在运行："
    echo "  neo4j start"
    echo ""
    echo "然后在浏览器中打开: http://localhost:7474"
    exit 1
fi

echo "✓ Neo4j运行正常"
echo ""

# 安装依赖（如果需要）
echo "检查依赖..."
if ! python3 -c "import neo4j" 2>/dev/null; then
    echo "安装依赖..."
    pip3 install -q sqlglot neo4j
fi
echo "✓ 依赖已就绪"
echo ""

# 解析并导入到Neo4j
echo "========================================"
echo "开始导入到Neo4j..."
echo "========================================"
echo ""

python3 import_to_neo4j.py "$SQL_FILE" --dialect "$DIALECT"

echo ""
echo "========================================"
echo "导入完成！"
echo "========================================"
echo ""
echo "下一步操作:"
echo ""
echo "1. 打开 Neo4j Browser:"
echo "   http://localhost:7474"
echo ""
echo "2. 复制并运行以下查询查看数据:"
echo ""
echo "   查看完整树结构:"
echo "   MATCH path = (root:RootNode)-[:CONTAINS*]->(leaf)"
echo "   WHERE root.id = 'ROOT'"
echo "   RETURN path"
echo ""
echo "   查看所有节点:"
echo "   MATCH (n) RETURN n LIMIT 25"
echo ""
echo "3. 或使用提供的查询文件:"
echo "   cat neo4j_visualization_queries.cypher"
echo ""
echo "4. 在Neo4j Browser中:"
echo "   - 点击左侧的节点可以查看详情"
echo "   - 点击右侧的节点样式可以为不同类型设置颜色"
echo "   - 使用Radial布局可以看到漂亮的树形结构"
echo ""
echo "提示: 按Ctrl+C可以停止Neo4j"
