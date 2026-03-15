# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a SQL lineage analysis tool (SQL血缘分析工具) that parses SQL scripts and creates a graph representation in Neo4j for blood lineage tracking and analysis. It extracts SQL nodes (CTEs, subqueries, tables, UNIONs) and their relationships for visualization and dependency analysis.

## Core Commands

### Installation and Setup
```bash
# Install dependencies
pip install -r requirements.txt
# or
pip install sqlglot neo4j
```

### Running the Parser
```bash
# Parse SQL file only (outputs JSON)
python3 sql_node_parser_v2.py <sql_file> <dialect>

# Example:
python3 sql_node_parser_v2.py 原始.sql oracle
```

### Importing to Neo4j
```bash
# Parse and import to Neo4j (recommended)
python3 import_to_neo4j.py <sql_file> --dialect <dialect>

# Example:
python3 import_to_neo4j.py 原始.sql --dialect oracle

# With custom Neo4j connection:
python3 import_to_neo4j.py <sql_file> --dialect mysql --uri bolt://localhost:7687 --user neo4j --password your_password

# Clear database before import:
python3 import_to_neo4j.py <sql_file> --dialect oracle --clear
```

### Quick Start Script
```bash
./quick_start.sh <sql_file> [dialect]
```

### Testing
```bash
# Run batch SQL tests
python3 batch_test_sql_files.py

# Analyze failures
python3 analyze_failures.py
```

## Architecture

### Core Components

**sql_node_parser_v2.py** - Main parsing engine
- `SQLPreprocessor`: Handles Oracle-specific syntax (TO_CHAR, DECODE, NVL, TO_DATE) and template variables (`<!VAR!>` format)
- `SQLNodeParser`: Uses sqlglot to parse SQL and extract nodes/relationships
- `Node` and `Relationship`: Data classes for graph representation

**import_to_neo4j.py** - Neo4j integration
- `Neo4jImporter`: Manages Neo4j connection and data import
- Maps parser output to Neo4j node labels and relationship types

### Node Types

| Parser Type | Neo4j Label | Description |
|-------------|-------------|-------------|
| ROOT | RootNode | Root node for entire SQL script |
| CT | CTE | Common Table Expression (WITH clause) |
| BLK | QueryBlock | Complete SELECT statement (UNION branch) |
| SQ | ScalarQuery | Nested subquery in SELECT clause |
| DT | DerivedTable | Subquery in FROM clause |
| WQ | WhereQuery | Subquery in WHERE clause |
| IQ | InQuery | Subquery in IN clause |
| TB | Table | Physical table |
| VW | View | Database view |
| UNION | UnionNode | UNION operation |

### Relationship Types

- **CONTAINS**: Hierarchical parent-child relationship (e.g., ROOT contains CTE, CTE contains subquery)
- **REFERENCES**: Data dependency relationship (e.g., query references table, CTE references another CTE)

### SQL Preprocessing Pipeline

The tool handles Oracle SQL compatibility through regex-based preprocessing:

1. **TO_CHAR**: `to_char(expr, 'format')` → `expr`
2. **TO_DATE**: `to_date(expr, 'format')` → `CAST(expr AS DATE)`
3. **NVL**: `nvl(a, b)` → `COALESCE(a, b)`
4. **DECODE**: Handled by sqlglot's Oracle dialect
5. **Template variables**: `<!VAR!>` → replacement values
6. **Dual table**: `FROM dual` → `FROM (SELECT 1 AS dummy)`

## Important Design Patterns

### Parser Flow
```
SQL Content → SQLPreprocessor → sqlglot AST → SQLNodeParser → Nodes + Relationships → Neo4j
```

### Node ID Generation
- Format: `{parent_id}_{type}_{name/counter}`
- Tables: `TB_{full_table_name}`
- Ensures globally unique IDs for graph representation

### Recursive Parsing
The parser uses `find_all()` from sqlglot to discover nested expressions and recursively processes them, building a tree structure that matches the SQL's hierarchical nature.

## Neo4j Query Patterns

### Common Queries
```cypher
-- View complete SQL tree structure
MATCH path = (root:RootNode)-[:CONTAINS*]->(leaf)
WHERE root.id = 'ROOT'
RETURN path

-- Find all tables and their usage
MATCH (t:Table)<-[:REFERENCES*]-(n)
RETURN t.name, collect(DISTINCT n.type), count(n)

-- Trace data lineage from table to root
MATCH path = (t:Table)<-[:REFERENCES*]-(:Node)-[:CONTAINS*]->(root:RootNode)
RETURN path

-- Find performance issues (deeply nested queries)
MATCH (sq:ScalarQuery)
WHERE sq.depth >= 3
RETURN sq.id, sq.sql, sq.depth
ORDER BY sq.depth DESC
```

## Common Issues and Solutions

### Oracle SQL Parse Errors
- **Issue**: sqlglot fails to parse Oracle-specific functions
- **Solution**: SQLPreprocessor converts Oracle syntax to MySQL-compatible forms before parsing
- **Skills**: Use `oracle-to-mysql-sql-preprocessing` or `sqlglot-oracle-parse-errors` skills

### Missing CTE References
- **Issue**: CTE references not properly tracked
- **Solution**: Parser collects CTE names in `cte_names` set during WITH clause parsing, then checks table references against this set

### Neo4j Connection Issues
- **Default URI**: `bolt://localhost:7687`
- **Default credentials**: `neo4j`/`password`
- Ensure Neo4j is running before import

### Template Variables
- Format: `<!VARNAME!>`
- Mappings defined in `SQLPreprocessor._handle_template_variables()`
- Common variables: `<!JEDW!>` (amount unit), `<!KSSJ!>`/`<!JSSJ!>` (date range)

## Testing and Debugging

### Batch Testing
- `batch_test_sql_files.py`: Tests multiple SQL files
- Results saved to `batch_test_report.json`
- Analyze failures with `analyze_failures.py`

### Performance Considerations
- Deeply nested scalar queries (depth > 3) indicate potential performance issues
- Tool tracks nesting depth in `Node.depth` field
- Large SQL can generate 300+ nodes and 1000+ relationships

## File Conventions

- Input: `.sql` files (ignored by git per .gitignore)
- Output: `*_nodes.json` files (ignored by git)
- Parser always outputs to `{input_file}_nodes.json`
- Neo4j Browser: http://localhost:7474
