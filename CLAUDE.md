# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SQL field lineage analysis system using sqlglot AST parsing. Achieves 85.2% perfect traceability on 27 real-world SQL tasks (23/27 tasks with 100% traceability).

**Core principle**: Only trace adjacent reference relationships (parent-child), never cross-layer tracing.

**Based on**: sqlglot AST (Abstract Syntax Tree) parsing

## Common Commands

### Basic SQL Analysis
```bash
# Analyze a SQL file and output to console
python main.py <sql_file>

# Export to JSON
python main.py <sql_file> --output result.json

# Export to Neo4j (requires Neo4j running)
python main.py <sql_file> --export-neo4j

# Disable metadata enhancement
python main.py <sql_file> --no-metadata

# Disable scope system
python main.py <sql_file> --no-scope
```

### Batch Analysis
```bash
# Batch analyze physical field mapping
python batch_analyze_physical_field_mapping.py

# Query Neo4j field lineage
python query_final_fields_lineage.py
```

### Quick Start Example
```bash
# Run quick start demo
python examples/demo_quick_start.py
```

### Dependencies
```bash
pip install -r requirements.txt
```

Required packages:
- `sqlglot>=25.0.0` - SQL parsing and AST
- `neo4j>=5.0.0` - Neo4j graph database

## Architecture

### 3-Layer Lineage System
```
Physical Table Layer (物理表层)
    ↓ PROVIDES
Subquery/CTE Layer (中间层)
    ↓ DERIVES
Outer Query Layer (外层查询)
```

### Node Types
- **TB** (Table) - Physical database tables
- **VW** (View) - Database views
- **CT** (CTE) - Common Table Expressions (WITH clauses)
- **SQ** (Subquery) - Nested subqueries
- **BLK** (Block) - Compound statement blocks
- **UNION** - UNION query nodes
- **ROOT** - Root query node

### Field Types
- **COLUMN** - Simple column reference (e.g., `table.column`)
- **FUNCTION** - Function calls (e.g., `TO_CHAR(date, 'YYYYMM')`)
- **CASE** - CASE expressions (e.g., `CASE WHEN x > 0 THEN 1 ELSE 0 END`)
- **AGGREGATION** - Aggregate functions (e.g., `SUM(amount)`, `COUNT(*)`)
- **ARITHMETIC** - Arithmetic expressions (e.g., `ZY_FK * 10000`)
- **SCALAR_QUERY** - Scalar subqueries
- **UNION** - UNION fields
- **LITERAL** - Literal values (no data source dependency, e.g., `'担保人' AS 担保人`)

### Relationship Types
- **REFERENCES** - References to CTEs or other nodes
- **CONTAINS** - Parent-child containment (e.g., subqueries)
- **PROVIDES** - Table to node field provision
- **DERIVES** - Field derivation relationships

## Critical Files

### `main.py`
Unified entry point with command-line interface.

**Usage**:
```python
python main.py <sql_file> [options]

Options:
  --dialect        SQL dialect (default: oracle)
  --output         Output JSON file path
  --export-neo4j   Export to Neo4j
  --no-metadata    Disable metadata enhancement
  --no-scope       Disable scope system
  --uri            Neo4j URI (default: bolt://localhost:7687)
  --user           Neo4j username (default: neo4j)
  --password       Neo4j password (default: password)
```

### `src/parsers/sql_node_parser_v2.py`
Core SQL parser implementing field lineage analysis using sqlglot AST.

**Key classes**:
- `SQLNodeParser` - Main parser class
- `SQLPreprocessor` - SQL preprocessing (Oracle syntax, template variables)
- `Node` - SQL statement node
- `Field` - Field metadata
- `FieldRelationship` - Field derivation relationships

**Key methods**:
- `_extract_field_dependencies()` - Extract field dependencies from expressions using `expr.find_all(exp.Column)`
- `_find_field_in_ctes()` - Multi-CTE field lookup (supports both REFERENCES and CONTAINS relationships)
- `_find_field_by_reference()` - Field lookup with case-insensitive matching
- `_infer_field_source()` - Field source inference
- `_build_cross_node_field_mappings()` - Build cross-node field mappings
- `verify_field_lineage()` - Verify field traceability

**Critical patterns**:
```python
# Extract column references from expressions
columns_in_expr = list(expr.find_all(exp.Column))

# Case-insensitive field matching
if field.column_name.upper() == column_name.upper():
    return field_id

# Handle ARITHMETIC expressions - extract first column for table_name
if isinstance(expr, (exp.Add, exp.Sub, exp.Mul, exp.Div)):
    result['field_type'] = 'ARITHMETIC'
    columns_in_expr = list(expr.find_all(exp.Column))
    if columns_in_expr:
        result['column_name'] = columns_in_expr[0].name

# Multi-CTE field lookup (adjacent relationships only)
def _find_field_in_ctes(self, column_name: str, parent_node_id: str) -> List[str]:
    referenced_nodes = []
    for rel in self.relationships:
        if rel.source_id == parent_node_id:
            # Support both CTE (REFERENCES) and subquery (CONTAINS) relationships
            if rel.type == 'REFERENCES' and rel.metadata.get('ref_type') == 'CTE':
                referenced_nodes.append(rel.target_id)
            elif rel.type == 'CONTAINS':
                target_node = self.nodes.get(rel.target_id)
                if target_node and target_node.type in ['CT', 'SQ']:
                    referenced_nodes.append(rel.target_id)
```

### `src/analyzers/enhanced_field_lineage.py`
Enhanced field lineage analyzer with metadata integration.

**Key classes**:
- `EnhancedFieldLineageAnalyzer` - Main analyzer class

**Usage**:
```python
from src.analyzers.enhanced_field_lineage import EnhancedFieldLineageAnalyzer

analyzer = EnhancedFieldLineageAnalyzer(
    metadata_files=[
        "metadata/大数据ods的实例库表字段.csv",
        "metadata/大数据dw和dm的实例库表字段.csv"
    ]
)

result = analyzer.analyze_sql(sql_content, dialect='oracle')
parser = result['parser']
fields = result['fields']
field_relationships = result['field_relationships']
```

### `src/exporters/import_to_neo4j.py`
Neo4j graph database integration.

**Key classes**:
- `Neo4jImporter` - Neo4j importer

**Neo4j schema**:
- `(:Node)` - SQL statement nodes
- `(:Table)` - Physical table nodes
- `(:Field)` - Field nodes with metadata
- `[:REFERENCES]`, `[:CONTAINS]`, `[:PROVIDES]`, `[:DERIVES]` - Relationship types

**Usage**:
```python
from src.exporters.import_to_neo4j import Neo4jImporter

importer = Neo4jImporter(uri="bolt://localhost:7687", user="neo4j", password="password")
importer.create_constraints()
importer.import_sql_parser_results(nodes, relationships, fields, field_relationships)
importer.close()
```

### `src/metadata/metadata_manager.py`
Metadata manager for database table and column information.

**Key classes**:
- `MetadataManager` - Metadata manager
- `TableMetadata` - Table metadata
- `ColumnMetadata` - Column metadata

### `src/core/` (Optional Scope System)
Optional scope system for advanced field propagation (not required for basic usage).

**Files**:
- `field_scope.py` - Field scope management
- `alias_manager.py` - Alias management
- `field_propagation.py` - Field propagation engine

## Important Constraints

### Adjacent Reference Relationships Only
**CRITICAL**: Only trace fields between directly related nodes (parent-child).

**DO**:
- Trace fields between parent and child nodes
- Look in current node's fields
- Look in directly referenced CTEs/subqueries
- Look in directly contained subqueries

**DO NOT**:
- Trace across multiple layers (e.g., CTE → CTE → outer query)
- Skip intermediate nodes
- Cross relationship boundaries

### column_name vs field.name
- `column_name` - The actual column name in the source table
- `field.name` - The field alias in the current query
- For calculated fields (FUNCTION, CASE, AGGREGATION, ARITHMETIC), `column_name` equals `field.name`

**Code pattern**:
```python
if field_info.get('field_type') in ['FUNCTION', 'CASE', 'AGGREGATION', 'ARITHMETIC']:
    final_column_name = field_name  # Use alias for calculated fields
else:
    final_column_name = field_info.get('column_name') or field_name
```

### Case-Insensitive Matching
All field name comparisons must be case-insensitive:
```python
# Correct
if field.column_name.upper() == column_name.upper():

# Incorrect
if field.column_name == column_name:
```

### ARITHMETIC Expression Handling
ARITHMETIC expressions require special handling:
```python
if isinstance(expr, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod)):
    result['field_type'] = 'ARITHMETIC'
    # Extract column references for table_name and column_name
    columns_in_expr = list(expr.find_all(exp.Column))
    if columns_in_expr:
        first_col = columns_in_expr[0]
        result['column_name'] = first_col.name
        if first_col.table:
            result['table_name'] = first_col.table
```

## Known Limitations

1. **LITERAL fields** - Fields like `'担保人' AS 担保人` have no data source (normal behavior, not a bug)
2. **SQL syntax errors** - Invalid SQL causes parsing failures (not a parser limitation)
3. **SELECT *** - Wildcard expansion requires schema metadata

## Testing Validation Results

- **Perfect traceability (100%)**: 23/27 tasks (85.2%)
- **Most complex task**: 24_信贷业务办理情况表 (684 nodes, 2520 fields) - 100% ✅
- **Partial traceability**: 2/27 (LITERAL fields - normal behavior)
- **Parsing errors**: 2/27 (SQL syntax issues in source files)

## sqlglot AST Integration

The entire project is built on sqlglot AST parsing:

```python
import sqlglot
from sqlglot import exp

# Parse SQL to AST
ast = sqlglot.parse_one(sql, dialect='oracle')

# Extract columns from expressions
for column in expr.find_all(exp.Column):
    table_name = column.table
    col_name = column.name

# Check expression types
if isinstance(expr, exp.Add):
    # Handle arithmetic addition
elif isinstance(expr, exp.Case):
    # Handle CASE expressions
elif isinstance(expr, exp.AggFunc):
    # Handle aggregate functions
```

**Key AST patterns**:
- `exp.Column` - Column reference
- `exp.Table` - Table reference
- `exp.Select` - SELECT statement
- `exp.Add`, `exp.Sub`, `exp.Mul`, `exp.Div` - Arithmetic operations
- `exp.Case` - CASE expression
- `exp.AggFunc` - Aggregate functions (SUM, COUNT, etc.)
- `exp.CTE` - Common Table Expression
- `exp.Union` - UNION operation

**Common operations**:
```python
# Find all columns in an expression
columns = list(expr.find_all(exp.Column))

# Get table and column names
table_name = column.table
column_name = column.name

# Check expression type
if isinstance(expr, exp.Select):
    # Handle SELECT statement

# Traverse AST
for child in expr.walk():
    if isinstance(child, exp.Column):
        # Process column
```

## Project Structure

```
血缘分析工具/
├── main.py                                  # Unified entry point
├── batch_analyze_physical_field_mapping.py  # Batch analysis tool
├── query_final_fields_lineage.py            # Neo4j query tool
│
├── src/                                     # Core code
│   ├── parsers/sql_node_parser_v2.py       # SQL parser (sqlglot AST)
│   ├── analyzers/enhanced_field_lineage.py # Enhanced analyzer
│   ├── exporters/import_to_neo4j.py        # Neo4j exporter
│   ├── metadata/metadata_manager.py        # Metadata manager
│   └── core/                               # Optional scope system
│
├── examples/demo_quick_start.py             # Quick start demo
├── tests/                                   # Unit & integration tests
└── metadata/                                # Metadata CSV files
```

## Performance Characteristics

- **Small SQL** (<50 lines): 0.01-0.03 seconds
- **Medium SQL** (50-200 lines): 0.03-0.1 seconds
- **Large SQL** (>200 lines): 0.1-0.5 seconds

**Most complex verified task**: 684 nodes, 2520 fields, 100% traceability ✅

## Verification Command

```bash
# Quick traceability test
python -c "from src.parsers.sql_node_parser_v2 import SQLNodeParser; p = SQLNodeParser(open('path/to.sql').read()); print(f'Traceability: {p.verify_field_lineage()*100:.1f}%')"
```

## Neo4j Query Examples

```cypher
-- View field lineage chain
MATCH path = (f1:Field)-[:DERIVES*]->(f2:Field)
WHERE f1.name = 'field_name'
RETURN path

-- Statistics
MATCH (f:Field)
WITH count(f) as total
MATCH (f:Field)-[:DERIVES]->()
RETURN total, count(f) as traced, round(100.0*count(f)/total, 1) as rate

-- Cross-node field relationships
MATCH (f1:Field)-[r:DERIVES]->(f2:Field)
WHERE r.metadata.cross_node = true
RETURN f1.name, f2.name, r.metadata.scope_chain
```
