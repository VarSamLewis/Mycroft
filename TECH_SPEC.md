# DataLineage MVP Technical Specification

## Overview

A tool that parses codebases containing SQL and Python (pyspark, pandas, polars) to produce an interactive data lineage graph. The primary use case is debugging upstream data issues by tracing lineage downstream.

## Goals

- Parse SQL files and embedded SQL strings in Python code
- Extract column-level lineage including transformations
- Provide an interactive web UI to explore the data model (ERD + lineage)
- Support mixed codebases (SQL + Python)

## Non-Goals (Future)

- Schema versioning / temporal lineage
- Runtime lineage capture
- Data quality integration
- MCP interface for querying source data

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Codebase      │     │     Neo4j       │     │    Web UI       │
│  (.sql, .py)    │────▶│ (schema +       │────▶│  (interactive)  │
└─────────────────┘     │  lineage graph) │     └─────────────────┘
                        └─────────────────┘
```

Single database (Neo4j) stores both schema metadata and lineage. Parse order doesn't matter - nodes are created/updated via MERGE.

### Components

#### 1. Parser Service

**Input:** Path to codebase

**Output:** Structured lineage data

**Responsibilities:**
- Glob for `.sql` and `.py` files
- Parse SQL using sqlglot
- Parse Python using `ast` module:
  - Extract SQL strings from `spark.sql()`, `pd.read_sql()`, `cursor.execute()`
  - Trace DataFrame operations (`.select()`, `.join()`, `.withColumn()`, etc.)
- Resolve `SELECT *` and DataFrame reads against known schema

**Libraries:**
- `sqlglot` - SQL parsing
- `ast` (stdlib) - Python parsing
- `neo4j` - Graph database driver

#### 2. Graph Store (Neo4j)

**Hierarchical Node Structure:**

```
(:Database)-[:HAS_SCHEMA]->(:Schema)-[:HAS_TABLE]->(:Table)-[:HAS_COLUMN]->(:Column)
```

**Node Types:**

| Node | Properties |
|------|------------|
| `Database` | name |
| `Schema` | name |
| `Table` | name, type (physical \| cte \| view \| dataframe), source_file |
| `Column` | name, data_type, is_nullable |

**Edge Types:**

| Edge | Description |
|------|-------------|
| `HAS_SCHEMA` | Database to Schema |
| `HAS_TABLE` | Schema to Table |
| `HAS_COLUMN` | Table to Column |
| `DERIVED_FROM` | Column lineage (with optional `transformation` property) |

**Example - Creating Schema:**

```cypher
MERGE (db:Database {name: "analytics"})
MERGE (s:Schema {name: "public"})
MERGE (db)-[:HAS_SCHEMA]->(s)
MERGE (t:Table {name: "users"})
MERGE (s)-[:HAS_TABLE]->(t)
MERGE (c:Column {name: "email", data_type: "VARCHAR"})
MERGE (t)-[:HAS_COLUMN]->(c)
```

**Example - Creating Lineage:**

```cypher
MATCH (source:Column {name: "name"})<-[:HAS_COLUMN]-(st:Table {name: "raw_users"})
MATCH (target:Column {name: "name_upper"})<-[:HAS_COLUMN]-(tt:Table {name: "users"})
MERGE (target)-[:DERIVED_FROM {transformation: "UPPER(name)"}]->(source)
```

**Example Queries:**

```cypher
// ERD for a schema - get all tables and columns
MATCH (s:Schema {name: "public"})-[:HAS_TABLE]->(t)-[:HAS_COLUMN]->(c)
RETURN t.name AS table, collect(c.name) AS columns

// Upstream lineage - where does this column come from?
MATCH (t:Table {name: "users"})-[:HAS_COLUMN]->(c:Column {name: "email"})
MATCH (c)-[:DERIVED_FROM*]->(source)<-[:HAS_COLUMN]-(source_table)
RETURN source.name, source_table.name

// Downstream lineage - what depends on this table?
MATCH (t:Table {name: "raw_events"})-[:HAS_COLUMN]->(c)
MATCH (c)<-[:DERIVED_FROM*]-(downstream)<-[:HAS_COLUMN]-(downstream_table)
RETURN DISTINCT downstream_table.name

// Full lineage path with transformations
MATCH (t:Table {name: "report"})-[:HAS_COLUMN]->(c:Column {name: "total"})
MATCH path = (c)-[:DERIVED_FROM*]->(source)
RETURN [rel in relationships(path) | rel.transformation] AS transformations
```

#### 3. Web UI

**Framework:** TBD (React, Svelte, or similar)

**Features:**
- Interactive graph visualization (d3.js or similar)
- Toggle between ERD view and lineage view
- Search for database/schema/table/column
- Click node to see upstream/downstream lineage
- Filter by schema, file source, or lineage depth
- Expand/collapse hierarchy levels

## Data Flow

1. **Ingest**
   - Parse all SQL and Python files
   - For DDL (CREATE TABLE): MERGE Database/Schema/Table/Column nodes
   - For DML/transformations: MERGE DERIVED_FROM edges between columns
   - Parse order doesn't matter - MERGE creates or updates

2. **Query**
   - User searches for a table/column
   - Traverse HAS_* edges for ERD view
   - Traverse DERIVED_FROM edges for lineage view

## File Structure

```
DataLineage/
├── main.py              # Core parsing logic (exists)
├── parsers/
│   ├── sql_parser.py    # SQL file parsing
│   └── python_parser.py # Python AST parsing for DataFrame ops
├── db/
│   └── neo4j.py         # Graph operations
├── ingest.py            # Orchestrates parsing a codebase
├── web/                 # Web UI
└── tests/
    └── test.py          # Tests (exists)
```

## Python Parsing Strategy

### SQL String Extraction

Detect and extract SQL from common patterns:

```python
# PySpark
spark.sql("SELECT ...")
spark.read.table("table_name")
df.write.saveAsTable("table_name")

# Pandas
pd.read_sql("SELECT ...", conn)
pd.read_sql_table("table_name", conn)
df.to_sql("table_name", conn)

# Raw SQL
cursor.execute("SELECT ...")
```

### DataFrame Operation Tracing

Map DataFrame methods to lineage:

| Operation | Lineage Effect |
|-----------|----------------|
| `.select("a", "b")` | Output has columns a, b from input |
| `.withColumn("x", expr)` | New column x derived from expr columns |
| `.join(other, on="key")` | Output has columns from both inputs |
| `.drop("col")` | Column removed from lineage |
| `.groupBy().agg()` | Aggregated columns derived from source |

## API

### CLI

```bash
# Ingest a codebase
datalineage ingest /path/to/codebase --db analytics

# Query lineage
datalineage upstream --table users --column email
datalineage downstream --table raw_events

# Export ERD
datalineage erd --schema public --format json
```

### Programmatic

```python
from datalineage import ingest, query

ingest("/path/to/codebase", db="analytics")

# Get upstream sources for a column
sources = query.upstream("users", "email")

# Get downstream dependents
dependents = query.downstream("raw_events")

# Get ERD for a schema
erd = query.erd("public")
```

## MVP Deliverables

1. SQL parser with CTE support (partially complete)
2. Python parser for SQL string extraction
3. Neo4j graph with hierarchical schema + lineage
4. Basic web UI with ERD and lineage visualization
5. CLI for ingestion and querying

## Open Questions

1. **Python DataFrame tracing depth** - How much static analysis is feasible? Start with SQL string extraction, add DataFrame ops incrementally.

2. **Web framework choice** - React for ecosystem, Svelte for simplicity?

3. **Graph visualization library** - d3.js, vis.js, or Cytoscape.js?

4. **Deployment** - Local Docker Compose for MVP? Cloud later?

5. **Default database/schema** - How to handle SQL that doesn't specify schema? Configurable default?
