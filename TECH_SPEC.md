# Mycroft — Technical Specification

## Overview

Mycroft parses codebases containing SQL and Python to produce an interactive data lineage graph. The primary use case is debugging upstream data issues by tracing lineage downstream.

## Goals

- Parse SQL files and embedded SQL strings in Python code.
- Extract column-level lineage including transformations.
- Store schema metadata and lineage in Neo4j.
- Provide an HTTP API for clients.
- Provide an interactive web UI to explore tables, columns, and lineage.
- Support mixed codebases (SQL + Python).

## Non-Goals (Future)

- Schema versioning / temporal lineage.
- Runtime lineage capture.
- Data quality integration.
- MCP interface for querying source data.
- Full static analysis of DataFrame operations (`.select`, `.withColumn`, `.join`, etc.).
- Search, filtering, and ERD export.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Codebase      │     │     Neo4j       │     │   Web UI        │
│  (.sql, .py)    │────▶│ (schema +       │◀────│  (React +       │
└─────────────────┘     │  lineage graph) │     │  React Flow)    │
                        └─────────────────┘     └─────────────────┘
                                   ▲
                                   │ HTTP
                        ┌─────────────────────┐
                        │       FastAPI       │
                        │  mycroft.consum_api │
                        └─────────────────────┘
                                   ▲
                                   │ (future)
                        ┌─────────────────────┐
                        │         CLI         │
                        │    mycroft.cli      │
                        └─────────────────────┘
```

Single database (Neo4j) stores both schema metadata and lineage. Parse order doesn't matter — nodes are created or updated via `MERGE`.

## Components

### 1. Parser Service — `src/mycroft/backend/`

**Input:** Path to a codebase directory.

**Output:** Structured lineage data.

**Files:**
- `src/mycroft/backend/parsing.py` — SQL and Python parsing logic.
- `src/mycroft/backend/main.py` — Ingestion orchestration.
- `src/mycroft/backend/file.py` — File discovery and reading.

**Responsibilities:**
- Glob for `.sql` and `.py` files.
- Parse SQL using `sqlglot`.
- Parse Python using `ast`:
  - Extract SQL strings from `spark.sql()`, `pd.read_sql()`, `cursor.execute()`.
  - Basic extraction of `spark.read.table()` / `df.write.saveAsTable()` references.
- Build a graph of databases, schemas, tables, columns, and `DERIVED_FROM` edges.

**Libraries:**
- `sqlglot` — SQL parsing.
- `ast` (stdlib) — Python parsing.
- `neo4j` — Graph database driver.

### 2. Graph Store — Neo4j — `src/mycroft/backend/db.py`

**Hierarchical Node Structure:**

```
(:Database)-[:HAS_SCHEMA]->(:Schema)-[:HAS_TABLE]->(:Table)-[:HAS_COLUMN]->(:Column)
```

**Transformation Nodes:**

```
(:Table)-[:HAS_TRANSFORMATION]->(:Transformation)
```

**Node Types:**

| Node | Properties |
|------|------------|
| `Database` | `key`, `name` |
| `Schema` | `key`, `name` |
| `Table` | `key`, `name`, `type` (physical \| cte \| view \| dataframe), `source_file` |
| `Column` | `key`, `name`, `data_type`, `is_nullable` |
| `Transformation` | `key`, `type`, `expression` |

**Edge Types:**

| Edge | Description |
|------|-------------|
| `HAS_SCHEMA` | Database to Schema |
| `HAS_TABLE` | Schema to Table |
| `HAS_COLUMN` | Table to Column |
| `HAS_TRANSFORMATION` | Table to Transformation |
| `DERIVED_FROM` | Column lineage (with optional `transformation` property) |

### 3. API — `src/mycroft/consum_api/app.py`

FastAPI application that owns the queries and response shapes.

**Endpoints:**

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tables` | List all tables |
| GET | `/tables/{table_key}` | Single table with columns and transformations |
| GET | `/lineage/upstream/{column_key}` | Upstream columns for a column |
| GET | `/lineage/downstream/{column_key}` | Downstream dependents of a column |

### 4. Web UI — `src/frontend/`

**Stack:**
- React 19
- Vite
- React Router v7
- React Query (TanStack Query)
- React Flow (`@xyflow/react`)
- Tailwind CSS

**Views:**

| Route | Component | Purpose |
|-------|-----------|---------|
| `/` | `RepoGraph` | Approximate repository-wide table connectivity |
| `/tables/:tableKey` | `TableDetail` | Table metadata, columns, and transformations |
| `/tables/:tableKey/lineage` | `TableLineage` | Table-level lineage graph |
| `/columns/:columnKey` | `ColumnLineage` | Column upstream/downstream lineage graph |
| `/columns/:columnKey/table` | `ColumnLineageTable` | Column lineage as a table |

The `SchemaExplorer` sidebar lists databases, schemas, tables, and columns for navigation.

### 5. CLI — `src/mycroft/cli/cli.py`

Currently a placeholder TUI clock app built with `textual`. Not connected to ingestion or querying.

### 6. Seed Data — `scripts/seed_data.py`

Populates Neo4j with curated test data covering deep chains, wide fan-out, diamond patterns, cross-database lineage, and a wide table. Used for frontend development and demos.

## File Structure

```
Mycroft/
├── pyproject.toml               # Python project metadata and dependencies
├── COMMANDS.md                  # Common commands
├── TECH_SPEC.md                 # This document
├── main.py                      # Parser demo / standalone script
├── scripts/
│   └── seed_data.py             # Neo4j seed data
├── src/
│   ├── mycroft/
│   │   ├── __init__.py
│   │   ├── backend/
│   │   │   ├── __init__.py
│   │   │   ├── main.py          # Ingestion entry point
│   │   │   ├── parsing.py       # SQL / Python parsing
│   │   │   ├── db.py            # Neo4j read/write helpers
│   │   │   └── file.py          # File discovery
│   │   ├── consum_api/
│   │   │   ├── __init__.py
│   │   │   └── app.py           # FastAPI application
│   │   └── cli/
│   │       ├── __init__.py
│   │       └── cli.py           # TUI placeholder
│   └── frontend/
│       ├── package.json
│       ├── vite.config.ts
│       └── src/
│           ├── api/client.ts
│           ├── components/
│           │   ├── ColumnLineage/
│           │   ├── ColumnLineageTable/
│           │   ├── RepoGraph/
│           │   ├── SchemaExplorer/
│           │   ├── TableDetail/
│           │   └── TableLineage/
│           ├── hooks/
│           ├── types.ts
│           └── App.tsx
└── tests/
    └── backend/
        └── unit_tests.py        # Parser unit tests
```

## Data Flow

1. **Ingest**
   - Run `python -m src.backend.main <path> [database] [schema]`.
   - Parse all `.sql` and `.py` files.
   - For DDL (`CREATE TABLE`): `MERGE` Database/Schema/Table/Column nodes.
   - For DML/transformations: `MERGE` `DERIVED_FROM` edges between columns.
   - Parse order doesn't matter — `MERGE` creates or updates.

2. **Query**
   - Web UI or CLI fetches from FastAPI.
   - FastAPI traverses Neo4j and returns structured responses.
   - UI renders tables, columns, and lineage graphs.

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

Not fully implemented. Planned mapping:

| Operation | Lineage Effect |
|-----------|----------------|
| `.select("a", "b")` | Output has columns a, b from input |
| `.withColumn("x", expr)` | New column x derived from expr columns |
| `.join(other, on="key")` | Output has columns from both inputs |
| `.drop("col")` | Column removed from lineage |
| `.groupBy().agg()` | Aggregated columns derived from source |

## API Usage

### HTTP

```bash
# List tables
curl http://localhost:8000/tables

# Table detail
curl http://localhost:8000/tables/acme.analytics.raw_events

# Upstream lineage
curl http://localhost:8000/lineage/upstream/acme.analytics.raw_events.event_id

# Downstream lineage
curl http://localhost:8000/lineage/downstream/acme.analytics.raw_events.event_id
```

### Python

```python
from mycroft.backend.main import ingest_codebase
from mycroft.backend.db import read_upstream_lineage, read_downstream_lineage

# Ingest
ingest_codebase("/path/to/codebase", database="analytics", schema="warehouse")

# Query lineage
read_upstream_lineage("analytics.warehouse.fact_orders.customer_sk")
read_downstream_lineage("analytics.warehouse.fact_orders.store_sk")
```

## Commands

See `COMMANDS.md` for the full list, including:

- Starting Neo4j in Docker.
- Installing dependencies.
- Seeding test data.
- Running the API and frontend dev servers.
- Running tests.
- Clearing the graph.

## MVP Deliverables

- [x] SQL parser with CTE support.
- [x] Python parser for SQL string extraction.
- [x] Neo4j graph with hierarchical schema + lineage.
- [x] FastAPI for querying the graph.
- [x] Basic web UI with schema explorer and lineage visualization.
- [x] Table transformation display.
- [ ] Functional CLI for ingestion and querying.
- [ ] Search across databases, schemas, tables, and columns.
- [ ] Filtering by schema, file source, or lineage depth.
- [ ] ERD export.
- [ ] Full DataFrame operation tracing.

## Open Questions

1. **Python DataFrame tracing depth** — How much static analysis is feasible? Currently limited to SQL string extraction; DataFrame ops are future work.
2. **CLI scope** — Should the CLI become the primary ingestion interface, or remain a thin wrapper around `src.backend.main`?
3. **Deployment** — Local Docker Compose for MVP? Cloud later?
4. **Default database/schema** — Currently configurable per ingestion run; should project-level defaults be supported?
