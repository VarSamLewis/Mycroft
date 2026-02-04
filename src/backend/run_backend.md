# Running the Backend

## 1. Start Neo4j

```bash
docker run -d --name neo4j -p 7687:7687 -p 7474:7474 -e NEO4J_AUTH=neo4j/password123 neo4j
```

Neo4j browser: http://localhost:7474
Bolt connection: bolt://localhost:7687

If the container already exists and is stopped:

```bash
docker start neo4j
```

## 2. Install dependencies

```bash
pip install -r requirements.txt
```

## 3. Ingest a codebase

```bash
python -m src.backend.main <path_to_directory> [database] [schema]
```

Examples:

```bash
# With defaults (database=default, schema=public)
python -m src.backend.main /path/to/your/sql/files

# With custom database and schema
python -m src.backend.main /path/to/your/sql/files analytics warehouse
```

Run from the repo root (`Mycroft/`), not from `src/backend/`.

## 4. Query lineage

From Python:

```python
from src.backend.db import read_upstream_lineage, read_downstream_lineage

# Everything that feeds into a column
read_upstream_lineage("default.public.fact_orders.customer_sk")

# Everything that depends on a column
read_downstream_lineage("default.public.fact_orders.store_sk")
```

From the Neo4j browser (http://localhost:7474):

```cypher
// All tables
MATCH (t:Table) RETURN t

// Full upstream lineage for a column
MATCH p=(c:Column {key: "default.public.fact_orders.customer_sk"})-[:DERIVED_FROM*]->() RETURN p

// All tables with column counts
MATCH (t:Table)-[:HAS_COLUMN]->(c:Column) RETURN t.name, count(c) as cols ORDER BY cols DESC
```

## 5. Clear the database

```python
from src.backend.db import clear_graph
clear_graph()
```
