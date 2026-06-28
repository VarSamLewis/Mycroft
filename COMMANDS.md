# Mycroft — Commands

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)
- Node.js + npm
- Docker

## Quick start

```bash
# 1. Start Neo4j (first time)
docker run -d --name neo4j -p 7687:7687 -p 7474:7474 -e NEO4J_AUTH=neo4j/password123 neo4j

# Or restart an existing container
docker start neo4j

# 2. Install Python dependencies
uv pip install -r src/backend/requirements.txt \
  -r src/consum_api/requirements.txt \
  -r src/cli/requirements.txt

# 3. Seed test data (optional; skip if ingesting your own codebase)
uv run scripts/seed_data.py

# 4. Start API (terminal 1)
uvicorn src.consum_api.app:app --host 0.0.0.0 --port 8000

# 5. Start frontend (terminal 2)
npm install --prefix src/frontend
npm run dev --prefix src/frontend
```

Open the UI at http://localhost:5173

API docs (Swagger) are available at http://localhost:8000/docs once the API is running.

## Ingest a codebase

```bash
uv run python -m src.backend.main <path> [database] [schema]
```

Examples:

```bash
# Defaults: database=default, schema=public
uv run python -m src.backend.main /path/to/codebase

# Custom database and schema
uv run python -m src.backend.main /path/to/codebase analytics warehouse
```

Run from the repo root so `src.*` imports resolve correctly.

## API endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tables` | List all tables |
| GET | `/tables/{table_key}` | Table details with columns |
| GET | `/lineage/upstream/{column_key}` | Upstream columns for a given column |
| GET | `/lineage/downstream/{column_key}` | Downstream dependents of a given column |

## Other commands

| What | Command |
|------|---------|
| Run backend unit tests | `uv run pytest tests/backend/unit_tests.py` |
| Run the parser demo | `uv run main.py` |
| Run CLI (TUI) placeholder | `uv run src/cli/cli.py` |
| Re-seed test data | `uv run scripts/seed_data.py` |
| Clear graph only | `uv run scripts/seed_data.py --clear` |
| Seed without clearing | `uv run scripts/seed_data.py --seed-only` |
| Clear Neo4j directly | `uv run python -c "from src.backend.db import clear_graph; clear_graph()"` |
| Build frontend | `npm run build --prefix src/frontend` |
| Preview built frontend | `npm run preview --prefix src/frontend` |
| Lint frontend | `npm run lint --prefix src/frontend` |
| Stop API | `pkill -f "uvicorn src.consum_api"` |
| Stop Neo4j | `docker stop neo4j` |
| Remove Neo4j container | `docker rm neo4j` |

## Query via Cypher (Neo4j browser at http://localhost:7474)

```cypher
// All tables
MATCH (t:Table) RETURN t

// Full upstream lineage path for a column
MATCH p = (c:Column {key: "acme.analytics.raw_events.event_id"})-[:DERIVED_FROM*]->(source)
RETURN p

// Downstream dependents of a column
MATCH p = (c:Column {key: "acme.analytics.raw_events.event_id"})<-[:DERIVED_FROM*]-(downstream)
RETURN p

// Tables with the most columns
MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
RETURN t.name, count(c) AS cols
ORDER BY cols DESC
```
