"""

⏺ Mycroft API — Summary

  - Framework: FastAPI
  - DB connection: Neo4j Bolt driver (port 7687), same neo4j package you already use
  - Location: src/api/

  Design:
  - The API owns the queries and response shapes
  - Clients (CLI, web) just fetch and render — no Cypher knowledge needed
  - One function per query, each with its own Pydantic response model for validation

  Endpoints:

  ┌──────────────────────────────────────┬────────────────────────────────┐
  │               Endpoint               │            Returns             │
  ├──────────────────────────────────────┼────────────────────────────────┤
  │ GET /lineage/upstream/{column_key}   │ Column's upstream lineage path │
  ├──────────────────────────────────────┼────────────────────────────────┤
  │ GET /lineage/downstream/{column_key} │ Column's downstream dependents │
  ├──────────────────────────────────────┼────────────────────────────────┤
  │ GET /tables                          │ All tables with metadata       │
  ├──────────────────────────────────────┼────────────────────────────────┤
  │ GET /tables/{table_key}              │ Single table with its columns  │
  └──────────────────────────────────────┴────────────────────────────────┘

  Architecture:

  CLI / Web Client
        ↓ HTTP
     FastAPI (src/api/)
        ↓ Bolt
       Neo4j
"""

from fastapi import FastAPI, Query

app = FastAPI(title="consum")

app.get("/data")


async def get_data() -> Dict:
    return data
