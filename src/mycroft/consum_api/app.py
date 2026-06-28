"""

⏺ Mycroft API — Summary

  - Framework: FastAPI
  - DB connection: Neo4j Bolt driver (port 7687), same neo4j package you already use
  - Location: src/mycroft/consum_api/

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
     FastAPI (src/mycroft/consum_api/)
        ↓ Bolt
       Neo4j
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
from pydantic import BaseModel

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"

driver = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global driver
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    yield
    driver.close()


app = FastAPI(title="consum", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Models ---


class Column(BaseModel):
    key: str
    name: str
    data_type: str | None = None
    is_nullable: bool | None = None


class Table(BaseModel):
    key: str
    name: str
    type: str | None = None
    source_file: str | None = None


class Transformation(BaseModel):
    key: str
    type: str
    expression: str | None = None


class TableDetail(Table):
    columns: list[Column]
    transformations: list[Transformation]


class LineageNode(BaseModel):
    key: str
    name: str
    data_type: str | None = None
    is_nullable: bool | None = None


# --- Endpoints ---


@app.get("/lineage/upstream/{column_key}", response_model=list[LineageNode])
async def get_upstream_lineage(column_key: str) -> list[LineageNode]:
    assert driver is not None
    with driver.session() as session:
        result = session.run(
            "MATCH (c:Column {key: $key})-[:DERIVED_FROM*]->(source:Column) RETURN source",
            key=column_key,
        )
        return [LineageNode(**record["source"]) for record in result]


@app.get("/lineage/downstream/{column_key}", response_model=list[LineageNode])
async def get_downstream_lineage(column_key: str) -> list[LineageNode]:
    assert driver is not None
    with driver.session() as session:
        result = session.run(
            "MATCH (c:Column {key: $key})<-[:DERIVED_FROM*]-(downstream:Column) RETURN downstream",
            key=column_key,
        )
        return [LineageNode(**record["downstream"]) for record in result]


@app.get("/tables", response_model=list[Table])
async def get_tables() -> list[Table]:
    assert driver is not None
    with driver.session() as session:
        result = session.run("MATCH (t:Table) RETURN t")
        return [Table(**record["t"]) for record in result]


@app.get("/tables/{table_key}", response_model=TableDetail)
async def get_table(table_key: str) -> TableDetail:
    assert driver is not None
    with driver.session() as session:
        result = session.run(
            "MATCH (t:Table {key: $key}) "
            "OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column) "
            "OPTIONAL MATCH (t)-[:HAS_TRANSFORMATION]->(tr:Transformation) "
            "RETURN t, collect(c) AS columns, collect(DISTINCT tr) AS transformations",
            key=table_key,
        )
        record = result.single()
        if not record:
            raise HTTPException(status_code=404, detail=f"Table '{table_key}' not found")
        return TableDetail(
            **record["t"],
            columns=[Column(**c) for c in record["columns"] if c],
            transformations=[Transformation(**tr) for tr in record["transformations"] if tr],
        )


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
