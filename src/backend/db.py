from neo4j import GraphDatabase


def write_graph_to_neo4j(
    graph, uri="bolt://localhost:7687", user="neo4j", password="password123"
):
    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:
        for db_key, db_data in graph["nodes"]["databases"].items():
            session.run(
                "MERGE (d:Database {key: $key}) SET d.name = $name",
                key=db_key,
                name=db_data["name"],
            )

        for schema_key, schema_data in graph["nodes"]["schemas"].items():
            session.run(
                "MERGE (s:Schema {key: $key}) SET s.name = $name",
                key=schema_key,
                name=schema_data["name"],
            )

        for table_key, table_data in graph["nodes"]["tables"].items():
            session.run(
                "MERGE (t:Table {key: $key}) SET t.name = $name, t.type = $type, t.source_file = $source_file",
                key=table_key,
                name=table_data["name"],
                type=table_data["type"],
                source_file=table_data["source_file"],
            )

        for col_key, col_data in graph["nodes"]["columns"].items():
            session.run(
                "MERGE (c:Column {key: $key}) SET c.name = $name, c.data_type = $data_type, c.is_nullable = $is_nullable",
                key=col_key,
                name=col_data["name"],
                data_type=col_data["data_type"],
                is_nullable=col_data["is_nullable"],
            )

        for edge in graph["edges"]["has_schema"]:
            session.run(
                "MATCH (d:Database {key: $from}), (s:Schema {key: $to}) MERGE (d)-[:HAS_SCHEMA]->(s)",
                **edge,
            )

        for edge in graph["edges"]["has_table"]:
            session.run(
                "MATCH (s:Schema {key: $from}), (t:Table {key: $to}) MERGE (s)-[:HAS_TABLE]->(t)",
                **edge,
            )

        for edge in graph["edges"]["has_column"]:
            session.run(
                "MATCH (t:Table {key: $from}), (c:Column {key: $to}) MERGE (t)-[:HAS_COLUMN]->(c)",
                **edge,
            )

        for edge in graph["edges"]["derived_from"]:
            session.run(
                "MATCH (c1:Column {key: $from}), (c2:Column {key: $to}) MERGE (c1)-[:DERIVED_FROM {transformation: $transformation}]->(c2)",
                **edge,
            )

    driver.close()


def read_upstream_lineage(column_key, uri="bolt://localhost:7687", user="neo4j", password="password123"):
    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:
        result = session.run(
            "MATCH (c:Column {key: $key})-[:DERIVED_FROM*]->(source) RETURN source",
            key=column_key,
        )
        sources = [record["source"] for record in result]

    driver.close()
    return sources


def read_downstream_lineage(column_key, uri="bolt://localhost:7687", user="neo4j", password="password123"):
    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:
        result = session.run(
            "MATCH (c:Column {key: $key})<-[:DERIVED_FROM*]-(downstream) RETURN downstream",
            key=column_key,
        )
        dependents = [record["downstream"] for record in result]

    driver.close()
    return dependents


def clear_graph(uri="bolt://localhost:7687", user="neo4j", password="password123"):
    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    driver.close()
