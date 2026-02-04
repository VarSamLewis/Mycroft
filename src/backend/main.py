import pprint

from src.backend.github import discover_files, read_file
from src.backend.parsing import extract_graph, extract_graph_from_python, merge_graphs
from src.backend.db import write_graph_to_neo4j


def ingest_codebase(codebase_path, database="default", schema="public"):
    files = discover_files(codebase_path)
    graphs = []

    for filepath in files:
        content = read_file(filepath)

        if filepath.endswith(".sql"):
            try:
                graph = extract_graph(
                    content,
                    database=database,
                    schema=schema,
                    source_file=filepath,
                )
                graphs.append(graph)
            except Exception:
                continue

        elif filepath.endswith(".py"):
            try:
                graph = extract_graph_from_python(
                    content,
                    database=database,
                    schema=schema,
                    source_file=filepath,
                )
                graphs.append(graph)
            except Exception:
                continue

    if not graphs:
        return

    merged = merge_graphs(graphs)
    write_graph_to_neo4j(merged)

    return merged


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m src.backend.main <codebase_path> [database] [schema]")
        sys.exit(1)

    path = sys.argv[1]
    db = sys.argv[2] if len(sys.argv) > 2 else "default"
    schema = sys.argv[3] if len(sys.argv) > 3 else "public"

    result = ingest_codebase(path, database=db, schema=schema)
    if result:
        pprint.pprint(result)
