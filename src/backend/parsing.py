import ast as python_ast

import sqlglot
from sqlglot import exp, parse_one


def split_query(sql: str):
    segments = sql.split(";")
    return [s.strip() for s in segments if s.strip()]


def _build_cte_map(tree):
    cte_map = {}
    for cte in tree.find_all(exp.CTE):
        cte_name = cte.alias
        cte_select = cte.find(exp.Select)
        if cte_select:
            from_node = cte_select.find(exp.From)
            source_table = from_node.find(exp.Table).name if from_node else None
            columns = []
            for expr in cte_select.expressions:
                col = expr.find(exp.Column)
                if col:
                    columns.append(
                        {
                            "name": expr.alias_or_name,
                            "source_column": col.name,
                            "source_table": source_table,
                        }
                    )
            cte_map[cte_name] = {"columns": columns, "source_table": source_table}
    return cte_map


def _get_table_type(table_name, target_name, cte_map):
    if table_name == target_name:
        return "physical"
    if table_name in cte_map:
        return "cte"
    return "physical"


def extract_graph(sql, database="default", schema="public", source_file=None):
    nodes = {
        "databases": {},
        "schemas": {},
        "tables": {},
        "columns": {},
    }
    edges = {
        "has_schema": [],
        "has_table": [],
        "has_column": [],
        "derived_from": [],
    }

    db_key = database
    nodes["databases"][db_key] = {"name": database}

    schema_key = f"{database}.{schema}"
    nodes["schemas"][schema_key] = {"name": schema}
    edges["has_schema"].append({"from": db_key, "to": schema_key})

    sql_strs = split_query(sql)
    for sql_str in sql_strs:
        if not sql_str:
            continue

        tree = parse_one(sql_str)
        cte_map = _build_cte_map(tree)

        target_node = tree.find(exp.Create)
        target_name = target_node.this.name if target_node else None

        alias_to_table = {}
        for table in tree.find_all(exp.Table):
            if table.name in cte_map:
                continue
            table_key = f"{schema_key}.{table.name}"
            nodes["tables"][table_key] = {
                "name": table.name,
                "type": _get_table_type(table.name, target_name, cte_map),
                "source_file": source_file,
            }
            edges["has_table"].append({"from": schema_key, "to": table_key})
            if table.alias:
                alias_to_table[table.alias] = table.name

        for cte_name, cte_info in cte_map.items():
            table_key = f"{schema_key}.{cte_name}"
            nodes["tables"][table_key] = {
                "name": cte_name,
                "type": "cte",
                "source_file": source_file,
            }
            edges["has_table"].append({"from": schema_key, "to": table_key})
            for cte_col in cte_info["columns"]:
                col_key = f"{table_key}.{cte_col['name']}"
                nodes["columns"][col_key] = {
                    "name": cte_col["name"],
                    "data_type": None,
                    "is_nullable": None,
                }
                edges["has_column"].append({"from": table_key, "to": col_key})

                source_table_name = cte_col["source_table"]
                source_table_key = f"{schema_key}.{source_table_name}"
                source_col_key = f"{source_table_key}.{cte_col['source_column']}"

                if source_col_key not in nodes["columns"]:
                    nodes["columns"][source_col_key] = {
                        "name": cte_col["source_column"],
                        "data_type": None,
                        "is_nullable": None,
                    }
                    edges["has_column"].append(
                        {"from": source_table_key, "to": source_col_key}
                    )

                edges["derived_from"].append(
                    {
                        "from": col_key,
                        "to": source_col_key,
                        "transformation": None,
                    }
                )

        main_select = None
        if target_node:
            main_select = target_node.find(exp.Select)
        if not main_select:
            main_select = tree.find(exp.Select)

        if main_select and target_name:
            target_table_key = f"{schema_key}.{target_name}"
            main_from = main_select.find(exp.From)
            main_source = main_from.find(exp.Table).name if main_from else None

            for expression in main_select.expressions:
                if isinstance(expression, exp.Star):
                    if main_source and main_source in cte_map:
                        for cte_col in cte_map[main_source]["columns"]:
                            target_col_key = f"{target_table_key}.{cte_col['name']}"
                            nodes["columns"][target_col_key] = {
                                "name": cte_col["name"],
                                "data_type": None,
                                "is_nullable": None,
                            }
                            edges["has_column"].append(
                                {"from": target_table_key, "to": target_col_key}
                            )

                            cte_table_key = f"{schema_key}.{main_source}"
                            cte_col_key = f"{cte_table_key}.{cte_col['name']}"
                            edges["derived_from"].append(
                                {
                                    "from": target_col_key,
                                    "to": cte_col_key,
                                    "transformation": None,
                                }
                            )
                    continue

                source_col = expression.find(exp.Column)
                target_col_name = expression.alias_or_name
                target_col_key = f"{target_table_key}.{target_col_name}"

                is_transformed = not isinstance(
                    expression, (exp.Column, exp.Alias)
                ) or not isinstance(expression.unalias(), exp.Column)
                transformation = expression.sql() if is_transformed else None

                nodes["columns"][target_col_key] = {
                    "name": target_col_name,
                    "data_type": None,
                    "is_nullable": None,
                }
                edges["has_column"].append(
                    {"from": target_table_key, "to": target_col_key}
                )

                if source_col:
                    source_table_ref = source_col.table
                    source_col_name = source_col.name

                    if source_table_ref in alias_to_table:
                        source_table_name = alias_to_table[source_table_ref]
                    elif source_table_ref in cte_map:
                        source_table_name = source_table_ref
                    else:
                        source_table_name = source_table_ref or main_source

                    if source_table_name in cte_map:
                        for cte_col in cte_map[source_table_name]["columns"]:
                            if cte_col["name"] == source_col_name:
                                source_table_name = cte_col["source_table"]
                                source_col_name = cte_col["source_column"]
                                break

                    source_table_key = f"{schema_key}.{source_table_name}"
                    source_col_key = f"{source_table_key}.{source_col_name}"

                    if source_col_key not in nodes["columns"]:
                        nodes["columns"][source_col_key] = {
                            "name": source_col_name,
                            "data_type": None,
                            "is_nullable": None,
                        }
                        edges["has_column"].append(
                            {"from": source_table_key, "to": source_col_key}
                        )

                    edges["derived_from"].append(
                        {
                            "from": target_col_key,
                            "to": source_col_key,
                            "transformation": transformation,
                        }
                    )

    return {"nodes": nodes, "edges": edges}


SQL_METHOD_NAMES = {"sql", "read_sql", "read_sql_query", "read_sql_table", "execute"}
TABLE_READ_METHODS = {"table"}
TABLE_WRITE_METHODS = {"saveAsTable", "to_sql", "insertInto"}


def _get_string_value(node):
    if isinstance(node, python_ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, python_ast.JoinedStr):
        parts = []
        for value in node.values:
            if isinstance(value, python_ast.Constant):
                parts.append(value.value)
            else:
                parts.append("???")
        return "".join(parts)
    return None


def extract_sql_from_python(code):
    results = {
        "sql_statements": [],
        "table_reads": [],
        "table_writes": [],
    }

    try:
        tree = python_ast.parse(code)
    except SyntaxError:
        return results

    for node in python_ast.walk(tree):
        if not isinstance(node, python_ast.Call):
            continue

        if isinstance(node.func, python_ast.Attribute):
            method_name = node.func.attr

            if method_name in SQL_METHOD_NAMES and node.args:
                sql_str = _get_string_value(node.args[0])
                if sql_str:
                    results["sql_statements"].append(
                        {
                            "sql": sql_str,
                            "line": node.lineno,
                        }
                    )

            elif method_name in TABLE_READ_METHODS and node.args:
                table_name = _get_string_value(node.args[0])
                if table_name:
                    results["table_reads"].append(
                        {
                            "table": table_name,
                            "line": node.lineno,
                        }
                    )

            elif method_name in TABLE_WRITE_METHODS and node.args:
                table_name = _get_string_value(node.args[0])
                if table_name:
                    results["table_writes"].append(
                        {
                            "table": table_name,
                            "line": node.lineno,
                        }
                    )

    return results


def merge_graphs(graphs):
    merged = {
        "nodes": {
            "databases": {},
            "schemas": {},
            "tables": {},
            "columns": {},
        },
        "edges": {
            "has_schema": [],
            "has_table": [],
            "has_column": [],
            "derived_from": [],
        },
    }

    seen_edges = {
        "has_schema": set(),
        "has_table": set(),
        "has_column": set(),
        "derived_from": set(),
    }

    for graph in graphs:
        for node_type in merged["nodes"]:
            merged["nodes"][node_type].update(graph["nodes"][node_type])

        for edge_type in merged["edges"]:
            for edge in graph["edges"][edge_type]:
                edge_key = (edge["from"], edge["to"])
                if edge_key not in seen_edges[edge_type]:
                    seen_edges[edge_type].add(edge_key)
                    merged["edges"][edge_type].append(edge)

    return merged


def extract_graph_from_python(
    code, database="default", schema="public", source_file=None
):
    extracted = extract_sql_from_python(code)
    graphs = []

    for sql_info in extracted["sql_statements"]:
        try:
            graph = extract_graph(
                sql_info["sql"],
                database=database,
                schema=schema,
                source_file=source_file,
            )
            graphs.append(graph)
        except Exception:
            continue

    if not graphs:
        return {
            "nodes": {"databases": {}, "schemas": {}, "tables": {}, "columns": {}},
            "edges": {
                "has_schema": [],
                "has_table": [],
                "has_column": [],
                "derived_from": [],
            },
        }

    return merge_graphs(graphs)
