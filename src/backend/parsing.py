import ast as python_ast

import sqlglot
from sqlglot import exp, parse_one


# ---------------------------------------------------------------------------
# Query splitting
# ---------------------------------------------------------------------------

def _is_comment_only(s: str) -> bool:
    lines = [line.strip() for line in s.splitlines()]
    return all(line == "" or line.startswith("--") for line in lines)


def split_query(sql: str):
    segments = sql.split(";")
    return [s.strip() for s in segments if s.strip() and not _is_comment_only(s)]


# ---------------------------------------------------------------------------
# Graph structure helpers
# ---------------------------------------------------------------------------

def _empty_graph():
    return {
        "nodes": {"databases": {}, "schemas": {}, "tables": {}, "columns": {}},
        "edges": {"has_schema": [], "has_table": [], "has_column": [], "derived_from": []},
    }


# ---------------------------------------------------------------------------
# GraphExtractor — builds the lineage graph from a single SQL string
# ---------------------------------------------------------------------------

class GraphExtractor:
    def __init__(self, database, schema, source_file):
        self.database = database
        self.schema_key = f"{database}.{schema}"
        self.source_file = source_file
        self.graph = _empty_graph()

        self.graph["nodes"]["databases"][database] = {"name": database}
        self.graph["nodes"]["schemas"][self.schema_key] = {"name": schema}
        self.graph["edges"]["has_schema"].append({"from": database, "to": self.schema_key})

        # Per-statement state, reset each iteration
        self.cte_map = {}
        self.alias_to_table = {}

    # -- public --------------------------------------------------------------

    def extract(self, sql: str) -> dict:
        for sql_str in split_query(sql):
            try:
                tree = parse_one(sql_str)
            except sqlglot.errors.ParseError:
                continue
            self._process_statement(tree)
        return self.graph

    # -- per-statement orchestration -----------------------------------------

    def _process_statement(self, tree):
        self.cte_map = self._build_cte_map(tree)
        target_name, target_node, insert_node, update_node = self._resolve_target(tree)

        self.alias_to_table = self._register_tables(tree, target_name)
        self._process_ctes()

        main_select = self._find_main_select(target_node, tree)
        if main_select and target_name:
            insert_col_names = self._get_insert_col_names(insert_node)
            self._process_select_lineage(main_select, target_name, insert_col_names)

        if update_node and target_name:
            self._process_update_lineage(update_node, target_name)

    # -- target resolution ---------------------------------------------------

    def _resolve_target(self, tree):
        target_node = tree.find(exp.Create)
        target_name = target_node.this.name if target_node else None
        insert_node = None
        update_node = None

        insert_node = tree.find(exp.Insert)
        if not target_name and insert_node:
            insert_table = insert_node.this.find(exp.Table)
            target_name = insert_table.name if insert_table else None
            target_node = insert_node

        update_node = tree.find(exp.Update)
        if not target_name and update_node:
            target_name = update_node.this.name if update_node.this else None

        return target_name, target_node, insert_node, update_node

    # -- table & CTE registration --------------------------------------------

    def _register_tables(self, tree, target_name) -> dict:
        alias_to_table = {}
        for table in tree.find_all(exp.Table):
            if not table.name or table.name in self.cte_map:
                continue
            table_key = f"{self.schema_key}.{table.name}"
            table_type = "cte" if table.name in self.cte_map else "physical"
            self.graph["nodes"]["tables"][table_key] = {
                "name": table.name,
                "type": table_type,
                "source_file": self.source_file,
            }
            self.graph["edges"]["has_table"].append({"from": self.schema_key, "to": table_key})
            if table.alias:
                alias_to_table[table.alias] = table.name
        return alias_to_table

    def _process_ctes(self):
        for cte_name, cte_info in self.cte_map.items():
            table_key = f"{self.schema_key}.{cte_name}"
            self.graph["nodes"]["tables"][table_key] = {
                "name": cte_name,
                "type": "cte",
                "source_file": self.source_file,
            }
            self.graph["edges"]["has_table"].append({"from": self.schema_key, "to": table_key})

            source_table_key = f"{self.schema_key}.{cte_info['source_table']}"
            for cte_col in cte_info["columns"]:
                col_key = self._add_column(table_key, cte_col["name"])
                source_col_key = self._add_column(source_table_key, cte_col["source_column"])
                self._add_derived_from(col_key, source_col_key, transformation=None)

    # -- SELECT lineage ------------------------------------------------------

    def _process_select_lineage(self, main_select, target_name, insert_col_names):
        target_table_key = f"{self.schema_key}.{target_name}"
        main_from = main_select.find(exp.From)
        main_source = main_from.find(exp.Table).name if main_from else None

        for idx, expression in enumerate(main_select.expressions):
            if isinstance(expression, exp.Star):
                self._expand_star(target_table_key, main_source)
                continue

            target_col_name = insert_col_names[idx] if idx < len(insert_col_names) else expression.alias_or_name
            target_col_key = self._add_column(target_table_key, target_col_name)

            source_col = expression.find(exp.Column)
            if not source_col:
                continue

            transformation = self._detect_transformation(expression)
            source_table_name, source_col_name = self._resolve_source(
                source_col.table, source_col.name, main_source
            )
            source_col_key = self._add_column(
                f"{self.schema_key}.{source_table_name}", source_col_name
            )
            self._add_derived_from(target_col_key, source_col_key, transformation)

    def _expand_star(self, target_table_key, main_source):
        if not main_source or main_source not in self.cte_map:
            return
        cte_table_key = f"{self.schema_key}.{main_source}"
        for cte_col in self.cte_map[main_source]["columns"]:
            target_col_key = self._add_column(target_table_key, cte_col["name"])
            cte_col_key = f"{cte_table_key}.{cte_col['name']}"
            self._add_derived_from(target_col_key, cte_col_key, transformation=None)

    # -- UPDATE lineage ------------------------------------------------------

    def _process_update_lineage(self, update_node, target_name):
        target_table_key = f"{self.schema_key}.{target_name}"
        update_from = update_node.find(exp.From)
        update_source = update_from.find(exp.Table).name if update_from else None

        for eq in update_node.args.get("expressions", []):
            target_col_key = self._add_column(target_table_key, eq.this.name)

            source_col = eq.expression.find(exp.Column)
            if not source_col:
                continue

            source_table_name = self.alias_to_table.get(source_col.table, source_col.table or update_source)
            source_col_key = self._add_column(
                f"{self.schema_key}.{source_table_name}", source_col.name
            )

            is_transformed = not isinstance(eq.expression, exp.Column)
            transformation = eq.expression.sql() if is_transformed else None
            self._add_derived_from(target_col_key, source_col_key, transformation)

    # -- shared helpers ------------------------------------------------------

    def _add_column(self, table_key: str, col_name: str) -> str:
        col_key = f"{table_key}.{col_name}"
        if col_key not in self.graph["nodes"]["columns"]:
            self.graph["nodes"]["columns"][col_key] = {
                "name": col_name,
                "data_type": None,
                "is_nullable": None,
            }
            self.graph["edges"]["has_column"].append({"from": table_key, "to": col_key})
        return col_key

    def _add_derived_from(self, from_key: str, to_key: str, transformation):
        self.graph["edges"]["derived_from"].append({
            "from": from_key,
            "to": to_key,
            "transformation": transformation,
        })

    def _resolve_source(self, table_ref: str, col_name: str, fallback: str):
        if table_ref in self.alias_to_table:
            source_table = self.alias_to_table[table_ref]
        elif table_ref in self.cte_map:
            source_table = table_ref
        else:
            source_table = table_ref or fallback

        # Trace through CTE to the real underlying source
        if source_table in self.cte_map:
            for cte_col in self.cte_map[source_table]["columns"]:
                if cte_col["name"] == col_name:
                    return cte_col["source_table"], cte_col["source_column"]

        return source_table, col_name

    @staticmethod
    def _detect_transformation(expression):
        is_transformed = not isinstance(
            expression, (exp.Column, exp.Alias)
        ) or not isinstance(expression.unalias(), exp.Column)
        return expression.sql() if is_transformed else None

    @staticmethod
    def _build_cte_map(tree) -> dict:
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
                        columns.append({
                            "name": expr.alias_or_name,
                            "source_column": col.name,
                            "source_table": source_table,
                        })
                cte_map[cte_name] = {"columns": columns, "source_table": source_table}
        return cte_map

    @staticmethod
    def _find_main_select(target_node, tree):
        main_select = target_node.find(exp.Select) if target_node else None
        return main_select or tree.find(exp.Select)

    @staticmethod
    def _get_insert_col_names(insert_node) -> list:
        if insert_node and insert_node.this and hasattr(insert_node.this, 'expressions'):
            return [e.name for e in insert_node.this.expressions]
        return []


# ---------------------------------------------------------------------------
# Module-level API (used by main.py and tests)
# ---------------------------------------------------------------------------

def extract_graph(sql, database="default", schema="public", source_file=None):
    return GraphExtractor(database, schema, source_file).extract(sql)


# ---------------------------------------------------------------------------
# Python source extraction
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Graph merging
# ---------------------------------------------------------------------------

def merge_graphs(graphs):
    merged = _empty_graph()
    seen_edges = {edge_type: set() for edge_type in merged["edges"]}

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
        return _empty_graph()

    return merge_graphs(graphs)
