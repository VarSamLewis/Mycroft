import pytest

from main import extract_graph, extract_graph_from_python, extract_sql_from_python, split_query


class TestSplitQuery:
    def test_single_statement(self):
        result = split_query("SELECT * FROM users")
        assert len(result) == 1
        assert result[0] == "SELECT * FROM users"

    def test_multiple_statements(self):
        result = split_query("SELECT * FROM a; SELECT * FROM b")
        assert len(result) == 2

    def test_empty_segments_removed(self):
        result = split_query("SELECT * FROM a; ; SELECT * FROM b;")
        assert len(result) == 2


class TestExtractGraph:
    def test_simple_ctas(self):
        sql = """
            CREATE TABLE TBL_C AS (
                SELECT
                    UPPER(A.COL_A) AS COL_A,
                    A.COL_B,
                    B.COL_C
                FROM TBL_A AS A
                INNER JOIN TBL_B AS B ON A.COL_JOIN = B.COL_JOIN
            )
        """
        result = extract_graph(sql, database="test_db", schema="public")

        assert "test_db" in result["nodes"]["databases"]
        assert "test_db.public" in result["nodes"]["schemas"]
        assert "test_db.public.TBL_C" in result["nodes"]["tables"]
        assert "test_db.public.TBL_A" in result["nodes"]["tables"]
        assert "test_db.public.TBL_B" in result["nodes"]["tables"]

        assert result["nodes"]["tables"]["test_db.public.TBL_C"]["type"] == "physical"

        assert "test_db.public.TBL_C.COL_A" in result["nodes"]["columns"]
        assert "test_db.public.TBL_C.COL_B" in result["nodes"]["columns"]
        assert "test_db.public.TBL_C.COL_C" in result["nodes"]["columns"]

    def test_ctas_lineage_edges(self):
        sql = """
            CREATE TABLE TBL_C AS (
                SELECT
                    UPPER(A.COL_A) AS COL_A,
                    A.COL_B
                FROM TBL_A AS A
            )
        """
        result = extract_graph(sql, database="test_db", schema="public")

        derived_edges = result["edges"]["derived_from"]
        col_a_edge = next(e for e in derived_edges if e["from"] == "test_db.public.TBL_C.COL_A")
        assert col_a_edge["to"] == "test_db.public.TBL_A.COL_A"
        assert "UPPER" in col_a_edge["transformation"]

        col_b_edge = next(e for e in derived_edges if e["from"] == "test_db.public.TBL_C.COL_B")
        assert col_b_edge["to"] == "test_db.public.TBL_A.COL_B"
        assert col_b_edge["transformation"] is None

    def test_cte_creates_table_node(self):
        sql = """
            WITH VW_A AS (
                SELECT COL_A, COL_B FROM TBL_A
            )
            CREATE TABLE TBL_B AS (SELECT * FROM VW_A)
        """
        result = extract_graph(sql, database="test_db", schema="public")

        assert "test_db.public.VW_A" in result["nodes"]["tables"]
        assert result["nodes"]["tables"]["test_db.public.VW_A"]["type"] == "cte"

    def test_cte_lineage_traces_through(self):
        sql = """
            WITH VW_A AS (
                SELECT COL_A, COL_B FROM TBL_A
            )
            CREATE TABLE TBL_B AS (SELECT * FROM VW_A)
        """
        result = extract_graph(sql, database="test_db", schema="public")

        derived_edges = result["edges"]["derived_from"]

        cte_to_source = [e for e in derived_edges if e["from"].startswith("test_db.public.VW_A")]
        assert len(cte_to_source) == 2
        assert all(e["to"].startswith("test_db.public.TBL_A") for e in cte_to_source)

    def test_simple_select(self):
        sql = "SELECT col_a, col_b FROM users"
        result = extract_graph(sql, database="mydb", schema="dbo")

        assert "mydb.dbo.users" in result["nodes"]["tables"]
        assert result["nodes"]["tables"]["mydb.dbo.users"]["type"] == "physical"

    def test_insert_into_select(self):
        sql = """
            INSERT INTO target_table (col_a, col_b)
            SELECT src_a, src_b FROM source_table
        """
        result = extract_graph(sql, database="warehouse", schema="staging")

        assert "warehouse.staging.source_table" in result["nodes"]["tables"]

    def test_multiple_joins(self):
        sql = """
            CREATE TABLE report AS (
                SELECT
                    a.id,
                    b.name,
                    c.value
                FROM table_a a
                JOIN table_b b ON a.id = b.a_id
                JOIN table_c c ON b.id = c.b_id
            )
        """
        result = extract_graph(sql, database="analytics", schema="public")

        assert "analytics.public.table_a" in result["nodes"]["tables"]
        assert "analytics.public.table_b" in result["nodes"]["tables"]
        assert "analytics.public.table_c" in result["nodes"]["tables"]
        assert "analytics.public.report" in result["nodes"]["tables"]

        derived_edges = result["edges"]["derived_from"]
        assert any(e["to"] == "analytics.public.table_a.id" for e in derived_edges)
        assert any(e["to"] == "analytics.public.table_b.name" for e in derived_edges)
        assert any(e["to"] == "analytics.public.table_c.value" for e in derived_edges)

    def test_subquery_in_from(self):
        sql = """
            CREATE TABLE result AS (
                SELECT sub.total
                FROM (SELECT SUM(amount) as total FROM orders) sub
            )
        """
        result = extract_graph(sql, database="db", schema="public")

        assert "db.public.orders" in result["nodes"]["tables"]
        assert "db.public.result" in result["nodes"]["tables"]

    def test_source_file_propagates(self):
        sql = "CREATE TABLE foo AS (SELECT a FROM bar)"
        result = extract_graph(sql, database="db", schema="public", source_file="models/foo.sql")

        assert result["nodes"]["tables"]["db.public.foo"]["source_file"] == "models/foo.sql"
        assert result["nodes"]["tables"]["db.public.bar"]["source_file"] == "models/foo.sql"


class TestExtractSqlFromPython:
    def test_spark_sql(self):
        code = 'df = spark.sql("SELECT a, b FROM users")'
        result = extract_sql_from_python(code)

        assert len(result["sql_statements"]) == 1
        assert "SELECT a, b FROM users" in result["sql_statements"][0]["sql"]

    def test_spark_sql_multiline(self):
        code = '''
df = spark.sql("""
    SELECT a, b
    FROM users
    WHERE active = true
""")
'''
        result = extract_sql_from_python(code)

        assert len(result["sql_statements"]) == 1
        assert "SELECT" in result["sql_statements"][0]["sql"]
        assert "users" in result["sql_statements"][0]["sql"]

    def test_read_table(self):
        code = 'df = spark.read.table("raw_events")'
        result = extract_sql_from_python(code)

        assert len(result["table_reads"]) == 1
        assert result["table_reads"][0]["table"] == "raw_events"

    def test_save_as_table(self):
        code = 'df.write.saveAsTable("processed_events")'
        result = extract_sql_from_python(code)

        assert len(result["table_writes"]) == 1
        assert result["table_writes"][0]["table"] == "processed_events"

    def test_pandas_read_sql(self):
        code = 'df = pd.read_sql("SELECT * FROM orders", conn)'
        result = extract_sql_from_python(code)

        assert len(result["sql_statements"]) == 1
        assert "SELECT * FROM orders" in result["sql_statements"][0]["sql"]

    def test_cursor_execute(self):
        code = 'cursor.execute("INSERT INTO logs VALUES (1, 2, 3)")'
        result = extract_sql_from_python(code)

        assert len(result["sql_statements"]) == 1
        assert "INSERT INTO logs" in result["sql_statements"][0]["sql"]

    def test_multiple_statements(self):
        code = '''
df1 = spark.sql("SELECT a FROM x")
df2 = spark.sql("SELECT b FROM y")
df3 = spark.read.table("z")
'''
        result = extract_sql_from_python(code)

        assert len(result["sql_statements"]) == 2
        assert len(result["table_reads"]) == 1

    def test_invalid_python_returns_empty(self):
        code = "this is not valid python {{{"
        result = extract_sql_from_python(code)

        assert result["sql_statements"] == []
        assert result["table_reads"] == []
        assert result["table_writes"] == []


class TestExtractGraphFromPython:
    def test_spark_sql_creates_graph(self):
        code = '''
spark.sql("""
    CREATE TABLE output AS (
        SELECT id, name FROM users
    )
""")
'''
        result = extract_graph_from_python(code, database="db", schema="public")

        assert "db.public.output" in result["nodes"]["tables"]
        assert "db.public.users" in result["nodes"]["tables"]

    def test_multiple_sql_statements_merged(self):
        code = '''
spark.sql("CREATE TABLE a AS (SELECT x FROM b)")
spark.sql("CREATE TABLE c AS (SELECT y FROM d)")
'''
        result = extract_graph_from_python(code, database="db", schema="public")

        assert "db.public.a" in result["nodes"]["tables"]
        assert "db.public.b" in result["nodes"]["tables"]
        assert "db.public.c" in result["nodes"]["tables"]
        assert "db.public.d" in result["nodes"]["tables"]

    def test_source_file_propagates(self):
        code = 'spark.sql("CREATE TABLE x AS (SELECT a FROM y)")'
        result = extract_graph_from_python(code, database="db", schema="public", source_file="jobs/etl.py")

        assert result["nodes"]["tables"]["db.public.x"]["source_file"] == "jobs/etl.py"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
