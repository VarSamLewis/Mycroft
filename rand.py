from sqlglot import exp, parse_one

SQL_CTAS = "CREATE TABLE users (id INT, name VARCHAR(100), created_at TIMESTAMP)"

ast = parse_one(SQL_CTAS)

for col_def in ast.find_all(exp.ColumnDef):
    print(col_def.name, col_def.args["kind"].sql())
