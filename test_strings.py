SQL_1 = "SELECT id, name FROM users"

SQL_2 = """
SELECT a.id, b.name
FROM orders a
JOIN customers b ON a.customer_id = b.id
"""

SQL_3 = """
CREATE TABLE cleaned_users AS (
    SELECT
        UPPER(name) AS name,
        LOWER(email) AS email,
        created_at
    FROM raw_users
    WHERE active = true
)
"""

PYSPARK_1 = '''
df = spark.sql("SELECT id, amount FROM transactions")
df.write.saveAsTable("output")
'''

PYSPARK_2 = '''
users = spark.read.table("users")
orders = spark.read.table("orders")
result = users.join(orders, "user_id")
result.write.saveAsTable("user_orders")
'''

SQL_4 = """
WITH active_users AS (
    SELECT id, name, email
    FROM users
    WHERE status = 'active'
),
user_orders AS (
    SELECT user_id, COUNT(*) as order_count
    FROM orders
    GROUP BY user_id
)
CREATE TABLE user_summary AS (
    SELECT
        a.id,
        a.name,
        a.email,
        COALESCE(b.order_count, 0) AS order_count
    FROM active_users a
    LEFT JOIN user_orders b ON a.id = b.user_id
)
"""

MIXED_1 = '''
raw = spark.read.table("raw_events")

enriched = spark.sql("""
    SELECT
        e.event_id,
        e.user_id,
        u.name as user_name,
        e.event_type,
        e.timestamp
    FROM events e
    JOIN users u ON e.user_id = u.id
    WHERE e.timestamp > '2024-01-01'
""")

enriched.write.saveAsTable("enriched_events")
'''

SQL_5 = """
CREATE TABLE sales_report AS (
    SELECT
        r.region_name,
        p.product_name,
        s.salesperson_name,
        SUM(o.quantity) AS total_quantity,
        SUM(o.quantity * o.unit_price) AS total_revenue,
        AVG(o.unit_price) AS avg_price
    FROM orders o
    JOIN products p ON o.product_id = p.id
    JOIN regions r ON o.region_id = r.id
    JOIN salespersons s ON o.salesperson_id = s.id
    JOIN (
        SELECT product_id, MAX(sale_date) as last_sale
        FROM orders
        GROUP BY product_id
    ) latest ON o.product_id = latest.product_id
    WHERE o.status = 'completed'
    GROUP BY r.region_name, p.product_name, s.salesperson_name
    HAVING SUM(o.quantity) > 100
)
"""

PYSPARK_3 = '''
spark.sql("""
    CREATE TABLE dim_customers AS (
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            a.street,
            a.city,
            a.state,
            a.zip_code,
            CASE
                WHEN c.signup_date < '2020-01-01' THEN 'legacy'
                WHEN c.signup_date < '2023-01-01' THEN 'established'
                ELSE 'new'
            END AS customer_segment
        FROM customers c
        LEFT JOIN addresses a ON c.address_id = a.id
    )
""")

spark.sql("""
    CREATE TABLE fact_orders AS (
        SELECT
            o.order_id,
            o.customer_id,
            o.order_date,
            o.ship_date,
            DATEDIFF(o.ship_date, o.order_date) AS fulfillment_days,
            SUM(oi.quantity * oi.unit_price) AS order_total,
            COUNT(oi.item_id) AS item_count
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY o.order_id, o.customer_id, o.order_date, o.ship_date
    )
""")

metrics = spark.read.table("fact_orders")
metrics.write.saveAsTable("order_metrics")
'''

MIXED_2 = '''
import pandas as pd
from pyspark.sql import functions as F

raw_transactions = spark.read.table("raw_transactions")
raw_accounts = spark.read.table("raw_accounts")
raw_customers = spark.read.table("raw_customers")

spark.sql("""
    CREATE TABLE stg_transactions AS (
        SELECT
            transaction_id,
            account_id,
            transaction_type,
            amount,
            currency,
            CASE
                WHEN currency = 'USD' THEN amount
                WHEN currency = 'EUR' THEN amount * 1.08
                WHEN currency = 'GBP' THEN amount * 1.27
                ELSE amount
            END AS amount_usd,
            transaction_date,
            merchant_name,
            merchant_category,
            is_fraud_flag
        FROM raw_transactions
        WHERE transaction_date >= '2024-01-01'
          AND status = 'completed'
    )
""")

spark.sql("""
    CREATE TABLE stg_accounts AS (
        SELECT
            a.account_id,
            a.account_type,
            a.open_date,
            a.status AS account_status,
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            c.phone,
            c.date_of_birth,
            FLOOR(DATEDIFF(CURRENT_DATE, c.date_of_birth) / 365) AS age,
            c.risk_score,
            CASE
                WHEN c.risk_score < 30 THEN 'low'
                WHEN c.risk_score < 70 THEN 'medium'
                ELSE 'high'
            END AS risk_category
        FROM raw_accounts a
        JOIN raw_customers c ON a.customer_id = c.customer_id
        WHERE a.status = 'active'
    )
""")

spark.sql("""
    WITH daily_volumes AS (
        SELECT
            account_id,
            transaction_date,
            SUM(amount_usd) AS daily_total,
            COUNT(*) AS transaction_count,
            SUM(CASE WHEN is_fraud_flag THEN 1 ELSE 0 END) AS fraud_count
        FROM stg_transactions
        GROUP BY account_id, transaction_date
    ),
    account_stats AS (
        SELECT
            account_id,
            AVG(daily_total) AS avg_daily_volume,
            MAX(daily_total) AS max_daily_volume,
            SUM(transaction_count) AS total_transactions,
            SUM(fraud_count) AS total_fraud_flags
        FROM daily_volumes
        GROUP BY account_id
    )
    CREATE TABLE analytics_account_summary AS (
        SELECT
            a.account_id,
            a.account_type,
            a.customer_id,
            a.first_name,
            a.last_name,
            a.age,
            a.risk_category,
            COALESCE(s.avg_daily_volume, 0) AS avg_daily_volume,
            COALESCE(s.max_daily_volume, 0) AS max_daily_volume,
            COALESCE(s.total_transactions, 0) AS total_transactions,
            COALESCE(s.total_fraud_flags, 0) AS total_fraud_flags,
            CASE
                WHEN s.total_fraud_flags > 5 THEN 'review'
                WHEN a.risk_category = 'high' AND s.avg_daily_volume > 10000 THEN 'monitor'
                ELSE 'normal'
            END AS alert_status
        FROM stg_accounts a
        LEFT JOIN account_stats s ON a.account_id = s.account_id
    )
""")

final = spark.read.table("analytics_account_summary")
final.write.saveAsTable("account_summary_final")
'''
