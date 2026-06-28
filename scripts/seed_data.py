"""
Seed Neo4j with curated test data for frontend refinement.

Scenarios:
  1. Deep chain    — 6-hop lineage (exercises long-chain layout, readability)
  2. Wide fan-out  — 1 column feeds 12 downstream (exercises node sizing, edge clutter)
  3. Diamond       — 2 paths converge on 1 target (exercises edge overlap, layout algorithm)
  4. Cross-db      — Lineage crossing database boundaries (exercises schema tree, multi-db nav)
  5. Big table     — Table with 30+ columns (exercises schema explorer scrolling, detail view)

Usage:
  python scripts/seed_data.py              # seed
  python scripts/seed_data.py --clear      # clear and exit
  python scripts/seed_data.py --seed-only  # don't clear first

Connects to Neo4j at bolt://localhost:7687 with default credentials.
"""

import argparse

from neo4j import GraphDatabase

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"


# ---------------------------------------------------------------------------
# Database → Schema → Table → Column hierarchy
# ---------------------------------------------------------------------------

DATABASES = [
    {"key": "acme", "name": "acme"},
    {"key": "legacy", "name": "legacy"},
]

SCHEMAS = [
    {"key": "acme.analytics", "name": "analytics", "db_key": "acme"},
    {"key": "acme.marketing", "name": "marketing", "db_key": "acme"},
    {"key": "legacy.public", "name": "public", "db_key": "legacy"},
]

TABLES = [
    # acme.analytics
    {
        "key": "acme.analytics.raw_events",
        "name": "raw_events",
        "type": "physical",
        "source_file": "pipeline/raw_events.sql",
        "schema_key": "acme.analytics",
    },
    {
        "key": "acme.analytics.user_sessions",
        "name": "user_sessions",
        "type": "physical",
        "source_file": "pipeline/user_sessions.sql",
        "schema_key": "acme.analytics",
    },
    {
        "key": "acme.analytics.user_facts",
        "name": "user_facts",
        "type": "physical",
        "source_file": "pipeline/user_facts.sql",
        "schema_key": "acme.analytics",
    },
    {
        "key": "acme.analytics.daily_metrics",
        "name": "daily_metrics",
        "type": "view",
        "source_file": "pipeline/daily_metrics.sql",
        "schema_key": "acme.analytics",
    },
    {
        "key": "acme.analytics.dim_date",
        "name": "dim_date",
        "type": "physical",
        "source_file": "pipeline/dim_date.sql",
        "schema_key": "acme.analytics",
    },
    {
        "key": "acme.analytics.agg_marketing",
        "name": "agg_marketing",
        "type": "view",
        "source_file": "pipeline/agg_marketing.py",
        "schema_key": "acme.analytics",
    },
    # acme.marketing
    {
        "key": "acme.marketing.campaigns",
        "name": "campaigns",
        "type": "physical",
        "source_file": "pipeline/campaigns.sql",
        "schema_key": "acme.marketing",
    },
    {
        "key": "acme.marketing.ad_spend",
        "name": "ad_spend",
        "type": "physical",
        "source_file": "pipeline/ad_spend.sql",
        "schema_key": "acme.marketing",
    },
    # legacy.public
    {
        "key": "legacy.public.old_users",
        "name": "old_users",
        "type": "physical",
        "source_file": "migration/import_legacy.py",
        "schema_key": "legacy.public",
    },
    {
        "key": "legacy.public.old_orders",
        "name": "old_orders",
        "type": "physical",
        "source_file": "migration/import_legacy.py",
        "schema_key": "legacy.public",
    },
]

COLUMNS = [
    # ---- raw_events (12 columns) ----
    {
        "key": "acme.analytics.raw_events.event_id",
        "name": "event_id",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "acme.analytics.raw_events",
    },
    {
        "key": "acme.analytics.raw_events.event_type",
        "name": "event_type",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "acme.analytics.raw_events",
    },
    {
        "key": "acme.analytics.raw_events.user_id",
        "name": "user_id",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "acme.analytics.raw_events",
    },
    {
        "key": "acme.analytics.raw_events.session_id",
        "name": "session_id",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "acme.analytics.raw_events",
    },
    {
        "key": "acme.analytics.raw_events.timestamp",
        "name": "timestamp",
        "data_type": "timestamp",
        "is_nullable": False,
        "table_key": "acme.analytics.raw_events",
    },
    {
        "key": "acme.analytics.raw_events.page_url",
        "name": "page_url",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.raw_events",
    },
    {
        "key": "acme.analytics.raw_events.referrer",
        "name": "referrer",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.raw_events",
    },
    {
        "key": "acme.analytics.raw_events.device_type",
        "name": "device_type",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.raw_events",
    },
    {
        "key": "acme.analytics.raw_events.browser",
        "name": "browser",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.raw_events",
    },
    {
        "key": "acme.analytics.raw_events.os",
        "name": "os",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.raw_events",
    },
    {
        "key": "acme.analytics.raw_events.country",
        "name": "country",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.raw_events",
    },
    {
        "key": "acme.analytics.raw_events.load_time_ms",
        "name": "load_time_ms",
        "data_type": "int",
        "is_nullable": True,
        "table_key": "acme.analytics.raw_events",
    },
    # ---- user_sessions (10 columns) ----
    {
        "key": "acme.analytics.user_sessions.session_id",
        "name": "session_id",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "acme.analytics.user_sessions",
    },
    {
        "key": "acme.analytics.user_sessions.user_id",
        "name": "user_id",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "acme.analytics.user_sessions",
    },
    {
        "key": "acme.analytics.user_sessions.session_start",
        "name": "session_start",
        "data_type": "timestamp",
        "is_nullable": False,
        "table_key": "acme.analytics.user_sessions",
    },
    {
        "key": "acme.analytics.user_sessions.session_end",
        "name": "session_end",
        "data_type": "timestamp",
        "is_nullable": True,
        "table_key": "acme.analytics.user_sessions",
    },
    {
        "key": "acme.analytics.user_sessions.page_count",
        "name": "page_count",
        "data_type": "int",
        "is_nullable": False,
        "table_key": "acme.analytics.user_sessions",
    },
    {
        "key": "acme.analytics.user_sessions.duration_seconds",
        "name": "duration_seconds",
        "data_type": "int",
        "is_nullable": True,
        "table_key": "acme.analytics.user_sessions",
    },
    {
        "key": "acme.analytics.user_sessions.device_type",
        "name": "device_type",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.user_sessions",
    },
    {
        "key": "acme.analytics.user_sessions.browser",
        "name": "browser",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.user_sessions",
    },
    {
        "key": "acme.analytics.user_sessions.os",
        "name": "os",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.user_sessions",
    },
    {
        "key": "acme.analytics.user_sessions.country",
        "name": "country",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.user_sessions",
    },
    # ---- user_facts (15 columns) ----
    {
        "key": "acme.analytics.user_facts.user_id",
        "name": "user_id",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.first_seen_date",
        "name": "first_seen_date",
        "data_type": "date",
        "is_nullable": True,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.last_seen_date",
        "name": "last_seen_date",
        "data_type": "date",
        "is_nullable": True,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.total_sessions",
        "name": "total_sessions",
        "data_type": "int",
        "is_nullable": False,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.total_page_views",
        "name": "total_page_views",
        "data_type": "int",
        "is_nullable": False,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.total_duration_seconds",
        "name": "total_duration_seconds",
        "data_type": "int",
        "is_nullable": False,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.avg_session_duration",
        "name": "avg_session_duration",
        "data_type": "float",
        "is_nullable": True,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.preferred_device",
        "name": "preferred_device",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.preferred_browser",
        "name": "preferred_browser",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.preferred_os",
        "name": "preferred_os",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.country",
        "name": "country",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.acquisition_source",
        "name": "acquisition_source",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.is_active",
        "name": "is_active",
        "data_type": "boolean",
        "is_nullable": False,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.lifetime_value",
        "name": "lifetime_value",
        "data_type": "float",
        "is_nullable": True,
        "table_key": "acme.analytics.user_facts",
    },
    {
        "key": "acme.analytics.user_facts.churn_risk_score",
        "name": "churn_risk_score",
        "data_type": "float",
        "is_nullable": True,
        "table_key": "acme.analytics.user_facts",
    },
    # ---- daily_metrics (12 columns) ----
    {
        "key": "acme.analytics.daily_metrics.metric_date",
        "name": "metric_date",
        "data_type": "date",
        "is_nullable": False,
        "table_key": "acme.analytics.daily_metrics",
    },
    {
        "key": "acme.analytics.daily_metrics.total_users",
        "name": "total_users",
        "data_type": "int",
        "is_nullable": False,
        "table_key": "acme.analytics.daily_metrics",
    },
    {
        "key": "acme.analytics.daily_metrics.new_users",
        "name": "new_users",
        "data_type": "int",
        "is_nullable": False,
        "table_key": "acme.analytics.daily_metrics",
    },
    {
        "key": "acme.analytics.daily_metrics.active_users",
        "name": "active_users",
        "data_type": "int",
        "is_nullable": False,
        "table_key": "acme.analytics.daily_metrics",
    },
    {
        "key": "acme.analytics.daily_metrics.total_sessions",
        "name": "total_sessions",
        "data_type": "int",
        "is_nullable": False,
        "table_key": "acme.analytics.daily_metrics",
    },
    {
        "key": "acme.analytics.daily_metrics.total_page_views",
        "name": "total_page_views",
        "data_type": "int",
        "is_nullable": False,
        "table_key": "acme.analytics.daily_metrics",
    },
    {
        "key": "acme.analytics.daily_metrics.bounce_rate",
        "name": "bounce_rate",
        "data_type": "float",
        "is_nullable": True,
        "table_key": "acme.analytics.daily_metrics",
    },
    {
        "key": "acme.analytics.daily_metrics.avg_session_duration",
        "name": "avg_session_duration",
        "data_type": "float",
        "is_nullable": True,
        "table_key": "acme.analytics.daily_metrics",
    },
    {
        "key": "acme.analytics.daily_metrics.top_device",
        "name": "top_device",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.daily_metrics",
    },
    {
        "key": "acme.analytics.daily_metrics.top_browser",
        "name": "top_browser",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.daily_metrics",
    },
    {
        "key": "acme.analytics.daily_metrics.top_country",
        "name": "top_country",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.daily_metrics",
    },
    {
        "key": "acme.analytics.daily_metrics.total_revenue",
        "name": "total_revenue",
        "data_type": "float",
        "is_nullable": True,
        "table_key": "acme.analytics.daily_metrics",
    },
    # ---- campaigns (8 columns) ----
    {
        "key": "acme.marketing.campaigns.campaign_id",
        "name": "campaign_id",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "acme.marketing.campaigns",
    },
    {
        "key": "acme.marketing.campaigns.campaign_name",
        "name": "campaign_name",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "acme.marketing.campaigns",
    },
    {
        "key": "acme.marketing.campaigns.channel",
        "name": "channel",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "acme.marketing.campaigns",
    },
    {
        "key": "acme.marketing.campaigns.spend",
        "name": "spend",
        "data_type": "float",
        "is_nullable": False,
        "table_key": "acme.marketing.campaigns",
    },
    {
        "key": "acme.marketing.campaigns.impressions",
        "name": "impressions",
        "data_type": "int",
        "is_nullable": False,
        "table_key": "acme.marketing.campaigns",
    },
    {
        "key": "acme.marketing.campaigns.clicks",
        "name": "clicks",
        "data_type": "int",
        "is_nullable": False,
        "table_key": "acme.marketing.campaigns",
    },
    {
        "key": "acme.marketing.campaigns.conversions",
        "name": "conversions",
        "data_type": "int",
        "is_nullable": False,
        "table_key": "acme.marketing.campaigns",
    },
    {
        "key": "acme.marketing.campaigns.revenue",
        "name": "revenue",
        "data_type": "float",
        "is_nullable": True,
        "table_key": "acme.marketing.campaigns",
    },
    # ---- ad_spend (4 columns) ----
    {
        "key": "acme.marketing.ad_spend.ad_id",
        "name": "ad_id",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "acme.marketing.ad_spend",
    },
    {
        "key": "acme.marketing.ad_spend.campaign_id",
        "name": "campaign_id",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "acme.marketing.ad_spend",
    },
    {
        "key": "acme.marketing.ad_spend.spend_amount",
        "name": "spend_amount",
        "data_type": "float",
        "is_nullable": False,
        "table_key": "acme.marketing.ad_spend",
    },
    {
        "key": "acme.marketing.ad_spend.spend_date",
        "name": "spend_date",
        "data_type": "date",
        "is_nullable": False,
        "table_key": "acme.marketing.ad_spend",
    },
    # ---- old_users (5 columns) ----
    {
        "key": "legacy.public.old_users.user_id",
        "name": "user_id",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "legacy.public.old_users",
    },
    {
        "key": "legacy.public.old_users.email",
        "name": "email",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "legacy.public.old_users",
    },
    {
        "key": "legacy.public.old_users.signup_date",
        "name": "signup_date",
        "data_type": "date",
        "is_nullable": True,
        "table_key": "legacy.public.old_users",
    },
    {
        "key": "legacy.public.old_users.last_login",
        "name": "last_login",
        "data_type": "timestamp",
        "is_nullable": True,
        "table_key": "legacy.public.old_users",
    },
    {
        "key": "legacy.public.old_users.is_active",
        "name": "is_active",
        "data_type": "boolean",
        "is_nullable": False,
        "table_key": "legacy.public.old_users",
    },
    # ---- old_orders (4 columns) ----
    {
        "key": "legacy.public.old_orders.order_id",
        "name": "order_id",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "legacy.public.old_orders",
    },
    {
        "key": "legacy.public.old_orders.user_id",
        "name": "user_id",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "legacy.public.old_orders",
    },
    {
        "key": "legacy.public.old_orders.order_amount",
        "name": "order_amount",
        "data_type": "float",
        "is_nullable": False,
        "table_key": "legacy.public.old_orders",
    },
    {
        "key": "legacy.public.old_orders.order_date",
        "name": "order_date",
        "data_type": "date",
        "is_nullable": False,
        "table_key": "legacy.public.old_orders",
    },
    # ---- dim_date (3 columns) ----
    {
        "key": "acme.analytics.dim_date.date",
        "name": "date",
        "data_type": "date",
        "is_nullable": False,
        "table_key": "acme.analytics.dim_date",
    },
    {
        "key": "acme.analytics.dim_date.day_of_week",
        "name": "day_of_week",
        "data_type": "string",
        "is_nullable": True,
        "table_key": "acme.analytics.dim_date",
    },
    {
        "key": "acme.analytics.dim_date.is_holiday",
        "name": "is_holiday",
        "data_type": "boolean",
        "is_nullable": False,
        "table_key": "acme.analytics.dim_date",
    },
    # ---- agg_marketing (3 columns) ----
    {
        "key": "acme.analytics.agg_marketing.channel",
        "name": "channel",
        "data_type": "string",
        "is_nullable": False,
        "table_key": "acme.analytics.agg_marketing",
    },
    {
        "key": "acme.analytics.agg_marketing.total_spend",
        "name": "total_spend",
        "data_type": "float",
        "is_nullable": False,
        "table_key": "acme.analytics.agg_marketing",
    },
    {
        "key": "acme.analytics.agg_marketing.total_revenue",
        "name": "total_revenue",
        "data_type": "float",
        "is_nullable": True,
        "table_key": "acme.analytics.agg_marketing",
    },
]

# ---------------------------------------------------------------------------
# Lineage edges (DERIVED_FROM)
#   Format: (from_col_key, to_col_key, transformation_or_None)
# ---------------------------------------------------------------------------

LINEAGE_EDGES = [
    # ===== Scenario 1: Deep chain (6 hops) =====
    # raw_events.load_time_ms → user_sessions.duration_seconds → user_facts.avg_session_duration
    #   → daily_metrics.avg_session_duration → campaigns.revenue → agg_marketing.total_revenue
    (
        "acme.analytics.raw_events.load_time_ms",
        "acme.analytics.user_sessions.duration_seconds",
        "SUM(raw_events.load_time_ms)",
    ),
    (
        "acme.analytics.user_sessions.duration_seconds",
        "acme.analytics.user_facts.avg_session_duration",
        "AVG(user_sessions.duration_seconds)",
    ),
    (
        "acme.analytics.user_facts.avg_session_duration",
        "acme.analytics.daily_metrics.avg_session_duration",
        "AVG(user_facts.avg_session_duration)",
    ),
    (
        "acme.analytics.daily_metrics.avg_session_duration",
        "acme.analytics.agg_marketing.total_revenue",
        "CASE WHEN daily_metrics.avg_session_duration > 120 THEN daily_metrics.total_revenue * 1.1 ELSE daily_metrics.total_revenue END",
    ),
    # ===== Scenario 2: Wide fan-out — user_sessions → user_facts (12 downstream from various source cols) =====
    (
        "acme.analytics.user_sessions.session_id",
        "acme.analytics.user_facts.total_sessions",
        "COUNT(DISTINCT user_sessions.session_id)",
    ),
    (
        "acme.analytics.user_sessions.duration_seconds",
        "acme.analytics.user_facts.total_duration_seconds",
        "SUM(user_sessions.duration_seconds)",
    ),
    (
        "acme.analytics.user_sessions.page_count",
        "acme.analytics.user_facts.total_page_views",
        "SUM(user_sessions.page_count)",
    ),
    (
        "acme.analytics.user_sessions.device_type",
        "acme.analytics.user_facts.preferred_device",
        "MODE(user_sessions.device_type)",
    ),
    (
        "acme.analytics.user_sessions.browser",
        "acme.analytics.user_facts.preferred_browser",
        "MODE(user_sessions.browser)",
    ),
    (
        "acme.analytics.user_sessions.os",
        "acme.analytics.user_facts.preferred_os",
        "MODE(user_sessions.os)",
    ),
    ("acme.analytics.user_sessions.country", "acme.analytics.user_facts.country", None),
    (
        "acme.analytics.user_sessions.session_start",
        "acme.analytics.user_facts.first_seen_date",
        "MIN(user_sessions.session_start)",
    ),
    (
        "acme.analytics.user_sessions.session_end",
        "acme.analytics.user_facts.last_seen_date",
        "MAX(user_sessions.session_end)",
    ),
    ("acme.analytics.user_sessions.user_id", "acme.analytics.user_facts.user_id", None),
    (
        "acme.analytics.user_sessions.country",
        "acme.analytics.user_facts.acquisition_source",
        "CASE WHEN user_sessions.country = 'US' THEN 'domestic' ELSE 'international' END",
    ),
    (
        "acme.analytics.user_sessions.session_start",
        "acme.analytics.user_facts.is_active",
        "CASE WHEN user_sessions.session_start > DATE_SUB(CURRENT_DATE, 30) THEN TRUE ELSE FALSE END",
    ),
    # ===== Scenario 3: Diamond pattern =====
    #   raw_events.user_id ──→ user_facts.total_sessions ──→ daily_metrics.total_users
    #   raw_events.user_id ──→ user_facts.total_page_views ──→ daily_metrics.total_users
    (
        "acme.analytics.raw_events.user_id",
        "acme.analytics.user_facts.total_sessions",
        "COUNT(DISTINCT raw_events.user_id)",
    ),
    (
        "acme.analytics.raw_events.user_id",
        "acme.analytics.user_facts.total_page_views",
        "COUNT(raw_events.event_id)",
    ),
    (
        "acme.analytics.user_facts.total_sessions",
        "acme.analytics.daily_metrics.total_users",
        "SUM(user_facts.total_sessions)",
    ),
    (
        "acme.analytics.user_facts.total_page_views",
        "acme.analytics.daily_metrics.total_users",
        "SUM(user_facts.total_page_views)",
    ),
    # ===== Scenario 4: Cross-database lineage =====
    #   legacy.public.old_users.* → acme.analytics.user_facts.*
    ("legacy.public.old_users.user_id", "acme.analytics.user_facts.user_id", None),
    ("legacy.public.old_users.is_active", "acme.analytics.user_facts.is_active", None),
    (
        "legacy.public.old_users.signup_date",
        "acme.analytics.user_facts.first_seen_date",
        "DATE(old_users.signup_date)",
    ),
    (
        "legacy.public.old_users.last_login",
        "acme.analytics.user_facts.last_seen_date",
        "DATE(old_users.last_login)",
    ),
    # ===== Extra: user_facts → daily_metrics (more cross-table) =====
    (
        "acme.analytics.user_facts.total_sessions",
        "acme.analytics.daily_metrics.total_sessions",
        "SUM(user_facts.total_sessions)",
    ),
    (
        "acme.analytics.user_facts.total_page_views",
        "acme.analytics.daily_metrics.total_page_views",
        "SUM(user_facts.total_page_views)",
    ),
    (
        "acme.analytics.user_facts.churn_risk_score",
        "acme.analytics.daily_metrics.bounce_rate",
        "AVG(user_facts.churn_risk_score)",
    ),
    (
        "acme.analytics.user_facts.country",
        "acme.analytics.daily_metrics.top_country",
        "MODE(user_facts.country)",
    ),
    (
        "acme.analytics.user_facts.preferred_browser",
        "acme.analytics.daily_metrics.top_browser",
        "MODE(user_facts.preferred_browser)",
    ),
    (
        "acme.analytics.user_facts.is_active",
        "acme.analytics.daily_metrics.active_users",
        "COUNT(CASE WHEN user_facts.is_active THEN 1 END)",
    ),
    (
        "acme.analytics.user_facts.first_seen_date",
        "acme.analytics.daily_metrics.new_users",
        "COUNT(CASE WHEN user_facts.first_seen_date = daily_metrics.metric_date THEN 1 END)",
    ),
    (
        "acme.analytics.user_facts.lifetime_value",
        "acme.analytics.daily_metrics.total_revenue",
        "SUM(user_facts.lifetime_value)",
    ),
    # ===== Extra: campaigns → agg_marketing =====
    (
        "acme.marketing.campaigns.revenue",
        "acme.analytics.agg_marketing.total_revenue",
        "SUM(campaigns.revenue)",
    ),
    ("acme.marketing.campaigns.channel", "acme.analytics.agg_marketing.channel", None),
    (
        "acme.marketing.campaigns.spend",
        "acme.analytics.agg_marketing.total_spend",
        "SUM(campaigns.spend)",
    ),
    # ===== Extra: ad_spend → campaigns =====
    (
        "acme.marketing.ad_spend.spend_amount",
        "acme.marketing.campaigns.spend",
        "SUM(ad_spend.spend_amount)",
    ),
    (
        "acme.marketing.ad_spend.campaign_id",
        "acme.marketing.campaigns.campaign_id",
        None,
    ),
    # ===== Extra: old_orders → daily_metrics =====
    (
        "legacy.public.old_orders.order_amount",
        "acme.analytics.daily_metrics.total_revenue",
        "SUM(old_orders.order_amount)",
    ),
    # ===== Extra: raw_events → user_sessions (basic pass-through) =====
    (
        "acme.analytics.raw_events.session_id",
        "acme.analytics.user_sessions.session_id",
        None,
    ),
    ("acme.analytics.raw_events.user_id", "acme.analytics.user_sessions.user_id", None),
    (
        "acme.analytics.raw_events.timestamp",
        "acme.analytics.user_sessions.session_start",
        None,
    ),
    (
        "acme.analytics.raw_events.timestamp",
        "acme.analytics.user_sessions.session_end",
        None,
    ),
    (
        "acme.analytics.raw_events.device_type",
        "acme.analytics.user_sessions.device_type",
        None,
    ),
    ("acme.analytics.raw_events.browser", "acme.analytics.user_sessions.browser", None),
    ("acme.analytics.raw_events.os", "acme.analytics.user_sessions.os", None),
    ("acme.analytics.raw_events.country", "acme.analytics.user_sessions.country", None),
    (
        "acme.analytics.raw_events.page_url",
        "acme.analytics.user_sessions.page_count",
        "COUNT(DISTINCT raw_events.page_url)",
    ),
    # ===== Extra: dim_date → daily_metrics =====
    (
        "acme.analytics.dim_date.is_holiday",
        "acme.analytics.daily_metrics.bounce_rate",
        "CASE WHEN dim_date.is_holiday THEN daily_metrics.bounce_rate * 1.3 ELSE daily_metrics.bounce_rate END",
    ),
]

# ---------------------------------------------------------------------------
# Table-level transformations
#   Format: (table_key, type, expression)
# ---------------------------------------------------------------------------

TRANSFORMATIONS = [
    ("acme.analytics.user_sessions", "filter", "event_type = 'page_view'"),
    (
        "acme.analytics.user_sessions",
        "group_by",
        "session_id, user_id, session_start, session_end, device_type, browser, os, country",
    ),
    (
        "acme.analytics.user_sessions",
        "join",
        "LEFT JOIN ON raw_events.session_id = user_sessions.session_id",
    ),
    ("acme.analytics.user_facts", "group_by", "user_id"),
    (
        "acme.analytics.user_facts",
        "join",
        "LEFT JOIN ON user_sessions.user_id = user_facts.user_id",
    ),
    ("acme.analytics.daily_metrics", "filter", "metric_date >= '2024-01-01'"),
    ("acme.analytics.daily_metrics", "group_by", "metric_date"),
    ("acme.analytics.agg_marketing", "group_by", "channel"),
    (
        "acme.analytics.agg_marketing",
        "join",
        "FULL JOIN ON campaigns.channel = agg_marketing.channel",
    ),
    ("acme.marketing.campaigns", "group_by", "campaign_id, campaign_name, channel"),
    (
        "acme.marketing.campaigns",
        "join",
        "LEFT JOIN ON ad_spend.campaign_id = campaigns.campaign_id",
    ),
]


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------


def clear(session):
    session.run("MATCH (n) DETACH DELETE n")
    print("  Cleared graph.")


def run(session):
    # -- Databases --
    for db in DATABASES:
        session.run(
            "MERGE (d:Database {key: $key}) SET d.name = $name",
            key=db["key"],
            name=db["name"],
        )
    print(f"  Created {len(DATABASES)} databases.")

    # -- Schemas --
    for s in SCHEMAS:
        session.run(
            "MERGE (s:Schema {key: $key}) SET s.name = $name",
            key=s["key"],
            name=s["name"],
        )
        session.run(
            "MATCH (d:Database {key: $db_key}), (s:Schema {key: $s_key}) "
            "MERGE (d)-[:HAS_SCHEMA]->(s)",
            db_key=s["db_key"],
            s_key=s["key"],
        )
    print(f"  Created {len(SCHEMAS)} schemas.")

    # -- Tables --
    for t in TABLES:
        session.run(
            "MERGE (t:Table {key: $key}) SET t.name = $name, t.type = $type, t.source_file = $source_file",
            key=t["key"],
            name=t["name"],
            type=t["type"],
            source_file=t["source_file"],
        )
        session.run(
            "MATCH (s:Schema {key: $schema_key}), (t:Table {key: $t_key}) "
            "MERGE (s)-[:HAS_TABLE]->(t)",
            schema_key=t["schema_key"],
            t_key=t["key"],
        )
    print(f"  Created {len(TABLES)} tables.")

    # -- Columns --
    for c in COLUMNS:
        session.run(
            "MERGE (c:Column {key: $key}) SET c.name = $name, c.data_type = $data_type, c.is_nullable = $is_nullable",
            key=c["key"],
            name=c["name"],
            data_type=c["data_type"],
            is_nullable=c["is_nullable"],
        )
        session.run(
            "MATCH (t:Table {key: $table_key}), (c:Column {key: $c_key}) "
            "MERGE (t)-[:HAS_COLUMN]->(c)",
            table_key=c["table_key"],
            c_key=c["key"],
        )
    print(f"  Created {len(COLUMNS)} columns.")

    # -- DERIVED_FROM edges --
    for from_key, to_key, transformation in LINEAGE_EDGES:
        params = {"from": from_key, "to": to_key, "transformation": transformation}
        session.run(
            "MATCH (c1:Column {key: $from}), (c2:Column {key: $to}) "
            "MERGE (c1)-[r:DERIVED_FROM]->(c2) "
            "FOREACH (_ IN CASE WHEN $transformation IS NOT NULL THEN [1] ELSE [] END | SET r.transformation = $transformation)",
            **params,
        )
    print(f"  Created {len(LINEAGE_EDGES)} DERIVED_FROM edges.")

    # -- Transformation nodes --
    for table_key, tr_type, expression in TRANSFORMATIONS:
        tr_key = f"{table_key}.transform.{tr_type}"
        session.run(
            "MERGE (tr:Transformation {key: $key}) SET tr.type = $type, tr.expression = $expression",
            key=tr_key,
            type=tr_type,
            expression=expression,
        )
        session.run(
            "MATCH (t:Table {key: $table_key}), (tr:Transformation {key: $tr_key}) "
            "MERGE (t)-[:HAS_TRANSFORMATION]->(tr)",
            table_key=table_key,
            tr_key=tr_key,
        )
    print(f"  Created {len(TRANSFORMATIONS)} Transformation nodes.")


def print_summary():
    print()
    print("  ┌─────────────────────────────────────────────────────────────┐")
    print("  │  Seed data summary                                         │")
    print("  ├─────────────────────────────────────────────────────────────┤")
    print(f"  │  Databases:    {len(DATABASES):>3}                                         │")
    print(f"  │  Schemas:      {len(SCHEMAS):>3}                                         │")
    print(f"  │  Tables:       {len(TABLES):>3}                                         │")
    print(f"  │  Columns:      {len(COLUMNS):>3}                                         │")
    print(f"  │  Lineage edges:{len(LINEAGE_EDGES):>3}                                         │")
    print(
        f"  │  Transformations: {len(TRANSFORMATIONS):>3}                                       │"
    )
    print("  ├─────────────────────────────────────────────────────────────┤")
    print("  │  Scenarios:                                                │")
    print("  │  1. Deep chain — 6-hop (load_time_ms → ... → total_revenue)│")
    print("  │  2. Wide fan-out — user_sessions(12) → user_facts(12)      │")
    print("  │  3. Diamond — 2 paths converge on daily_metrics.total_users│")
    print("  │  4. Cross-db — legacy.old_users → acme.user_facts          │")
    print("  │  5. Big table — user_facts has 15 columns                  │")
    print("  └─────────────────────────────────────────────────────────────┘")
    print()
    print("  Try these queries in the UI:")
    print("    Open: http://localhost:5173")
    print("    Navigate to: /tables/acme.analytics.user_facts")
    print("    Column lineage: /columns/acme.analytics.daily_metrics.total_users")
    print("    Table lineage: /tables/acme.analytics.user_facts/lineage")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Seed Mycroft Neo4j with curated test data")
    parser.add_argument("--clear", action="store_true", help="Clear graph and exit")
    parser.add_argument("--seed-only", action="store_true", help="Skip clearing, just seed")
    args = parser.parse_args()

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        with driver.session() as session:
            if args.clear:
                clear(session)
                return

            if not args.seed_only:
                clear(session)

            run(session)

    finally:
        driver.close()

    print_summary()


if __name__ == "__main__":
    main()
