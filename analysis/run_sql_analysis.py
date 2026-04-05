"""
Loads the four synthetic CSVs into an in-memory SQLite database,
runs each retention analysis query, and writes results to analysis/output/.

Usage:
    python analysis/run_sql_analysis.py
"""
from pathlib import Path
import sqlite3

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "analysis" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


QUERIES = {
    "01_executive_overview_la": """
        SELECT
          substr(o.order_date, 1, 7) AS order_month,
          COUNT(DISTINCT o.user_id) AS active_users,
          COUNT(*) AS total_orders,
          ROUND(CAST(COUNT(*) AS REAL) / COUNT(DISTINCT o.user_id), 2) AS avg_orders_per_user,
          ROUND(AVG(o.order_value), 2) AS avg_order_value,
          ROUND(AVG(CASE WHEN o.promo_applied THEN 1.0 ELSE 0.0 END), 3) AS promo_order_share,
          ROUND(AVG(o.delivery_time_min), 1) AS avg_delivery_time_min,
          ROUND(AVG(CASE WHEN o.is_late_delivery THEN 1.0 ELSE 0.0 END), 3) AS late_delivery_rate
        FROM orders o
        JOIN users u
          ON o.user_id = u.user_id
        WHERE u.city = 'Los Angeles'
        GROUP BY 1
        ORDER BY 1
    """,
    "02_cohort_retention_la": """
        WITH first_orders AS (
          SELECT
            u.user_id,
            u.first_order_date,
            substr(u.first_order_date, 1, 7) AS cohort_month
          FROM users u
          WHERE u.city = 'Los Angeles'
        ),
        retention_flag AS (
          SELECT
            f.user_id,
            f.cohort_month,
            CASE
              WHEN EXISTS (
                SELECT 1
                FROM orders o
                WHERE o.user_id = f.user_id
                  AND o.order_date > f.first_order_date
                  AND o.order_date <= date(f.first_order_date, '+30 day')
              ) THEN 1 ELSE 0
            END AS retained_30d
          FROM first_orders f
        )
        SELECT
          cohort_month,
          COUNT(*) AS users_in_cohort,
          SUM(retained_30d) AS retained_users_30d,
          ROUND(AVG(retained_30d * 1.0), 3) AS retention_30d_rate
        FROM retention_flag
        GROUP BY 1
        ORDER BY 1
    """,
    "03_segment_performance_la": """
        WITH la_monthly AS (
          SELECT m.*
          FROM user_monthly_metrics m
          JOIN users u
            ON m.user_id = u.user_id
          WHERE u.city = 'Los Angeles'
        )
        SELECT
          segment_label,
          COUNT(*) AS month_rows,
          ROUND(AVG(orders_count), 2) AS avg_orders_count,
          ROUND(AVG(avg_order_value), 2) AS avg_order_value,
          ROUND(AVG(promo_order_share), 3) AS avg_promo_order_share,
          ROUND(AVG(avg_delivery_time_min), 1) AS avg_delivery_time_min,
          ROUND(AVG(local_merchant_order_share), 3) AS avg_local_merchant_order_share,
          ROUND(AVG(CASE WHEN retained_next_30d THEN 1.0 ELSE 0.0 END), 3) AS retention_next_30d_rate
        FROM la_monthly
        GROUP BY 1
        ORDER BY retention_next_30d_rate DESC
    """,
    "04_merchant_mix_la": """
        WITH user_order_counts AS (
          SELECT
            user_id,
            COUNT(*) AS total_orders
          FROM orders
          GROUP BY 1
        ),
        la_orders AS (
          SELECT
            o.*,
            m.merchant_type,
            m.is_local_brand,
            m.is_trend_driven,
            CASE WHEN uoc.total_orders > 1 THEN 1 ELSE 0 END AS repeat_user_flag
          FROM orders o
          JOIN users u
            ON o.user_id = u.user_id
          JOIN merchants m
            ON o.merchant_id = m.merchant_id
          JOIN user_order_counts uoc
            ON o.user_id = uoc.user_id
          WHERE u.city = 'Los Angeles'
        )
        SELECT
          merchant_type,
          is_local_brand,
          is_trend_driven,
          COUNT(*) AS orders,
          ROUND(AVG(order_value), 2) AS avg_order_value,
          ROUND(AVG(repeat_user_flag * 1.0), 3) AS repeat_user_share
        FROM la_orders
        GROUP BY 1, 2, 3
        ORDER BY orders DESC, repeat_user_share DESC
    """,
    "05_experience_impact_la": """
        WITH la_monthly AS (
          SELECT m.*
          FROM user_monthly_metrics m
          JOIN users u
            ON m.user_id = u.user_id
          WHERE u.city = 'Los Angeles'
        )
        SELECT
          CASE
            WHEN avg_delivery_time_min >= 42 THEN 'Slow delivery months'
            WHEN avg_delivery_time_min >= 36 THEN 'Medium delivery months'
            ELSE 'Fast delivery months'
          END AS delivery_experience_bucket,
          COUNT(*) AS month_rows,
          ROUND(AVG(avg_delivery_time_min), 1) AS avg_delivery_time_min,
          ROUND(AVG(CASE WHEN retained_next_30d THEN 1.0 ELSE 0.0 END), 3) AS retention_rate
        FROM la_monthly
        GROUP BY 1
        ORDER BY month_rows DESC
    """,
    "06_promo_dependency_la": """
        WITH user_promo AS (
          SELECT
            o.user_id,
            AVG(CASE WHEN o.promo_applied THEN 1.0 ELSE 0.0 END) AS promo_share
          FROM orders o
          JOIN users u
            ON o.user_id = u.user_id
          WHERE u.city = 'Los Angeles'
          GROUP BY 1
        ),
        user_retention AS (
          SELECT
            user_id,
            MAX(CASE WHEN retained_next_30d THEN 1 ELSE 0 END) AS retained_flag
          FROM user_monthly_metrics
          GROUP BY 1
        )
        SELECT
          CASE
            WHEN up.promo_share >= 0.70 THEN 'High promo dependency'
            WHEN up.promo_share >= 0.30 THEN 'Medium promo dependency'
            ELSE 'Low promo dependency'
          END AS promo_bucket,
          COUNT(*) AS users,
          ROUND(AVG(up.promo_share), 3) AS avg_promo_share,
          ROUND(AVG(ur.retained_flag * 1.0), 3) AS retention_rate
        FROM user_promo up
        JOIN user_retention ur
          ON up.user_id = ur.user_id
        GROUP BY 1
        ORDER BY users DESC
    """,
}


def load_tables(conn: sqlite3.Connection) -> None:
    pd.read_csv(DATA_DIR / "users.csv").to_sql("users", conn, index=False, if_exists="replace")
    pd.read_csv(DATA_DIR / "merchants.csv").to_sql("merchants", conn, index=False, if_exists="replace")
    pd.read_csv(DATA_DIR / "orders.csv").to_sql("orders", conn, index=False, if_exists="replace")
    pd.read_csv(DATA_DIR / "user_monthly_metrics.csv").to_sql(
        "user_monthly_metrics", conn, index=False, if_exists="replace"
    )


def main() -> None:
    conn = sqlite3.connect(":memory:")
    load_tables(conn)

    for name, query in QUERIES.items():
        df = pd.read_sql_query(query, conn)
        out_path = OUTPUT_DIR / f"{name}.csv"
        df.to_csv(out_path, index=False)
        print(f"Wrote {out_path.name}: {df.shape[0]} rows x {df.shape[1]} cols")

    conn.close()


if __name__ == "__main__":
    main()
