# Postmates LA: Retention & Frequency Analysis

An end-to-end analysis of user retention and order frequency patterns for Postmates' Los Angeles market. The project investigates what drives repeat behavior, which user segments are worth prioritizing, and how supply-side decisions (merchant mix, promo strategy) interact with long-term retention.

---

## Business Question

Postmates operates as a differentiated brand inside the Uber Delivery ecosystem — centered on local culture and trend-driven discovery. The core question this analysis addresses:

> **Does Postmates' differentiated merchant mix actually drive higher repeat behavior in LA — and if so, how do you operationalize that into a retention strategy?**

---

## Data Model

Synthetic dataset designed to mirror a standard delivery app data warehouse. Four tables, all SQL-compatible.

| Table | Grain | Key Fields |
|---|---|---|
| `users` | One row per user | `signup_date`, `first_order_date`, `city`, `acquisition_channel`, `is_uber_cross_user` |
| `merchants` | One row per merchant | `merchant_type`, `is_local_brand`, `is_trend_driven`, `city` |
| `orders` | One row per order | `user_id`, `merchant_id`, `order_value`, `promo_applied`, `delivery_time_min` |
| `user_monthly_metrics` | One row per user per month | `orders_count`, `promo_order_share`, `retained_next_30d`, `segment_label` |

The dataset encodes realistic behavioral patterns: habitual users order more frequently and skew toward local/trend merchants; promo-acquired users activate quickly but retain at lower rates; delivery speed has a measurable effect on next-month retention.

To regenerate the data:

```bash
python src/generate_synthetic_data.py
```

---

## Analysis

Six SQL queries covering the core retention questions, run via a Python SQLite runner.

```bash
python analysis/run_sql_analysis.py
```

| Output | What it answers |
|---|---|
| `01_executive_overview_la` | Monthly active users, order frequency, promo share trend |
| `02_cohort_retention_la` | 30-day retention by acquisition cohort |
| `03_segment_performance_la` | Retention, frequency, and AOV by user segment |
| `04_merchant_mix_la` | Repeat user share by merchant type and local/trend classification |
| `05_experience_impact_la` | Retention by delivery speed bucket |
| `06_promo_dependency_la` | Retention curve across low / medium / high promo dependency |

Results are written to `analysis/output/`.

---

## Key Findings

- **~19% of users order exactly once and never return** — the single largest addressable retention gap, ahead of any experience or promo issue
- **Local + trend merchants show 90–96% repeat user share** vs. 85–88% for generic merchants — Postmates' brand positioning has measurable behavioral backing
- **High promo dependency correlates with lower retention (25%)** — heavy promotional acquisition builds volume but not habit; medium dependency is the functional sweet spot
- **Delivery speed has a 4.6pp retention impact** between fast and slow buckets — a controllable experience variable worth tracking alongside acquisition quality
- **Converting 20% of one-time users to a second order** ≈ +175 incremental orders at current AOV (~$4K GMV floor, with further upside if behavior compounds toward habitual rates)

---

## Structure

```
├── data/                        # Synthetic datasets (CSV)
├── src/
│   └── generate_synthetic_data.py
├── analysis/
│   ├── postmates_retention_queries.sql
│   ├── run_sql_analysis.py
│   └── output/                  # Query results (CSV)
└── deck/
    └── postmates_la_retention_analysis.pptx
```
