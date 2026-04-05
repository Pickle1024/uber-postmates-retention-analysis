"""
Generates synthetic data for the Postmates LA retention analysis.

Produces four SQL-compatible CSV tables (users, merchants, orders,
user_monthly_metrics) with realistic behavioral distributions for the
Los Angeles market. Seed is fixed for reproducibility.

Usage:
    python src/generate_synthetic_data.py
"""
from pathlib import Path
import numpy as np
import pandas as pd


SEED = 42
np.random.seed(SEED)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


CITY_COUNTS = {
    "Los Angeles": 120,
    "San Francisco": 60,
    "San Diego": 45,
    "Seattle": 40,
    "Phoenix": 35,
}

MERCHANT_TYPES = [
    "Fast Casual",
    "Sushi",
    "Boba/Dessert",
    "Grocery",
    "Convenience",
    "Healthy",
    "Pizza",
    "Late Night",
]

CITIES = ["Los Angeles", "San Francisco", "San Diego", "Seattle", "Phoenix"]
CITY_PROBS = [0.50, 0.18, 0.12, 0.10, 0.10]
CHANNELS = ["Paid Social", "Referral", "Organic", "Uber Cross-App", "Local Campaign"]
LATENT_SEGMENTS = ["one_time", "light_user", "habitual_user", "promo_dependent"]
LATENT_PROBS = [0.30, 0.35, 0.20, 0.15]

BASE_ORDER_VALUE = {
    "Fast Casual": 18,
    "Sushi": 28,
    "Boba/Dessert": 14,
    "Grocery": 32,
    "Convenience": 20,
    "Healthy": 24,
    "Pizza": 22,
    "Late Night": 19,
}


def build_merchants() -> pd.DataFrame:
    rows = []
    merchant_id = 1

    for city, count in CITY_COUNTS.items():
        for _ in range(count):
            merchant_type = np.random.choice(MERCHANT_TYPES)
            is_local = np.random.rand() < (0.65 if city == "Los Angeles" else 0.45)
            trend_prob = 0.35 if merchant_type in {"Boba/Dessert", "Healthy"} else 0.15
            if city == "Los Angeles":
                trend_prob += 0.10
            is_trend = np.random.rand() < trend_prob

            rows.append(
                {
                    "merchant_id": f"M{merchant_id:04d}",
                    "city": city,
                    "merchant_type": merchant_type,
                    "is_local_brand": bool(is_local),
                    "is_trend_driven": bool(is_trend),
                }
            )
            merchant_id += 1

    return pd.DataFrame(rows)


def build_users(n_users: int = 5000) -> pd.DataFrame:
    date_pool = pd.date_range("2025-07-01", "2026-03-31", freq="D")
    rows = []

    for i in range(1, n_users + 1):
        city = np.random.choice(CITIES, p=CITY_PROBS)

        if city == "Los Angeles":
            channel_probs = [0.25, 0.15, 0.20, 0.20, 0.20]
        else:
            channel_probs = [0.18, 0.15, 0.22, 0.30, 0.15]

        acquisition_channel = np.random.choice(CHANNELS, p=channel_probs)
        initial_promo_received = np.random.rand() < (
            0.70 if acquisition_channel in {"Paid Social", "Referral"} else 0.50
        )
        is_uber_cross_user = np.random.rand() < (
            0.70 if acquisition_channel == "Uber Cross-App" else 0.25
        )

        signup_date = pd.Timestamp(np.random.choice(date_pool))
        delay = np.random.randint(0, 15) if initial_promo_received else np.random.randint(0, 46)
        first_order_date = signup_date + pd.Timedelta(days=int(delay))
        first_order_date = min(first_order_date, pd.Timestamp("2026-03-31"))

        rows.append(
            {
                "user_id": f"U{i:06d}",
                "signup_date": signup_date,
                "first_order_date": first_order_date,
                "city": city,
                "acquisition_channel": acquisition_channel,
                "initial_promo_received": bool(initial_promo_received),
                "is_uber_cross_user": bool(is_uber_cross_user),
                "latent_segment": np.random.choice(LATENT_SEGMENTS, p=LATENT_PROBS),
            }
        )

    return pd.DataFrame(rows)


def sample_order_count(segment: str) -> int:
    if segment == "one_time":
        return 1
    if segment == "light_user":
        return np.random.randint(2, 5)
    if segment == "habitual_user":
        return np.random.randint(5, 13)
    return np.random.randint(2, 8)


def sample_gap(segment: str) -> int:
    if segment == "habitual_user":
        return np.random.randint(4, 13)
    if segment == "light_user":
        return np.random.randint(10, 31)
    if segment == "promo_dependent":
        return np.random.randint(7, 21)
    return np.random.randint(20, 50)


def build_orders(users: pd.DataFrame, merchants: pd.DataFrame) -> pd.DataFrame:
    rows = []
    order_id = 1

    for _, user in users.iterrows():
        segment = user["latent_segment"]
        n_orders = sample_order_count(segment)
        city_merchants = merchants[merchants["city"] == user["city"]].copy()

        order_dates = [user["first_order_date"]]
        current_date = user["first_order_date"]

        for _ in range(1, n_orders):
            current_date = current_date + pd.Timedelta(days=int(sample_gap(segment)))
            if current_date > pd.Timestamp("2026-03-31"):
                break
            order_dates.append(current_date)

        for idx, order_date in enumerate(order_dates):
            merchant_pool = city_merchants.copy()
            if segment == "habitual_user":
                weights = (
                    1
                    + merchant_pool["is_local_brand"].astype(int) * 1.0
                    + merchant_pool["is_trend_driven"].astype(int) * 0.8
                )
            else:
                weights = (
                    1
                    + merchant_pool["is_local_brand"].astype(int) * 0.4
                    + merchant_pool["is_trend_driven"].astype(int) * 0.3
                )

            merchant = merchant_pool.sample(1, weights=weights).iloc[0]
            order_value = max(8, np.random.normal(BASE_ORDER_VALUE[merchant["merchant_type"]], 6))
            if segment == "habitual_user":
                order_value += 2

            if idx == 0:
                promo_applied = user["initial_promo_received"]
            elif segment == "promo_dependent":
                promo_applied = np.random.rand() < 0.65
            else:
                promo_applied = np.random.rand() < 0.25

            delivery_time = int(np.clip(np.random.normal(36, 10), 18, 75))
            is_late_delivery = bool((delivery_time > 45) and (np.random.rand() < 0.75))
            support_contact_flag = bool(np.random.rand() < (0.18 if is_late_delivery else 0.04))

            rows.append(
                {
                    "order_id": f"O{order_id:07d}",
                    "user_id": user["user_id"],
                    "merchant_id": merchant["merchant_id"],
                    "order_date": order_date,
                    "order_value": round(order_value, 2),
                    "promo_applied": bool(promo_applied),
                    "delivery_time_min": delivery_time,
                    "is_late_delivery": is_late_delivery,
                    "support_contact_flag": support_contact_flag,
                    "is_first_order": idx == 0,
                }
            )
            order_id += 1

    return pd.DataFrame(rows)


def assign_segment(total_orders: int, promo_share: float) -> str:
    if total_orders == 1:
        return "one_time"
    if promo_share > 0.60 and total_orders >= 2:
        return "promo_dependent"
    if total_orders >= 5:
        return "habitual_user"
    return "light_user"


def build_monthly_metrics(orders: pd.DataFrame, merchants: pd.DataFrame) -> pd.DataFrame:
    orders_aug = orders.copy()
    orders_aug["metric_month"] = orders_aug["order_date"].values.astype("datetime64[M]")
    orders_aug = orders_aug.merge(
        merchants[["merchant_id", "is_local_brand"]],
        on="merchant_id",
        how="left",
    )

    monthly = (
        orders_aug.groupby(["user_id", "metric_month"])
        .agg(
            orders_count=("order_id", "count"),
            avg_order_value=("order_value", "mean"),
            promo_order_share=("promo_applied", "mean"),
            avg_delivery_time_min=("delivery_time_min", "mean"),
            local_merchant_order_share=("is_local_brand", "mean"),
            last_order_date=("order_date", "max"),
        )
        .reset_index()
    )

    monthly["avg_order_value"] = monthly["avg_order_value"].round(2)
    monthly["promo_order_share"] = monthly["promo_order_share"].round(2)
    monthly["avg_delivery_time_min"] = monthly["avg_delivery_time_min"].round(1)
    monthly["local_merchant_order_share"] = monthly["local_merchant_order_share"].round(2)

    month_end = monthly["metric_month"] + pd.offsets.MonthEnd(1)
    monthly["days_since_last_order"] = (month_end - monthly["last_order_date"]).dt.days

    order_lookup = orders[["user_id", "order_date"]].copy()

    def retained_next_30d(row: pd.Series) -> bool:
        future = order_lookup[
            (order_lookup["user_id"] == row["user_id"])
            & (order_lookup["order_date"] > row["last_order_date"])
            & (order_lookup["order_date"] <= row["last_order_date"] + pd.Timedelta(days=30))
        ]
        return len(future) > 0

    monthly["retained_next_30d"] = monthly.apply(retained_next_30d, axis=1)

    user_total_orders = orders.groupby("user_id").size().rename("total_orders").reset_index()
    user_promo_share = (
        orders.groupby("user_id")["promo_applied"].mean().rename("user_promo_share").reset_index()
    )
    segment_df = user_total_orders.merge(user_promo_share, on="user_id")
    segment_df["segment_label"] = segment_df.apply(
        lambda row: assign_segment(int(row["total_orders"]), float(row["user_promo_share"])),
        axis=1,
    )

    monthly = monthly.merge(segment_df[["user_id", "segment_label"]], on="user_id", how="left")
    monthly = monthly.drop(columns=["last_order_date"])
    return monthly


def export_all(users: pd.DataFrame, merchants: pd.DataFrame, orders: pd.DataFrame, monthly: pd.DataFrame) -> None:
    users.drop(columns=["latent_segment"]).to_csv(DATA_DIR / "users.csv", index=False)
    merchants.to_csv(DATA_DIR / "merchants.csv", index=False)
    orders.to_csv(DATA_DIR / "orders.csv", index=False)
    monthly.to_csv(DATA_DIR / "user_monthly_metrics.csv", index=False)


def print_summary(users: pd.DataFrame, merchants: pd.DataFrame, orders: pd.DataFrame, monthly: pd.DataFrame) -> None:
    la_users = users[users["city"] == "Los Angeles"]["user_id"].nunique()
    retained_rate = monthly["retained_next_30d"].mean()
    avg_orders = orders.groupby("user_id").size().mean()
    la_local_share = (
        orders.merge(merchants[["merchant_id", "is_local_brand"]], on="merchant_id", how="left")
        .merge(users[["user_id", "city"]], on="user_id", how="left")
        .query("city == 'Los Angeles'")["is_local_brand"]
        .mean()
    )

    print("Synthetic data generated successfully.")
    print(f"Users: {len(users):,}")
    print(f"Merchants: {len(merchants):,}")
    print(f"Orders: {len(orders):,}")
    print(f"Monthly metric rows: {len(monthly):,}")
    print(f"LA users: {la_users:,}")
    print(f"Average orders per user: {avg_orders:.2f}")
    print(f"Average retained_next_30d rate: {retained_rate:.2%}")
    print(f"LA local merchant order share: {la_local_share:.2%}")


def main() -> None:
    merchants = build_merchants()
    users = build_users()
    orders = build_orders(users, merchants)
    monthly = build_monthly_metrics(orders, merchants)
    export_all(users, merchants, orders, monthly)
    print_summary(users, merchants, orders, monthly)


if __name__ == "__main__":
    main()
