import pandas as pd

# ----------------------------
# Load data
# ----------------------------
df = pd.read_csv("data/transformed data/training_activities.csv")  # <-- replace with your actual file

# Convert start_date column to datetime
df["start_date"] = pd.to_datetime(df["start_date"], utc=True)

# ----------------------------
# Marathon date
# ----------------------------
marathon_date = pd.Timestamp("2025-10-12", tz="UTC")

# 4 weeks = 28 days before marathon
start_period = marathon_date - pd.Timedelta(days=28)

# Filter last 4 weeks of training
df_last4 = df[(df["start_date"] >= start_period) & (df["start_date"] <= marathon_date)]

# ----------------------------
# Compute useful metrics
# ----------------------------

# distance is in meters → convert to km
df_last4["distance_km"] = df_last4["distance"] / 1000  

# moving_time is in seconds → convert to hours
df_last4["moving_hours"] = df_last4["moving_time"] / 3600

# speed in km/h
df_last4["km_per_hour"] = df_last4["distance_km"] / df_last4["moving_hours"]

# ----------------------------
# Group by ISO calendar week
# ----------------------------
df_last4["year_week"] = df_last4["start_date"].dt.isocalendar().week
df_last4["year"] = df_last4["start_date"].dt.isocalendar().year

weekly_summary = (
    df_last4
    .groupby(["year", "year_week"])
    .agg(
        avg_km_per_week=("distance_km", "mean"),
        avg_km_per_hour=("km_per_hour", "mean"),
        total_km=("distance_km", "sum"),
        total_activities=("distance_km", "count")
    )
    .reset_index()
)

print(weekly_summary)
