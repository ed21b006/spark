import os
import time
import json
import pandas as pd
import ray

ray.init(ignore_reinit_error=True)

# ---------------------------------------------------------
# BULLETPROOF FIX: Force Ray to use the symlinks we created
# ---------------------------------------------------------
common_base = "/tmp/assignment8"

if not os.path.exists(common_base):
    print(f"CRITICAL ERROR: {common_base} does not exist! Did you run 'ln -s $(pwd) /tmp/assignment8'?")
    exit(1)

data_dir = os.path.join(common_base, "data")
zone_file = os.path.join(common_base, "data/taxi_zone_lookup.csv")
output_path = os.path.join(common_base, "output/ray_cleaned")

print(f"Starting Ray pipeline using data directory: {data_dir}")
timings = {}

# ... (The rest of your Ray code remains exactly the same starting from INGESTION)

timings = {}
print(f"Starting Ray pipeline using data directory: {data_dir}")

# ... (The rest of the Ray code remains exactly the same starting from INGESTION)

timings = {}
print("Starting Ray pipeline...")

# 2. INGESTION
t0 = time.time()
print("\n1. Ingesting parquet files...")
ds = ray.data.read_parquet(data_dir)
initial_count = ds.count()
print(f"loaded {initial_count} rows")
timings["ingestion"] = time.time() - t0

# 3. CLEANSING
t0 = time.time()
print("\n2. Cleansing data...")

def clean_batch(df: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "PULocationID", "DOLocationID", "fare_amount"]
    df = df.dropna(subset=key_cols)
    df = df.drop_duplicates()
    
    # Filter bad data
    df = df[(df["trip_distance"] > 0) & (df["fare_amount"] >= 0)]
    
    # Format timestamps
    df["pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    return df[df["dropoff_datetime"] > df["pickup_datetime"]]

ds = ds.map_batches(clean_batch, batch_format="pandas")
cleaned_count = ds.count()
print(f"after cleaning: {cleaned_count} rows (removed {initial_count - cleaned_count})")
timings["cleansing"] = time.time() - t0

# 4. TRANSFORMATION & UDF
t0 = time.time()
print("\n3. Transforming data (Join & UDF)...")

# Load and broadcast the small zone lookup table for the Heavy Join
zone_df = pd.read_csv(zone_file)

def transform_batch(df: pd.DataFrame) -> pd.DataFrame:
    # Pickup Join
    df = df.merge(zone_df[['LocationID', 'Borough', 'Zone']], left_on='PULocationID', right_on='LocationID', how='left')
    df.rename(columns={'Borough': 'pickup_borough', 'Zone': 'pickup_zone'}, inplace=True)
    df.drop('LocationID', axis=1, inplace=True)

    # Dropoff Join
    df = df.merge(zone_df[['LocationID', 'Borough', 'Zone']], left_on='DOLocationID', right_on='LocationID', how='left')
    df.rename(columns={'Borough': 'dropoff_borough', 'Zone': 'dropoff_zone'}, inplace=True)
    df.drop('LocationID', axis=1, inplace=True)
    return df

ds = ds.map_batches(transform_batch, batch_format="pandas")

# UDF Execution (Isolated to measure Python-native advantage)
udf_start = time.time()
def apply_udf(df: pd.DataFrame) -> pd.DataFrame:
    # Python-native speed calculation (vectorized in Pandas for speed, but executes in native Python memory)
    duration_hours = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 3600.0
    df["avg_speed_mph"] = df["trip_distance"] / duration_hours
    df["pickup_hour"] = df["pickup_datetime"].dt.hour
    
    return df[(df["avg_speed_mph"].isna()) | (df["avg_speed_mph"] <= 100)]

ds = ds.map_batches(apply_udf, batch_format="pandas")

# Force execution to measure UDF time accurately
final_count = ds.count() 
udf_time = time.time() - udf_start

timings["transformation"] = time.time() - t0
timings["udf_overhead"] = udf_time
print(f"final row count: {final_count}")
print(f"UDF execution time: {udf_time:.2f}s")

# 5. EXPORT
t0 = time.time()
print("\n4. Exporting to parquet...")
os.makedirs(os.path.dirname(output_path), exist_ok=True)
ds.write_parquet(output_path)
timings["export"] = time.time() - t0
print(f"saved to: {output_path}")

# Calculate Total
timings["total"] = sum(timings.values())

# 6. JSON EXPORT (Using Ray Decorator)
@ray.remote
def save_timings_to_json(metrics, filename):
    """Ray Core Task to asynchronously save metrics."""
    with open(filename, 'w') as f:
        json.dump(metrics, f, indent=4)
    return os.path.abspath(filename)

# Dispatch the task to the cluster
future = save_timings_to_json.remote(timings, "ray_timings.json")
json_path = ray.get(future) # Wait for it to finish

print("\n" + "=" * 50)
print("RAY PIPELINE TIMING SUMMARY")
print("=" * 50)
for step, t in timings.items():
    print(f"  {step:20s}: {t:8.2f}s")
print("=" * 50)
print(f"Timings successfully saved to {json_path}")

ray.shutdown()