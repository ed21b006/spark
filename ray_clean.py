import os
import sys
import time
import yaml
import pandas as pd
import ray
from dotenv import load_dotenv

load_dotenv()

# load config with sensible defaults
try:
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f) or {}
except FileNotFoundError:
    config = {
        "cluster": {"mode": "local"},
        "paths": {
            "data_dir": "data",
            "zone_lookup_file": "data/taxi_zone_lookup.csv",
            "ray_output": "output/ray_cleaned"
        }
    }

data_dir = config.get("paths", {}).get("data_dir")
zone_file = config.get("paths", {}).get("zone_lookup_file")
output_path = config.get("paths", {}).get("ray_output")
cluster_mode = config.get("cluster", {}).get("mode", "local")
ray_address = os.getenv("RAY_HEAD_ADDRESS", "auto")

# resource calculations (80% of local resources)
total_cpus = os.cpu_count() or 1
cpus_to_use = max(1, int(total_cpus * 0.8))
per_task_cpus = max(1, cpus_to_use // 4)

def _get_mem_bytes():
    try:
        with open("/proc/meminfo") as f:
            for l in f:
                if l.startswith("MemTotal"):
                    kb = int(l.split()[1])
                    return int(kb * 1024 * 0.8)
    except Exception:
        return None

mem_bytes = _get_mem_bytes()

if "--dry-run" in sys.argv:
    print(f"cpus_to_use={cpus_to_use}, mem_bytes={mem_bytes}, per_task_cpus={per_task_cpus}")
    sys.exit(0)

# init ray
if cluster_mode == "cluster":
    print(f"Connecting to Ray cluster at: {ray_address}")
    ray.init(address=ray_address)
else:
    ray.init(num_cpus=cpus_to_use)

print(f"Ray cluster resources: {ray.cluster_resources()}")

# INGESTION
print("\n1. Ingesting parquet files...")
parquet_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".parquet")]
print(f"  Found {len(parquet_files)} parquet files")
trip_ds = ray.data.read_parquet(parquet_files, override_num_blocks=max(16, cpus_to_use * 4))
initial_count = trip_ds.count()
print(f"  Loaded {initial_count} rows")

# CLEANSING
print("\n2. Cleansing data...")
key_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "PULocationID", "DOLocationID", "fare_amount"]

def clean_batch(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=key_cols)
    df = df.drop_duplicates()
    df = df[df["trip_distance"] > 0]
    df = df[df["fare_amount"] >= 0]
    df = df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]
    df["pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    return df

trip_ds = trip_ds.map_batches(clean_batch, batch_format="pandas", num_cpus=per_task_cpus, batch_size=2048)
cleaned_count = trip_ds.count()
print(f"  After cleaning: {cleaned_count} rows (removed {initial_count - cleaned_count:,})")

# TRANSFORMATION
print("\n3. Transforming data...")
zone_df = pd.read_csv(zone_file)
zone_ref = ray.put(zone_df)

def transform_batch(df: pd.DataFrame, zone_data=None) -> pd.DataFrame:
    zone = zone_data.rename(columns={"LocationID": "PULocationID", "Borough": "pickup_borough", "Zone": "pickup_zone"})[["PULocationID", "pickup_borough", "pickup_zone"]]
    df = df.merge(zone, on="PULocationID", how="left")
    zone_do = zone_data.rename(columns={"LocationID": "DOLocationID", "Borough": "dropoff_borough", "Zone": "dropoff_zone"})[["DOLocationID", "dropoff_borough", "dropoff_zone"]]
    df = df.merge(zone_do, on="DOLocationID", how="left")
    return df

trip_ds = trip_ds.map_batches(lambda b: transform_batch(b, zone_data=ray.get(zone_ref)), batch_format="pandas", num_cpus=per_task_cpus, batch_size=2048)

# UDF
print("\n4. Calculating speed metrics...")

def calc_speed_batch(df: pd.DataFrame) -> pd.DataFrame:
    duration = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 3600.0
    df["avg_speed_mph"] = df["trip_distance"] / duration
    df.loc[duration <= 0, "avg_speed_mph"] = None
    df["pickup_hour"] = df["pickup_datetime"].dt.hour
    df = df[(df["avg_speed_mph"].isna()) | (df["avg_speed_mph"] <= 100)]
    return df

trip_ds = trip_ds.map_batches(calc_speed_batch, batch_format="pandas", num_cpus=per_task_cpus, batch_size=2048)
final_count = trip_ds.count()
print(f"  Final row count: {final_count}")

# EXPORT
print("\n5. Exporting to parquet...")
os.makedirs(output_path, exist_ok=True)
trip_ds.write_parquet(output_path)
print(f"  Saved to {output_path}")

# SUMMARY
print("\nPipeline complete")
ray.shutdown()
