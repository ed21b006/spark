import os
import time
import yaml
import pandas as pd
import ray
from dotenv import load_dotenv

load_dotenv()

# load config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

data_dir = config["paths"]["data_dir"]
zone_file = config["paths"]["zone_lookup_file"]
output_path = config["paths"]["ray_output"]
cluster_mode = config["cluster"]["mode"]

ray_address = os.getenv("RAY_HEAD_ADDRESS", "auto")

# init ray
if cluster_mode == "cluster":
    print(f"Connecting to Ray cluster at: {ray_address}")
    ray.init(address=ray_address)
else:
    print("Starting Ray in local mode")
    ray.init()

print(f"Ray cluster resources: {ray.cluster_resources()}")
timings = {}

# INGESTION
t0 = time.time()
print("\n1. Ingesting parquet files...")

parquet_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".parquet")]
trip_ds = ray.data.read_parquet(parquet_files, override_num_blocks=256)
initial_count = trip_ds.count()
print(f"loaded {initial_count} rows")
timings["ingestion"] = time.time() - t0

# CLEANSING
t0 = time.time()
print("\n2. Cleansing data...")

key_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "PULocationID", "DOLocationID", "fare_amount"]

def clean_batch(df: pd.DataFrame) -> pd.DataFrame:
    # drop nulls in key columns
    df = df.dropna(subset=key_cols)

    # remove duplicates
    df = df.drop_duplicates()

    # filter bad data
    df = df[df["trip_distance"] > 0]
    df = df[df["fare_amount"] >= 0]
    df = df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]

    # ensure timestamp types
    df["pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    return df

trip_ds = trip_ds.map_batches(clean_batch, batch_format="pandas", num_cpus=2, batch_size=2048)
cleaned_count = trip_ds.count()
print(f"  after cleaning: {cleaned_count} rows (removed {initial_count - cleaned_count})")
timings["cleansing"] = time.time() - t0

# TRANSFORMATION
t0 = time.time()
print("\n3. Transforming data...")

# load zone lookup
zone_df = pd.read_csv(zone_file)

# put zone_df in object store so all workers can access it
zone_ref = ray.put(zone_df)

def transform_batch(df: pd.DataFrame, zone_data=None) -> pd.DataFrame:
    # join - pickup location
    zone = zone_data.rename(columns={
        "LocationID": "PULocationID",
        "Borough": "pickup_borough",
        "Zone": "pickup_zone"
    })[["PULocationID", "pickup_borough", "pickup_zone"]]
    df = df.merge(zone, on="PULocationID", how="left")

    # join - dropoff location
    zone_do = zone_data.rename(columns={
        "LocationID": "DOLocationID",
        "Borough": "dropoff_borough",
        "Zone": "dropoff_zone"
    })[["DOLocationID", "dropoff_borough", "dropoff_zone"]]
    df = df.merge(zone_do, on="DOLocationID", how="left")

    return df

trip_ds = trip_ds.map_batches(
    lambda batch: transform_batch(batch, zone_data=ray.get(zone_ref)),
    batch_format="pandas",
    num_cpus=2,
    batch_size=2048
)

# UDF for avg speed
udf_start = time.time()

def calc_speed_batch(df):
    duration = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 3600.0
    df["avg_speed_mph"] = df["trip_distance"] / duration
    df.loc[duration <= 0, "avg_speed_mph"] = None
    df["pickup_hour"] = df["pickup_datetime"].dt.hour
    df = df[(df["avg_speed_mph"].isna()) | (df["avg_speed_mph"] <= 100)]
    return df

trip_ds = trip_ds.map_batches(calc_speed_batch, batch_format="pandas", num_cpus=2, batch_size=2048)
final_count = trip_ds.count()

udf_time = time.time() - udf_start
timings["transformation"] = time.time() - t0
timings["udf_overhead"] = udf_time

print(f"final row count: {final_count}")
print(f"UDF execution time: {udf_time:.2f}s")

# EXPORT
t0 = time.time()
print("\n4. Exporting to parquet...")

os.makedirs(output_path, exist_ok=True)
trip_ds.write_parquet(output_path)

timings["export"] = time.time() - t0
print(f"saved to: {output_path}")

# print summary
total = sum(timings.values())
timings["total"] = total
print("\n" + "=" * 50)
print("RAY PIPELINE TIMING SUMMARY")
print("=" * 50)
for step, t in timings.items():
    print(f"{step:20s}: {t:8.2f}s")
print("=" * 50)

ray.shutdown()
