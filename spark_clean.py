import os
import time
import yaml
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, udf, hour, when
from pyspark.sql.types import DoubleType, StructType, StructField, TimestampType, LongType

load_dotenv()

# load config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

cluster_mode = config["cluster"]["mode"]

if cluster_mode == "cluster":
    common_base = "/tmp/assignment8"
    if not os.path.exists(common_base):
        print("tmp/assignment8 doesnt exist")
        exit(1)
    data_dir = os.path.join(common_base, config["paths"]["data_dir"])
    zone_file = os.path.join(common_base, config["paths"]["zone_lookup_file"])
    output_path = os.path.join(common_base, config["paths"]["spark_output"])
else:
    data_dir = os.path.abspath(config["paths"]["data_dir"])
    zone_file = os.path.abspath(config["paths"]["zone_lookup_file"])
    output_path = os.path.abspath(config["paths"]["spark_output"])

spark_master = os.getenv("SPARK_MASTER_URL", "local[*]")

# use local mode if not in cluster mode
master_url = spark_master if cluster_mode == "cluster" else "local[*]"

print(f"Starting Spark pipeline (mode: {cluster_mode}, master: {master_url})")
timings = {}

# create spark session
spark = SparkSession.builder \
    .appName("NYC_Taxi_Cleaning") \
    .master(master_url) \
    .config("spark.python.worker.faulthandler.enabled", "true") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.memoryOverhead", "1g") \
    .config("spark.network.timeout", "800s") \
    .getOrCreate()

# INGESTION
t0 = time.time()
print("\n1. Ingesting parquet files...")
parquet_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".parquet")]

trip_df = None
for p in parquet_files:
    df = spark.read.parquet(p).select(
        col("tpep_pickup_datetime").cast("timestamp"),
        col("tpep_dropoff_datetime").cast("timestamp"),
        col("trip_distance").cast("double"),
        col("PULocationID").cast("long"),
        col("DOLocationID").cast("long"),
        col("fare_amount").cast("double")
    )
    if trip_df is None:
        trip_df = df
    else:
        trip_df = trip_df.union(df)

initial_count = trip_df.count()
print(f"loaded {initial_count} rows")
timings["ingestion"] = time.time() - t0

# CLEANSING
t0 = time.time()
print("\n2. Cleansing data...")

# drop rows with nulls in key columns
key_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "PULocationID", "DOLocationID", "fare_amount"]
trip_df = trip_df.dropna(subset=key_cols)

# remove duplicates
trip_df = trip_df.dropDuplicates()

# filter out bad data
trip_df = trip_df.filter(
    (col("trip_distance") > 0) &
    (col("fare_amount") >= 0) &
    (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
)

# format timestamps
trip_df = trip_df.withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
trip_df = trip_df.withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))

cleaned_count = trip_df.count()
print(f"after cleaning: {cleaned_count} rows (removed {initial_count - cleaned_count})")
timings["cleansing"] = time.time() - t0

# TRANSFORMATION
t0 = time.time()
print("\n3. Transforming data...")

# load zone lookup
zone_df = spark.read.csv(zone_file, header=True, inferSchema=True)

# pickup location join
trip_df = trip_df.join(
    zone_df.select(
        col("LocationID"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone")
    ),
    trip_df["PULocationID"] == zone_df["LocationID"],
    "left"
).drop("LocationID")

# dropoff location join
trip_df = trip_df.join(
    zone_df.select(
        col("LocationID"),
        col("Borough").alias("dropoff_borough"),
        col("Zone").alias("dropoff_zone")
    ),
    trip_df["DOLocationID"] == zone_df["LocationID"],
    "left"
).drop("LocationID")

# UDF for avg speed calculation
udf_start = time.time()

def calc_avg_speed(distance, pickup_ts, dropoff_ts):
    if distance is None or pickup_ts is None or dropoff_ts is None:
        return None
    duration_hours = (dropoff_ts - pickup_ts).total_seconds() / 3600.0
    if duration_hours <= 0:
        return None
    return float(distance / duration_hours)

avg_speed_udf = udf(calc_avg_speed, DoubleType())

trip_df = trip_df.withColumn(
    "avg_speed_mph",
    avg_speed_udf(col("trip_distance"), col("pickup_datetime"), col("dropoff_datetime"))
)

# extract pickup hour
trip_df = trip_df.withColumn("pickup_hour", hour(col("pickup_datetime")))

# filter out unrealistic speeds
trip_df = trip_df.filter(
    (col("avg_speed_mph").isNull()) | (col("avg_speed_mph") <= 100)
)

udf_time = time.time() - udf_start
final_count = trip_df.count()
timings["transformation"] = time.time() - t0
timings["udf_overhead"] = udf_time

print(f"final row count: {final_count}")
print(f"UDF execution time: {udf_time:.2f}s")

# EXPORT
t0 = time.time()
print("\n4. Exporting to parquet...")

os.makedirs(os.path.dirname(output_path), exist_ok=True)
trip_df.write.mode("overwrite").option("compression", "gzip").parquet(output_path)

timings["export"] = time.time() - t0
print(f"saved to: {output_path}")

# print summary
total = sum(timings.values())
timings["total"] = total
print("\n" + "=" * 50)
print("SPARK PIPELINE TIMING SUMMARY")
print("=" * 50)
for step, t in timings.items():
    print(f"  {step:20s}: {t:8.2f}s")
print("=" * 50)

spark.stop()
