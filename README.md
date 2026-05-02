# DA5402 A8: Spark vs. Ray — The Data Engineering Duel

Comparing Apache Spark and Ray for distributed data preprocessing on NYC Taxi Trip Data (~2GB).

## What This Does

Implements an identical 4-step data preprocessing pipeline in both PySpark and Ray:
1. **Ingestion** — Load multiple parquet files (6 months of NYC yellow taxi data)
2. **Cleansing** — Drop nulls, remove duplicates, filter bad records, format timestamps
3. **Transformation** — Join with taxi zone lookup table + calculate average speed using Python UDF
4. **Export** — Write cleaned data to parquet

## Project Structure

```
├── config.yaml          # all configuration (paths, URLs, cluster settings)
├── .env                 # cluster connection strings (spark master, ray head)
├── requirements.txt     # python dependencies
├── download_data.py     # downloads NYC taxi data (~2GB)
├── spark_clean.py       # PySpark preprocessing pipeline
├── ray_clean.py         # Ray Data preprocessing pipeline
├── benchmark.py         # runs both and compares performance
├── instruction.md       # detailed setup guide
├── data/                # raw parquet files (not in git)
└── output/              # cleaned output (not in git)
```

## Quick Start

```bash
# install dependencies
pip install -r requirements.txt

# download dataset (~2GB)
python download_data.py

# run spark pipeline
python spark_clean.py

# run ray pipeline
python ray_clean.py

# run both and compare
python benchmark.py
```

## Cluster Mode

By default, both pipelines run locally. To run on a 2-node cluster:

1. Edit `config.yaml` → set `cluster.mode: "cluster"`
2. Edit `.env` → set actual `SPARK_MASTER_URL` and `RAY_HEAD_ADDRESS`
3. See `instruction.md` for full cluster setup steps

## Dataset

- **Source**: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Files**: Yellow taxi trip data (Jan–Jun 2023) + Taxi Zone Lookup Table
- **Size**: ~2GB total

## Key Difference: UDF Performance

The Python UDF for calculating average speed highlights a key architectural difference:
- **Spark**: Python UDF requires serialization between JVM and Python processes (overhead)
- **Ray**: Python-native execution, no serialization barrier

This is measured separately in both pipelines as `udf_overhead`.
