import os
import yaml
import requests

# load config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

data_dir = config["paths"]["data_dir"]
base_url = config["dataset"]["base_url"]
zone_url = config["dataset"]["zone_lookup_url"]
months = config["dataset"]["months"]
zone_file = config["paths"]["zone_lookup_file"]

os.makedirs(data_dir, exist_ok=True)


def download_file(url, dest):
    if os.path.exists(dest):
        print(f"  already exists: {dest}, skipping")
        return
    print(f"  downloading: {url}")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    downloaded = 0
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total > 0:
                pct = (downloaded / total) * 100
                print(f"\r  {pct:.1f}%", end="", flush=True)
    print(f"\n  saved: {dest} ({downloaded / 1024 / 1024:.1f} MB)")


# download trip data parquet files
print("Downloading NYC Yellow Taxi Trip Data...")
for month in months:
    filename = f"yellow_tripdata_{month}.parquet"
    url = f"{base_url}/{filename}"
    dest = os.path.join(data_dir, filename)
    download_file(url, dest)

# download zone lookup csv
print("\nDownloading Taxi Zone Lookup Table...")
download_file(zone_url, zone_file)

print("\nDone! All files downloaded.")
