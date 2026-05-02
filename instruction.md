# Personal Setup Instructions

## My Laptop (Master/Head Node)

### Prerequisites
```bash
# java (needed for spark)
sudo apt install default-jdk -y
java -version

# python stuff
pip install -r requirements.txt
```

### Download Data
```bash
python download_data.py
# this downloads ~2GB of NYC taxi data into data/
```

### Test Locally First
```bash
# run both pipelines in local mode (default)
python spark_clean.py
python ray_clean.py
```

---

## Friend's Laptop (Worker Node)

### What They Need to Install

```bash
# java
sudo apt install default-jdk -y

# python + same deps
pip install pyspark ray[data] pandas pyarrow

# make sure python version matches mine
python3 --version
```

### Network Setup
- Both laptops must be on the **same network** (same WiFi or wired LAN)
- Find my IP: `hostname -I` or `ip addr show`
- Find friend's IP: same command on their laptop
- Make sure we can ping each other: `ping <their-ip>`

---

## Spark Cluster Setup (2-Node)

### On My Laptop (Master)
```bash
# find where spark is installed
echo $SPARK_HOME
export SPARK_HOME=$(python -c "import os, pyspark; print(os.path.dirname(pyspark.__file__))")

# start spark master (runs in foreground, leave terminal open)
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master

# check master UI at http://localhost:8080
# note the master URL: spark://<MY_IP>:7077
```

### On Friend's Laptop (Worker)
```bash
# they need spark installed too
# export SPARK_HOME similarly
export SPARK_HOME=$(python -c "import os, pyspark; print(os.path.dirname(pyspark.__file__))")

# start worker, connecting to my master (runs in foreground)
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker spark://<MY_IP>:7077

# verify on master UI - should show 2 workers
```

### Data Sharing for Spark
Spark needs data accessible on all nodes **at the exact same absolute path**.

Easiest way to guarantee identical paths without changing usernames: Create a symlink to `/tmp/assignment8`.

1. Run this on **your laptop** inside the project folder:
   ```bash
   ln -s $(pwd) /tmp/assignment8
   ```
2. Copy the project folder to **friend's laptop**:
   ```bash
   scp -r ../assignment8 friend@<FRIEND_IP>:~/
   ```
3. Run this on **friend's laptop** inside their project folder:
   ```bash
   cd ~/assignment8
   ln -s $(pwd) /tmp/assignment8
   ```

### Run Spark Pipeline
```bash
# update .env
# SPARK_MASTER_URL=spark://<MY_IP>:7077

# update config.yaml
# cluster.mode: "cluster"

python spark_clean.py
```

### Take Screenshots
- Spark Master UI: `http://<MY_IP>:8080` — shows 2 workers
- Spark App UI: `http://<MY_IP>:4040` — shows job progress during run

---

## Ray Cluster Setup (2-Node)

### On My Laptop (Head Node)
```bash
# start ray head node
ray start --head --port=6379 --dashboard-host=0.0.0.0

# note the address printed, something like: <MY_IP>:6379
# dashboard at http://localhost:8265
```

### On Friend's Laptop (Worker Node)
```bash
# join my ray cluster
ray start --address=192.168.0.117:6379
```

### Verify
```bash
# on my laptop
python -c "import ray; ray.init(address='auto'); print(ray.cluster_resources()); ray.shutdown()"
# should show combined CPU/memory from both machines
```

### Data for Ray
Ray needs data accessible on the head node (it streams to workers). So no need to copy data to friend's laptop for ray.data operations.

### Run Ray Pipeline
```bash
# update .env
# RAY_HEAD_ADDRESS=auto (or <MY_IP>:6379)

# update config.yaml
# cluster.mode: "cluster"

python ray_clean.py
```

### Take Screenshots
- Ray Dashboard: `http://<MY_IP>:8265` — shows cluster resources + job progress

---

## Running Benchmarks

```bash
# make sure cluster is set up for whichever mode you want
python benchmark.py
```

This runs both pipelines back to back and prints a comparison table.

---

## Cleanup

### Stop Spark Cluster
```bash
# Since we started Spark using spark-class in the foreground,
# simply go to the terminal running the Master or Worker and press Ctrl+C.
```

### Stop Ray Cluster
```bash
# on both laptops
ray stop
```

---

## Troubleshooting

- **Spark worker can't connect**: check firewall, make sure port 7077 is open
  ```bash
  sudo ufw allow 7077
  sudo ufw allow 8080
  ```
- **Ray worker can't connect**: check firewall, ray uses ports 6379, 8265, and random high ports
  ```bash
  sudo ufw allow 6379
  sudo ufw allow 8265
  ```
- **Python version mismatch**: both laptops MUST have same python version
- **Module not found on worker**: install same requirements.txt on friend's laptop
