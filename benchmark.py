import subprocess
import time
import sys
import psutil
import threading

# monitor peak resource usage in background
peak_cpu = [0]
peak_mem = [0]
monitoring = [True]

def monitor_resources():
    while monitoring[0]:
        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory().percent
        if cpu > peak_cpu[0]:
            peak_cpu[0] = cpu
        if mem > peak_mem[0]:
            peak_mem[0] = mem


def run_pipeline(script_name):
    print(f"\n{'='*60}")
    print(f"Running {script_name}...")
    print(f"{'='*60}")

    # reset monitors
    peak_cpu[0] = 0
    peak_mem[0] = 0
    monitoring[0] = True

    monitor_thread = threading.Thread(target=monitor_resources, daemon=True)
    monitor_thread.start()

    start = time.time()
    result = subprocess.run(
        [sys.executable, script_name],
        capture_output=False
    )
    elapsed = time.time() - start
    monitoring[0] = False
    monitor_thread.join(timeout=2)

    return {
        "script": script_name,
        "wall_clock_time": elapsed,
        "return_code": result.returncode,
        "peak_cpu_percent": peak_cpu[0],
        "peak_mem_percent": peak_mem[0]
    }


if __name__ == "__main__":
    print("Starting benchmark: Spark vs Ray")
    print(f"System: {psutil.cpu_count()} CPUs, {psutil.virtual_memory().total / 1024**3:.1f} GB RAM")

    spark_result = run_pipeline("spark_clean.py")
    ray_result = run_pipeline("ray_clean.py")

    # print comparison
    print("\n" + "=" * 60)
    print("BENCHMARK COMPARISON: SPARK vs RAY")
    print("=" * 60)
    print(f"{'Metric':<25} {'Spark':>15} {'Ray':>15}")
    print("-" * 55)
    print(f"{'Wall Clock Time (s)':<25} {spark_result['wall_clock_time']:>15.2f} {ray_result['wall_clock_time']:>15.2f}")
    print(f"{'Peak CPU (%)':<25} {spark_result['peak_cpu_percent']:>15.1f} {ray_result['peak_cpu_percent']:>15.1f}")
    print(f"{'Peak Memory (%)':<25} {spark_result['peak_mem_percent']:>15.1f} {ray_result['peak_mem_percent']:>15.1f}")
    print(f"{'Exit Code':<25} {spark_result['return_code']:>15} {ray_result['return_code']:>15}")
    print("=" * 60)

    # determine winner
    if spark_result['wall_clock_time'] < ray_result['wall_clock_time']:
        winner = "Spark"
        diff = ray_result['wall_clock_time'] - spark_result['wall_clock_time']
    else:
        winner = "Ray"
        diff = spark_result['wall_clock_time'] - ray_result['wall_clock_time']

    print(f"\nWinner (by wall-clock time): {winner} (faster by {diff:.2f}s)")
