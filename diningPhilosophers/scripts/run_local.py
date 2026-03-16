import subprocess
from datetime import datetime
from influxdb_client import InfluxDBClient
from pathlib import Path
import time
import argparse
import csv

URL = "http://localhost:8086"
TOKEN = "auth"
ORG = "dapr"
BUCKET = "bucket"

def get_data(result_folder: str, duration: int):
    client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)

    query_api = client.query_api()

    metrics_query = f"""
    from(bucket: "bucket")
      |> range(start: -{duration}s)
      |> filter(fn: (r) => r["_measurement"] == "total_meals")
      |> cumulativeSum(columns: ["_value"])
      |> aggregateWindow(every: 1s, fn: last, createEmpty: true)
      |> fill(usePrevious: true)
      |> group(columns: ["_time"])
      |> sum()
      |> group()
    """

    metrics = query_api.query_csv(query=metrics_query, org=ORG)

    with open(result_folder + "/metrics.csv", "w") as file:
        writer = csv.writer(file)
        for line in metrics:
            if line:
                writer.writerow(line)

    client.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=600)

    args = parser.parse_args()
    duration = args.duration

    print(f"Starting Dinning Philosophers (Dapr) for {duration} seconds...")
    try:
        subprocess.run(["docker", "compose", "up", "-d"])
        print("Containers launched successfully 🚀")
    except subprocess.CalledProcessError as e:
        print("Failed to launch Containers 💥 Return Code:", e.returncode)
        exit(1)

    time.sleep(duration)

    now = datetime.now().strftime("%m_%d_%H_%M")

    result_folder = f"./local_metrics/duration_{duration}_sec/{now}"

    Path(result_folder).mkdir(parents=True, exist_ok=True)

    print("Gathering data...")

    get_data(result_folder, duration)

    print(f"Saving results at: {result_folder}")

    print("Shutting down...")
    try:
        subprocess.run(["docker", "compose", "down", "-v"])
        print("Containers shut down successfully!")
    except subprocess.CalledProcessError as e:
        print("Failed to stop Containers, Return Code:", e.returncode)
        exit(1)

if __name__ == "__main__":
    main()

