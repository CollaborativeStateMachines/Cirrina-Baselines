import subprocess
from datetime import datetime
from pathlib import Path
import time
import argparse
import shutil

OUTPUT_PATH = "../output"
NUM_PHILOSOPHERS=6

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

    shutil.copytree(OUTPUT_PATH, result_folder)

    print(f"Metrics copied to -> {result_folder}")

    print("Cleaning up...")
    try:
        subprocess.run(["docker", "compose", "down", "-v"])
        print("Containers shut down successfully!")
    except subprocess.CalledProcessError as e:
        print("Failed to stop Containers, Return Code:", e.returncode)
        exit(1)

    for i in range(NUM_PHILOSOPHERS):
        file = Path(OUTPUT_PATH) / f"w{i}" / "metrics" / "total_meals.csv"
        file.unlink(missing_ok=True)

if __name__ == "__main__":
    main()

