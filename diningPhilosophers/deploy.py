import logging
import time
import os
import tarfile
from pathlib import Path
import enoslib as en
from tqdm import tqdm

en.init_logging(level=logging.INFO)

# --- Experiment parameters ---
ACTOR_IMAGE = "collaborativestatemachines/cirrina-baselines-diningPhilosophers"
REDIS_IMAGE = "redis:8.2.4-alpine"
SIDECAR_IMAGE = "daprio/daprd:edge"
PLACEMENT_IMAGE = "daprio/placement:1.16.0"
COMPONENTS_PATH = "/tmp/dapr-components"
LOCAL_ROOT        = Path("./results")
TIME_BEFORE_FETCH = 60 * 20
NUM_RUNS          = 5
# -------------------------------------

# Infrastructure Reservation
conf = (
    en.G5kConf.from_settings(job_name=Path(__file__).name, walltime="03:00:00")
    .add_machine(roles=["arbitrator"], cluster="gros", nodes=1)
    .add_machine(roles=["worker"], cluster="gros", nodes=6)
)

provider = en.G5k(conf)
roles, networks = provider.init()

# Initial Docker engine deployment
registry_opts = dict(type="external", ip="docker-cache.grid5000.fr", port=80)
d = en.Docker(
    agent=roles["arbitrator"] + roles["worker"],
    bind_var_docker="/tmp/docker",
    registry_opts=registry_opts
)
d.deploy()

# Retrieve arbitrator hostname
arbitrator_addr = roles["arbitrator"][0].address

# Dynamically create pub sub configuration
pubsub_yaml = f"""apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: pubsub
spec:
  type: pubsub.redis
  version: v1
  metadata:
    - name: redisHost
      value: "{arbitrator_addr}:6379"
    - name: redisPassword
      value: ""
"""

Path("../config/redis/components-grid5000/pubsub.yaml").write_text(pubsub_yaml)

# Network emulation
netem = en.NetemHTB()
netem.add_constraints(
    src=roles["worker"], dest=roles["arbitrator"],
    delay="40ms", rate="1gbit", symmetric=True
)
netem.deploy()

for run_idx in range(1, NUM_RUNS + 1):
    run_label = f"run{run_idx}"
    print(f"\n>>> Starting {run_label}...")

    # Ensure a fresh metrics directory on every node and push component files
    with en.actions(roles=roles) as a:
        a.file(path="/tmp/metrics", state="absent")
        a.file(path="/tmp/metrics", state="directory", mode="0777")
        a.file(path=COMPONENTS_PATH, state="directory")
        a.copy(src="../config/redis/components-grid5000/", dest=COMPONENTS_PATH + "/")

    # Deploy Containers on Arbitrator
    with en.actions(roles=roles["arbitrator"]) as a:
        a.docker_container(
            name="redis", image=REDIS_IMAGE,
            network_mode="host", state="started"
        )
        a.docker_container(
            name="placement", image=PLACEMENT_IMAGE,
            network_mode="host", state="started",
            command=["./placement", "--port", "50006"]
        )
        a.docker_container(
            name="arbitrator-sidecar", image=SIDECAR_IMAGE,
            network_mode="host", state="started",
            command=[
                "./daprd",
                "--app-id", "arbitrator-sidecar",
                "--app-port", "3000",
                "--resources-path", "/components",
                "--placement-host-address", "localhost:50006",
                "--metrics-port", "9091",
            ],
            volumes=[f"{COMPONENTS_PATH}:/components"],
        )
        a.docker_container(
            name="arbitrator", image=ACTOR_IMAGE,
            network_mode="host", state="started",
            volumes=["/tmp/metrics:/metrics:rw"],
            env={
                "ROLE": "arbitrator",
                "NUMBER_OF_PHILOSOPHERS": "6",
                "DAPR_HTTP_ENDPOINT": "http://localhost:3500",
                "DAPR_GRPC_ENDPOINT": "http://localhost:50001",
                "METRICS_DIRECTORY": "/metrics",
            },
        )

    # Deploy Containers on Workers
    for i, host in enumerate(roles["worker"]):
        with en.actions(pattern_hosts=host.address, roles=roles) as a:
            a.docker_container(
                name="redis", image=REDIS_IMAGE,
                network_mode="host", state="started"
            )
            a.docker_container(
                name="placement", image=PLACEMENT_IMAGE,
                network_mode="host", state="started",
                command=["./placement", "--port", "50006"]
            )
            a.docker_container(
                name=f"w{i}-sidecar", image=SIDECAR_IMAGE,
                network_mode="host", state="started",
                command=[
                    "./daprd",
                    "--app-id", f"w{i}-sidecar",
                    "--app-port", "3000",
                    "--resources-path", "/components",
                    "--placement-host-address", "localhost:50006",
                    "--metrics-port", "9091",
                ],
                volumes=[f"{COMPONENTS_PATH}:/components"],
            )
            a.docker_container(
                name=f"w{i}", image=ACTOR_IMAGE,
                network_mode="host", state="started",
                volumes=["/tmp/metrics:/metrics:rw"],
                env={
                    "PHILOSOPHER_ID": str(i),
                    "DAPR_HTTP_ENDPOINT": "http://localhost:3500",
                    "DAPR_GRPC_ENDPOINT": "http://localhost:50001",
                    "METRICS_DIRECTORY": "/metrics",
                },
            )

    # Wait for data collection
    print(f"--- {run_label}: Collecting data for {TIME_BEFORE_FETCH}s ---")
    for _ in tqdm(range(TIME_BEFORE_FETCH), desc=run_label, unit="s", mininterval=60):
        time.sleep(1)

    # Fetch and Organize Results locally into run1, run2, etc.
    run_dest = LOCAL_ROOT / run_label
    run_dest.mkdir(parents=True, exist_ok=True)

    with en.actions(roles=roles) as a:
        a.archive(path="/tmp/metrics", dest="/tmp/metrics.tar.gz", format="gz")
        a.fetch(src="/tmp/metrics.tar.gz", dest=str(run_dest), flat=False)

    # Local extraction
    print(f"--- {run_label}: Extracting results ---")
    for host in en.get_hosts(roles):
        host_dir = run_dest / host.address
        tar_path = host_dir / "tmp" / "metrics.tar.gz"

        if tar_path.exists():
            with tarfile.open(tar_path, "r:gz") as tar:
                tar.extractall(path=host_dir)

            tar_path.unlink()
            try:
                (host_dir / "tmp").rmdir()
            except OSError:
                pass
    # Clean up all containers for next run
    with en.actions(roles=roles) as a:
        a.shell("docker rm -f $(docker ps -aq) || true")

print(f"\n--- SUCCESS: All {NUM_RUNS} runs finished. Data in {LOCAL_ROOT} ---")
provider.destroy()