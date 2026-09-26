"""One-command demo runner for the whole project.

``run_demo.py`` does everything end to end:

    1. Ensure the virtual environment exists & required libraries are installed
       (auto-runs scripts/bootstrap.ps1 when .venv is missing).
    2. Download the real Sentiment140 dataset if it is missing (fallback: a
       built-in 400-tweet demo set when offline).
    3. Split the dataset into 4 deterministic parts:
           data/split/train.csv       - PART 1: model training
           data/split/test.csv        - PART 2: testing (accuracy etc.)
           data/split/validation.csv  - PART 3: validation (extra metrics)
           data/split/simulation.csv  - PART 4: live simulation feed (ONLY the
                                        demo streams tweets from this part)
    4. Train the model on PART 1 and evaluate on PARTS 2 & 3.
    5. Docker Kafka -> Spark streaming -> producer streaming PART 4 as if it
       were live -> Streamlit dashboard (browser opens automatically).

Press Ctrl+C to stop everything and bring Docker down cleanly.

Usage (from the project root):

    pwsh -c "& .venv\\Scripts\\python.exe scripts\\run_demo.py"

Or simply (the script bootstraps the venv itself):

    python scripts\\run_demo.py

Options:

    --messages-per-second 10        publish rate (default 10)
    --limit 50000                   train on first N PART-1 rows (faster startup)
    --csv <path>                    use a single CSV directly (no 4-way split)
    --port 8501                     Streamlit port
    --no-dashboard                  skip opening the dashboard
    --keep-output                   don't wipe output/checkpoints and Kafka data
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import shutil
import subprocess
import sys
import time
import urllib.request
import webbrowser
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = BASE_DIR / "src"
SCRIPTS_DIR = BASE_DIR / "scripts"
LOG_DIR = BASE_DIR / "logs"
DEMO_DATASET = BASE_DIR / "data" / "demo_sample.csv"
SPLIT_DIR = BASE_DIR / "data" / "split"
SPLIT_PARTS = ("train", "test", "validation", "simulation")

SENTIMENT140_ZIP_URL = "https://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip"
SENTIMENT140_CSV_NAME = "training.1600000.processed.noemoticon.csv"

# Small built-in set of clearly positive/negative tweets, used ONLY when the
# real Sentiment140 file can't be downloaded so the live demo always works.
DEMO_TWEETS: list[tuple[str, str]] = [
    ("4", "I absolutely love this product it is amazing"),
    ("4", "this movie was fantastic highly recommend"),
    ("4", "awesome service quick and friendly staff"),
    ("4", "what a wonderful day today feels great"),
    ("4", "the best experience I ever had thank you"),
    ("4", "so happy with my new phone it works perfectly"),
    ("4", "great news my project was accepted"),
    ("4", "feeling very good excited about the future"),
    ("4", "the food here is delicious and filling"),
    ("4", "my team won the championship so proud"),
    ("4", "this song is beautiful and moves me"),
    ("4", "excellent customer support solved everything"),
    ("4", "my vacation was perfect every minute of it"),
    ("4", "the app update is much better than before"),
    ("4", "life is good and I am thankful"),
    ("4", "gorgeous sunset tonight what a blessing"),
    ("4", "the presentation went smoothly great job"),
    ("4", "my favorite team scored such a thrill"),
    ("4", "delivered on time and in perfect condition"),
    ("4", "highly impressed with this book recommended"),
    ("4", "wonderful news from the doctor everything fine"),
    ("4", "the discounts this weekend are amazing"),
    ("4", "my code compiles first try incredible"),
    ("4", "coffee and rain my perfect morning"),
    ("4", "thanks for the birthday wishes you all rock"),
    ("0", "broken useless piece of junk do not bother"),
    ("0", "terrible experience never coming back again"),
    ("0", "the movie was boring horrible waste of time"),
    ("0", "I hate this product do not buy it awful"),
    ("0", "the service was rude and disappointing"),
    ("0", "this is the worst thing I ever bought"),
    ("0", "my order arrived late and damaged"),
    ("0", "so frustrated with this company right now"),
    ("0", "the app keeps crashing infuriating"),
    ("0", "horrible customer support nobody answers"),
    ("0", "do not trust them they cheated me"),
    ("0", "my flight was cancelled again ridiculous"),
    ("0", "this weather is miserable and depressing"),
    ("0", "my exam went terribly I am so sad"),
    ("0", "the food was cold tasteless disgusting"),
    ("0", "waste of money complete scam stay away"),
    ("0", "my laptop died on day two unforgivable"),
    ("0", "the traffic today was unbelievably bad"),
    ("0", "this update made everything worse"),
    ("0", "deeply disappointed with the quality"),
    ("0", "the manager was rude and unhelpful"),
    ("0", "my package got stolen what a nightmare"),
    ("0", "the show was a disaster boring"),
    ("0", "overpriced and poor value do not recommend"),
    ("0", "so angry right now do not talk to me"),
    ("0", "the battery lasts two hours unacceptable"),
]


def log(message: str) -> None:
    print(f"[demo] {message}", flush=True)


def run(args_list: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(args_list, cwd=str(BASE_DIR), **kwargs)


def ensure_venv() -> Path:
    """Create the venv + install libraries if missing, then run under it."""
    venv_py = BASE_DIR / ".venv" / "Scripts" / "python.exe"
    if venv_py.exists():
        return venv_py
    log("No virtual environment found - running scripts/bootstrap.ps1 "
        "(downloads portable JRE, winutils and pip-installs the requirements)...")
    script = SCRIPTS_DIR / "bootstrap.ps1"
    if not script.exists():
        raise RuntimeError(f"Missing {script} - cannot bootstrap the environment.")
    result = run([
        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
        "-File", str(script),
    ])
    if result.returncode != 0 or not venv_py.exists():
        raise RuntimeError(
            "Bootstrap failed. Re-run it manually and check the output:\n"
            f"  powershell -ExecutionPolicy Bypass -File {script}"
        )
    log(".venv created. Relaunching this script with the venv interpreter...")
    code = subprocess.call(
        [str(venv_py), str(Path(__file__).resolve()), *sys.argv[1:]],
        cwd=str(BASE_DIR),
    )
    raise SystemExit(code)


REQUIRED_PACKAGES = ("confluent_kafka", "pyspark", "streamlit", "pandas")


def ensure_libraries(venv_py: Path) -> None:
    """Install requirements.txt when any required import is missing."""
    def _found() -> bool:
        return all(importlib.util.find_spec(pkg) is not None for pkg in REQUIRED_PACKAGES)

    if _found():
        return
    log("Installing required Python libraries into the venv "
        "(pip install -r requirements.txt)...")
    result = run([str(venv_py), "-m", "pip", "install",
                  "-r", str(BASE_DIR / "requirements.txt")])
    if result.returncode != 0 or not _found():
        raise RuntimeError(
            "pip install failed. Run it manually:\n"
            f"  {venv_py} -m pip install -r requirements.txt"
        )
    log("Libraries installed.")


def download_sentiment140(dest: Path) -> bool:
    """Download + extract the real Sentiment140 CSV. Returns True on success."""
    if dest.exists():
        return True
    log(f"Downloading Sentiment140 (~80 MB) from {SENTIMENT140_ZIP_URL} ...")
    tmp_zip = BASE_DIR / "data" / "trainingandtestdata.zip"
    tmp_zip.parent.mkdir(parents=True, exist_ok=True)
    try:
        urllib.request.urlretrieve(SENTIMENT140_ZIP_URL, tmp_zip)
        with zipfile.ZipFile(tmp_zip) as zf:
            zf.extract(SENTIMENT140_CSV_NAME, dest.parent)
        tmp_zip.unlink(missing_ok=True)
        log(f"Dataset ready: {dest}")
        return True
    except Exception as exc:  # offline / blocked network -> fall back to demo.
        log(f"Could not download the real dataset ({exc.__class__.__name__}).")
        tmp_zip.unlink(missing_ok=True)
        return False


def write_demo_dataset() -> Path:
    """Generate a small labelled CSV so the live demo runs without downloads."""
    if DEMO_DATASET.exists():
        return DEMO_DATASET
    DEMO_DATASET.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    with open(DEMO_DATASET, "w", encoding="latin-1", newline="") as f:
        writer = csv.writer(f)
        for i in range(400):
            label, text = DEMO_TWEETS[i % len(DEMO_TWEETS)]
            stamp = (now.replace(microsecond=0) - timedelta(minutes=i % 300)
                     ).strftime("%a %b %d %H:%M:%S +0000 %Y")
            writer.writerow([label, str(1000 + i), stamp, "NO_QUERY",
                             f"user{i % 50}", text])
    log(f"Generated built-in demo dataset: {DEMO_DATASET} (400 tweets)")
    return DEMO_DATASET


if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import config  # noqa: E402

VENV_PY = ensure_venv()
ensure_libraries(VENV_PY)

from dataset_split import split_dataset  # noqa: E402

PROCS: list[tuple[str, subprocess.Popen]] = []
KAFKA_READY_MARKERS = ("(kafka) started", "Kafka Server started")

logging_configured = False


def ensure_logging() -> None:
    global logging_configured
    if not logging_configured and not LOG_DIR.exists():
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging_configured = True


def docker_started() -> bool:
    return run(["docker", "ps"], capture_output=True).returncode == 0


def start_docker_desktop() -> None:
    log("Docker is not running -> starting Docker Desktop (this can take a minute)...")
    try:
        subprocess.Popen(
            ["cmd", "/c", "start", "",
             "C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe"],
            cwd=str(BASE_DIR),
        )
    except FileNotFoundError:
        log("Could not auto-start Docker Desktop; please start it manually.")
    for _ in range(60):
        time.sleep(5)
        if docker_started():
            log("Docker is ready.")
            return
    raise RuntimeError("Docker did not become ready in time.")


def kafka_ready() -> bool:
    try:
        out = run(
            ["docker", "logs", "sml-kafka"], capture_output=True, timeout=30
        )
        combined = (out.stdout or b"").decode(errors="ignore")
        return any(m in combined for m in KAFKA_READY_MARKERS)
    except Exception:
        return False


def wait_kafka(timeout_s: int = 300) -> None:
    log("Waiting for the Kafka broker to become ready...")
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if kafka_ready():
            log("Kafka broker is up (container: sml-kafka).")
            return
        time.sleep(5)
    raise RuntimeError("Kafka did not become ready in time. See: docker logs sml-kafka")


def start_process(name: str, cmd: list[str]) -> None:
    ensure_logging()
    out = open(LOG_DIR / f"{name}.log", "a", encoding="utf-8")
    proc = subprocess.Popen(cmd, cwd=str(BASE_DIR), stdout=out, stderr=subprocess.STDOUT)
    PROCS.append((name, proc))
    log(f"Started {name} (pid {proc.pid}) -> logs/{name}.log")
    time.sleep(2)


def wait_streaming_ready(timeout_s: int = 180) -> None:
    log("Waiting for the Spark streaming application to connect to Kafka...")
    logfile = LOG_DIR / "streaming.log"
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if logfile.exists():
            text = logfile.read_text(encoding="utf-8", errors="ignore")
            if "Streaming queries started" in text:
                log("Streaming queries are running (predictions + trends).")
                return
            if "Traceback" in text or "ERROR" in text:
                raise RuntimeError(
                    "The streaming application failed. See logs/streaming.log"
                )
        time.sleep(3)
    raise RuntimeError("Timed out waiting for the streaming app. See logs/streaming.log")


def dashboard_ready(url: str, timeout_s: int = 90) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url + "/_stcore/health", timeout=5) as resp:
                if resp.read() == b"ok":
                    return True
        except Exception:
            pass
        time.sleep(3)
    return False


def stop_all() -> None:
    log("Stopping application processes...")
    for name, proc in PROCS:
        try:
            if proc.poll() is None:
                subprocess.run(
                    ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                    capture_output=True,
                )
                log(f"Stopped {name} (pid {proc.pid})")
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
    PROCS.clear()
    time.sleep(2)


def cleanup(kill_docker: bool) -> None:
    stop_all()
    if kill_docker and docker_started():
        log("Stopping Kafka container...")
        run(["docker", "compose", "down", "-v" if not args.keep_output else ""],
            capture_output=True)


def prepare_data(csv_arg: str | None) -> dict:
    """Resolve the data plan.

    Returns a dict with 'kind' ('split' | 'demo') and the relevant paths:
        split -> train/test/validation/simulation part files
        demo  -> train/stream point at the same single CSV
    """

    if csv_arg:
        path = Path(csv_arg)
        if not path.exists():
            raise FileNotFoundError(f"Dataset not found: {path}")
        log(f"Using a single dataset directly: {path} (no 4-way split).")
        return {
            "kind": "demo",
            "train": path,
            "test": None,
            "valid": None,
            "stream": path,
            "note": f"single dataset: {path.name}",
        }

    if config.DATASET_PATH.exists():
        log("Sentiment140 dataset found - splitting into 4 parts "
            "(train 60% / test 15% / validation 15% / simulation 10%).")
        parts = {name: SPLIT_DIR / f"{name}.csv" for name in SPLIT_PARTS}
        if not all(path.exists() for path in parts.values()):
            split_dataset(config.DATASET_PATH, SPLIT_DIR)
            for name, path in parts.items():
                if not path.exists():
                    raise RuntimeError(f"Expected split part missing: {path}")
        train_rows = sum(
            1 for _ in open(parts["train"], "r", encoding="latin-1", newline="")
        )
        log(f"4 way split ready: train({train_rows:,}) / test / validation / "
            f"simulation({sum(1 for _ in open(parts['simulation'], 'r', encoding='latin-1', newline='')):,})")
        return {
            "kind": "split",
            "train": parts["train"],
            "test": parts["test"],
            "valid": parts["validation"],
            "stream": parts["simulation"],
            "note": "simulation.csv (PART 4) - 10% hold-out tweets streamed live",
        }

    log("Real Sentiment140 dataset not found - trying to download it...")
    if not download_sentiment140(config.DATASET_PATH):
        log("Offline fallback: using the built-in 400-tweet demo dataset.")
        demo = write_demo_dataset()
        return {
            "kind": "demo",
            "train": demo,
            "test": None,
            "valid": None,
            "stream": demo,
            "note": "built-in demo tweets (offline fallback)",
        }
    return prepare_data(None)


def start_training_if_needed(plan: dict, limit: int | None) -> subprocess.Popen | None:
    """Start training (Part 1) + evaluation (Parts 2/3) unless a model exists."""
    if config.MODEL_DIR.joinpath("metadata").exists():
        log(f"Model already present at {config.MODEL_DIR} - skipping training.")
        return None
    log("No trained model found -> training on PART 1 (train.csv) and evaluating "
        "on PARTS 2 & 3 (test/validation)...")
    cmd = [str(VENV_PY), "src/train_model.py"]
    if plan["kind"] == "split":
        cmd += [
            f"--train-dataset={plan['train']}",
            f"--test-dataset={plan['test']}",
            f"--valid-dataset={plan['valid']}",
        ]
    else:
        cmd += [f"--dataset={plan['train']}"]
    if limit:
        cmd += [f"--limit={limit}"]
    ensure_logging()
    out = open(LOG_DIR / "train.log", "a", encoding="utf-8")
    proc = subprocess.Popen(cmd, cwd=str(BASE_DIR), stdout=out, stderr=subprocess.STDOUT)
    log(f"Training running in the background (pid {proc.pid}) -> logs/train.log")
    return proc


def wait_for_training(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    log("Waiting for model training/validation to finish...")
    proc.wait()
    if proc.returncode != 0 or not config.MODEL_DIR.joinpath("metadata").exists():
        raise RuntimeError(
            "Model training failed. See logs/train.log.\n"
            "You can also train manually with:  python src/train_model.py"
        )
    log("Model trained and validated. Metrics saved to artifacts/model_metrics.json.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the whole project for a demo")
    parser.add_argument("--messages-per-second", type=int, default=10)
    parser.add_argument("--limit", type=int, default=None,
                        help="train on first N PART-1 rows (faster)")
    parser.add_argument("--csv", default=None,
                        help="use a single CSV directly (no 4-way split)")
    parser.add_argument("--port", type=int, default=8501)
    parser.add_argument("--no-dashboard", action="store_true")
    parser.add_argument("--keep-output", action="store_true",
                        help="do not wipe previous output/checkpoints/Kafka data")
    global args
    args = parser.parse_args()

    log("=== Starting Real-Time Social Media Sentiment Monitoring demo ===")

    config.ensure_java_and_hadoop_env()
    config.ensure_directories()

    plan = prepare_data(args.csv)
    train_proc = start_training_if_needed(plan, args.limit)

    if not docker_started():
        start_docker_desktop()

    log("Starting Kafka with Docker Compose...")
    run(["docker", "compose", "up", "-d"])
    wait_kafka()

    wait_for_training(train_proc)

    if not args.keep_output:
        for p in (config.PREDICTIONS_DIR, config.TRENDS_DIR,
                  config.CHECKPOINT_PREDICTIONS_DIR, config.CHECKPOINT_TRENDS_DIR):
            shutil.rmtree(p, ignore_errors=True)
        log("Cleaned previous output/ and checkpoints/.")

    start_process("streaming", [str(VENV_PY), "src/streaming_sentiment.py"])
    wait_streaming_ready()

    start_process("producer", [
        str(VENV_PY), "src/kafka_producer.py",
        f"--dataset={plan['stream']}",
        f"--messages-per-second={args.messages_per_second}",
        "--repeat=0",
    ])

    # The producer must start publishing straight away; fail loudly if it dies.
    time.sleep(8)
    producer_proc = PROCS[-1][1] if PROCS and PROCS[-1][0] == "producer" else None
    if producer_proc is not None and producer_proc.poll() is not None:
        logfile = LOG_DIR / "producer.log"
        tail = logfile.read_text(encoding="utf-8", errors="ignore").splitlines()[-6:]
        raise RuntimeError(
            "The producer stopped unexpectedly. See logs/producer.log. Details:\n"
            + "\n".join(tail)
        )

    if not args.no_dashboard:
        url = f"http://localhost:{args.port}"
        start_process("dashboard", [
            str(VENV_PY), "-m", "streamlit", "run", "dashboard/app.py",
            "--server.headless", "true",
            "--server.port", str(args.port),
        ])
        log("Waiting for the dashboard to come up...")
        if dashboard_ready(url):
            webbrowser.open(url)
            log(f"Dashboard open at {url}")

    log("=" * 62)
    log("  LIVE - all components running:")
    log(f"    Kafka broker ......... localhost:9092 (docker)")
    log(f"    Spark streaming ...... predictions + 1-min trends")
    log(f"    Producer ............. {args.messages_per_second} msg/s (looping)")
    log(f"    Live tweet source .... {plan['note']}")
    log(f"    Dashboard ............ http://localhost:{args.port}")
    log("")
    log("  Press Ctrl+C in this window to stop everything.")
    log("=" * 62)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("Stopping demo...")
        cleanup(kill_docker=not args.keep_output)
        log("Done. See you next time!")
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        if PROCS:
            cleanup(kill_docker=not getattr(args, "keep_output", False))