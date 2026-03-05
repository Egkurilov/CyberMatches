from __future__ import annotations

import logging
import os
import signal
import threading
import time
from pathlib import Path
from typing import Callable

from cybermatches.common.metrics import start_metrics_server, update_uptime
from cybermatches.parsers.cs2 import worker_once as cs2_worker_once
from cybermatches.parsers.dota import worker_once as dota_worker_once


BASE_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "scheduler.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("scheduler")


class WorkerThread(threading.Thread):
    def __init__(self, name: str, interval_sec: int, fn: Callable[[], None], stop_event: threading.Event):
        super().__init__(name=name, daemon=True)
        self.interval_sec = max(5, int(interval_sec))
        self.fn = fn
        self.stop_event = stop_event

    def run(self) -> None:
        logger.info("%s started (interval=%ss)", self.name, self.interval_sec)
        while not self.stop_event.is_set():
            start_ts = time.time()
            update_uptime(self.name)
            try:
                self.fn()
            except Exception as exc:
                logger.exception("%s failed: %s", self.name, exc)

            elapsed = time.time() - start_ts
            sleep_for = self.interval_sec - elapsed
            if sleep_for > 0:
                self.stop_event.wait(sleep_for)

        logger.info("%s stopped", self.name)


def main() -> None:
    start_metrics_server()

    stop_event = threading.Event()

    def _handle_signal(signum, _frame):
        logger.info("Received signal %s, stopping...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    dota_interval = int(os.getenv("DOTA_INTERVAL_SECONDS", "600"))
    cs2_interval = int(os.getenv("CS2_INTERVAL_SECONDS", "600"))

    threads = [
        WorkerThread("dota", dota_interval, dota_worker_once, stop_event),
        WorkerThread("cs2", cs2_interval, cs2_worker_once, stop_event),
    ]

    for t in threads:
        t.start()

    while not stop_event.is_set():
        time.sleep(1)


if __name__ == "__main__":
    main()
