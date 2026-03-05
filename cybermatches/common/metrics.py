from __future__ import annotations

import os
import time
from typing import Optional

from prometheus_client import Gauge, Info, start_http_server

_START_TIME = time.time()
_STARTED = False

_PARSE_INFO = Info(
    "cybermatches_parse_info",
    "Last parse metadata",
)

UPTIME_SECONDS = Gauge(
    "cybermatches_uptime_seconds",
    "Parser uptime in seconds",
    ["game"],
)

MATCHES_TOTAL = Gauge(
    "cybermatches_matches_total",
    "Total matches in storage",
    ["game"],
)

MATCHES_TODAY = Gauge(
    "cybermatches_matches_today",
    "Matches scheduled for today (MSK)",
    ["game"],
)

PARSE_LAST_SUCCESS = Gauge(
    "cybermatches_parse_last_success",
    "Last parse success (1) or failure (0)",
    ["game"],
)

PARSE_LAST_DURATION_SECONDS = Gauge(
    "cybermatches_parse_last_duration_seconds",
    "Duration of the last parse run",
    ["game"],
)

PARSE_LAST_TIMESTAMP = Gauge(
    "cybermatches_parse_last_timestamp",
    "Unix timestamp of the last parse run",
    ["game"],
)

PARSE_LAST_ERROR = Gauge(
    "cybermatches_parse_last_error",
    "Last parse had error (1) or not (0)",
    ["game"],
)

PARSE_LAST_PARSED_MATCHES = Gauge(
    "cybermatches_parse_last_parsed_matches",
    "Matches parsed in the last run",
    ["game"],
)

PARSE_LAST_DEDUPED_MATCHES = Gauge(
    "cybermatches_parse_last_deduped_matches",
    "Matches after deduplication in the last run",
    ["game"],
)


def start_metrics_server() -> None:
    global _STARTED
    if _STARTED:
        return

    addr = os.getenv("METRICS_ADDR", "0.0.0.0")
    port = int(os.getenv("METRICS_PORT", "9108"))
    start_http_server(port, addr)
    _STARTED = True


def update_uptime(game: str) -> None:
    UPTIME_SECONDS.labels(game).set(time.time() - _START_TIME)


def update_counts(game: str, total: int, today: int) -> None:
    MATCHES_TOTAL.labels(game).set(total)
    MATCHES_TODAY.labels(game).set(today)


def record_parse_result(
    game: str,
    success: bool,
    duration_sec: float,
    parsed_count: Optional[int] = None,
    deduped_count: Optional[int] = None,
    error: Optional[str] = None,
) -> None:
    PARSE_LAST_SUCCESS.labels(game).set(1 if success else 0)
    PARSE_LAST_ERROR.labels(game).set(1 if error else 0)
    PARSE_LAST_DURATION_SECONDS.labels(game).set(duration_sec)
    PARSE_LAST_TIMESTAMP.labels(game).set(time.time())
    if parsed_count is not None:
        PARSE_LAST_PARSED_MATCHES.labels(game).set(parsed_count)
    if deduped_count is not None:
        PARSE_LAST_DEDUPED_MATCHES.labels(game).set(deduped_count)

    _PARSE_INFO.info(
        {
            "game": game,
            "success": "1" if success else "0",
            "error": error or "",
        }
    )
