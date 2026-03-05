from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo

from bs4 import Tag


MONTHS: dict[str, int] = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}

DEFAULT_TZ_IANA_MAP = {
    "UTC": "UTC",
    "GMT": "UTC",
    "CET": "Europe/Berlin",
    "CEST": "Europe/Berlin",
    "EET": "Europe/Athens",
    "EEST": "Europe/Athens",
    "WET": "Europe/Lisbon",
    "MSK": "Europe/Moscow",
    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
    "EST": "America/New_York",
    "EDT": "America/New_York",
    "CST": "Asia/Shanghai",
    "HKT": "Asia/Hong_Kong",
    "SGT": "Asia/Singapore",
    "JST": "Asia/Tokyo",
    "KST": "Asia/Seoul",
    "IST": "Asia/Kolkata",
    "GST": "Asia/Dubai",
    "PET": "America/Lima",
    "BRT": "America/Sao_Paulo",
}

_TIME_RE = re.compile(
    r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})\s*-\s*(\d{1,2}):(\d{2})\s*([A-Z]{2,6})?"
)


def parse_time_to_msk(time_str: str, tz_map: Optional[dict[str, str]] = None) -> Optional[datetime]:
    if not time_str:
        return None

    cleaned = re.sub(r"<.*?>", "", time_str)
    cleaned = " ".join(cleaned.split())

    m = re.search(
        r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})\s*-\s*(\d{1,2}):(\d{2})\s*([A-Z]{2,4})",
        cleaned,
    )
    if not m:
        return None

    month_name, day, year, hour, minute, tz_abbr = m.groups()
    month = MONTHS.get(month_name)
    if not month:
        return None

    try:
        dt_naive = datetime(int(year), month, int(day), int(hour), int(minute))
    except ValueError:
        return None

    tz_name = (tz_map or DEFAULT_TZ_IANA_MAP).get(tz_abbr)
    if not tz_name:
        tz_name = "UTC"

    try:
        src_tz = ZoneInfo(tz_name)
        dt_src = dt_naive.replace(tzinfo=src_tz)
        return dt_src.astimezone(ZoneInfo("Europe/Moscow"))
    except Exception:
        return None


def parse_time_to_target_tz(
    time_str: str,
    target_tz: ZoneInfo,
    container: Optional[Tag] = None,
    tz_map: Optional[dict[str, str]] = None,
) -> Optional[datetime]:
    if not time_str:
        return None

    cleaned = re.sub(r"<.*?>", "", time_str)
    cleaned = " ".join(cleaned.split())

    m = _TIME_RE.search(cleaned)
    if not m:
        return None

    month_name, day, year, hour, minute, tz_abbr = m.groups()
    month = MONTHS.get(month_name)
    if not month:
        return None

    try:
        dt_naive = datetime(int(year), month, int(day), int(hour), int(minute))
    except ValueError:
        return None

    offset = None
    if container:
        ab = container.select_one(".timer-object-date abbr[data-tz]")
        if ab:
            offset = (ab.get("data-tz") or "").strip() or None

    if offset and re.match(r"^[\+\-]\d{1,2}:\d{2}$", offset):
        sign = 1 if offset.startswith("+") else -1
        hh, mm = offset[1:].split(":")
        delta = timedelta(hours=int(hh) * sign, minutes=int(mm) * sign)
        dt_utc = (dt_naive - delta).replace(tzinfo=timezone.utc)
        return dt_utc.astimezone(target_tz)

    tz_abbr = (tz_abbr or "").strip()
    tz_name = (tz_map or DEFAULT_TZ_IANA_MAP).get(tz_abbr, "UTC")
    try:
        src_tz = ZoneInfo(tz_name)
        dt_src = dt_naive.replace(tzinfo=src_tz)
        return dt_src.astimezone(target_tz)
    except Exception:
        return None


def parse_liquipedia_time(
    raw: str,
    target_tz: ZoneInfo,
    tz_map: Optional[dict[str, str]] = None,
) -> tuple[datetime | None, datetime | None]:
    raw = (raw or "").strip()
    if not raw:
        return None, None

    m = re.match(r"^(.*?\d{4})\s*-\s*(\d{2}:\d{2})([A-Z]+)$", raw)
    if not m:
        return None, None

    date_part, time_part, tz_abbr = m.groups()
    tz_abbr = tz_abbr.strip()

    tz_name = (tz_map or DEFAULT_TZ_IANA_MAP).get(tz_abbr)
    if not tz_name:
        return None, None

    try:
        naive = datetime.strptime(f"{date_part} {time_part}", "%B %d, %Y %H:%M")
    except ValueError:
        return None, None

    dt_local = naive.replace(tzinfo=ZoneInfo(tz_name))
    dt_utc = dt_local.astimezone(ZoneInfo("UTC"))
    return dt_utc, dt_local.astimezone(target_tz)
