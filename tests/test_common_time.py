from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo

from cybermatches.common.time import parse_liquipedia_time, parse_time_to_msk, parse_time_to_target_tz


def test_parse_time_to_msk():
    dt = parse_time_to_msk("January 8, 2026 - 13:00 IST")
    assert dt is not None
    assert dt.hour == 10
    assert dt.minute == 30


def test_parse_liquipedia_time():
    target_tz = ZoneInfo("Europe/Moscow")
    dt_utc, dt_msk = parse_liquipedia_time("December 6, 2025 - 13:40CET", target_tz)
    assert dt_utc is not None
    assert dt_msk is not None
    assert dt_utc.hour == 12
    assert dt_utc.minute == 40
    assert dt_msk.hour == 15
    assert dt_msk.minute == 40


def test_parse_time_to_target_tz():
    target_tz = ZoneInfo("Europe/Moscow")
    dt = parse_time_to_target_tz("December 6, 2025 - 13:40 CET", target_tz)
    assert dt is not None
    assert dt.hour == 15
    assert dt.minute == 40
