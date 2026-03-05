from __future__ import annotations

import re
from typing import Optional

from bs4 import Tag


_PAGE_MISSING_RE = re.compile(r"\s*\(page does not exist\)\s*$", re.IGNORECASE)
_SCORE_RE = re.compile(r"^\s*(\d+)\s*[:\-]\s*(\d+)\s*$")
_BO_RE = re.compile(r"(?:\(|\b)bo\s*([0-9]+)", re.IGNORECASE)


def strip_page_does_not_exist(name: str) -> str:
    if not name:
        return ""
    return _PAGE_MISSING_RE.sub("", name).strip()


def extract_team_name_from_tag(tag: Optional[Tag]) -> str:
    if not tag:
        return ""

    title = tag.get("title")
    if title:
        clean = strip_page_does_not_exist(title)
        if clean:
            return clean

    return strip_page_does_not_exist(tag.get_text(strip=True))


def normalize_team_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    name = name.strip()
    return name or None


def is_placeholder_team(name: Optional[str]) -> bool:
    if not name:
        return True
    normalized = name.strip().lower()
    return normalized in {"tbd", "tba", "to be decided", "to be determined", ""}


def parse_bo_int(bo: Optional[str]) -> Optional[int]:
    if not bo:
        return None
    m = _BO_RE.search(bo)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def parse_score_tuple(score: Optional[str], max_points: int = 10) -> Optional[tuple[int, int]]:
    if not score:
        return None
    m = _SCORE_RE.match(score)
    if not m:
        return None
    left = int(m.group(1))
    right = int(m.group(2))
    if left < 0 or right < 0 or left > max_points or right > max_points:
        return None
    return left, right
