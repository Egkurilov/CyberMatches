from bs4 import BeautifulSoup

from cybermatches.common.text import (
    extract_team_name_from_tag,
    is_placeholder_team,
    parse_bo_int,
    parse_score_tuple,
    strip_page_does_not_exist,
)


def test_strip_page_does_not_exist():
    assert strip_page_does_not_exist("Team A (page does not exist)") == "Team A"
    assert strip_page_does_not_exist("Team B") == "Team B"


def test_extract_team_name_from_tag_prefers_title():
    soup = BeautifulSoup('<a title="Team X (page does not exist)">X</a>', "html.parser")
    tag = soup.find("a")
    assert extract_team_name_from_tag(tag) == "Team X"


def test_parse_bo_int():
    assert parse_bo_int("Bo3") == 3
    assert parse_bo_int("(bo5)") == 5
    assert parse_bo_int("") is None


def test_parse_score_tuple_default_max():
    assert parse_score_tuple("2:1") == (2, 1)
    assert parse_score_tuple("2025:14") is None


def test_parse_score_tuple_custom_max():
    assert parse_score_tuple("13:16", max_points=50) == (13, 16)


def test_is_placeholder_team():
    assert is_placeholder_team("TBD") is True
    assert is_placeholder_team("to be decided") is True
    assert is_placeholder_team("Team Liquid") is False
