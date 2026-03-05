#!/usr/bin/env python3
from cybermatches.parsers.cs2 import update_scores_from_match_pages, refresh_statuses_in_db


def main() -> None:
    update_scores_from_match_pages()
    refresh_statuses_in_db()


if __name__ == "__main__":
    main()
