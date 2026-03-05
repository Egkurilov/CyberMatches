#!/usr/bin/env python3
from cybermatches.parsers.dota import update_scores_with_retry, refresh_statuses_in_db


def main() -> None:
    update_scores_with_retry()
    refresh_statuses_in_db()


if __name__ == "__main__":
    main()
