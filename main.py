import sys

from app.pipeline.clean_events import clean_events_jsonl
from app.pipeline.dedupe_events import dedupe_events_jsonl


def clean_jsonl():
    print("running dedupe_events_jsonl")
    dedupe_events_jsonl()
    print("running clean_events_jsonl")
    clean_events_jsonl()


def load_db():
    from app.pipeline.load_database import load_db as run_load_db

    run_load_db()


def check_db():
    from app.db import check_db_connection

    check_db_connection()
    print("database connection ok")


def run_all():
    clean_jsonl()
    load_db()


COMMANDS = {
    "clean_jsonl": clean_jsonl,
    "clean-jsonl": clean_jsonl,
    "load_db": load_db,
    "load-db": load_db,
    "check_db": check_db,
    "check-db": check_db,
    "all": run_all,
}


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else "clean_jsonl"

    if command not in COMMANDS:
        valid_commands = ", ".join(sorted(COMMANDS))
        raise SystemExit(
            f"unknown command: {command}\nvalid commands: {valid_commands}"
        )

    COMMANDS[command]()
    print(f"{command} finished")


if __name__ == "__main__":
    main()
