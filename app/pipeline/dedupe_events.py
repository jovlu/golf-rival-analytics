import json
from pathlib import Path

from app.pipeline.paths import DEDUPED_EVENTS_FILE, EVENTS_FILE


def dedupe_events_jsonl(
    src_file: Path = EVENTS_FILE,
    dst_file: Path = DEDUPED_EVENTS_FILE,
):
    events_by_id = {}

    with src_file.open("r", encoding="utf-8") as source:
        for line in source:
            original_line = line.strip()

            if original_line == "":
                continue

            try:
                current_row = json.loads(original_line)
            except json.JSONDecodeError:
                continue

            if type(current_row) is not dict or type(current_row.get("id")) is not int:
                continue

            current_id = current_row["id"]
            saved_row = events_by_id.get(current_id)

            if saved_row is None:
                events_by_id[current_id] = current_row
                continue

            saved_timestamp = saved_row.get("timestamp")
            current_timestamp = current_row.get("timestamp")
            if type(saved_timestamp) is int and type(current_timestamp) is int:
                if current_timestamp < saved_timestamp:
                    events_by_id[current_id] = current_row
            elif type(current_timestamp) is int:
                events_by_id[current_id] = current_row

    deduped_rows = list(events_by_id.values())
    deduped_rows.sort(
        key=lambda row: (
            type(row.get("timestamp")) is not int,
            row.get("timestamp", 0),
            row["id"],
        )
    )

    with dst_file.open("w", encoding="utf-8") as output:
        for row in deduped_rows:
            output.write(json.dumps(row))
            output.write("\n")


if __name__ == "__main__":
    dedupe_events_jsonl()
