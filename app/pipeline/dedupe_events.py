import json
from pathlib import Path

from app.pipeline.paths import DEDUPED_EVENTS_FILE, EVENTS_FILE


def dedupe_events_jsonl(
    src_file: Path = EVENTS_FILE,
    dst_file: Path = DEDUPED_EVENTS_FILE,
):
    seen_ids = set()

    with src_file.open("r", encoding="utf-8") as source, dst_file.open(
        "w", encoding="utf-8"
    ) as output:
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
            if current_id in seen_ids:
                continue

            seen_ids.add(current_id)
            output.write(json.dumps(current_row))
            output.write("\n")


if __name__ == "__main__":
    dedupe_events_jsonl()
