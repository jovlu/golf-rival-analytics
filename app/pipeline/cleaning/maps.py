import json
from pathlib import Path

from app.pipeline.paths import MAPS_FILE


def load_valid_map_ids(maps_file: Path = MAPS_FILE):
    valid_map_ids = set()

    with maps_file.open("r", encoding="utf-8") as source:
        for line in source:
            stripped_line = line.strip()
            if stripped_line == "":
                continue

            try:
                row = json.loads(stripped_line)
            except json.JSONDecodeError:
                continue

            map_id = row.get("id")
            if type(map_id) is str:
                valid_map_ids.add(map_id)

    return valid_map_ids
