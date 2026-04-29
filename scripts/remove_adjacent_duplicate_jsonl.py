import json

# NOTE: THIS USES DICT + SORT FOR SIMPLICITY. A LIST PLUS AN ID->INDEX MAP
# WOULD PRESERVE ORDER IN O(N) WITHOUT THE FINAL SORT.

src_file = "events.jsonl"
dst_file = "events.deduped.jsonl"

events_by_id = {}

source = open(src_file, "r", encoding="utf-8")

for line in source:
    original_line = line.strip()

    if original_line == "":
        continue

    try:
        current_row = json.loads(original_line)
    except json.JSONDecodeError:
        # Malformed JSON cannot be deduped, so we skip it here.
        continue

    if type(current_row) is not dict or type(current_row.get("id")) is not int:
        # Rows without an integer id cannot participate in id-based deduplication.
        continue

    current_id = current_row["id"]
    saved_row = events_by_id.get(current_id)

    if saved_row is not None:
        # Prefer the earliest valid timestamp when duplicates share an id.
        if type(saved_row.get("timestamp")) is int and type(current_row.get("timestamp")) is int:
            if current_row["timestamp"] < saved_row["timestamp"]:
                events_by_id[current_id] = current_row
        elif type(current_row.get("timestamp")) is int:
            events_by_id[current_id] = current_row
    else:
        events_by_id[current_id] = current_row

source.close()

deduped_rows = list(events_by_id.values())
deduped_rows.sort(
    key=lambda row: (
        type(row.get("timestamp")) is not int,
        row.get("timestamp", 0),
        row["id"],
    )
)

output = open(dst_file, "w", encoding="utf-8")

for row in deduped_rows:
    output.write(json.dumps(row))
    output.write("\n")

output.close()
