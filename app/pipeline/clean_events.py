import json
from pathlib import Path

from app.pipeline.cleaning.maps import load_valid_map_ids
from app.pipeline.cleaning.match import (
    discard_active_match_for_user,
    discard_inactive_matches,
    process_match_rows_for_timestamp_group,
)
from app.pipeline.cleaning.registration import is_valid_registration_event
from app.pipeline.cleaning.session_ping import is_valid_session_ping_event
from app.pipeline.paths import CLEANED_EVENTS_FILE, DEDUPED_EVENTS_FILE, MAPS_FILE


def get_output_priority(row):
    event_type = row.get("event_type") if type(row) is dict else None
    if event_type == "registration":
        return 0

    if event_type == "session_ping":
        event_data = row.get("event_data")
        if type(event_data) is dict and event_data.get("state") == "ended":
            return 4

        return 1

    if event_type == "match_finish":
        return 2

    if event_type == "match_start":
        return 3

    return 5


def get_timestamp_group_timestamp(row_items):
    for _, row in row_items:
        if type(row) is dict and type(row.get("timestamp")) is int:
            return row["timestamp"]

    return None


def collect_valid_row_indexes_for_timestamp_group(
    row_items,
    valid_row_indexes,
    user_id_to_username,
    seen_usernames,
    active_session_last_ping_timestamp_by_user_id,
    valid_map_ids,
    active_match_by_user_id,
    pending_matches_by_key,
):
    ending_session_user_ids = set()
    current_timestamp = get_timestamp_group_timestamp(row_items)

    if current_timestamp is not None:
        discard_inactive_matches(
            active_match_by_user_id,
            pending_matches_by_key,
            active_session_last_ping_timestamp_by_user_id,
            current_timestamp,
        )

    for row_index, row in row_items:
        event_type = row.get("event_type") if type(row) is dict else None

        if event_type != "registration":
            continue

        if is_valid_registration_event(row, user_id_to_username, seen_usernames):
            valid_row_indexes.add(row_index)

    for row_index, row in row_items:
        event_type = row.get("event_type") if type(row) is dict else None

        if event_type != "session_ping":
            continue

        if is_valid_session_ping_event(
            row,
            user_id_to_username,
            active_session_last_ping_timestamp_by_user_id,
            ending_session_user_ids,
        ):
            valid_row_indexes.add(row_index)

    process_match_rows_for_timestamp_group(
        row_items,
        valid_row_indexes,
        user_id_to_username,
        active_session_last_ping_timestamp_by_user_id,
        active_match_by_user_id,
        pending_matches_by_key,
        ending_session_user_ids,
        valid_map_ids,
    )

    for user_id in ending_session_user_ids:
        discard_active_match_for_user(
            user_id, active_match_by_user_id, pending_matches_by_key
        )
        active_session_last_ping_timestamp_by_user_id.pop(user_id, None)


def clean_events_jsonl(
    src_file: Path = DEDUPED_EVENTS_FILE,
    dst_file: Path = CLEANED_EVENTS_FILE,
    maps_file: Path = MAPS_FILE,
):
    user_id_to_username = {}
    seen_usernames = set()
    active_session_last_ping_timestamp_by_user_id = {}
    valid_map_ids = load_valid_map_ids(maps_file)
    active_match_by_user_id = {}
    pending_matches_by_key = {}
    valid_row_indexes = set()
    rows = []
    have_current_timestamp = False

    with src_file.open("r", encoding="utf-8") as source:
        current_timestamp = None
        current_timestamp_rows = []

        for line in source:
            stripped_line = line.strip()
            if stripped_line == "":
                continue

            try:
                row = json.loads(stripped_line)
            except json.JSONDecodeError:
                continue

            row_index = len(rows)
            rows.append(row)
            row_timestamp = row.get("timestamp") if type(row) is dict else None
            row_item = (row_index, row)

            if not have_current_timestamp:
                current_timestamp = row_timestamp
                current_timestamp_rows.append(row_item)
                have_current_timestamp = True
                continue

            if row_timestamp == current_timestamp:
                current_timestamp_rows.append(row_item)
                continue

            collect_valid_row_indexes_for_timestamp_group(
                current_timestamp_rows,
                valid_row_indexes,
                user_id_to_username,
                seen_usernames,
                active_session_last_ping_timestamp_by_user_id,
                valid_map_ids,
                active_match_by_user_id,
                pending_matches_by_key,
            )

            current_timestamp = row_timestamp
            current_timestamp_rows = [row_item]

        if current_timestamp_rows:
            collect_valid_row_indexes_for_timestamp_group(
                current_timestamp_rows,
                valid_row_indexes,
                user_id_to_username,
                seen_usernames,
                active_session_last_ping_timestamp_by_user_id,
                valid_map_ids,
                active_match_by_user_id,
                pending_matches_by_key,
            )

    sorted_valid_row_indexes = sorted(
        valid_row_indexes,
        key=lambda row_index: (
            rows[row_index].get("timestamp") if type(rows[row_index]) is dict else 0,
            get_output_priority(rows[row_index]),
            row_index,
        ),
    )

    with dst_file.open("w", encoding="utf-8") as output:
        for row_index in sorted_valid_row_indexes:
            output.write(json.dumps(rows[row_index]))
            output.write("\n")


if __name__ == "__main__":
    clean_events_jsonl()
