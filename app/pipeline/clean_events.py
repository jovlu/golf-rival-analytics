import json
from pathlib import Path

from app.pipeline.paths import CLEANED_EVENTS_FILE, DEDUPED_EVENTS_FILE, MAPS_FILE


ALLOWED_EVENT_TYPES = {
    "registration",
    "session_ping",
    "match_start",
    "match_finish",
}

ALLOWED_DEVICE_OS_VALUES = {"Android", "iOS"}
ALLOWED_SESSION_PING_STATES = {"started", "in_progress", "ended"}
ALLOWED_MATCH_OUTCOMES = {0, 0.5, 1}
SESSION_TIMEOUT_SECONDS = 120


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


def is_valid_registration_event(row, user_id_to_username, seen_usernames):
    user_id = row.get("user_id")
    if type(user_id) is not str:
        return False

    event_data = row.get("event_data")

    if type(event_data) is not dict:
        return False

    country = event_data.get("country")
    if type(country) is not str:
        return False

    username = event_data.get("username")
    if type(username) is not str:
        return False

    if user_id in user_id_to_username:
        return False

    if username in seen_usernames:
        return False

    device_os = event_data.get("device_os")
    if device_os not in ALLOWED_DEVICE_OS_VALUES:
        return False

    user_id_to_username[user_id] = username
    seen_usernames.add(username)
    return True


def has_active_session(user_id, timestamp, active_session_last_ping_timestamp_by_user_id):
    last_ping_timestamp = active_session_last_ping_timestamp_by_user_id.get(user_id)
    if last_ping_timestamp is None:
        return False

    return timestamp - last_ping_timestamp <= SESSION_TIMEOUT_SECONDS


def is_valid_session_ping_event(
    row, user_id_to_username, active_session_last_ping_timestamp_by_user_id
):
    user_id = row.get("user_id")
    if type(user_id) is not str:
        return False

    if user_id not in user_id_to_username:
        return False

    event_data = row.get("event_data")
    if type(event_data) is not dict:
        return False

    timestamp = row.get("timestamp")
    if type(timestamp) is not int:
        return False

    state = event_data.get("state")
    if state not in ALLOWED_SESSION_PING_STATES:
        return False

    device_os = event_data.get("device_os")
    if type(device_os) is not str:
        return False

    session_is_active = has_active_session(
        user_id, timestamp, active_session_last_ping_timestamp_by_user_id
    )

    if not session_is_active:
        if state != "started":
            return False

        active_session_last_ping_timestamp_by_user_id[user_id] = timestamp
        return True

    if state == "started":
        return False

    if state == "ended":
        del active_session_last_ping_timestamp_by_user_id[user_id]
        return True

    active_session_last_ping_timestamp_by_user_id[user_id] = timestamp
    return True


def get_match_event_info(row, valid_map_ids):
    user_id = row.get("user_id")
    if type(user_id) is not str:
        return None

    event_data = row.get("event_data")
    if type(event_data) is not dict:
        return None

    map_id = event_data.get("map_id")
    if map_id not in valid_map_ids:
        return None

    opponent_id = event_data.get("opponent_id")
    if type(opponent_id) is not str:
        return None

    return {
        "user_id": user_id,
        "opponent_id": opponent_id,
        "map_id": map_id,
    }


def is_valid_match_finish_outcome(row):
    event_data = row.get("event_data")
    if type(event_data) is not dict:
        return False

    outcome = event_data.get("outcome")
    return outcome in ALLOWED_MATCH_OUTCOMES


def make_match_pair_key(event_type, map_id, user_id, opponent_id):
    if user_id < opponent_id:
        return (event_type, map_id, user_id, opponent_id)

    return (event_type, map_id, opponent_id, user_id)


def is_valid_match_pair(
    match_rows,
    match_row_count_by_user_id,
    active_match_by_user_id,
    user_id_to_username,
):
    if len(match_rows) != 2:
        return False

    first_row = match_rows[0]
    second_row = match_rows[1]

    if first_row["user_id"] == second_row["user_id"]:
        return False

    if first_row["user_id"] not in user_id_to_username:
        return False

    if second_row["user_id"] not in user_id_to_username:
        return False

    if match_row_count_by_user_id.get(first_row["user_id"]) != 1:
        return False

    if match_row_count_by_user_id.get(second_row["user_id"]) != 1:
        return False

    if first_row["opponent_id"] != second_row["user_id"]:
        return False

    if second_row["opponent_id"] != first_row["user_id"]:
        return False

    if first_row["event_type"] == "match_start":
        if first_row["user_id"] in active_match_by_user_id:
            return False

        if second_row["user_id"] in active_match_by_user_id:
            return False

        return True

    first_active_match = active_match_by_user_id.get(first_row["user_id"])
    if first_active_match != (first_row["opponent_id"], first_row["map_id"]):
        return False

    second_active_match = active_match_by_user_id.get(second_row["user_id"])
    if second_active_match != (second_row["opponent_id"], second_row["map_id"]):
        return False

    return True


def apply_match_pair(match_rows, active_match_by_user_id):
    first_row = match_rows[0]
    second_row = match_rows[1]

    if first_row["event_type"] == "match_start":
        active_match_by_user_id[first_row["user_id"]] = (
            first_row["opponent_id"],
            first_row["map_id"],
        )
        active_match_by_user_id[second_row["user_id"]] = (
            second_row["opponent_id"],
            second_row["map_id"],
        )
        return

    del active_match_by_user_id[first_row["user_id"]]
    del active_match_by_user_id[second_row["user_id"]]


def is_valid_event(
    row,
    user_id_to_username,
    seen_usernames,
    active_session_last_ping_timestamp_by_user_id,
):
    if type(row) is not dict:
        return False

    event_type = row.get("event_type")
    if event_type not in ALLOWED_EVENT_TYPES:
        return False

    if event_type == "registration":
        return is_valid_registration_event(row, user_id_to_username, seen_usernames)

    if event_type == "session_ping":
        return is_valid_session_ping_event(
            row, user_id_to_username, active_session_last_ping_timestamp_by_user_id
        )

    return False


def write_valid_rows_for_timestamp_group(
    rows,
    output,
    user_id_to_username,
    seen_usernames,
    active_session_last_ping_timestamp_by_user_id,
    valid_map_ids,
    active_match_by_user_id,
):
    valid_row_indexes = set()
    match_rows_by_key = {}
    match_row_count_by_user_id = {}

    for row_index, row in enumerate(rows):
        event_type = row.get("event_type") if type(row) is dict else None

        if event_type == "match_start" or event_type == "match_finish":
            user_id = row.get("user_id")
            if type(user_id) is str:
                match_row_count_by_user_id[user_id] = (
                    match_row_count_by_user_id.get(user_id, 0) + 1
                )

            match_info = get_match_event_info(row, valid_map_ids)
            if match_info is None:
                continue

            if event_type == "match_finish" and not is_valid_match_finish_outcome(row):
                continue

            match_info["event_type"] = event_type
            match_info["row_index"] = row_index

            match_key = make_match_pair_key(
                event_type,
                match_info["map_id"],
                match_info["user_id"],
                match_info["opponent_id"],
            )
            if match_key not in match_rows_by_key:
                match_rows_by_key[match_key] = []

            match_rows_by_key[match_key].append(match_info)
            continue

        if is_valid_event(
            row,
            user_id_to_username,
            seen_usernames,
            active_session_last_ping_timestamp_by_user_id,
        ):
            valid_row_indexes.add(row_index)

    for match_rows in match_rows_by_key.values():
        if not is_valid_match_pair(
            match_rows,
            match_row_count_by_user_id,
            active_match_by_user_id,
            user_id_to_username,
        ):
            continue

        apply_match_pair(match_rows, active_match_by_user_id)

        for match_row in match_rows:
            valid_row_indexes.add(match_row["row_index"])

    for row_index, row in enumerate(rows):
        if row_index in valid_row_indexes:
            output.write(json.dumps(row))
            output.write("\n")


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
    have_current_timestamp = False

    with src_file.open("r", encoding="utf-8") as source, dst_file.open(
        "w", encoding="utf-8"
    ) as output:
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

            row_timestamp = row.get("timestamp") if type(row) is dict else None

            if not have_current_timestamp:
                current_timestamp = row_timestamp
                current_timestamp_rows.append(row)
                have_current_timestamp = True
                continue

            if row_timestamp == current_timestamp:
                current_timestamp_rows.append(row)
                continue

            write_valid_rows_for_timestamp_group(
                current_timestamp_rows,
                output,
                user_id_to_username,
                seen_usernames,
                active_session_last_ping_timestamp_by_user_id,
                valid_map_ids,
                active_match_by_user_id,
            )

            current_timestamp = row_timestamp
            current_timestamp_rows = [row]

        if current_timestamp_rows:
            write_valid_rows_for_timestamp_group(
                current_timestamp_rows,
                output,
                user_id_to_username,
                seen_usernames,
                active_session_last_ping_timestamp_by_user_id,
                valid_map_ids,
                active_match_by_user_id,
            )


if __name__ == "__main__":
    clean_events_jsonl()
