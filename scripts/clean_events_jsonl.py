import json
from pathlib import Path


project_root = Path(__file__).resolve().parent.parent

src_file = project_root / "events.deduped.jsonl"
dst_file = project_root / "events.cleaned.jsonl"
maps_file = project_root / "maps.jsonl"

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


def load_valid_map_ids():
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


def is_valid_match_start_event(row, valid_map_ids, active_match_user_ids):
    user_id = row.get("user_id")
    if type(user_id) is not str:
        return False

    if user_id in active_match_user_ids:
        return False

    event_data = row.get("event_data")
    if type(event_data) is not dict:
        return False

    map_id = event_data.get("map_id")
    if map_id not in valid_map_ids:
        return False

    opponent_id = event_data.get("opponent_id")
    if type(opponent_id) is not str:
        return False

    active_match_user_ids.add(user_id)
    return True


def is_valid_match_finish_event(row, valid_map_ids, active_match_user_ids):
    user_id = row.get("user_id")
    if type(user_id) is not str:
        return False

    if user_id not in active_match_user_ids:
        return False

    event_data = row.get("event_data")
    if type(event_data) is not dict:
        return False

    map_id = event_data.get("map_id")
    if map_id not in valid_map_ids:
        return False

    opponent_id = event_data.get("opponent_id")
    if type(opponent_id) is not str:
        return False

    outcome = event_data.get("outcome")
    if outcome not in ALLOWED_MATCH_OUTCOMES:
        return False

    active_match_user_ids.remove(user_id)
    return True


def is_valid_event(
    row,
    user_id_to_username,
    seen_usernames,
    active_session_last_ping_timestamp_by_user_id,
    valid_map_ids,
    active_match_user_ids,
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

    if event_type == "match_start":
        return is_valid_match_start_event(row, valid_map_ids, active_match_user_ids)

    if event_type == "match_finish":
        return is_valid_match_finish_event(row, valid_map_ids, active_match_user_ids)

    return True


user_id_to_username = {}
seen_usernames = set()
active_session_last_ping_timestamp_by_user_id = {}
valid_map_ids = load_valid_map_ids()
active_match_user_ids = set()

with src_file.open("r", encoding="utf-8") as source, dst_file.open(
    "w", encoding="utf-8"
) as output:
    for line in source:
        stripped_line = line.strip()
        if stripped_line == "":
            continue

        try:
            row = json.loads(stripped_line)
        except json.JSONDecodeError:
            continue

        if not is_valid_event(
            row,
            user_id_to_username,
            seen_usernames,
            active_session_last_ping_timestamp_by_user_id,
            valid_map_ids,
            active_match_user_ids,
        ):
            continue

        output.write(json.dumps(row))
        output.write("\n")
