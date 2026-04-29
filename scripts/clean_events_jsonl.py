import json
from pathlib import Path


project_root = Path(__file__).resolve().parent.parent

src_file = project_root / "events.deduped.jsonl"
dst_file = project_root / "events.cleaned.jsonl"

ALLOWED_EVENT_TYPES = {
    "registration",
    "session_ping",
    "match_start",
    "match_finish",
}

ALLOWED_DEVICE_OS_VALUES = {"Android", "iOS"}


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


def is_valid_event(row, user_id_to_username, seen_usernames):
    if type(row) is not dict:
        return False

    event_type = row.get("event_type")
    if event_type not in ALLOWED_EVENT_TYPES:
        return False

    if event_type == "registration":
        return is_valid_registration_event(row, user_id_to_username, seen_usernames)

    return True


user_id_to_username = {}
seen_usernames = set()

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

        if not is_valid_event(row, user_id_to_username, seen_usernames):
            continue

        output.write(json.dumps(row))
        output.write("\n")
