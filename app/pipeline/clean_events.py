import json
from pathlib import Path
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictFloat,
    StrictInt,
    StrictStr,
    ValidationError,
    field_validator,
)

from app.pipeline.paths import CLEANED_EVENTS_FILE, DEDUPED_EVENTS_FILE, MAPS_FILE


SESSION_TIMEOUT_SECONDS = 120


class StrictEventModel(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)


class RegistrationEventData(StrictEventModel):
    country: StrictStr
    username: StrictStr
    device_os: Literal["Android", "iOS"]


class RegistrationEvent(StrictEventModel):
    id: StrictInt
    timestamp: StrictInt
    event_type: Literal["registration"]
    user_id: StrictStr
    event_data: RegistrationEventData


class SessionPingEventData(StrictEventModel):
    state: Literal["started", "in_progress", "ended"]
    device_os: StrictStr


class SessionPingEvent(StrictEventModel):
    id: StrictInt
    timestamp: StrictInt
    event_type: Literal["session_ping"]
    user_id: StrictStr
    event_data: SessionPingEventData


class MatchStartEventData(StrictEventModel):
    map_id: StrictStr
    opponent_id: StrictStr


class MatchStartEvent(StrictEventModel):
    id: StrictInt
    timestamp: StrictInt
    event_type: Literal["match_start"]
    user_id: StrictStr
    event_data: MatchStartEventData


class MatchFinishEventData(MatchStartEventData):
    outcome: StrictInt | StrictFloat

    @field_validator("outcome")
    @classmethod
    def validate_outcome(cls, outcome):
        if outcome not in {0, 0.5, 1}:
            raise ValueError("outcome must be 0, 0.5, or 1")

        return outcome


class MatchFinishEvent(StrictEventModel):
    id: StrictInt
    timestamp: StrictInt
    event_type: Literal["match_finish"]
    user_id: StrictStr
    event_data: MatchFinishEventData


MATCH_EVENT_MODELS = {
    "match_start": MatchStartEvent,
    "match_finish": MatchFinishEvent,
}


def parse_event(row, event_model):
    try:
        return event_model.model_validate(row)
    except ValidationError:
        return None


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
    event = parse_event(row, RegistrationEvent)
    if event is None:
        return False

    if event.user_id in user_id_to_username:
        return False

    if event.event_data.username in seen_usernames:
        return False

    user_id_to_username[event.user_id] = event.event_data.username
    seen_usernames.add(event.event_data.username)
    return True


def has_active_session(user_id, timestamp, active_session_last_ping_timestamp_by_user_id):
    last_ping_timestamp = active_session_last_ping_timestamp_by_user_id.get(user_id)
    if last_ping_timestamp is None:
        return False

    return timestamp - last_ping_timestamp <= SESSION_TIMEOUT_SECONDS


def is_valid_session_ping_event(
    row,
    user_id_to_username,
    active_session_last_ping_timestamp_by_user_id,
    ending_session_user_ids=None,
):
    event = parse_event(row, SessionPingEvent)
    if event is None:
        return False

    if event.user_id not in user_id_to_username:
        return False

    session_is_active = has_active_session(
        event.user_id,
        event.timestamp,
        active_session_last_ping_timestamp_by_user_id,
    )

    if not session_is_active:
        if event.event_data.state != "started":
            return False

        active_session_last_ping_timestamp_by_user_id[event.user_id] = event.timestamp
        return True

    if event.event_data.state == "started":
        return False

    if event.event_data.state == "ended":
        if ending_session_user_ids is None:
            del active_session_last_ping_timestamp_by_user_id[event.user_id]
        else:
            ending_session_user_ids.add(event.user_id)
        return True

    active_session_last_ping_timestamp_by_user_id[event.user_id] = event.timestamp
    return True


def get_match_event_info(row, valid_map_ids):
    event_type = row.get("event_type") if type(row) is dict else None
    event_model = MATCH_EVENT_MODELS.get(event_type)
    if event_model is None:
        return None

    event = parse_event(row, event_model)
    if event is None:
        return None

    if event.event_data.map_id not in valid_map_ids:
        return None

    match_info = {
        "user_id": event.user_id,
        "opponent_id": event.event_data.opponent_id,
        "map_id": event.event_data.map_id,
        "timestamp": event.timestamp,
    }

    if event.event_type == "match_finish":
        match_info["outcome"] = event.event_data.outcome

    return match_info


def make_match_pair_key(event_type, map_id, user_id, opponent_id):
    if user_id < opponent_id:
        return (event_type, map_id, user_id, opponent_id)

    return (event_type, map_id, opponent_id, user_id)


def is_valid_match_pair(
    match_rows,
    match_row_count_by_user_id,
    active_match_by_user_id,
    active_session_last_ping_timestamp_by_user_id,
    ending_session_user_ids,
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
        if first_row["user_id"] in ending_session_user_ids:
            return False

        if second_row["user_id"] in ending_session_user_ids:
            return False

        if not has_active_session(
            first_row["user_id"],
            first_row["timestamp"],
            active_session_last_ping_timestamp_by_user_id,
        ):
            return False

        if not has_active_session(
            second_row["user_id"],
            second_row["timestamp"],
            active_session_last_ping_timestamp_by_user_id,
        ):
            return False

        if first_row["user_id"] in active_match_by_user_id:
            return False

        if second_row["user_id"] in active_match_by_user_id:
            return False

        return True

    if first_row["outcome"] + second_row["outcome"] != 1:
        return False

    if not has_active_session(
        first_row["user_id"],
        first_row["timestamp"],
        active_session_last_ping_timestamp_by_user_id,
    ):
        return False

    if not has_active_session(
        second_row["user_id"],
        second_row["timestamp"],
        active_session_last_ping_timestamp_by_user_id,
    ):
        return False

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
    ending_session_user_ids = set()

    for row_index, row in enumerate(rows):
        event_type = row.get("event_type") if type(row) is dict else None

        if event_type != "registration":
            continue

        if is_valid_registration_event(row, user_id_to_username, seen_usernames):
            valid_row_indexes.add(row_index)

    for row_index, row in enumerate(rows):
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

    for row_index, row in enumerate(rows):
        event_type = row.get("event_type") if type(row) is dict else None

        if event_type != "match_start" and event_type != "match_finish":
            continue

        user_id = row.get("user_id")
        if type(user_id) is str:
            match_row_count_by_user_id[user_id] = (
                match_row_count_by_user_id.get(user_id, 0) + 1
            )

        match_info = get_match_event_info(row, valid_map_ids)
        if match_info is None:
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

    for match_rows in match_rows_by_key.values():
        if not is_valid_match_pair(
            match_rows,
            match_row_count_by_user_id,
            active_match_by_user_id,
            active_session_last_ping_timestamp_by_user_id,
            ending_session_user_ids,
            user_id_to_username,
        ):
            continue

        apply_match_pair(match_rows, active_match_by_user_id)

        for match_row in match_rows:
            valid_row_indexes.add(match_row["row_index"])

    for user_id in ending_session_user_ids:
        active_session_last_ping_timestamp_by_user_id.pop(user_id, None)

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
