from app.pipeline.cleaning.schemas import MATCH_EVENT_MODELS, parse_event
from app.pipeline.cleaning.session_ping import has_active_session


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


def make_active_match_key(map_id, user_id, opponent_id):
    if user_id < opponent_id:
        return (map_id, user_id, opponent_id)

    return (map_id, opponent_id, user_id)


def is_valid_match_pair(
    match_rows,
    match_row_count_by_event_type_and_user_id,
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

    event_type = first_row["event_type"]

    if match_row_count_by_event_type_and_user_id.get(
        (event_type, first_row["user_id"])
    ) != 1:
        return False

    if match_row_count_by_event_type_and_user_id.get(
        (event_type, second_row["user_id"])
    ) != 1:
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


def discard_active_match_for_user(
    user_id,
    active_match_by_user_id,
    pending_matches_by_key,
):
    active_match = active_match_by_user_id.pop(user_id, None)
    if active_match is None:
        return

    opponent_id, map_id = active_match
    active_match_key = make_active_match_key(map_id, user_id, opponent_id)
    pending_matches_by_key.pop(active_match_key, None)

    if active_match_by_user_id.get(opponent_id) == (user_id, map_id):
        del active_match_by_user_id[opponent_id]


def discard_inactive_matches(
    active_match_by_user_id,
    pending_matches_by_key,
    active_session_last_ping_timestamp_by_user_id,
    current_timestamp,
):
    for user_id in list(active_match_by_user_id):
        active_match = active_match_by_user_id.get(user_id)
        if active_match is None:
            continue

        opponent_id, _ = active_match
        if not has_active_session(
            user_id,
            current_timestamp,
            active_session_last_ping_timestamp_by_user_id,
        ) or not has_active_session(
            opponent_id,
            current_timestamp,
            active_session_last_ping_timestamp_by_user_id,
        ):
            discard_active_match_for_user(
                user_id, active_match_by_user_id, pending_matches_by_key
            )


def process_match_rows_for_timestamp_group(
    row_items,
    valid_row_indexes,
    user_id_to_username,
    active_session_last_ping_timestamp_by_user_id,
    active_match_by_user_id,
    pending_matches_by_key,
    ending_session_user_ids,
    valid_map_ids,
):
    match_rows_by_key = {}
    match_row_count_by_event_type_and_user_id = {}

    for row_index, row in row_items:
        event_type = row.get("event_type") if type(row) is dict else None

        if event_type != "match_start" and event_type != "match_finish":
            continue

        user_id = row.get("user_id")
        if type(user_id) is str:
            count_key = (event_type, user_id)
            match_row_count_by_event_type_and_user_id[count_key] = (
                match_row_count_by_event_type_and_user_id.get(count_key, 0) + 1
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

    sorted_match_groups = sorted(
        match_rows_by_key.values(),
        key=lambda match_rows: 0 if match_rows[0]["event_type"] == "match_finish" else 1,
    )

    for match_rows in sorted_match_groups:
        if not is_valid_match_pair(
            match_rows,
            match_row_count_by_event_type_and_user_id,
            active_match_by_user_id,
            active_session_last_ping_timestamp_by_user_id,
            ending_session_user_ids,
            user_id_to_username,
        ):
            continue

        active_match_key = make_active_match_key(
            match_rows[0]["map_id"],
            match_rows[0]["user_id"],
            match_rows[0]["opponent_id"],
        )

        if match_rows[0]["event_type"] == "match_start":
            apply_match_pair(match_rows, active_match_by_user_id)
            pending_matches_by_key[active_match_key] = [
                match_row["row_index"] for match_row in match_rows
            ]
            continue

        pending_start_row_indexes = pending_matches_by_key.pop(active_match_key, None)
        if pending_start_row_indexes is None:
            continue

        apply_match_pair(match_rows, active_match_by_user_id)
        valid_row_indexes.update(pending_start_row_indexes)
        for match_row in match_rows:
            valid_row_indexes.add(match_row["row_index"])
