from app.pipeline.cleaning.schemas import SessionPingEvent, parse_event


SESSION_TIMEOUT_SECONDS = 120


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
