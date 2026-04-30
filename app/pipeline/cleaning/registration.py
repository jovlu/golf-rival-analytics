from app.pipeline.cleaning.schemas import RegistrationEvent, parse_event


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
