import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from app.db import Base, SessionLocal, engine
from app.models import GameMap, Match, MatchParticipation, Session, User
from app.pipeline.paths import CLEANED_EVENTS_FILE, MAPS_FILE


SESSION_TIMEOUT_SECONDS = 120


@dataclass
class ActiveSessionState:
    session_row: Session
    last_ping_at: datetime


@dataclass
class ActiveMatchState:
    started_at: datetime
    participant_session_ids: dict[str, int | None]


def to_datetime(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def load_jsonl_rows(file_path: Path):
    with file_path.open("r", encoding="utf-8") as source:
        for line in source:
            stripped_line = line.strip()
            if stripped_line == "":
                continue

            yield json.loads(stripped_line)


def ensure_database_is_empty(db):
    if db.scalar(select(GameMap.map_id).limit(1)) is not None:
        raise RuntimeError("maps table is not empty")

    if db.scalar(select(User.user_id).limit(1)) is not None:
        raise RuntimeError("users table is not empty")

    if db.scalar(select(Session.session_id).limit(1)) is not None:
        raise RuntimeError("sessions table is not empty")

    if db.scalar(select(Match.match_id).limit(1)) is not None:
        raise RuntimeError("matches table is not empty")

    if db.scalar(select(MatchParticipation.match_id).limit(1)) is not None:
        raise RuntimeError("match_participations table is not empty")


def load_maps(db, maps_file: Path = MAPS_FILE):
    for row in load_jsonl_rows(maps_file):
        db.add(
            GameMap(
                map_id=row["id"],
                name=row["name"],
            )
        )


def process_registration(row, db):
    event_data = row["event_data"]
    db.add(
        User(
            user_id=row["user_id"],
            username=event_data["username"],
            country=event_data["country"],
            registered_at=to_datetime(row["timestamp"]),
        )
    )
    db.flush()


def process_session_ping(row, db, active_sessions_by_user_id):
    user_id = row["user_id"]
    timestamp = to_datetime(row["timestamp"])
    event_data = row["event_data"]
    state = event_data["state"]

    expire_active_session_if_needed(user_id, timestamp, active_sessions_by_user_id)

    if state == "started":
        session_row = Session(
            user_id=user_id,
            device_os=event_data["device_os"],
            started_at=timestamp,
        )
        db.add(session_row)
        db.flush()
        active_sessions_by_user_id[user_id] = ActiveSessionState(
            session_row=session_row,
            last_ping_at=timestamp,
        )
        return

    active_session = active_sessions_by_user_id.get(user_id)
    if active_session is None:
        return

    active_session.last_ping_at = timestamp

    if state == "ended":
        close_active_session(active_session, timestamp)
        del active_sessions_by_user_id[user_id]


def get_match_pair_key(map_id, user_id, opponent_id):
    if user_id < opponent_id:
        return (map_id, user_id, opponent_id)

    return (map_id, opponent_id, user_id)


def flush_match_rows(
    match_rows,
    db,
    active_sessions_by_user_id,
    active_matches_by_key,
):
    match_rows_by_key = {}

    for row in match_rows:
        event_data = row["event_data"]
        match_key = get_match_pair_key(
            event_data["map_id"],
            row["user_id"],
            event_data["opponent_id"],
        )
        if match_key not in match_rows_by_key:
            match_rows_by_key[match_key] = []

        match_rows_by_key[match_key].append(row)

    for match_key, pair_rows in match_rows_by_key.items():
        if len(pair_rows) != 2:
            continue

        event_type = pair_rows[0]["event_type"]
        timestamp = to_datetime(pair_rows[0]["timestamp"])
        first_row = pair_rows[0]
        second_row = pair_rows[1]
        first_user_id = first_row["user_id"]
        second_user_id = second_row["user_id"]

        if event_type == "match_start":
            active_matches_by_key[match_key] = ActiveMatchState(
                started_at=timestamp,
                participant_session_ids={
                    first_user_id: get_active_session_id(
                        first_user_id,
                        timestamp,
                        active_sessions_by_user_id,
                    ),
                    second_user_id: get_active_session_id(
                        second_user_id,
                        timestamp,
                        active_sessions_by_user_id,
                    ),
                },
            )
            continue

        active_match = active_matches_by_key.get(match_key)
        if active_match is None:
            continue

        match_row = Match(
            map_id=first_row["event_data"]["map_id"],
            started_at=active_match.started_at,
            ended_at=timestamp,
            ended_date=timestamp.date(),
            duration_seconds=int((timestamp - active_match.started_at).total_seconds()),
        )
        db.add(match_row)
        db.flush()

        for row in pair_rows:
            user_id = row["user_id"]
            db.add(
                MatchParticipation(
                    match_id=match_row.match_id,
                    user_id=user_id,
                    session_id=active_match.participant_session_ids.get(user_id),
                    outcome=Decimal(str(row["event_data"]["outcome"])),
                )
            )

        del active_matches_by_key[match_key]


def close_active_session(active_session, ended_at):
    session_row = active_session.session_row
    session_row.ended_at = ended_at
    session_row.duration_seconds = int(
        (session_row.ended_at - session_row.started_at).total_seconds()
    )


def expire_active_session_if_needed(user_id, current_at, active_sessions_by_user_id):
    active_session = active_sessions_by_user_id.get(user_id)
    if active_session is None:
        return

    timeout_at = active_session.last_ping_at + timedelta(seconds=SESSION_TIMEOUT_SECONDS)
    if current_at > timeout_at:
        close_active_session(active_session, timeout_at)
        del active_sessions_by_user_id[user_id]


def get_active_session_id(user_id, current_at, active_sessions_by_user_id):
    expire_active_session_if_needed(user_id, current_at, active_sessions_by_user_id)

    active_session = active_sessions_by_user_id.get(user_id)
    if active_session is None:
        return None

    return active_session.session_row.session_id


def close_remaining_active_sessions(active_sessions_by_user_id):
    for user_id in list(active_sessions_by_user_id):
        active_session = active_sessions_by_user_id[user_id]
        timeout_at = active_session.last_ping_at + timedelta(
            seconds=SESSION_TIMEOUT_SECONDS
        )
        close_active_session(active_session, timeout_at)
        del active_sessions_by_user_id[user_id]


def process_timestamp_group(
    rows,
    db,
    active_sessions_by_user_id,
    active_matches_by_key,
):
    pending_match_rows = []

    for row in rows:
        event_type = row["event_type"]

        if event_type == "match_start" or event_type == "match_finish":
            pending_match_rows.append(row)
            continue

        if pending_match_rows:
            flush_match_rows(
                pending_match_rows,
                db,
                active_sessions_by_user_id,
                active_matches_by_key,
            )
            pending_match_rows = []

        if event_type == "registration":
            process_registration(row, db)
            continue

        if event_type == "session_ping":
            process_session_ping(row, db, active_sessions_by_user_id)

    if pending_match_rows:
        flush_match_rows(
            pending_match_rows,
            db,
            active_sessions_by_user_id,
            active_matches_by_key,
        )


def load_cleaned_events(db, cleaned_events_file: Path = CLEANED_EVENTS_FILE):
    active_sessions_by_user_id = {}
    active_matches_by_key = {}
    current_timestamp = None
    current_rows = []
    have_timestamp = False

    for row in load_jsonl_rows(cleaned_events_file):
        row_timestamp = row["timestamp"]

        if not have_timestamp:
            current_timestamp = row_timestamp
            current_rows.append(row)
            have_timestamp = True
            continue

        if row_timestamp == current_timestamp:
            current_rows.append(row)
            continue

        process_timestamp_group(
            current_rows,
            db,
            active_sessions_by_user_id,
            active_matches_by_key,
        )

        current_timestamp = row_timestamp
        current_rows = [row]

    if current_rows:
        process_timestamp_group(
            current_rows,
            db,
            active_sessions_by_user_id,
            active_matches_by_key,
        )

    close_remaining_active_sessions(active_sessions_by_user_id)


def load_db():
    Base.metadata.create_all(bind=engine)

    with SessionLocal.begin() as db:
        ensure_database_is_empty(db)
        load_maps(db)
        load_cleaned_events(db)


if __name__ == "__main__":
    load_db()
