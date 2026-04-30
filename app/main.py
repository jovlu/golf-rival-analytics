from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import and_, select
from sqlalchemy.orm import Session as DatabaseSession

from app.db import check_db_connection, get_db
from app.models import GameMap, Match, MatchParticipation, Session, User


app = FastAPI()


class UserStatsEntry(BaseModel):
    username: str
    country: str
    fav_map: str | None
    fav_map_win_ratio: float
    total_playtime: int
    total_win_ratio: float
    avg_matches_per_session: float
    registration_date: date


class MapStatsEntry(BaseModel):
    date: date
    avg_playtime: float
    best_player_username: str
    match_cnt: int


@dataclass
class MapStats:
    name: str
    match_count: int = 0
    outcome_sum: Decimal = Decimal("0")


@dataclass
class DailyMapStats:
    total_duration_seconds: int = 0
    match_count: int = 0


@dataclass
class PlayerMapStats:
    match_count: int = 0
    outcome_sum: Decimal = Decimal("0")


@dataclass
class UserStats:
    total_playtime: int = 0
    session_count: int = 0
    match_count: int = 0
    outcome_sum: Decimal = Decimal("0")
    maps: dict[str, MapStats] = field(default_factory=dict)


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/health")
def health():
    try:
        check_db_connection()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {"status": "ok", "database": "ok"}


def normalize_list_filter(values: list[str] | None) -> list[str] | None:
    if values is None:
        return None

    normalized_values = []
    for value in values:
        for item in value.split(","):
            stripped_item = item.strip()
            if stripped_item:
                normalized_values.append(stripped_item)

    return normalized_values or None


def decimal_ratio(numerator: Decimal, denominator: int) -> float:
    if denominator == 0:
        return 0.0

    return float(numerator / Decimal(denominator))


def build_user_stats_entry(user: User, stats: UserStats) -> UserStatsEntry:
    favorite_map = None
    favorite_map_win_ratio = 0.0

    if stats.maps:
        favorite_map_stats = sorted(
            stats.maps.values(),
            key=lambda map_stats: (
                -decimal_ratio(map_stats.outcome_sum, map_stats.match_count),
                -map_stats.match_count,
                map_stats.name,
            ),
        )[0]
        favorite_map = favorite_map_stats.name
        favorite_map_win_ratio = decimal_ratio(
            favorite_map_stats.outcome_sum, favorite_map_stats.match_count
        )

    return UserStatsEntry(
        username=user.username,
        country=user.country,
        fav_map=favorite_map,
        fav_map_win_ratio=favorite_map_win_ratio,
        total_playtime=stats.total_playtime,
        total_win_ratio=decimal_ratio(stats.outcome_sum, stats.match_count),
        avg_matches_per_session=(
            stats.match_count / stats.session_count if stats.session_count else 0.0
        ),
        registration_date=user.registered_at.date(),
    )


def get_best_player_username(stats_by_username: dict[str, PlayerMapStats]) -> str:
    if not stats_by_username:
        raise ValueError("cannot choose best player without player stats")

    return sorted(
        stats_by_username.items(),
        key=lambda item: (
            -decimal_ratio(item[1].outcome_sum, item[1].match_count),
            -item[1].match_count,
            item[0],
        ),
    )[0][0]


@app.get("/user-stats", response_model=list[UserStatsEntry])
def user_stats(
    db: Annotated[DatabaseSession, Depends(get_db)],
    countries: Annotated[list[str] | None, Query()] = None,
    oss: Annotated[list[str] | None, Query(alias="OSs")] = None,
):
    country_filter = normalize_list_filter(countries)
    os_filter = normalize_list_filter(oss)

    user_statement = select(User)
    if country_filter is not None:
        user_statement = user_statement.where(User.country.in_(country_filter))

    users = db.scalars(user_statement).all()
    if not users:
        return []

    stats_by_user_id = {user.user_id: UserStats() for user in users}
    user_ids = list(stats_by_user_id)

    session_statement = select(
        Session.user_id,
        Session.duration_seconds,
    ).where(Session.user_id.in_(user_ids))
    if os_filter is not None:
        session_statement = session_statement.where(Session.device_os.in_(os_filter))

    for user_id, duration_seconds in db.execute(session_statement):
        user_stats_for_user = stats_by_user_id[user_id]
        user_stats_for_user.session_count += 1
        user_stats_for_user.total_playtime += duration_seconds or 0

    match_statement = (
        select(
            MatchParticipation.user_id,
            MatchParticipation.outcome,
            GameMap.map_id,
            GameMap.name,
        )
        .join(Match, MatchParticipation.match_id == Match.match_id)
        .join(GameMap, Match.map_id == GameMap.map_id)
        .join(
            Session,
            and_(
                MatchParticipation.session_id == Session.session_id,
                MatchParticipation.user_id == Session.user_id,
            ),
        )
        .where(MatchParticipation.user_id.in_(user_ids))
    )
    if os_filter is not None:
        match_statement = match_statement.where(Session.device_os.in_(os_filter))

    for user_id, outcome, map_id, map_name in db.execute(match_statement):
        user_stats_for_user = stats_by_user_id[user_id]
        user_stats_for_user.match_count += 1
        user_stats_for_user.outcome_sum += outcome

        if map_id not in user_stats_for_user.maps:
            user_stats_for_user.maps[map_id] = MapStats(name=map_name)

        map_stats = user_stats_for_user.maps[map_id]
        map_stats.match_count += 1
        map_stats.outcome_sum += outcome

    entries = [
        build_user_stats_entry(user, stats_by_user_id[user.user_id]) for user in users
    ]
    return sorted(entries, key=lambda entry: (-entry.total_playtime, entry.username))


@app.get("/map-stats/{map_name}", response_model=list[MapStatsEntry])
def map_stats(
    map_name: str,
    db: Annotated[DatabaseSession, Depends(get_db)],
    date_from: Annotated[date | None, Query()] = None,
    date_to: Annotated[date | None, Query()] = None,
):
    game_map = db.scalar(select(GameMap).where(GameMap.name == map_name))
    if game_map is None:
        raise HTTPException(status_code=404, detail="map not found")

    match_statement = select(
        Match.match_id,
        Match.ended_date,
        Match.duration_seconds,
    ).where(Match.map_id == game_map.map_id)
    if date_to is not None:
        match_statement = match_statement.where(Match.ended_date <= date_to)

    daily_stats_by_date: dict[date, DailyMapStats] = {}
    output_dates = set()

    for _, ended_date, duration_seconds in db.execute(
        match_statement.order_by(Match.ended_date, Match.match_id)
    ):
        if date_from is not None and ended_date < date_from:
            continue

        output_dates.add(ended_date)
        daily_stats = daily_stats_by_date.setdefault(ended_date, DailyMapStats())
        daily_stats.match_count += 1
        daily_stats.total_duration_seconds += duration_seconds

    if not output_dates:
        return []

    max_output_date = max(output_dates)
    participation_statement = (
        select(
            Match.ended_date,
            User.username,
            MatchParticipation.outcome,
        )
        .join(Match, MatchParticipation.match_id == Match.match_id)
        .join(User, MatchParticipation.user_id == User.user_id)
        .where(
            Match.map_id == game_map.map_id,
            Match.ended_date <= max_output_date,
        )
        .order_by(Match.ended_date, User.username)
    )

    participation_rows_by_date: dict[date, list[tuple[str, Decimal]]] = {}
    for ended_date, username, outcome in db.execute(participation_statement):
        participation_rows_by_date.setdefault(ended_date, []).append((username, outcome))

    cumulative_stats_by_username: dict[str, PlayerMapStats] = {}
    best_player_by_date: dict[date, str] = {}

    for current_date in sorted(participation_rows_by_date):
        for username, outcome in participation_rows_by_date[current_date]:
            player_stats = cumulative_stats_by_username.setdefault(
                username, PlayerMapStats()
            )
            player_stats.match_count += 1
            player_stats.outcome_sum += outcome

        if current_date in output_dates:
            best_player_by_date[current_date] = get_best_player_username(
                cumulative_stats_by_username
            )

    entries = []
    for current_date in sorted(output_dates, reverse=True):
        daily_stats = daily_stats_by_date[current_date]
        entries.append(
            MapStatsEntry(
                date=current_date,
                avg_playtime=(
                    daily_stats.total_duration_seconds / daily_stats.match_count
                ),
                best_player_username=best_player_by_date[current_date],
                match_cnt=daily_stats.match_count,
            )
        )

    return entries
