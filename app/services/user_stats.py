from dataclasses import dataclass, field
from decimal import Decimal

from sqlalchemy import and_, select
from sqlalchemy.orm import Session as DatabaseSession

from app.models import GameMap, Match, MatchParticipation, Session, User
from app.schemas import UserStatsEntry
from app.services.common import decimal_ratio, normalize_list_filter


@dataclass
class MapStats:
    name: str
    match_count: int = 0
    outcome_sum: Decimal = Decimal("0")


@dataclass
class UserStats:
    total_playtime: int = 0
    session_count: int = 0
    match_count: int = 0
    outcome_sum: Decimal = Decimal("0")
    maps: dict[str, MapStats] = field(default_factory=dict)


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


def get_user_stats(
    db: DatabaseSession,
    countries: list[str] | None = None,
    oss: list[str] | None = None,
) -> list[UserStatsEntry]:
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
