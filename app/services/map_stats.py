from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session as DatabaseSession

from app.models import GameMap, Match, MatchParticipation, User
from app.schemas import MapStatsEntry
from app.services.common import decimal_ratio


class MapNotFoundError(ValueError):
    pass


@dataclass
class DailyMapStats:
    total_duration_seconds: int = 0
    match_count: int = 0


@dataclass
class PlayerMapStats:
    match_count: int = 0
    outcome_sum: Decimal = Decimal("0")


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


def get_map_stats(
    map_name: str,
    db: DatabaseSession,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[MapStatsEntry]:
    game_map = db.scalar(select(GameMap).where(GameMap.name == map_name))
    if game_map is None:
        raise MapNotFoundError(map_name)

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


def get_latest_match_date(db: DatabaseSession) -> date | None:
    return db.scalar(select(func.max(Match.ended_date)))


def get_map_names(db: DatabaseSession) -> list[str]:
    return list(db.scalars(select(GameMap.name).order_by(GameMap.name)).all())
