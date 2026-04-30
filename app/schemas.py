from datetime import date

from pydantic import BaseModel


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
