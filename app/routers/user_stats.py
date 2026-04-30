from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session as DatabaseSession

from app.db import get_db
from app.schemas import UserStatsEntry
from app.services.user_stats import get_user_stats


router = APIRouter()


@router.get("/user-stats", response_model=list[UserStatsEntry])
def user_stats(
    db: Annotated[DatabaseSession, Depends(get_db)],
    countries: Annotated[list[str] | None, Query()] = None,
    oss: Annotated[list[str] | None, Query(alias="OSs")] = None,
):
    return get_user_stats(db, countries=countries, oss=oss)
