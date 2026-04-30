from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, DateTime, ForeignKey, Index, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class User(Base):
    __tablename__ = "users"

    user_id: Mapped[str] = mapped_column(String, primary_key=True)
    username: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    country: Mapped[str] = mapped_column(String(3), nullable=False, index=True)
    registered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )


class GameMap(Base):
    __tablename__ = "maps"

    map_id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)


class Session(Base):
    __tablename__ = "sessions"

    session_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.user_id"), nullable=False, index=True
    )
    device_os: Mapped[str] = mapped_column(String, nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    duration_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)


class Match(Base):
    __tablename__ = "matches"

    match_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    map_id: Mapped[str] = mapped_column(
        String, ForeignKey("maps.map_id"), nullable=False, index=True
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    ended_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    ended_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    duration_seconds: Mapped[int] = mapped_column(Integer, nullable=False)


class MatchParticipation(Base):
    __tablename__ = "match_participations"

    match_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("matches.match_id"), primary_key=True
    )
    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.user_id"), primary_key=True
    )
    session_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("sessions.session_id"), nullable=True, index=True
    )
    outcome: Mapped[Decimal] = mapped_column(Numeric(3, 1), nullable=False)


Index("ix_match_participations_user_id", MatchParticipation.user_id)
