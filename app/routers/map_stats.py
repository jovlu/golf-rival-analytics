from datetime import date, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session as DatabaseSession

from app.db import get_db
from app.schemas import MapStatsEntry
from app.services.map_stats import (
    MapNotFoundError,
    get_latest_match_date,
    get_map_names,
    get_map_stats,
)


router = APIRouter()


@router.get("/map-stats/{map_name}", response_model=list[MapStatsEntry])
def map_stats(
    map_name: str,
    db: Annotated[DatabaseSession, Depends(get_db)],
    date_from: Annotated[date | None, Query()] = None,
    date_to: Annotated[date | None, Query()] = None,
):
    try:
        return get_map_stats(map_name, db, date_from=date_from, date_to=date_to)
    except MapNotFoundError as exc:
        raise HTTPException(status_code=404, detail="map not found") from exc


@router.get("/map-stats-chart", response_class=HTMLResponse)
def map_stats_chart(db: Annotated[DatabaseSession, Depends(get_db)]):
    import plotly.graph_objects as go

    date_to = get_latest_match_date(db)
    if date_to is None:
        return HTMLResponse("<h1>No match data available</h1>")

    date_from = date_to - timedelta(days=6)
    output_dates = [date_from + timedelta(days=offset) for offset in range(7)]

    fig = go.Figure()
    for map_name in get_map_names(db):
        entries = get_map_stats(map_name, db, date_from=date_from, date_to=date_to)
        count_by_date = {entry.date: entry.match_cnt for entry in entries}
        fig.add_trace(
            go.Scatter(
                x=output_dates,
                y=[count_by_date.get(output_date, 0) for output_date in output_dates],
                mode="lines+markers",
                name=map_name,
            )
        )

    fig.update_layout(
        title=f"Match Count by Map ({date_from} to {date_to})",
        xaxis_title="Date",
        yaxis_title="Match Count",
        hovermode="x unified",
        template="plotly_white",
    )

    return HTMLResponse(fig.to_html(full_html=True, include_plotlyjs=True))
