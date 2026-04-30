from fastapi import APIRouter, HTTPException

from app.db import check_db_connection


router = APIRouter()


@router.get("/")
def root():
    return {"status": "ok"}


@router.get("/health")
def health():
    try:
        check_db_connection()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {"status": "ok", "database": "ok"}
