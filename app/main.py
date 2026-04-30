from fastapi import FastAPI

from app.routers import health, map_stats, user_stats


def create_app() -> FastAPI:
    app = FastAPI()
    app.include_router(health.router)
    app.include_router(user_stats.router)
    app.include_router(map_stats.router)
    return app


app = create_app()
