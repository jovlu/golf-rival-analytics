import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker


load_dotenv()


class Base(DeclarativeBase):
    pass


engine = create_engine(os.environ["DATABASE_URL"])

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def check_db_connection():
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
