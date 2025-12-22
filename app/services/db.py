"""
Database bağlantı servisi
"""
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from app.config import get_settings

settings = get_settings()

# SQLAlchemy engine
engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """FastAPI dependency için database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_connection():
    """Context manager ile connection"""
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()


def query_to_df(sql: str, params: dict = None) -> pd.DataFrame:
    """SQL sorgusunu pandas DataFrame olarak döndür"""
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def execute_query(sql: str, params: dict = None) -> list[dict]:
    """SQL sorgusunu çalıştır ve dict listesi döndür"""
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]

