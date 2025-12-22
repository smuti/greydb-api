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


def query_to_df(sql: str, params: dict = None, commit: bool = False) -> pd.DataFrame:
    """SQL sorgusunu pandas DataFrame olarak döndür"""
    with engine.connect() as conn:
        result = pd.read_sql(text(sql), conn, params=params)
        if commit:
            conn.commit()
        return result


def execute_query(sql: str, params: dict = None) -> list[dict]:
    """SQL sorgusunu çalıştır ve dict listesi döndür"""
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]


def execute_insert(sql: str, params: dict = None) -> dict | None:
    """INSERT/UPDATE/DELETE çalıştır, RETURNING varsa sonucu döndür"""
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        conn.commit()
        try:
            row = result.fetchone()
            if row:
                columns = result.keys()
                return dict(zip(columns, row))
        except:
            pass
        return None


def execute_insert_many(sql: str, params_list: list[dict]) -> None:
    """Birden fazla INSERT çalıştır"""
    with engine.connect() as conn:
        for params in params_list:
            conn.execute(text(sql), params)
        conn.commit()

