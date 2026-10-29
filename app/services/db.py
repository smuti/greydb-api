"""
Database bağlantı servisi
"""
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from contextvars import ContextVar
from app.config import get_settings

settings = get_settings()

# SQLAlchemy'nin sürücü otomatik-seçimi sqlalchemy sürümüne göre psycopg (v3) ile
# psycopg2 arasında değişebiliyor; biz sadece psycopg2-binary kuruyoruz, o yüzden
# sürücüyü URL'de açıkça belirtip belirsizliği ortadan kaldırıyoruz.
database_url = settings.database_url
if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql+psycopg2://", 1)
elif database_url.startswith("postgresql://") and "+psycopg2" not in database_url:
    database_url = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)

# SQLAlchemy engine
# Uçlar artık iş parçacığı havuzunda eş zamanlı çalışır: varsayılan 5+10 bağlantı kuyruğa sokar.
engine = create_engine(database_url, pool_pre_ping=True, pool_size=10, max_overflow=15, pool_recycle=1800)
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


def query_to_df(sql: str, params = None, commit: bool = False) -> pd.DataFrame:
    """SQL sorgusunu pandas DataFrame olarak döndür
    
    Args:
        sql: SQL sorgusu (%s veya :param formatında)
        params: tuple veya dict olabilir
        commit: True ise commit yap
    """
    with engine.connect() as conn:
        # Eğer params tuple ise, psycopg2 formatından sqlalchemy formatına çevir
        if params is not None and isinstance(params, tuple):
            # %s placeholder'ları :p0, :p1, ... formatına çevir
            import re
            placeholders = re.findall(r'%s', sql)
            for i, _ in enumerate(placeholders):
                sql = sql.replace('%s', f':p{i}', 1)
            params = {f'p{i}': v for i, v in enumerate(params)}
        
        result = pd.read_sql(text(sql), conn, params=params)
        if commit:
            conn.commit()
        return result


# transaction() bloğu içinde açık olan bağlantı (aynı iş parçacığı/bağlam). Yoksa her çağrı kendi bağlantısını açar.
_islem_baglantisi: ContextVar = ContextVar("islem_baglantisi", default=None)


@contextmanager
def transaction():
    """Bloktaki tüm execute_query/execute_insert çağrılarını TEK bağlantıda ve TEK işlemde çalıştırır.
    Blok hatasız biterse commit, hata olursa rollback (yarım kayıt kalmaz). İç içe kullanılırsa dıştaki işleme katılır."""
    if _islem_baglantisi.get() is not None:
        yield _islem_baglantisi.get()
        return
    conn = engine.connect()
    token = _islem_baglantisi.set(conn)
    try:
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        _islem_baglantisi.reset(token)
        conn.close()


def execute_query(sql: str, params: dict = None) -> list[dict]:
    """SQL sorgusunu çalıştır ve dict listesi döndür"""
    def calistir(conn):
        result = conn.execute(text(sql), params or {})
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]

    conn = _islem_baglantisi.get()
    if conn is not None:
        return calistir(conn)
    with engine.connect() as conn:
        return calistir(conn)


def execute_insert(sql: str, params: dict = None) -> dict | None:
    """INSERT/UPDATE/DELETE çalıştır, RETURNING varsa sonucu döndür.
    transaction() içindeyse commit işlem sonunda yapılır, değilse hemen."""
    def calistir(conn, hemen_commit):
        result = conn.execute(text(sql), params or {})
        if hemen_commit:
            conn.commit()
        try:
            row = result.fetchone()
            if row:
                columns = result.keys()
                return dict(zip(columns, row))
        except:
            pass
        return None

    conn = _islem_baglantisi.get()
    if conn is not None:
        return calistir(conn, False)
    with engine.connect() as conn:
        return calistir(conn, True)


def execute_insert_many(sql: str, params_list: list[dict]) -> None:
    """Birden fazla INSERT çalıştır"""
    conn = _islem_baglantisi.get()
    if conn is not None:
        for params in params_list:
            conn.execute(text(sql), params)
        return
    with engine.connect() as conn:
        for params in params_list:
            conn.execute(text(sql), params)
        conn.commit()

