"""
Tek seferlik migration: Skorjin Spor Toto sayfasının hazır hafta paketi.

  greydb.spor_toto_skorjin_paketleri : dönem (iddaa dönem no) başına tek satır; paket JSONB (dağılım, 15 maç okuması, sistemler, genel toto bilgisi)

Pakette Fuat'ın tercihi/yorumu/modeli YOK (uç böyle paketi reddeder).
Kullanım:  DATABASE_URL=... python scripts/create_spor_toto_skorjin_paketleri.py
"""
import os
import sys

from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("HATA: DATABASE_URL ortam değişkeni gerekli.")
    sys.exit(1)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+psycopg2" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

DDL = [
    """CREATE TABLE IF NOT EXISTS greydb.spor_toto_skorjin_paketleri (
        donem_no INTEGER PRIMARY KEY,
        kapanis TIMESTAMP NOT NULL,
        paket JSONB NOT NULL,
        guncellendi TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
]

engine = create_engine(DATABASE_URL)
with engine.begin() as conn:
    for sql in DDL:
        conn.execute(text(sql))
    n = conn.execute(text("SELECT count(*) FROM information_schema.tables WHERE table_schema='greydb' AND table_name = 'spor_toto_skorjin_paketleri'")).scalar()
print(f"OK: {n}/1 tablo hazır.")
