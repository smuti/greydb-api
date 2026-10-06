"""
Tek seferlik migration: greydb.skorjin_match_analysis tablosunu oluşturur.

"Skorjin'e Sor" ile henüz başlamamış bir maç için üretilen şablonlu analizin
önbelleği — fotmob_match_id başına tek satır, ilk istekte üretilir, sonraki
isteklere aynı metin döner.

Kullanım:
    DATABASE_URL=... python scripts/create_skorjin_match_analysis_table.py
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

engine = create_engine(DATABASE_URL)

DDL = """
CREATE TABLE IF NOT EXISTS greydb.skorjin_match_analysis (
    fotmob_match_id BIGINT PRIMARY KEY,
    home_team TEXT,
    away_team TEXT,
    league TEXT,
    analysis TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

with engine.begin() as conn:
    conn.execute(text(DDL))

print("OK: greydb.skorjin_match_analysis hazır.")
