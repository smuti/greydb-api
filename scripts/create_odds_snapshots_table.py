"""
Tek seferlik migration: greydb.odds_snapshots tablosunu oluşturur.

İddaa oran takibi — admin "Durumu Kaydet"e her bastığında o anki tüm ham
market/oran verisini (allMarkets, categories, popularMarkets) ve o anki
kural-bazlı yorumu yeni bir satır olarak ekler (append-only, üzerine
yazma yok) — zamanla aynı maç için bir oran geçmişi birikir.

Kullanım:
    DATABASE_URL=... python scripts/create_odds_snapshots_table.py
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
CREATE TABLE IF NOT EXISTS greydb.odds_snapshots (
    id SERIAL PRIMARY KEY,
    iddaa_match_id BIGINT NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    total_markets INTEGER,
    raw_data JSONB NOT NULL,
    interpretation JSONB,
    created_by_email TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_odds_snapshots_match_id
    ON greydb.odds_snapshots (iddaa_match_id, created_at DESC);
"""

with engine.begin() as conn:
    conn.execute(text(DDL))

print("OK: greydb.odds_snapshots hazır.")
