"""
Tek seferlik migration: greydb.gol_karne_tahmin tablosunu oluşturur (gol modeli karnesi).

Günlük anlık görüntü işi (POST /api/skorjin/prematch-hazirla, dry_run=false) aynı DDL'i CREATE IF NOT EXISTS ile zaten çalıştırır;
bu betik tabloyu işten önce elle kurmak içindir.

Kullanım:
    DATABASE_URL=... python scripts/create_gol_karne_table.py
"""
import os
import sys

from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.services.gol_karne import TABLO_DDL  # noqa: E402

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("HATA: DATABASE_URL ortam değişkeni gerekli.")
    sys.exit(1)

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+psycopg2" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

with create_engine(DATABASE_URL).begin() as conn:
    for parca in TABLO_DDL.split(";"):
        if parca.strip():
            conn.execute(text(parca))

print("OK: greydb.gol_karne_tahmin hazır.")
