"""
Tek seferlik migration: greydb.tercih_izleme tablosunu oluşturur.

Tercih izleme: matchforge günlük listesindeki her tercih (olasılık + o andaki iddaa oranı) maçtan önce eklemeli olarak kaydedilir
(kickoff'a ~24, ~6, ~1 saat kala), maç bitince sonucu yazılır. Yalnızca iç analiz içindir.
Yalnızca CREATE TABLE/INDEX IF NOT EXISTS çalıştırır: veri silmez ya da değiştirmez, tekrar çalıştırılabilir.

Kullanım:
    DATABASE_URL=... python scripts/create_tercih_izleme_table.py
"""
import os
import sys

from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.services.tercih_izleme import TABLO_DDL  # noqa: E402

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("HATA: DATABASE_URL ortam değişkeni gerekli.")
    sys.exit(1)

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+psycopg2" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

engine = create_engine(DATABASE_URL)

with engine.begin() as conn:
    for parca in [p.strip() for p in TABLO_DDL.split(";") if p.strip()]:
        conn.execute(text(parca))
    n = conn.execute(text("SELECT count(*) FROM greydb.tercih_izleme")).scalar()

print(f"OK: greydb.tercih_izleme hazır ({n} satır).")
