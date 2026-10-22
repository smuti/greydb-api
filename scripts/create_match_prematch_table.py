"""
Tek seferlik migration: greydb.match_prematch tablosunu oluşturur.

Skorjin maç öncesi servisinin okuduğu anlık görüntü: fotmob_match_id başına tek satır; form, H2H, skor matrisi, oran ve
oynanma yüzdesi her biri kendi zaman damgasıyla. Satırları POST /api/skorjin/prematch-hazirla yazar (zamanlayıcı çağırır).
Aynı DDL uç tarafından da (dry_run=false) CREATE IF NOT EXISTS ile çalıştırılır; bu betik elle kurulum içindir.

Kullanım:
    DATABASE_URL=... python scripts/create_match_prematch_table.py
Skorjin için salt-okunur rol (örnek):
    GRANT USAGE ON SCHEMA greydb TO skorjin_ro; GRANT SELECT ON greydb.match_prematch TO skorjin_ro;
    GRANT USAGE ON SCHEMA public TO skorjin_ro; GRANT SELECT ON public.teams, public.upcoming_matches TO skorjin_ro;
"""
import os
import sys

from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.services.skorjin_prematch import TABLO_DDL  # noqa: E402

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

print("OK: greydb.match_prematch hazır.")
