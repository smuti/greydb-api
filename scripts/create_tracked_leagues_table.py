"""
Tek seferlik, tekrar çalıştırılabilir migration: greydb.tracked_leagues tablosunu oluşturur ve başlangıç listesini yükler.

Maçlar sayfasının hangi ligleri hangi sırayla gösterdiği burada durur. Var olan bir satırın
`aktif`/`oncelik`/`ad` değerlerine DOKUNMAZ (elle yapılan değişiklikler korunur); yalnızca eksik ligleri ekler.

Kullanım:
    DATABASE_URL=... python scripts/create_tracked_leagues_table.py
"""
import os
import sys

from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.services.tracked_leagues_seed import SEED  # noqa: E402

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("HATA: DATABASE_URL ortam değişkeni gerekli.")
    sys.exit(1)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+psycopg2" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

DDL = """
CREATE TABLE IF NOT EXISTS greydb.tracked_leagues (
    anahtar TEXT PRIMARY KEY,
    ad TEXT NOT NULL,
    oncelik INTEGER NOT NULL,
    aktif BOOLEAN NOT NULL DEFAULT TRUE,
    fotmob_ids INTEGER[],
    ulke TEXT,
    ad_kalibi TEXT,
    logo_id INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

# Tablo logo_id'siz kurulmuş olabilir: sütunu ekle, yalnızca boş olanları başlangıç verisinden doldur (elle girilene dokunma).
ALTER = "ALTER TABLE greydb.tracked_leagues ADD COLUMN IF NOT EXISTS logo_id INTEGER"
LOGO_DOLDUR = "UPDATE greydb.tracked_leagues SET logo_id = :logo_id WHERE anahtar = :anahtar AND logo_id IS NULL"

INSERT = """
INSERT INTO greydb.tracked_leagues (anahtar, ad, oncelik, fotmob_ids, ulke, ad_kalibi, logo_id)
VALUES (:anahtar, :ad, :oncelik, :fotmob_ids, :ulke, :ad_kalibi, :logo_id)
ON CONFLICT (anahtar) DO NOTHING
"""

if __name__ == "__main__":
    engine = create_engine(DATABASE_URL)
    with engine.begin() as conn:
        conn.execute(text(DDL))
        conn.execute(text(ALTER))
        for s in SEED:
            conn.execute(text(INSERT), {"fotmob_ids": None, "ulke": None, "ad_kalibi": None, "logo_id": None, **s})
            conn.execute(text(LOGO_DOLDUR), {"logo_id": s["logo_id"], "anahtar": s["anahtar"]})
        n = conn.execute(text("SELECT count(*) FROM greydb.tracked_leagues")).scalar()
    print(f"Tamam: greydb.tracked_leagues hazır ({n} lig).")
