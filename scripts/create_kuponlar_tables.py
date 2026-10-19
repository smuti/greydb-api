"""
Tek seferlik, tekrar çalıştırılabilir migration: greydb.kuponlar ve greydb.kupon_bacaklari tablolarını oluşturur.

Yeni kupon modeli eski "tahmin" (greydb.predictions) ve eski kupon (greydb.coupons / coupon_matches) yapılarının yerine geçer.
Eski tablolara DOKUNMAZ (arşiv olarak kalır; taşıma ayrı betikle).

Kullanım:  DATABASE_URL=... python scripts/create_kuponlar_tables.py
"""
import os
import sys

from sqlalchemy import create_engine, text

URL = os.environ.get("DATABASE_URL")
if not URL:
    print("HATA: DATABASE_URL ortam değişkeni gerekli.")
    sys.exit(1)
if URL.startswith("postgres://"):
    URL = URL.replace("postgres://", "postgresql+psycopg2://", 1)
elif URL.startswith("postgresql://") and "+psycopg2" not in URL:
    URL = URL.replace("postgresql://", "postgresql+psycopg2://", 1)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS greydb.kuponlar (
        id SERIAL PRIMARY KEY,
        seviye TEXT NOT NULL CHECK (seviye IN ('acik','bayi','elit')),
        durum TEXT NOT NULL DEFAULT 'taslak' CHECK (durum IN ('taslak','yayinda','arsiv')),
        zamanlama TEXT NOT NULL DEFAULT 'pre' CHECK (zamanlama IN ('pre','live')),
        one_cikan BOOLEAN NOT NULL DEFAULT FALSE,
        sistem_k INTEGER CHECK (sistem_k IS NULL OR sistem_k >= 2),
        nedenler JSONB NOT NULL DEFAULT '[]'::jsonb,
        ana_risk TEXT,
        yorum TEXT,
        sinyal TEXT,
        risk_seviyesi TEXT,
        toplam_oran NUMERIC(10,2),
        en_yuksek_odeme NUMERIC(10,2),
        kombinasyon_sayisi INTEGER NOT NULL DEFAULT 1,
        sonuc TEXT CHECK (sonuc IS NULL OR sonuc IN ('won','lost','void')),
        odeme NUMERIC(12,4),
        sonuc_zamani TIMESTAMP,
        yayin_zamani TIMESTAMP,
        olusturan_email TEXT NOT NULL,
        kaynak TEXT,
        olusturma TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
        guncelleme TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS greydb.kupon_bacaklari (
        id SERIAL PRIMARY KEY,
        kupon_id INTEGER NOT NULL REFERENCES greydb.kuponlar(id) ON DELETE CASCADE,
        sira INTEGER NOT NULL DEFAULT 0,
        iddaa_event_id BIGINT,
        fotmob_match_id INTEGER,
        ev TEXT NOT NULL,
        dep TEXT NOT NULL,
        lig TEXT,
        mac_zamani TIMESTAMP NOT NULL,
        bahis_turu TEXT NOT NULL,
        secim TEXT NOT NULL,
        oran NUMERIC(10,2) NOT NULL CHECK (oran > 0),
        sonuc TEXT NOT NULL DEFAULT 'pending' CHECK (sonuc IN ('pending','won','lost','void')),
        sonuc_kaynagi TEXT CHECK (sonuc_kaynagi IS NULL OR sonuc_kaynagi IN ('otomatik','elle')),
        sonuc_zamani TIMESTAMP
    )
    """,
    "ALTER TABLE greydb.kupon_bacaklari ADD COLUMN IF NOT EXISTS ev_takim_id INTEGER",
    "ALTER TABLE greydb.kupon_bacaklari ADD COLUMN IF NOT EXISTS dep_takim_id INTEGER",
    "CREATE INDEX IF NOT EXISTS idx_kuponlar_durum_yayin ON greydb.kuponlar (durum, yayin_zamani DESC)",
    "CREATE INDEX IF NOT EXISTS idx_kuponlar_seviye ON greydb.kuponlar (seviye)",
    "CREATE INDEX IF NOT EXISTS idx_kuponlar_sonuc ON greydb.kuponlar (sonuc)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_kuponlar_kaynak ON greydb.kuponlar (kaynak) WHERE kaynak IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_kupon_bacaklari_kupon ON greydb.kupon_bacaklari (kupon_id, sira)",
    "CREATE INDEX IF NOT EXISTS idx_kupon_bacaklari_bekleyen ON greydb.kupon_bacaklari (mac_zamani) WHERE sonuc = 'pending'",
]

if __name__ == "__main__":
    engine = create_engine(URL)
    with engine.begin() as conn:
        for q in DDL:
            conn.execute(text(q))
        n = conn.execute(text("SELECT count(*) FROM greydb.kuponlar")).scalar()
    print(f"Tamam: greydb.kuponlar ve greydb.kupon_bacaklari hazır ({n} kupon).")
