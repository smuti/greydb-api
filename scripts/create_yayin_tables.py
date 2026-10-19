"""
Tek seferlik, tekrar çalıştırılabilir migration: yayın sayfası tabloları.

  greydb.yayin_sayfalari  hazırlanmış yayın sayfaları (anlık görüntü paketi; sayfa yalnızca bunu okur, dış servise istek atmaz)
  greydb.yayin_takimlari  takım rengi (logodan otomatik, admin elle düzeltebilir) ve logo (FotMob'dan bir kez alınır, bizden servis edilir)

Mevcut tablolara DOKUNMAZ. Kullanım:  DATABASE_URL=... python scripts/create_yayin_tables.py
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
    CREATE TABLE IF NOT EXISTS greydb.yayin_sayfalari (
        id SERIAL PRIMARY KEY,
        kod TEXT NOT NULL UNIQUE,
        tur TEXT NOT NULL CHECK (tur IN ('toto','gun')),
        etiket TEXT NOT NULL,
        tarih DATE NOT NULL,
        program_adi TEXT NOT NULL,
        marka TEXT NOT NULL DEFAULT 'GREYSPORTS',
        yayin_tarihi DATE NOT NULL,
        yayin_saat TEXT NOT NULL,
        bayi_kodu TEXT NOT NULL DEFAULT '300908',
        kaynak JSONB NOT NULL DEFAULT '{}'::jsonb,
        maclar JSONB NOT NULL DEFAULT '[]'::jsonb,
        durum TEXT NOT NULL DEFAULT 'hazirlaniyor' CHECK (durum IN ('hazirlaniyor','hazir','hata')),
        hata TEXT,
        olusturan TEXT,
        olusturma TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
        guncelleme TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS greydb.yayin_takimlari (
        fotmob_team_id INTEGER PRIMARY KEY,
        ad TEXT,
        renk TEXT NOT NULL,
        renk_elle TEXT,
        logo BYTEA,
        logo_tur TEXT NOT NULL DEFAULT 'image/png',
        guncelleme TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_yayin_sayfalari_olusturma ON greydb.yayin_sayfalari (olusturma DESC)",
]

if __name__ == "__main__":
    engine = create_engine(URL)
    with engine.begin() as conn:
        for q in DDL:
            conn.execute(text(q))
        n = conn.execute(text("SELECT count(*) FROM greydb.yayin_sayfalari")).scalar()
        k = conn.execute(text("SELECT count(*) FROM greydb.yayin_takimlari")).scalar()
    print(f"Tamam: greydb.yayin_sayfalari ({n} sayfa) ve greydb.yayin_takimlari ({k} takım) hazır.")
