"""
Tek seferlik migration: Spor Toto arşivi (resmî sonuçlar) ve oynanma yüzdesi geçmişi.

  greydb.spor_toto_arsiv_haftalar : resmî hafta (oyun programı): 15/14/13/12 bilen sayıları ve ikramiyeleri, sonuç dağılımı, temiz bayrağı
  greydb.spor_toto_arsiv_maclar   : haftanın 15 maçı (skor, 1/0/2, noter)
  greydb.spor_toto_oynanma        : iddaa Spor Toto sayfasındaki oynanma yüzdelerinin zaman damgalı kaydı (geçmişi yok, bugünden birikir)

Bunlar herkese açık resmî sonuçlardır; Fuat'ın tercihleri/yorumları (spor_toto_weeks / spor_toto_matches) burada YOK.
Kullanım:  DATABASE_URL=... python scripts/create_spor_toto_arsiv_tables.py
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
    """CREATE TABLE IF NOT EXISTS greydb.spor_toto_arsiv_haftalar (
        round_id INTEGER PRIMARY KEY,
        sezon TEXT NOT NULL,
        oyun_no INTEGER,
        ad TEXT,
        kapanis TIMESTAMP,
        bilen_15 INTEGER, bilen_14 INTEGER, bilen_13 INTEGER, bilen_12 INTEGER,
        ikramiye_15 NUMERIC(16,2), ikramiye_14 NUMERIC(16,2), ikramiye_13 NUMERIC(16,2), ikramiye_12 NUMERIC(16,2),
        mac_sayisi SMALLINT NOT NULL CHECK (mac_sayisi BETWEEN 0 AND 15),
        ev SMALLINT NOT NULL, beraberlik SMALLINT NOT NULL, deplasman SMALLINT NOT NULL,
        noter_sayisi SMALLINT NOT NULL DEFAULT 0,
        temiz BOOLEAN NOT NULL DEFAULT FALSE,
        guncellendi TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    "CREATE INDEX IF NOT EXISTS idx_st_arsiv_haftalar_sezon ON greydb.spor_toto_arsiv_haftalar (sezon, oyun_no)",
    """CREATE TABLE IF NOT EXISTS greydb.spor_toto_arsiv_maclar (
        round_id INTEGER NOT NULL REFERENCES greydb.spor_toto_arsiv_haftalar (round_id) ON DELETE CASCADE,
        sira SMALLINT NOT NULL CHECK (sira BETWEEN 1 AND 15),
        tarih TIMESTAMP,
        ev TEXT, dep TEXT,
        skor_ev SMALLINT, skor_dep SMALLINT,
        sonuc CHAR(1) CHECK (sonuc IN ('1','0','2')),
        noter BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (round_id, sira)
    )""",
    """CREATE TABLE IF NOT EXISTS greydb.spor_toto_oynanma (
        id SERIAL PRIMARY KEY,
        donem_no INTEGER NOT NULL,   -- iddaa'nın dönem numarası (resmî oyun programı numarasından farklı; kapanış saatiyle eşleşir)
        mac_sira SMALLINT NOT NULL,
        ev TEXT, dep TEXT,
        mac_tarihi TIMESTAMP,
        kapanis TIMESTAMP,
        yuzde_1 SMALLINT, yuzde_0 SMALLINT, yuzde_2 SMALLINT,
        kaydedildi TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    "CREATE INDEX IF NOT EXISTS idx_st_oynanma_hafta ON greydb.spor_toto_oynanma (donem_no, mac_sira, kaydedildi DESC)",
]

engine = create_engine(DATABASE_URL)
with engine.begin() as conn:
    for sql in DDL:
        conn.execute(text(sql))
    n = conn.execute(text("SELECT count(*) FROM information_schema.tables WHERE table_schema='greydb' AND table_name IN ('spor_toto_arsiv_haftalar','spor_toto_arsiv_maclar','spor_toto_oynanma')")).scalar()
print(f"OK: {n}/3 tablo hazır.")
