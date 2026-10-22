"""
Tek seferlik, tekrar çalıştırılabilir migration: skor sayacı tabloları (admin → işçi Mac'ler).

  greydb.sayac_oyuncular  kesilmiş oyuncu görselleri (şeffaf PNG, admin yükler). Bir oyuncunun birden çok pozu olabilir.
  greydb.sayac_isleri     "Üret" ile açılan işler; işçi Mac'ler kuyruktan alır, videoyu KENDİ diskinde üretir (sunucuya yüklenmez)
  greydb.sayac_isciler    işçilerin son görülme zamanı (admin "işçi çevrimiçi mi" göstersin)

Mevcut tablolara DOKUNMAZ. Kullanım:  DATABASE_URL=... python scripts/create_sayac_tables.py
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
    CREATE TABLE IF NOT EXISTS greydb.sayac_oyuncular (
        id SERIAL PRIMARY KEY,
        fotmob_player_id INTEGER NOT NULL,
        ad TEXT NOT NULL,
        fotmob_team_id INTEGER NOT NULL,
        poz TEXT NOT NULL DEFAULT 'varsayilan',
        gorsel BYTEA NOT NULL CHECK (octet_length(gorsel) <= 8388608),
        gorsel_tur TEXT NOT NULL DEFAULT 'image/png' CHECK (gorsel_tur = 'image/png'),
        genislik INTEGER NOT NULL CHECK (genislik >= 350),
        yukseklik INTEGER NOT NULL CHECK (yukseklik > 0),
        kafa_orani REAL NOT NULL DEFAULT 0.5 CHECK (kafa_orani BETWEEN 0 AND 1),
        yildiz BOOLEAN NOT NULL DEFAULT FALSE,
        aktif BOOLEAN NOT NULL DEFAULT TRUE,
        yukleyen TEXT,
        olusturma TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
        UNIQUE (fotmob_player_id, poz)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_sayac_oyuncular_takim ON greydb.sayac_oyuncular (fotmob_team_id) WHERE aktif",
    # her takımda en çok bir "yıldız" poz: "Üret" dendiğinde otomatik seçilen
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_sayac_oyuncular_yildiz ON greydb.sayac_oyuncular (fotmob_team_id) WHERE yildiz AND aktif",
    """
    CREATE TABLE IF NOT EXISTS greydb.sayac_isleri (
        id SERIAL PRIMARY KEY,
        fotmob_match_id INTEGER NOT NULL,
        ev_team_id INTEGER NOT NULL,
        dep_team_id INTEGER NOT NULL,
        ev_ad TEXT NOT NULL,
        dep_ad TEXT NOT NULL,
        ev_oyuncu_id INTEGER NOT NULL REFERENCES greydb.sayac_oyuncular(id),
        dep_oyuncu_id INTEGER NOT NULL REFERENCES greydb.sayac_oyuncular(id),
        girdi JSONB NOT NULL,                       -- lig, tarih, saat, evSeed, depSeed, evGlow, depGlow (üretim anındaki donuk kopya)
        durum TEXT NOT NULL DEFAULT 'onizleme_bekliyor' CHECK (durum IN ('onizleme_bekliyor','onizleme_calisiyor','onizleme_hazir','bekliyor','calisiyor','hazir','hata','iptal')),
        isci TEXT,                                  -- işi alan Mac'in adı
        dosya_yolu TEXT,                            -- işçinin DİSKİNDEKİ yol (sunucuya yüklenmez)
        deneme INTEGER NOT NULL DEFAULT 0,
        hata TEXT,
        olusturan TEXT,
        olusturma TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
        baslama TIMESTAMP,
        kalp_atisi TIMESTAMP,                       -- işçi çalışırken periyodik günceller; kesilirse iş kuyruğa döner
        onizleme BYTEA,                             -- işçinin ürettiği JPEG still (admin onayı için; MP4 değil)
        bitis TIMESTAMP
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_sayac_isleri_durum ON greydb.sayac_isleri (durum, id)",
    # aynı maç için aynı anda tek aktif iş (çift tıklama koruması)
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_sayac_isleri_aktif ON greydb.sayac_isleri (fotmob_match_id) WHERE durum IN ('onizleme_bekliyor','onizleme_calisiyor','onizleme_hazir','bekliyor','calisiyor')",
    """
    CREATE TABLE IF NOT EXISTS greydb.sayac_isciler (
        ad TEXT PRIMARY KEY,
        son_gorulme TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
        surum TEXT
    )
    """,
]

if __name__ == "__main__":
    engine = create_engine(URL)
    with engine.begin() as conn:
        for q in DDL:
            conn.execute(text(q))
        n = conn.execute(text("SELECT count(*) FROM greydb.sayac_oyuncular")).scalar()
        k = conn.execute(text("SELECT count(*) FROM greydb.sayac_isleri")).scalar()
    print(f"Tamam: greydb.sayac_oyuncular ({n} poz), greydb.sayac_isleri ({k} iş), greydb.sayac_isciler hazır.")
