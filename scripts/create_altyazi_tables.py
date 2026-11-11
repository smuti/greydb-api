"""
Tek seferlik, tekrar çalıştırılabilir migration: CapCut otomatik altyazı tabloları (admin → işçi Mac).

  greydb.capcut_projeleri   işçi Mac'in bildirdiği CapCut projeleri (işçi başına bütünüyle yenilenir)
  greydb.altyazi_isleri     döküm / yazma / geri alma işleri (kelimeler, parçalar, durum)
  greydb.altyazi_isciler    altyazı işçisinin son görülme zamanı (sayaç işçisinden bağımsız)
  greydb.altyazi_sozluk     takım/oyuncu adı düzeltmeleri ve Whisper ipuçları

Mevcut tablolara DOKUNMAZ. Kullanım:  DATABASE_URL=... python scripts/create_altyazi_tables.py
Durum/aşama listeleri app/routers/altyazi.py ile aynı olmalı (test denetler): yeni durum eklenirse CHECK'i de genişlet.
"""
import os
import sys

DURUMLAR = ("sirada", "calisiyor", "dokum_hazir", "onay_bekliyor", "capcut_acik_bekliyor", "yazildi", "geri_alindi", "hata", "iptal")
ASAMALAR = ("dokum", "yaz", "geri_al")


def _liste(x):
    return ",".join(f"'{d}'" for d in x)


DDL = [
    """
    CREATE TABLE IF NOT EXISTS greydb.capcut_projeleri (
        isci TEXT NOT NULL,
        draft_id TEXT NOT NULL,
        ad TEXT NOT NULL,
        klasor TEXT NOT NULL,
        degisme TIMESTAMPTZ,
        sure_sn NUMERIC,
        tuval JSONB,                                 -- {w, h, fps}
        capcut_surum TEXT,
        medya JSONB,                                 -- [{ad, var_mi}]
        altyazi_var BOOLEAN NOT NULL DEFAULT FALSE,
        guncellendi TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
        PRIMARY KEY (isci, draft_id)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS greydb.altyazi_isleri (
        id BIGSERIAL PRIMARY KEY,
        isci TEXT NOT NULL,                          -- projenin bulunduğu Mac; iş yalnız onun
        draft_id TEXT NOT NULL,
        proje_ad TEXT,
        asama TEXT NOT NULL DEFAULT 'dokum' CHECK (asama IN ({_liste(ASAMALAR)})),
        durum TEXT NOT NULL DEFAULT 'sirada' CHECK (durum IN ({_liste(DURUMLAR)})),
        alt_adim TEXT,
        ayarlar JSONB NOT NULL DEFAULT '{{}}'::jsonb,   -- hazır ayar, boyut, yükseklik, renk
        kelimeler JSONB,                             -- kaynak zamanında kelimeler (kesim değişirse yeniden hesap için)
        parcalar JSONB,                              -- ekrandaki altyazı parçaları (admin düzeltir)
        imza TEXT,                                   -- dökümün alındığı kesimlerin imzası
        tuval JSONB,
        sure NUMERIC,
        uyarilar JSONB,
        onizleme BYTEA CHECK (onizleme IS NULL OR octet_length(onizleme) <= 26214400),   -- 360p MP4; yazıldıktan sonra silinir
        yedek_yolu TEXT,                             -- işçinin DİSKİNDEKİ yedek klasörü
        hata TEXT,
        deneme INTEGER NOT NULL DEFAULT 0,
        olusturan TEXT,
        olusturuldu TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
        guncellendi TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
        kalp_atisi TIMESTAMP
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_altyazi_isleri_durum ON greydb.altyazi_isleri (isci, durum, id)",
    # aynı projede aynı anda tek aktif iş (çift tıklama koruması)
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_altyazi_isleri_aktif ON greydb.altyazi_isleri (isci, draft_id) WHERE durum IN ('sirada','calisiyor','capcut_acik_bekliyor')",
    """
    CREATE TABLE IF NOT EXISTS greydb.altyazi_isciler (
        ad TEXT PRIMARY KEY,
        son_gorulme TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
        surum TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS greydb.altyazi_sozluk (
        id BIGSERIAL PRIMARY KEY,
        yanlis TEXT NOT NULL DEFAULT '',             -- boşsa yalnız Whisper'a ipucu
        dogru TEXT NOT NULL,
        tur TEXT NOT NULL DEFAULT 'genel' CHECK (tur IN ('takim','oyuncu','genel')),
        ekleyen TEXT,
        eklendi TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
    )
    """,
]


def main() -> int:
    from sqlalchemy import create_engine, text
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("HATA: DATABASE_URL ortam değişkeni gerekli.")
        return 1
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    engine = create_engine(url)
    with engine.begin() as conn:
        for q in DDL:
            conn.execute(text(q))
        n = conn.execute(text("SELECT count(*) FROM greydb.altyazi_isleri")).scalar()
    print(f"Tamam: capcut_projeleri, altyazi_isleri ({n} iş), altyazi_isciler, altyazi_sozluk hazır.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
