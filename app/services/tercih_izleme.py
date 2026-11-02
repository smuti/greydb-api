"""Tercih izleme: matchforge günlük listesindeki her tercihi (olasılık + o andaki iddaa oranı) maçtan önce kaydeder, maç bitince sonucu yazar.

Kayıtlar eklemelidir (hiçbiri ezilmez): bir maç için kickoff'a ~24, ~6 ve ~1 saat kala olmak üzere en çok üç pencere yazılır; ilki "açılış", sonuncusu "kapanış" oranıdır.
Yalnızca iç analiz içindir; site ve postlarda gösterilmez.
"""

# (pencere adı, kickoff'a kalan saat): bir maç, kalan süre bu değerin altına indiğinde o pencerenin kaydını bir kez alır
PENCERELER = (("acilis", 24.0), ("orta", 6.0), ("kapanis", 1.0))

TABLO_DDL = """
CREATE TABLE IF NOT EXISTS greydb.tercih_izleme (
    id BIGSERIAL PRIMARY KEY,
    fotmob_match_id BIGINT NOT NULL,
    iddaa_match_id BIGINT,
    kickoff TIMESTAMPTZ NOT NULL,
    lig_ad TEXT,
    ev TEXT NOT NULL,
    dep TEXT NOT NULL,
    pencere TEXT NOT NULL,
    kategori TEXT NOT NULL,
    secim TEXT NOT NULL,
    olasilik DOUBLE PRECISION NOT NULL,
    oran DOUBLE PRECISION,
    guvenilirlik TEXT,
    model_surum TEXT,
    kaydedildi TIMESTAMPTZ NOT NULL DEFAULT now(),
    sonuc BOOLEAN,
    sonuclandi TIMESTAMPTZ,
    UNIQUE (fotmob_match_id, pencere, kategori, secim)
);
CREATE INDEX IF NOT EXISTS idx_tercih_izleme_bekleyen
    ON greydb.tercih_izleme (kickoff)
    WHERE sonuc IS NULL;
CREATE INDEX IF NOT EXISTS idx_tercih_izleme_mac
    ON greydb.tercih_izleme (fotmob_match_id, pencere);
"""
