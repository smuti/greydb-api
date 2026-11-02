"""Tercih izleme tablosu: eklemeli kayıt (her pencerede yeni satır), aynı pencere/tercih ikinci kez yazılmaz, sonuç sonradan doldurulur."""
import re

from app.services.tercih_izleme import PENCERELER, TABLO_DDL


def test_ddl_tablo_ve_idempotent():
    assert "CREATE TABLE IF NOT EXISTS greydb.tercih_izleme" in TABLO_DDL
    assert "DROP" not in TABLO_DDL.upper() and "DELETE" not in TABLO_DDL.upper() and "TRUNCATE" not in TABLO_DDL.upper()
    assert all("IF NOT EXISTS" in s for s in re.findall(r"CREATE (?:UNIQUE )?INDEX[^;]*", TABLO_DDL))


def test_ddl_sutunlar():
    for s in ("fotmob_match_id BIGINT NOT NULL", "iddaa_match_id BIGINT", "kickoff TIMESTAMPTZ NOT NULL", "pencere TEXT NOT NULL", "kategori TEXT NOT NULL",
              "secim TEXT NOT NULL", "olasilik DOUBLE PRECISION NOT NULL", "oran DOUBLE PRECISION", "kaydedildi TIMESTAMPTZ NOT NULL DEFAULT now()",
              "sonuc BOOLEAN", "sonuclandi TIMESTAMPTZ"):
        assert s in TABLO_DDL, s


def test_ddl_ayni_pencere_ikinci_kez_yazilmaz():
    assert re.search(r"UNIQUE[^;]*\(fotmob_match_id, pencere, kategori, secim\)", TABLO_DDL)


def test_ddl_sonuclanmamislar_icin_kismi_indeks():
    assert re.search(r"CREATE INDEX IF NOT EXISTS \w+\s+ON greydb\.tercih_izleme \(kickoff\)\s+WHERE sonuc IS NULL", TABLO_DDL)


def test_pencereler_kickoffa_kalan_saat():
    assert PENCERELER == (("acilis", 24.0), ("orta", 6.0), ("kapanis", 1.0))
