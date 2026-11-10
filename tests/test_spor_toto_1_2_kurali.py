"""1-2 senaryosu veritabanı kuralları: CHECK kısıtları '1-2'yi kabul etmeli, taslak kaydı tek işlemde yapılmalı (yarım kayıt ve eski taslakların kaybı olmasın)."""
import importlib.util
from contextlib import contextmanager
from pathlib import Path

import pytest

from app.routers import spor_toto as r

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "spor_toto_senaryo_1_2.py"


def modul():
    spec = importlib.util.spec_from_file_location("spor_toto_senaryo_1_2", SCRIPT)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m


def test_migration_iki_kisiti_da_1_2_ile_genisletir():
    ddl = " ".join(modul().DDL)
    for tablo, kisit in (("spor_toto_entries", "spor_toto_entries_senaryo_check"), ("spor_toto_matches", "spor_toto_matches_secilen_senaryo_check")):
        assert tablo in ddl and kisit in ddl
    assert ddl.count("'1-2'") >= 2                                  # her iki CHECK'te
    for kod in ("'1'", "'0'", "'2'", "'1-0'", "'0-2'", "'1-0-2'"):  # eskileri kaybetmez
        assert ddl.count(kod) >= 2, kod
    assert "DROP CONSTRAINT IF EXISTS" in ddl                       # tekrar çalıştırılabilir


class SahteIslem:
    def __init__(self): self.olaylar = []

    @contextmanager
    def __call__(self):
        self.olaylar.append("baslat")
        try:
            yield
        except BaseException:
            self.olaylar.append("geri_al"); raise
        else:
            self.olaylar.append("onayla")


SENARYOLAR = [{"senaryo": c, "metin": "m", "onerilen": c == "1"} for c in ["1", "0", "2", "1-0", "1-2", "0-2", "1-0-2"]]


def test_taslak_kaydi_tek_islemde_ve_basarida_onaylanir(monkeypatch):
    islem, sql = SahteIslem(), []
    monkeypatch.setattr(r, "transaction", islem); monkeypatch.setattr(r, "execute_insert", lambda q, p=None: sql.append((q, p)))
    r._save_entries(7, SENARYOLAR)
    assert islem.olaylar == ["baslat", "onayla"]
    assert sql[0][0].startswith("DELETE") and len(sql) == 1 + len(SENARYOLAR)
    assert [p["senaryo"] for _, p in sql[1:]] == ["1", "0", "2", "1-0", "1-2", "0-2", "1-0-2"]


def test_taslak_kaydi_ortada_hata_verirse_islem_geri_alinir(monkeypatch):
    islem, sayac = SahteIslem(), {"n": 0}

    def ekle(q, p=None):
        sayac["n"] += 1
        if p and p.get("senaryo") == "1-2": raise RuntimeError("CHECK ihlali")

    monkeypatch.setattr(r, "transaction", islem); monkeypatch.setattr(r, "execute_insert", ekle)
    with pytest.raises(RuntimeError):
        r._save_entries(7, SENARYOLAR)
    assert islem.olaylar == ["baslat", "geri_al"]                  # gerçek transaction() burada rollback yapar: eski taslaklar silinmez
