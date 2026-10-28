"""Günlük koşuya karne kancası: matris yazılırken karne kaydı da düşer; karne hatası ana satırı bozmaz; model_yok nedenleri ayrılır."""
from dataclasses import replace
from datetime import datetime

from app.services.skorjin_prematch import hazirla
from tests.test_skorjin_prematch_hazirla import Sahte, SIMDI, mac


def m11():
    m = [[0.0] * 11 for _ in range(11)]
    m[1][0], m[1][1] = 0.6, 0.4
    return m


def model(**ek):
    return {1001: {"p1": 0.6, "p0": 0.4, "p2": 0.0, "lam_h": 1.4, "lam_a": 0.9, "score_matrix": m11(), "model_version": "harman-v1-2026-10-m2",
                   "as_of": "2026-10-22", "new_team_home": False, "new_team_away": False, **ek}}


def kos(sahte, kapsam="gunluk", dry_run=False, karne=None):
    k = replace(sahte.kaynaklar(), karne=karne)
    return hazirla(kapsam, dry_run, k, SIMDI, "harman-v1")


def test_yazarken_karne_kaydi_dusulur():
    kayitlar = []
    sahte = Sahte(model=model())
    rapor = kos(sahte, karne=kayitlar.append)
    assert len(kayitlar) == 1 and rapor["karne_kaydi"] == 1 and rapor["hazirlanan"] == 1
    assert kayitlar[0]["fotmob_match_id"] == 1001 and kayitlar[0]["model_version"] == "harman-v1-2026-10-m2" and kayitlar[0]["kaynak"] == "canli"


def test_kuru_calismada_karne_yazilmaz_ama_sayilir():
    kayitlar = []
    rapor = kos(Sahte(model=model()), dry_run=True, karne=kayitlar.append)
    assert kayitlar == [] and rapor["karne_kaydi"] == 1


def test_oran_kapsaminda_karne_dokunulmaz():
    kayitlar = []
    rapor = kos(Sahte(mevcut=[1001], model=model()), kapsam="oran", karne=kayitlar.append)
    assert kayitlar == [] and rapor.get("karne_kaydi", 0) == 0


def test_karne_hatasi_ana_satiri_bozmaz_ve_raporlanir():
    def patla(_):
        raise RuntimeError("db")
    sahte = Sahte(model=model())
    rapor = kos(sahte, karne=patla)
    assert rapor["hazirlanan"] == 1 and len(sahte.yazilan) == 1
    assert rapor["karne_kaydi"] == 0 and any("karne" in h["hata"] for h in rapor["hatalar"])


def test_karne_kaynagi_yoksa_eskisi_gibi_calisir():
    rapor = kos(Sahte(model=model()))
    assert rapor["hazirlanan"] == 1 and rapor["karne_kaydi"] == 0


def test_model_yok_nedenleri_ayrilir():
    kapsam_disi = kos(Sahte(maclar=[mac(lig=99)], model={}))
    assert kapsam_disi["model_yok"] == 1 and kapsam_disi["model_yok_ayrinti"] == {"kapsam_disi": 1, "yeni_takim": 0, "matris_yok": 0}
    yeni = kos(Sahte(model=model(new_team_home=True)))
    assert yeni["model_yok_ayrinti"] == {"kapsam_disi": 0, "yeni_takim": 1, "matris_yok": 0}
    bos = kos(Sahte(model={1001: {"p1": 0.5, "p0": 0.3, "p2": 0.2, "new_team_home": False, "new_team_away": False}}))
    assert bos["model_yok_ayrinti"] == {"kapsam_disi": 0, "yeni_takim": 0, "matris_yok": 1}
