"""Gol karnesi: skor matrisinden pazar olayları, puanlama ve sapma uyarıları (saf fonksiyonlar; veritabanı/ağ yok)."""
import math

import pytest

from app.services import gol_karne as g


def matris(kutle):
    """11x11 matris; kutle = {(ev, dep): olasılık}."""
    m = [[0.0] * 11 for _ in range(11)]
    for (i, j), p in kutle.items():
        m[i][j] = p
    return m


def olaylar(m, hg, ag):
    return {o["pazar"]: o for o in g.pazar_olaylari(m, hg, ag)}


# ---------------------------------------------------------------- pazar olayları
def test_tek_skor_matrisi_iki_sifir_icin_olasiliklar():
    o = olaylar(matris({(2, 0): 1.0}), 2, 0)
    assert o["toplam_ust_1.5"]["p"] == 1.0 and o["toplam_ust_2.5"]["p"] == 0.0
    assert o["kg_var"]["p"] == 0.0 and o["ms_1"]["p"] == 1.0 and o["ms_0"]["p"] == 0.0
    assert o["ev_ust_1.5"]["p"] == 1.0 and o["dep_ust_0.5"]["p"] == 0.0


def test_olasilik_matristen_toplanir():
    o = olaylar(matris({(0, 0): 0.5, (1, 1): 0.3, (3, 0): 0.2}), 0, 0)
    assert o["toplam_ust_0.5"]["p"] == pytest.approx(0.5)
    assert o["toplam_ust_2.5"]["p"] == pytest.approx(0.2)
    assert o["kg_var"]["p"] == pytest.approx(0.3)
    assert o["ms_0"]["p"] == pytest.approx(0.8) and o["ms_1"]["p"] == pytest.approx(0.2)
    assert o["ev_ust_2.5"]["p"] == pytest.approx(0.2) and o["dep_ust_0.5"]["p"] == pytest.approx(0.3)


def test_gerceklesen_sonuc_olay_basina_0_1_yazilir():
    o = olaylar(matris({(1, 1): 1.0}), 3, 0)         # gerçek skor 3-0, tahmin matrisinden bağımsız
    assert o["toplam_ust_2.5"]["y"] == 1 and o["toplam_ust_3.5"]["y"] == 0
    assert o["kg_var"]["y"] == 0 and o["ev_ust_2.5"]["y"] == 1 and o["dep_ust_0.5"]["y"] == 0
    assert (o["ms_1"]["y"], o["ms_0"]["y"], o["ms_2"]["y"]) == (1, 0, 0)
    assert (o["cs_10"]["y"], o["cs_02"]["y"], o["cs_12"]["y"]) == (1, 0, 1)


def test_cifte_sans_ms_toplamlarindan_turer():
    o = olaylar(matris({(2, 1): 0.4, (1, 1): 0.25, (0, 2): 0.35}), 1, 1)
    assert o["cs_10"]["p"] == pytest.approx(o["ms_1"]["p"] + o["ms_0"]["p"])
    assert o["cs_02"]["p"] == pytest.approx(o["ms_0"]["p"] + o["ms_2"]["p"])
    assert o["cs_12"]["p"] == pytest.approx(o["ms_1"]["p"] + o["ms_2"]["p"])


def test_pazar_kumesi_sabit_ve_tekrarsiz():
    adlar = [o["pazar"] for o in g.pazar_olaylari(matris({(0, 0): 1.0}), 0, 0)]
    assert len(adlar) == len(set(adlar)) == 5 + 1 + 3 + 3 + 3 + 3          # toplam 0,5..4,5 · KG · ev 0,5..2,5 · dep 0,5..2,5 · MS · ÇŞ
    assert {"toplam_ust_0.5", "toplam_ust_4.5", "kg_var", "ev_ust_2.5", "dep_ust_2.5", "ms_2", "cs_12"} <= set(adlar)


def test_gecersiz_matris_reddedilir():
    with pytest.raises(ValueError):
        g.pazar_olaylari([[1.0]], 0, 0)                                      # 11x11 değil
    with pytest.raises(ValueError):
        g.pazar_olaylari(matris({(0, 0): 0.8}), 0, 0)                        # toplam 1 değil
    with pytest.raises(ValueError):
        g.pazar_olaylari(matris({(0, 0): 1.0}), None, 0)                     # skor yok


# ---------------------------------------------------------------- puanlama
def satir(p, y, pazar="toplam_ust_2.5", surum="m2", kaynak="canli"):
    return {"pazar": pazar, "p": p, "y": y, "model_version": surum, "kaynak": kaynak}


def test_puanla_brier_logloss_ve_z_elle_hesapla():
    t = g.puanla([satir(0.8, 1), satir(0.6, 0)])
    assert len(t) == 1
    r = t[0]
    assert r["n"] == 2 and r["ort_p"] == pytest.approx(0.7) and r["gercek"] == pytest.approx(0.5)
    assert r["brier"] == pytest.approx(0.2)
    assert r["logloss"] == pytest.approx(-(math.log(0.8) + math.log(0.4)) / 2)
    se = math.sqrt(0.8 * 0.2 + 0.6 * 0.4) / 2
    assert r["z"] == pytest.approx((0.5 - 0.7) / se)


def test_puanla_gruplari_pazar_surum_ve_kaynaga_gore_ayirir():
    t = g.puanla([satir(.7, 1), satir(.7, 1, kaynak="geri_test"), satir(.7, 0, surum="m1"), satir(.7, 1, pazar="kg_var")])
    anahtar = {(r["pazar"], r["model_version"], r["kaynak"]): r["n"] for r in t}
    assert anahtar == {("toplam_ust_2.5", "m2", "canli"): 1, ("toplam_ust_2.5", "m2", "geri_test"): 1,
                       ("toplam_ust_2.5", "m1", "canli"): 1, ("kg_var", "m2", "canli"): 1}


def test_puanla_uc_olasiliklarda_sonlu_kalir():
    r = g.puanla([satir(1.0, 0), satir(0.0, 1)])[0]
    assert math.isfinite(r["logloss"]) and math.isfinite(r["z"])


def test_puanla_bos_girdi_bos_liste():
    assert g.puanla([]) == []


# ---------------------------------------------------------------- uyarılar
def test_uyari_az_ornekte_susar_cok_ornekte_buyuk_sapmada_konusur():
    az = g.puanla([satir(0.9, 0)] * 10)                                       # büyük sapma ama n=10
    assert g.uyarilar(az, min_n=40, esik_z=2.0) == []
    cok = g.puanla([satir(0.9, 0)] * 25 + [satir(0.9, 1)] * 25)              # n=50, gerçek %50 / bizim %90
    u = g.uyarilar(cok, min_n=40, esik_z=2.0)
    assert len(u) == 1 and u[0]["pazar"] == "toplam_ust_2.5" and u[0]["z"] < -2 and u[0]["yon"] == "fazla_iddiali"


def test_uyari_temkinli_yonu_isaretler():
    t = g.puanla([satir(0.5, 1)] * 45 + [satir(0.5, 0)] * 5)                 # bizim %50 / gerçek %90
    assert g.uyarilar(t, min_n=40, esik_z=2.0)[0]["yon"] == "fazla_temkinli"
