"""Gol modeli geri testi: günlük as_of ile geçmiş maçları yeniden tahmin eder (sızıntısız), geliştirme/sınav ayrımı. Model enjekte edilir; DB/ağ yok."""
from datetime import date, datetime

import pytest

from app.services import gol_geri_test as t


def m11():
    m = [[0.0] * 11 for _ in range(11)]
    m[1][0], m[1][1] = 0.6, 0.4
    return m


def sonuc(**ek):
    return {"p1": 0.6, "p0": 0.4, "p2": 0.0, "lam_h": 1.4, "lam_a": 0.9, "score_matrix": m11(), "model_version": "harman-v1-2026-10-m2",
            "as_of": "x", "new_team_home": False, "new_team_away": False, **ek}


def mac(fid, gun, saat=19, ev=1, dep=2, lig=8, hs=2, as_=1):
    return {"fotmob_match_id": fid, "home_team_id": ev, "away_team_id": dep, "home_name": "A", "away_name": "B", "league_id": lig,
            "match_date": datetime(2026, 10, gun, saat, 0), "home_score": hs, "away_score": as_}


class SahteModel:
    def __init__(self, yeni=()):
        self.kurulumlar, self.yeni = [], set(yeni)

    def kur(self, as_of):
        self.kurulumlar.append(as_of)
        return lambda ev, dep, lig: sonuc(new_team_home=ev in self.yeni)


# ---------------------------------------------------------------- pencereler
def test_gun_pencereleri_kapsayici_ve_gunluk():
    assert t.gun_pencereleri(date(2026, 10, 1), date(2026, 10, 3)) == [date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3)]
    assert t.gun_pencereleri(date(2026, 10, 5), date(2026, 10, 5)) == [date(2026, 10, 5)]
    assert t.gun_pencereleri(date(2026, 10, 6), date(2026, 10, 5)) == []


# ---------------------------------------------------------------- üretim
def test_her_mac_gunu_icin_tek_model_kurulur_as_of_gun_basidir():
    s = SahteModel()
    satirlar, ozet = t.uret([mac(1, 25), mac(2, 25, saat=22), mac(3, 26)], s.kur)
    assert s.kurulumlar == [datetime(2026, 10, 25), datetime(2026, 10, 26)]
    assert len(satirlar) == 3 and ozet["gun"] == 2 and ozet["mac"] == 3


def test_sizinti_yok_model_as_of_hicbir_mac_kickoffundan_sonra_degil():
    s = SahteModel()
    maclar = [mac(1, 25, saat=0), mac(2, 25, saat=23), mac(3, 27)]
    t.uret(maclar, s.kur)
    for m in maclar:
        assert datetime.combine(m["match_date"].date(), datetime.min.time()) in s.kurulumlar
    assert all(a <= min(m["match_date"] for m in maclar if m["match_date"].date() == a.date()) for a in s.kurulumlar)


def test_satir_alanlari_geri_test_kaynagi_ve_gercek_skor_icerir():
    satirlar, _ = t.uret([mac(7, 25, hs=3, as_=0)], SahteModel().kur)
    s = satirlar[0]
    assert s["kaynak"] == "geri_test" and s["fotmob_match_id"] == 7 and s["model_version"] == "harman-v1-2026-10-m2"
    assert s["home_score"] == 3 and s["away_score"] == 0 and s["kickoff"] == datetime(2026, 10, 25, 19, 0)
    assert s["matrix"] == m11()


def test_yeni_takim_ve_matrissiz_maclar_atlanir_ve_nedeni_sayilir():
    s = SahteModel(yeni=[9])
    satirlar, ozet = t.uret([mac(1, 25), mac(2, 25, ev=9), mac(3, 25, lig=99)], s.kur)
    assert [x["fotmob_match_id"] for x in satirlar] == [1]
    assert ozet["atlanan"]["yeni_takim"] == 1 and ozet["atlanan"]["kapsam_disi"] == 1
    assert ozet["mac"] == 1


def test_model_kurulumu_basarisiz_olursa_o_gun_atlanir_is_devam_eder():
    cagri = []

    def kur(as_of):
        cagri.append(as_of)
        if as_of.day == 25:
            raise RuntimeError("fit")
        return lambda ev, dep, lig: sonuc()
    satirlar, ozet = t.uret([mac(1, 25), mac(2, 26)], kur)
    assert [x["fotmob_match_id"] for x in satirlar] == [2] and ozet["hata_gun"] == 1


# ---------------------------------------------------------------- sınav ayrımı ve rapor
def test_sinav_ayir_kickoffa_gore_boler():
    satirlar, _ = t.uret([mac(1, 10), mac(2, 20), mac(3, 28)], SahteModel().kur)
    gelistirme, sinav = t.sinav_ayir(satirlar, datetime(2026, 10, 20))
    assert [x["fotmob_match_id"] for x in gelistirme] == [1] and [x["fotmob_match_id"] for x in sinav] == [2, 3]


def test_bolumlu_rapor_gelistirme_ve_sinavi_ayri_puanlar():
    satirlar, _ = t.uret([mac(1, 10), mac(2, 20), mac(3, 28)], SahteModel().kur)
    r = t.bolumlu_rapor(satirlar, datetime(2026, 10, 20))
    assert r["gelistirme"]["mac"] == 1 and r["sinav"]["mac"] == 2
    assert {x["kaynak"] for x in r["sinav"]["tablo"]} == {"geri_test"}


# ---------------------------------------------------------------- JSONL
def test_jsonl_gidis_donus_tarihleri_korur():
    satirlar, _ = t.uret([mac(1, 25)], SahteModel().kur)
    geri = [t.satir_oku(t.satir_json(s)) for s in satirlar]
    assert geri == satirlar and isinstance(geri[0]["kickoff"], datetime)
