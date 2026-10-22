"""Skorjin maç öncesi anlık görüntüsü: saf dönüşümler (veritabanı, ağ yok)."""
from datetime import datetime

import pytest

from app.services.skorjin_prematch import (
    h2h_listesi, iddaa_kalabalik, iddaa_oranlari, model_verisi, takim_maclari,
)

CFG = {"1_1": {"n": "Maç Sonucu"}, "1_89": {"n": "Karşılıklı Gol"}, "1_101": {"n": "Alt/Üst {0}"}}
OLAY = {"i": 3219202, "hn": "Trabzonspor", "an": "Beşiktaş", "m": [
    {"i": 100, "t": 1, "st": 1, "o": [{"no": 1, "odd": 2.41, "n": "1"}, {"no": 2, "odd": 3.42, "n": "0"}, {"no": 3, "odd": 2.3, "n": "2"}]},
    {"i": 101, "t": 1, "st": 89, "o": [{"no": 1, "odd": 1.38, "n": "Var"}, {"no": 2, "odd": 2.6, "n": "Yok"}]},
    {"i": 102, "t": 1, "st": 101, "sov": "2.5", "o": [{"no": 1, "odd": 2.7, "n": "Alt"}, {"no": 2, "odd": 1.45, "n": "Üst"}]},
    {"i": 103, "t": 1, "st": 101, "sov": "3.5", "o": [{"no": 1, "odd": 1.5, "n": "Alt"}, {"no": 2, "odd": 2.4, "n": "Üst"}]},
]}
YUZDE = {"100": {"1": 11.0, "2": 5.0, "3": 4.0}, "102": {"1": 2.0, "2": 11.0}}


def satir(tarih, ev, dep, hs, as_, ev_ad="Ev FK", dep_ad="Dep SK"):
    return {"tarih": tarih, "home_team_id": ev, "away_team_id": dep, "home_score": hs, "away_score": as_, "ev_ad": ev_ad, "dep_ad": dep_ad}


# ---------------------------------------------------------------- form
def test_takim_maclari_takimin_penceresinden_yazar_ve_sirayi_korur():
    rows = [satir(datetime(2026, 9, 17, 17, 0), 1, 7, 2, 0, "Yıldız FK", "Rakip A"),
            satir(datetime(2026, 9, 10, 17, 0), 8, 1, 3, 1, "Rakip B", "Yıldız FK")]
    assert takim_maclari(rows, 1) == [
        {"d": "2026-09-17", "v": "H", "gf": 2, "ga": 0, "opp": "Rakip A"},
        {"d": "2026-09-10", "v": "A", "gf": 1, "ga": 3, "opp": "Rakip B"},
    ]


def test_takim_maclari_skorsuz_maci_atlar():
    rows = [satir(datetime(2026, 9, 17), 1, 7, None, None), satir(datetime(2026, 9, 10), 1, 7, 1, 1)]
    assert [m["d"] for m in takim_maclari(rows, 1)] == ["2026-09-10"]


# ---------------------------------------------------------------- h2h
def test_h2h_listesi_oynandigi_gibi_ev_dep_adlariyla():
    rows = [satir(datetime(2026, 4, 3, 18, 0), 1, 2, 2, 1, "Yıldız FK", "Kuzey SK"),
            satir(datetime(2025, 11, 9, 18, 0), 2, 1, 0, 0, "Kuzey SK", "Yıldız FK"),
            satir(datetime(2025, 5, 9), 2, 1, None, None, "Kuzey SK", "Yıldız FK")]
    assert h2h_listesi(rows) == [
        {"d": "2026-04-03", "home": "Yıldız FK", "away": "Kuzey SK", "hg": 2, "ag": 1},
        {"d": "2025-11-09", "home": "Kuzey SK", "away": "Yıldız FK", "hg": 0, "ag": 0},
    ]


# ---------------------------------------------------------------- oranlar
def test_oranlar_mac_sonucu_ve_2_5_alt_ust():
    assert iddaa_oranlari(OLAY, CFG) == {"home": 2.41, "draw": 3.42, "away": 2.3, "over25": 1.45, "under25": 2.7}


def test_oranlar_alt_ust_yoksa_bos_birakir_mac_sonucu_yoksa_none():
    sade = {"m": [OLAY["m"][0]]}
    assert iddaa_oranlari(sade, CFG) == {"home": 2.41, "draw": 3.42, "away": 2.3, "over25": None, "under25": None}
    assert iddaa_oranlari({"m": [OLAY["m"][1]]}, CFG) is None
    assert iddaa_oranlari({"m": []}, CFG) is None
    assert iddaa_oranlari(None, CFG) is None


def test_oranlar_gecersiz_oran_none():
    bozuk = {"m": [{"i": 100, "t": 1, "st": 1, "o": [{"no": 1, "odd": 0.5, "n": "1"}, {"no": 2, "odd": 3.4, "n": "0"}, {"no": 3, "odd": 2.3, "n": "2"}]}]}
    assert iddaa_oranlari(bozuk, CFG) is None


# ---------------------------------------------------------------- oynanma yüzdesi
def test_kalabalik_mac_sonucu_icinde_normalize_edilir():
    k = iddaa_kalabalik(OLAY, YUZDE, CFG)
    assert k == {"home": 0.55, "draw": 0.25, "away": 0.2}
    assert sum(k.values()) == pytest.approx(1.0)


def test_kalabalik_veri_yoksa_none():
    assert iddaa_kalabalik(OLAY, {}, CFG) is None
    assert iddaa_kalabalik(OLAY, {"100": {"1": 0.0, "2": 0.0, "3": 0.0}}, CFG) is None
    assert iddaa_kalabalik(None, YUZDE, CFG) is None


# ---------------------------------------------------------------- model
def test_model_verisi_matris_harman_ve_surum():
    sonuc = {"p1": 0.5, "p0": 0.3, "p2": 0.2, "score_matrix": [[0.5, 0.2], [0.2, 0.1]], "dc": [0.4, 0.4, 0.2]}
    assert model_verisi(sonuc, "v1") == {"matrix": [[0.5, 0.2], [0.2, 0.1]], "blend": {"home": 0.5, "draw": 0.3, "away": 0.2},
                                          "model_version": "v1"}


def test_model_verisi_matris_yoksa_none():
    assert model_verisi({"p1": 0.5, "p0": 0.3, "p2": 0.2}, "v1") is None
    assert model_verisi(None, "v1") is None
