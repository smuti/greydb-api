"""Canlı gol olasılığı izleme: kayıt doğrulama, gerçekleşen (10 dk penceresi ve maç sonu), sonuçlandırma ve kalibrasyon özeti. DB ve ağa dokunmaz."""
from datetime import datetime, timedelta

import pytest

from app.services import canli_gol_izleme as c

O = {"on": 0.2, "kalan": 0.6}


def govde(**ek):
    g = {"fotmob_match_id": 123, "lig_ad": "Süper Lig", "ev": "A", "dep": "B", "model_surum": "canli-gol-1.0", "faz": "ilk", "dakika": 31,
         "skor": {"ev": 1, "dep": 0}, "kirmizi": {"ev": 0, "dep": 0}, "golBeklentisi": {"ev": 1.6, "dep": 1.1},
         "olasilik": {"ev": O, "dep": {"on": 0.1, "kalan": 0.4}, "herhangi": {"on": 0.28, "kalan": 0.76}}}
    g.update(ek)
    return g


def test_kayit_hazirla_kova_ve_alanlar():
    k = c.kayit_hazirla(govde())
    assert k["kova"] == 6 and k["dakika"] == 31 and k["faz"] == "ilk"
    assert k["olasilik"]["dep"] == {"on": 0.1, "kalan": 0.4}
    assert k["onceki"] == {"ev": 1.6, "dep": 1.1}


@pytest.mark.parametrize("ek", [
    {"olasilik": {"ev": {"on": 1.2, "kalan": 0.5}, "dep": O, "herhangi": O}},
    {"olasilik": {"ev": O, "dep": O}},
    {"olasilik": {"ev": {"on": 0.1}, "dep": O, "herhangi": O}},
    {"faz": "uzatma"}, {"dakika": -1}, {"model_surum": ""}, {"skor": {"ev": 1}},
])
def test_kayit_hazirla_gecersiz(ek):
    with pytest.raises(ValueError):
        c.kayit_hazirla(govde(**ek))


def test_gerceklesen_pencere_ve_mac_sonu():
    sonuc = {"skor": {"ev": 2, "dep": 1}, "goller": [{"team": "home", "minute": 20}, {"team": "away", "minute": 38}, {"team": "home", "minute": 70}]}
    g = c.gerceklesen({"dakika": 31, "skor": {"ev": 1, "dep": 0}}, sonuc)
    assert g == {"ev": {"on": 0, "kalan": 1}, "dep": {"on": 1, "kalan": 1}, "herhangi": {"on": 1, "kalan": 1}}
    g = c.gerceklesen({"dakika": 70, "skor": {"ev": 2, "dep": 1}}, sonuc)                # 70'teki gol kayıttan önce sayılır (skora girmiş)
    assert g == {"ev": {"on": 0, "kalan": 0}, "dep": {"on": 0, "kalan": 0}, "herhangi": {"on": 0, "kalan": 0}}


def test_gerceklesen_uzatma_ve_eksik_gol_listesi():
    sonuc = {"skor": {"ev": 1, "dep": 1}, "goller": [{"team": "home", "minute": 10}, {"team": "away", "minute": 90}]}
    assert c.gerceklesen({"dakika": 93, "skor": {"ev": 1, "dep": 0}}, sonuc)["dep"] == {"on": 1, "kalan": 1}   # 90+ uzatmada pencere = kalan süre
    eksik = {"skor": {"ev": 2, "dep": 0}, "goller": [{"team": "home", "minute": 10}]}
    g = c.gerceklesen({"dakika": 31, "skor": {"ev": 1, "dep": 0}}, eksik)
    assert g["ev"] == {"on": None, "kalan": 1} and g["herhangi"]["on"] is None                              # gol listesi skorla uyuşmazsa pencere yazılmaz


def _kaynak(satirlar, veri, yazilan):
    return c.Kaynaklar(bekleyen=lambda son: [s for s in satirlar if s["kaydedildi"] < son], mac_sonucu=lambda fid: veri.get(fid),
                       sonuc_yaz=lambda id_, g, an: yazilan.append((id_, g)))


def test_sonuclandir_bitmis_bitmemis_iptal_kuru():
    simdi = datetime(2026, 11, 20, 12)
    eski = simdi - timedelta(hours=3)
    satirlar = [{"id": 1, "fotmob_match_id": 10, "dakika": 31, "skor": {"ev": 0, "dep": 0}, "kaydedildi": eski},
                {"id": 2, "fotmob_match_id": 11, "dakika": 50, "skor": {"ev": 0, "dep": 0}, "kaydedildi": eski},
                {"id": 3, "fotmob_match_id": 12, "dakika": 10, "skor": {"ev": 0, "dep": 0}, "kaydedildi": eski}]
    veri = {10: {"finished": True, "cancelled": False, "skor": {"ev": 0, "dep": 1}, "goller": [{"team": "away", "minute": 35}]},
            11: {"finished": False, "cancelled": False}, 12: {"finished": False, "cancelled": True}}
    yazilan = []
    r = c.sonuclandir_bekleyenler(simdi, _kaynak(satirlar, veri, yazilan), dry_run=True)
    assert r["islenen"] == 1 and r["bitmemis"] == 1 and r["iptal"] == 1 and yazilan == []
    c.sonuclandir_bekleyenler(simdi, _kaynak(satirlar, veri, yazilan), dry_run=False)
    assert yazilan[0] == (1, {"ev": {"on": 0, "kalan": 0}, "dep": {"on": 1, "kalan": 1}, "herhangi": {"on": 1, "kalan": 1}})
    assert (3, None) in yazilan and all(i != 2 for i, _ in yazilan)


def test_mac_sonucu_canli_yanittan():
    d = {"match": {"status": "finished", "score": {"home": 2, "away": 0}},
         "events": [{"type": "Goal", "team": "home", "minute": 12}, {"type": "Goal", "team": "home", "minute": 80}, {"type": "Card", "team": "away", "minute": 30}]}
    assert c.sonuc_cozumle(d) == {"finished": True, "cancelled": False, "skor": {"ev": 2, "dep": 0},
                                  "goller": [{"team": "home", "minute": 12}, {"team": "home", "minute": 80}]}


def test_ozet_kalibrasyon_ve_dilim():
    s = lambda p, on, faz="ilk", dk=20, fid=1: {"fotmob_match_id": fid, "faz": faz, "dakika": dk,
                                                  "olasilik": {"ev": {"on": p, "kalan": 0.5}, "dep": O, "herhangi": O},
                                                  "gerceklesen": {"ev": {"on": on, "kalan": 1}, "dep": {"on": 0, "kalan": 0}, "herhangi": {"on": None, "kalan": 1}}}
    o = c.ozet([s(0.25, 1), s(0.15, 0, fid=2), s(0.35, 0, "ikinci", 70, 3)])
    h = o["hedefler"]["ev_on"]
    assert o["maclar"] == 3 and h["n"] == 3 and h["ort_tahmin"] == 25.0 and h["gercek"] == 33.3
    assert [b["bant"] for b in h["bantlar"]] == ["10-20", "20-30", "30-40"]
    assert [d["dilim"] for d in h["dilimler"]] == ["15-30", "60-75"]
    assert "herhangi_on" not in o["hedefler"]                          # gerçekleşeni yazılmayan pencere sayılmaz
    assert o["hedefler"]["ev_kalan"]["gercek"] == 100.0
