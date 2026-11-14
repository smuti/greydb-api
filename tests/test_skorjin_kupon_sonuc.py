"""Skorjin kuponlarını maç bitince sonuçlandıran iş: erken/maç yok/veri yok bacağa yazmaz; bitmiş maçta won/lost yazar; var olan sonuç korunur."""
from datetime import datetime

from app.services import skorjin_kupon_sonuc as S

SIMDI = datetime(2026, 11, 15, 12, 0)


def bacak(mac_id, ad, market, secim, zaman="2026-11-14T22:45:00Z", **ek):
    return {"macId": mac_id, "ev": ad[0], "dep": ad[1], "lig": None, "macZamani": zaman, "marketId": 1, "marketAd": market, "secim": secim, "oran": 1.3, "bizimP": 0.8, **ek}


def kupon(kid, *bacaklar):
    return {"id": str(kid), "durum": "yayinda", "bacaklar": list(bacaklar)}


def bul_tablo(maclar):
    return lambda b: maclar.get(b["iddaa_event_id"])


def test_biten_macta_won_ve_lost_yazar_ve_kuponu_dondurur():
    maclar = {1: {"home": 2, "away": 1, "home_name": "Portekiz", "away_name": "Danimarka"}, 2: {"home": 3, "away": 0, "home_name": "Romanya", "away_name": "Polonya"}}
    k = kupon(7, bacak(1, ("Portekiz", "Danimarka"), "Maç Sonucu", "1"), bacak(2, ("Romanya", "Polonya"), "Ev Sahibi Alt/Üst 1.5", "Alt"))
    degisen, rapor = S.sonuclandir([k], bul_tablo(maclar), lambda m: m, SIMDI)
    assert [b["sonuc"] for b in degisen[7]] == ["won", "lost"]
    assert degisen[7][0]["sonucKaynak"] == "otomatik" and rapor["sonuclanan"] == 2
    assert "sonuc" not in k["bacaklar"][0]                                              # girdi değişmez


def test_maci_yok_ya_da_erken_olan_bacak_yazilmaz():
    k = kupon(8, bacak(3, ("A", "B"), "Maç Sonucu", "1"), bacak(4, ("C", "D"), "Maç Sonucu", "1", zaman="2026-11-15T11:00:00Z"))
    degisen, rapor = S.sonuclandir([k], bul_tablo({}), lambda m: m, SIMDI)
    assert degisen == {} and rapor["mac_yok"] == 1 and rapor["erken"] == 1


def test_sonucu_olan_bacak_korunur_ve_taninmayan_pazar_beklemede_kalir():
    maclar = {5: {"home": 1, "away": 0, "home_name": "A", "away_name": "B"}}
    k = kupon(9, bacak(5, ("A", "B"), "Maç Sonucu", "2", sonuc="won", sonucKaynak="elle"), bacak(5, ("A", "B"), "Uydurma Pazar XYZ", "Evet"))
    degisen, rapor = S.sonuclandir([k], bul_tablo(maclar), lambda m: m, SIMDI)
    assert degisen == {} and rapor["desteklenmiyor"] + rapor["veri_yok"] == 1
