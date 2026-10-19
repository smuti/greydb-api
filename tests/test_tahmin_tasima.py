"""Eski tahmin satırını yeni kupon modeline dönüştürme (saf). Veritabanı yok."""
from datetime import datetime

from app.services.tahmin_tasima import tahmin_kuponu

SATIR = {"id": 12, "home_team": "Galatasaray", "away_team": "Fenerbahçe", "league": "Süper Lig", "match_date": datetime(2026, 10, 25, 17, 0),
         "match_fotmob_id": 3219202, "market_name": "Maç Sonucu ve Alt/Üst 1.5", "pick": "1 ve Üst", "odds": 1.8, "content": "Kısa yorum", "status": "active",
         "result": "won", "show_on_homepage": True, "radar_type": "acik", "signal_strength": "guclu", "risk_level": "orta", "reasons": ["Form iyi", "Rakip eksik"],
         "main_risk": "Erken gol", "created_by_email": "editor@x", "created_at": datetime(2026, 10, 20, 9, 0), "updated_at": datetime(2026, 10, 25, 20, 0)}


def test_temel_alan_esleme():
    k = tahmin_kuponu(SATIR)
    assert k["seviye"] == "acik" and k["durum"] == "yayinda" and k["sistem_k"] is None and k["kaynak"] == "tahmin:12"
    assert k["nedenler"] == ["Form iyi", "Rakip eksik"] and k["ana_risk"] == "Erken gol" and k["yorum"] == "Kısa yorum"
    assert k["sinyal"] == "guclu" and k["risk_seviyesi"] == "orta" and k["olusturan_email"] == "editor@x"
    assert k["toplam_oran"] == 1.8 and k["one_cikan"] is False       # tümünde varsayılan 'ana sayfada göster' doğruydu: anlamsız, öne çıkan sayılmaz


def test_bacak_eslemesi_iddaa_kimligi_yanlis_adli_alandan():
    b = tahmin_kuponu(SATIR)["bacaklar"][0]
    assert b["iddaa_event_id"] == 3219202 and b["fotmob_match_id"] is None            # match_fotmob_id aslında iddaa kimliği
    assert (b["ev"], b["dep"], b["lig"], b["bahis_turu"], b["secim"], b["oran"]) == ("Galatasaray", "Fenerbahçe", "Süper Lig", "Maç Sonucu ve Alt/Üst 1.5", "1 ve Üst", 1.8)
    assert b["mac_zamani"] == "2026-10-25T17:00:00Z" and b["sonuc"] == "won"


def test_durum_ve_sonuc_esleme():
    assert tahmin_kuponu({**SATIR, "status": "draft"})["durum"] == "taslak"
    assert tahmin_kuponu({**SATIR, "status": "archived"})["durum"] == "arsiv"
    assert tahmin_kuponu({**SATIR, "status": "problem"})["durum"] == "taslak"
    for girdi, beklenen in ((None, "pending"), ("pending", "pending"), ("lost", "lost"), ("void", "void")):
        assert tahmin_kuponu({**SATIR, "result": girdi})["bacaklar"][0]["sonuc"] == beklenen


def test_eksik_alanlar_cokmez():
    k = tahmin_kuponu({**SATIR, "reasons": None, "main_risk": None, "content": None, "signal_strength": None, "match_fotmob_id": None})
    assert k["nedenler"] == [] and k["ana_risk"] is None and k["bacaklar"][0]["iddaa_event_id"] is None
