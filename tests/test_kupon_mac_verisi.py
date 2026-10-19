"""Veritabanı satırlarından bacak sonucu motorunun beklediği 'mac' sözlüğü. Saf dönüşüm: veritabanı yok."""
from datetime import datetime

from app.services.kupon_hesap import bacak_sonucu
from app.services.kupon_mac_verisi import mac_sozlugu, maci_sec

MAC = {"id": 77, "fotmob_match_id": 5904771, "home_score": 2, "away_score": 1, "finished": True,
       "ev_ic_id": 10, "ev_ad": "Galatasaray", "dep_ic_id": 20, "dep_ad": "Fenerbahçe"}
STAT = {"home_shots": 15, "away_shots": 9, "home_shots_on_target": 6, "away_shots_on_target": 3, "home_corners": 7, "away_corners": 2,
        "home_fouls": 11, "away_fouls": 13, "home_yellow_cards": 2, "away_yellow_cards": 3, "home_red_cards": 0, "away_red_cards": 0,
        "home_possession": 58, "away_possession": 42}
ILERI = {"home_offsides": 3, "away_offsides": 1}
OLAYLAR = [
    {"id": 1, "team_id": 10, "event_type": "GOAL", "minute": 20, "added_time": None, "player_name": "Mauro Icardi", "assisted_by": "Dries Mertens", "is_own_goal": False, "is_penalty": False},
    {"id": 2, "team_id": 20, "event_type": "GOAL", "minute": 55, "added_time": None, "player_name": "Edin Dzeko", "assisted_by": None, "is_own_goal": False, "is_penalty": True},
    {"id": 3, "team_id": 10, "event_type": "GOAL", "minute": 80, "added_time": None, "player_name": "Mauro Icardi", "assisted_by": None, "is_own_goal": False, "is_penalty": False},
    {"id": 4, "team_id": 20, "event_type": "YELLOW_CARD", "minute": 40, "added_time": None, "player_name": "Edin Dzeko", "assisted_by": None, "is_own_goal": False, "is_penalty": False},
]
OYUNCU = [{"team_id": 10, "player_name": "Mauro Icardi", "goals": 2, "assists": 0, "total_shots": 5, "shots_on_target": 3, "fouls_committed": 1, "fouls_won": 3, "saves": 0, "total_passes": 30, "accurate_passes": 25},
          {"team_id": 20, "player_name": "Edin Dzeko", "goals": 1, "assists": 0, "total_shots": 3, "shots_on_target": 2, "fouls_committed": 2, "fouls_won": 1, "saves": 0, "total_passes": 20, "accurate_passes": 15}]


def test_skor_ve_takim_adlari():
    m = mac_sozlugu(MAC, None, None, [], [])
    assert (m["home"], m["away"], m["home_name"], m["away_name"]) == (2, 1, "Galatasaray", "Fenerbahçe")
    assert m["stats"] == {} and m["players"] == [] and m["events"] == []


def test_istatistik_demetleri():
    m = mac_sozlugu(MAC, STAT, ILERI, [], [])
    assert m["stats"]["corners"] == (7, 2) and m["stats"]["sot"] == (6, 3) and m["stats"]["shots"] == (15, 9)
    assert m["stats"]["yellow"] == (2, 3) and m["stats"]["red"] == (0, 0) and m["stats"]["possession"] == (58, 42)
    assert m["stats"]["offsides"] == (3, 1) and m["stats"]["fouls"] == (11, 13)


def test_eksik_istatistik_anahtari_hic_konmaz():
    m = mac_sozlugu(MAC, {**STAT, "home_corners": None}, None, [], [])
    assert "corners" not in m["stats"]
    assert bacak_sonucu("Toplam Korner Alt/Üst 9.5", "Üst", m) == "veri_yok"


def test_olaylar_ve_oyuncular_motorla_calisir():
    m = mac_sozlugu(MAC, STAT, ILERI, OLAYLAR, OYUNCU)
    assert bacak_sonucu("İlk Yarı Sonucu", "1", m) == "won"                 # devre 1-0 (20. dk gol)
    assert bacak_sonucu("Oyuncu Gol Atar", "Icardi", m) == "won"
    assert bacak_sonucu("Oyuncu Kart Görür", "Edin Dzeko", m) == "won"
    assert bacak_sonucu("Penaltı Olur Mu", "Evet", m) == "won"
    assert bacak_sonucu("Takım İsabetli Şut", "Galatasaray 6+", m) == "won"
    assert bacak_sonucu("Toplam Korner Alt/Üst 8.5", "Üst", m) == "won"     # 7+2 = 9 korner


def test_maci_sec_tek_ve_belirsiz():
    aday = [{"fotmob_match_id": 1, "ev_ad": "Galatasaray", "dep_ad": "Fenerbahçe", "match_date": datetime(2026, 10, 25, 17)},
            {"fotmob_match_id": 2, "ev_ad": "Galatasaray", "dep_ad": "Fenerbahçe", "match_date": datetime(2026, 10, 25, 17)}]
    assert maci_sec([aday[0]], "Galatasaray", "Fenerbahce")["fotmob_match_id"] == 1
    assert maci_sec(aday, "Galatasaray", "Fenerbahçe") is None            # iki aday: belirsiz
    assert maci_sec([aday[0]], "Beşiktaş", "Fenerbahçe") is None
