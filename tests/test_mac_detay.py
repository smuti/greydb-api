"""Bitmiş maçın bütün verisi kendi veritabanımızdan (FotMob'a istek yok). Gerçek veritabanına dokunmaz."""
from datetime import date, datetime

from fastapi.testclient import TestClient

from app.main import app
from app.routers import match_detail as md
from app.services.mac_detay import cift_istatistik, olay_listesi, kadro_listesi

client = TestClient(app)

ANA = {"id": 77, "fotmob_match_id": 5904771, "match_date": datetime(2026, 10, 17, 13, 0), "round": 8, "finished": True, "cancelled": False,
       "home_score": 0, "away_score": 1, "lig_id": 71, "lig_ad": "Super Lig", "lig_ulke": "TUR",
       "ev_ic_id": 10, "ev_id": 7800, "ev_ad": "Gençlerbirliği", "dep_ic_id": 20, "dep_id": 8637, "dep_ad": "Galatasaray"}


def kur(monkeypatch, ana=ANA, planli=None, **tablolar):
    sorgular = []

    def execute_query(sql, params=None):
        sorgular.append((sql, params))
        if "public.upcoming_matches" in sql:
            return [planli] if planli else []
        for ad in ("match_advanced_stats", "match_stats", "match_events", "match_lineups", "match_formations", "player_availability", "match_player_stats"):
            if f"public.{ad}" in sql:
                return tablolar.get(ad, [])
        return [ana] if ana else []

    monkeypatch.setattr(md, "execute_query", execute_query)
    return sorgular


# ---- Saf yardımcılar ----
def test_cift_istatistik_home_away_ciftlerini_birlestirir():
    s = cift_istatistik({"match_id": 1, "created_at": "x", "home_xg": 1.2, "away_xg": 0.4, "home_shots": 10, "away_shots": 3})
    assert s == {"xg": {"home": 1.2, "away": 0.4}, "shots": {"home": 10, "away": 3}}


def test_cift_istatistik_iki_tarafi_da_bos_olani_atar_tek_tarafi_bos_olani_korur():
    s = cift_istatistik({"home_a": None, "away_a": None, "home_b": 5, "away_b": None})
    assert "a" not in s and s["b"] == {"home": 5, "away": None}


def test_olaylar_dakikaya_gore_siralanir_ve_takim_tarafi_bulunur():
    satirlar = [
        {"id": 2, "team_id": 20, "event_type": "Goal", "minute": 74, "added_time": None, "player_name": "Icardi", "assisted_by": "Torreira", "is_own_goal": False, "is_penalty": False, "player_in": None, "player_out": None},
        {"id": 1, "team_id": 10, "event_type": "Card", "minute": 30, "added_time": 2, "player_name": "X", "assisted_by": None, "is_own_goal": False, "is_penalty": False, "player_in": None, "player_out": None},
    ]
    o = olay_listesi(satirlar, ev_ic_id=10, dep_ic_id=20)
    assert [x["dakika"] for x in o] == [30, 74]
    assert o[0]["takim"] == "home" and o[1]["takim"] == "away"
    assert o[1]["oyuncu"] == "Icardi" and o[1]["asist"] == "Torreira" and o[0]["ek_dakika"] == 2


def test_kadro_ilk_11_ve_yedekler_ayri_dizilis_eklenir():
    satirlar = [
        {"team_id": 10, "player_name": "A", "shirt_number": 1, "position": "GK", "is_starter": True, "age": 30, "rating": 6.5},
        {"team_id": 10, "player_name": "B", "shirt_number": 12, "position": "GK", "is_starter": False, "age": 21, "rating": None},
        {"team_id": 20, "player_name": "C", "shirt_number": 9, "position": "FW", "is_starter": True, "age": 28, "rating": 7.9},
    ]
    k = kadro_listesi(satirlar, ev_ic_id=10, dep_ic_id=20, dizilis={"home_formation": "4-4-2", "away_formation": "4-2-3-1"})
    assert [p["ad"] for p in k["home"]["ilk11"]] == ["A"] and [p["ad"] for p in k["home"]["yedekler"]] == ["B"]
    assert k["home"]["dizilis"] == "4-4-2" and k["away"]["dizilis"] == "4-2-3-1"
    assert k["away"]["ilk11"][0]["numara"] == 9 and k["away"]["ilk11"][0]["puan"] == 7.9


def test_kadro_dizilis_yoksa_none():
    k = kadro_listesi([], ev_ic_id=1, dep_ic_id=2, dizilis=None)
    assert k["home"]["dizilis"] is None and k["home"]["ilk11"] == []


# ---- Uç ----
def test_uc_maci_ve_tum_bolumleri_doner(monkeypatch):
    kur(monkeypatch,
        match_stats=[{"match_id": 77, "home_xg": 0.5, "away_xg": 1.8, "home_shots": 6, "away_shots": 14, "created_at": "x"}],
        match_events=[{"id": 1, "team_id": 20, "event_type": "Goal", "minute": 74, "added_time": None, "player_name": "Icardi", "assisted_by": None, "is_own_goal": False, "is_penalty": False, "player_in": None, "player_out": None}],
        match_lineups=[{"team_id": 10, "player_name": "A", "shirt_number": 1, "position": "GK", "is_starter": True, "age": 30, "rating": 6.0}],
        match_formations=[{"home_formation": "4-4-2", "away_formation": "4-2-3-1"}],
        player_availability=[{"team_id": 10, "player_name": "Z", "status": "injured", "reason": "Diz", "expected_return": date(2026, 11, 1)}],
        match_player_stats=[{"team_id": 20, "player_name": "Icardi", "rating": 8.1, "minutes_played": 90, "goals": 1, "assists": 0, "xg": 0.9, "total_shots": 4},
                            {"team_id": 10, "player_name": "A", "rating": 6.0, "minutes_played": 90, "goals": 0, "assists": 0, "xg": 0.0, "total_shots": 0}])
    r = client.get("/api/match-detail/5904771")
    j = r.json()
    assert r.status_code == 200
    assert j["mac"]["home"] == {"fotmob_team_id": 7800, "ad": "Gençlerbirliği", "skor": 0}
    assert j["mac"]["away"] == {"fotmob_team_id": 8637, "ad": "Galatasaray", "skor": 1}
    assert j["mac"]["lig"] == {"fotmob_league_id": 71, "ad": "Super Lig", "ulke": "TUR"} and j["mac"]["tur"] == "8"
    assert j["mac"]["tarih"] == "2026-10-17T13:00:00Z" and j["mac"]["bitti"] is True
    assert j["istatistik"]["xg"] == {"home": 0.5, "away": 1.8}
    assert j["olaylar"][0]["takim"] == "away" and j["kadro"]["home"]["dizilis"] == "4-4-2"
    assert j["sakat_cezali"] == [{"takim": "home", "oyuncu": "Z", "durum": "injured", "neden": "Diz", "donus": "2026-11-01"}]
    assert [p["oyuncu"] for p in j["oyuncu_istatistik"]] == ["Icardi", "A"]         # puana göre azalan
    assert j["veri"] == {"istatistik": True, "olaylar": True, "kadro": True}


def test_uc_veri_bayraklari_bos_tablolari_yansitir(monkeypatch):
    kur(monkeypatch)
    j = client.get("/api/match-detail/5904771").json()
    assert j["veri"] == {"istatistik": False, "olaylar": False, "kadro": False}
    assert j["istatistik"] is None and j["olaylar"] == [] and j["sakat_cezali"] == []


def test_uc_veritabaninda_olmayan_mac_404(monkeypatch):
    kur(monkeypatch, ana=None)
    assert client.get("/api/match-detail/1").status_code == 404


def test_uc_sorgular_parametreli(monkeypatch):
    sorgular = kur(monkeypatch)
    client.get("/api/match-detail/5904771")
    assert all("5904771" not in s and "77" not in s for s, _ in sorgular)       # f-string ile gömülmez
    assert sorgular[0][1] == {"fid": 5904771}
    iddaa = [x for x in sorgular if "iddaa_event_id" in x[0]]
    assert len(iddaa) == 1 and iddaa[0][1] == {"fid": 5904771}                    # iddaa kimliği de parametreli
    assert all(p == {"id": 77} for s, p in sorgular[1:] if "iddaa_event_id" not in s)


def test_uc_ham_json_ve_buyuk_alanlari_donmez(monkeypatch):
    kur(monkeypatch)
    j = client.get("/api/match-detail/5904771").json()
    assert "raw_match_details" not in str(j)


def test_kendi_kalesine_gol_rakibin_skoruna_yazilir():
    """Gerçek maçta (Gençlerbirliği-Galatasaray) tek gol ev sahibi kalecinin kendi kalesine golü: takım=home ama skor_takimi=away."""
    satirlar = [
        {"id": 1, "team_id": 10, "event_type": "GOAL", "minute": 88, "player_name": "Kaleci", "is_own_goal": True, "is_penalty": False},
        {"id": 2, "team_id": 20, "event_type": "GOAL", "minute": 90, "player_name": "Forvet", "is_own_goal": False, "is_penalty": False},
        {"id": 3, "team_id": 10, "event_type": "YELLOW_CARD", "minute": 10, "player_name": "Y", "is_own_goal": False, "is_penalty": False},
    ]
    o = olay_listesi(satirlar, ev_ic_id=10, dep_ic_id=20)
    kart, kendi, normal = o[0], o[1], o[2]
    assert kendi["takim"] == "home" and kendi["skor_takimi"] == "away"
    assert normal["takim"] == "away" and normal["skor_takimi"] == "away"
    assert kart["skor_takimi"] is None               # gol olmayan olayda yok


# ---- Henüz oynanmamış maç: matches'ta yoksa upcoming_matches'tan (FotMob'a gitmeden maç önü sayfası) ----
PLANLI = {"fotmob_match_id": 5904780, "match_date": datetime(2026, 10, 25, 17, 0), "round": 9, "lig_id": 71, "lig_ad": "Super Lig", "lig_ulke": "TUR",
          "ev_ad": "Galatasaray", "ev_id": 8637, "dep_ad": "Fenerbahçe", "dep_id": 8695}


def test_uc_matches_te_yoksa_planli_maci_doner(monkeypatch):
    kur(monkeypatch, ana=None, planli=PLANLI)
    r = client.get("/api/match-detail/5904780")
    j = r.json()
    assert r.status_code == 200 and j["kaynak"] == "planli"
    assert j["mac"]["home"] == {"fotmob_team_id": 8637, "ad": "Galatasaray", "skor": None}
    assert j["mac"]["away"] == {"fotmob_team_id": 8695, "ad": "Fenerbahçe", "skor": None}
    assert j["mac"]["tarih"] == "2026-10-25T17:00:00Z" and j["mac"]["tur"] == "9" and j["mac"]["bitti"] is False and j["mac"]["iptal"] is False
    assert j["mac"]["lig"] == {"fotmob_league_id": 71, "ad": "Super Lig", "ulke": "TUR"}
    assert j["istatistik"] is None and j["olaylar"] == [] and j["veri"] == {"istatistik": False, "olaylar": False, "kadro": False}


def test_uc_ne_matches_ne_planli_varsa_404(monkeypatch):
    kur(monkeypatch, ana=None, planli=None)
    assert client.get("/api/match-detail/1").status_code == 404


def test_uc_matches_te_varsa_kaynak_mac_ve_planliya_bakilmaz(monkeypatch):
    sorgular = kur(monkeypatch, planli=PLANLI)
    j = client.get("/api/match-detail/5904771").json()
    assert j["kaynak"] == "mac"
    assert not any("upcoming_matches" in s for s, _ in sorgular)


def test_uc_planli_sorgusu_parametreli(monkeypatch):
    sorgular = kur(monkeypatch, ana=None, planli=PLANLI)
    client.get("/api/match-detail/5904780")
    s, p = [x for x in sorgular if "upcoming_matches" in x[0]][0]
    assert p == {"fid": 5904780} and "5904780" not in s


def test_uc_iddaa_event_id_baglandiysa_doner_degilse_none(monkeypatch):
    kur(monkeypatch, ana={**ANA, "iddaa_event_id": 3320665})
    assert client.get("/api/match-detail/5904771").json()["mac"]["iddaa_event_id"] == 3320665
    kur(monkeypatch)
    assert client.get("/api/match-detail/5904771").json()["mac"]["iddaa_event_id"] is None


def test_uc_planli_macta_da_iddaa_event_id_doner(monkeypatch):
    kur(monkeypatch, ana=None, planli={**PLANLI, "iddaa_event_id": 3320626})
    assert client.get("/api/match-detail/5904780").json()["mac"]["iddaa_event_id"] == 3320626
