"""Maç öncesi bağlam: iki takımın son formu ve aralarındaki karşılaşma geçmişi, GÜNCEL tablodan (public.matches).
Eski greydb.vw_* görünümleri Aralık 2025'te donmuş olduğu için kullanılmaz. Gerçek veritabanına dokunmaz."""
import re
from datetime import datetime

from fastapi.testclient import TestClient

from app.main import app
from app.routers import match_context as mc
from app.services.mac_baglam import form_ozeti, h2h_ozeti

client = TestClient(app)


def satir(tarih, ev_id, dep_id, ev_gol, dep_gol, ev="A", dep="B", lig="Super Lig"):
    return {"tarih": tarih, "home_team_id": ev_id, "away_team_id": dep_id, "home_score": ev_gol, "away_score": dep_gol, "ev_ad": ev, "dep_ad": dep, "lig_ad": lig}


def test_form_sonuc_takim_penceresinden_G_B_M():
    s = [satir(datetime(2026, 10, 1), 1, 2, 2, 0, "A", "B"),       # A evinde kazandı
         satir(datetime(2026, 9, 24), 3, 1, 1, 1, "C", "A"),       # A deplasmanda berabere
         satir(datetime(2026, 9, 17), 1, 4, 0, 3, "A", "D")]       # A evinde kaybetti
    f = form_ozeti(s, takim_ic_id=1)
    assert [m["sonuc"] for m in f["maclar"]] == ["G", "B", "M"]
    assert [m["rakip"] for m in f["maclar"]] == ["B", "C", "D"]
    assert [m["ev_mi"] for m in f["maclar"]] == [True, False, True]
    assert (f["maclar"][1]["atilan"], f["maclar"][1]["yenilen"]) == (1, 1)
    assert f["ozet"] == {"galibiyet": 1, "beraberlik": 1, "maglubiyet": 1, "atilan": 3, "yenilen": 4, "dizi": "GBM"}


def test_form_bos_liste_bos_ozet():
    f = form_ozeti([], takim_ic_id=1)
    assert f["maclar"] == [] and f["ozet"]["dizi"] == "" and f["ozet"]["galibiyet"] == 0


def test_form_skoru_olmayan_maci_saymaz():
    assert form_ozeti([satir(datetime(2026, 10, 1), 1, 2, None, None)], takim_ic_id=1)["maclar"] == []


def test_h2h_galibiyetler_ev_deplasman_fark_etmeden_takima_yazilir():
    s = [satir(datetime(2026, 5, 1), 1, 2, 2, 1, "A", "B"),        # A kazandı
         satir(datetime(2025, 12, 1), 2, 1, 1, 1, "B", "A"),       # berabere
         satir(datetime(2025, 5, 1), 2, 1, 3, 0, "B", "A")]        # B kazandı (evinde)
    h = h2h_ozeti(s, ev_ic_id=1, dep_ic_id=2)
    assert h["ozet"] == {"mac_sayisi": 3, "home_galibiyet": 1, "beraberlik": 1, "away_galibiyet": 1, "home_gol": 3, "away_gol": 5}
    assert h["maclar"][0] == {"tarih": "2026-05-01T00:00:00Z", "ev": "A", "dep": "B", "ev_gol": 2, "dep_gol": 1, "lig": "Super Lig"}


def test_h2h_bos():
    assert h2h_ozeti([], 1, 2)["ozet"]["mac_sayisi"] == 0


# ---- Uç ----
TEMEL = {"tarih": datetime(2026, 10, 25, 17, 0), "ev_ic_id": 10, "dep_ic_id": 20}


def kur(monkeypatch, temel=TEMEL, form_ev=None, form_dep=None, h2h=None):
    sorgular = []

    def execute_query(sql, params=None):
        sorgular.append((sql, params))
        if "HEAD_FORM" in sql:
            return (form_ev if params["t"] == 10 else form_dep) or []
        if "HEAD_H2H" in sql:
            return h2h or []
        return [temel] if temel else []

    monkeypatch.setattr(mc, "execute_query", execute_query)
    return sorgular


def test_uc_form_ve_h2h_doner(monkeypatch):
    kur(monkeypatch,
        form_ev=[satir(datetime(2026, 10, 18), 10, 99, 1, 0, "Ev", "X")],
        form_dep=[satir(datetime(2026, 10, 17), 98, 20, 2, 2, "Y", "Dep")],
        h2h=[satir(datetime(2026, 3, 1), 10, 20, 0, 1, "Ev", "Dep")])
    r = client.get("/api/match-context/5904780")
    j = r.json()
    assert r.status_code == 200
    assert j["form"]["home"]["ozet"]["dizi"] == "G" and j["form"]["away"]["ozet"]["dizi"] == "B"
    assert j["h2h"]["ozet"]["away_galibiyet"] == 1 and j["h2h"]["ozet"]["mac_sayisi"] == 1


def test_uc_yalnizca_maci_oncesi_biten_maclari_ister_ve_parametrelidir(monkeypatch):
    sorgular = kur(monkeypatch)
    client.get("/api/match-context/5904780")
    for sql, p in [x for x in sorgular if "HEAD_" in x[0]]:
        low = sql.lower()
        assert "m.finished" in low and "cancelled" in low and "m.match_date < :tarih" in low
        assert p["tarih"] == TEMEL["tarih"]
        sade = re.sub(r"LIMIT \d+", "", sql)
        assert "5904780" not in sql and not re.search(r"\b(10|20)\b", sade)      # kimlikler f-string ile gömülmez, parametreli


def test_uc_mac_yoksa_404(monkeypatch):
    kur(monkeypatch, temel=None)
    assert client.get("/api/match-context/1").status_code == 404


def test_uc_takim_kimligi_bilinmiyorsa_bos_baglam_doner_cokmez(monkeypatch):
    kur(monkeypatch, temel={"tarih": datetime(2026, 10, 25), "ev_ic_id": None, "dep_ic_id": None})
    j = client.get("/api/match-context/1").json()
    assert j["form"]["home"]["maclar"] == [] and j["h2h"]["maclar"] == []
