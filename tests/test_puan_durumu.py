"""Puan durumu kendi veritabanımızdaki bitmiş maçlardan hesaplanır (FotMob'a istek yok). Gerçek veritabanına dokunmaz."""
import zlib
from datetime import date, datetime

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import standings as st
from app.services.puan_durumu import hesapla, sezon_araligi, guncel_sezon

client = TestClient(app)


def mac(ev, ev_gol, dep_gol, dep, gun=1, ev_id=None, dep_id=None):
    return {"tarih": datetime(2026, 8, gun), "ev_ad": ev, "dep_ad": dep, "ev_gol": ev_gol, "dep_gol": dep_gol,
            "ev_id": ev_id or zlib.crc32(ev.encode()) % 100000, "dep_id": dep_id or zlib.crc32(dep.encode()) % 100000}


def test_galibiyet_3_beraberlik_1_maglubiyet_0_puan():
    t = hesapla([mac("A", 2, 0, "B"), mac("C", 1, 1, "A", gun=2)])
    a = next(r for r in t if r["takim"] == "A")
    assert (a["oynanan"], a["galibiyet"], a["beraberlik"], a["maglubiyet"], a["puan"]) == (2, 1, 1, 0, 4)
    b = next(r for r in t if r["takim"] == "B")
    assert (b["maglubiyet"], b["puan"], b["atilan"], b["yenilen"], b["averaj"]) == (1, 0, 0, 2, -2)


def test_siralama_puan_sonra_averaj_sonra_atilan_gol_sonra_ad():
    # X ve Y 3'er puan; averaj eşit (+1); X daha çok gol atmış
    t = hesapla([mac("X", 3, 2, "P"), mac("Y", 1, 0, "Q"), mac("Z", 0, 0, "R"), mac("S", 0, 0, "T")])
    adlar = [r["takim"] for r in t]
    assert adlar.index("X") < adlar.index("Y")          # atılan gol
    assert adlar.index("Y") < adlar.index("Z")          # 3 puan > 1 puan
    # tam eşitlikte alfabetik (deterministik)
    assert [r["takim"] for r in hesapla([mac("B", 0, 0, "A")])] == ["A", "B"]


def test_sira_numaralari_1den_baslar():
    t = hesapla([mac("A", 1, 0, "B"), mac("C", 2, 0, "D")])
    assert [r["sira"] for r in t] == [1, 2, 3, 4]


def test_skoru_olmayan_mac_sayilmaz():
    assert hesapla([mac("A", None, None, "B")]) == []


def test_form_son_5_maci_eskiden_yeniye_G_B_M():
    maclar = [mac("A", 1, 0, "B", 1), mac("A", 0, 0, "C", 2), mac("A", 0, 3, "D", 3), mac("A", 2, 1, "E", 4),
              mac("A", 1, 0, "F", 5), mac("A", 4, 0, "G", 6), mac("A", 0, 1, "H", 7)]
    a = next(r for r in hesapla(maclar) if r["takim"] == "A")
    assert a["form"] == ["M", "G", "G", "G", "M"]       # son 5: 3. gün … 7. gün
    assert a["oynanan"] == 7


def test_takim_kimligi_ve_adi_tasinir():
    t = hesapla([mac("Galatasaray", 1, 0, "Fenerbahçe", ev_id=8637, dep_id=8695)])
    assert t[0]["takim_id"] == 8637 and t[0]["takim"] == "Galatasaray"


def test_deplasman_ve_ic_saha_ayni_takimda_birlesir():
    t = hesapla([mac("A", 1, 0, "B", 1, ev_id=1, dep_id=2), mac("B", 2, 0, "A", 2, ev_id=2, dep_id=1)])
    assert len(t) == 2 and all(r["oynanan"] == 2 for r in t)


# ---- Sezon ----
def test_sezon_araligi_temmuz_temmuza():
    assert sezon_araligi(2026) == (datetime(2026, 7, 1), datetime(2027, 7, 1))


def test_guncel_sezon_temmuzdan_itibaren_yeni_sezon():
    assert guncel_sezon(date(2026, 10, 18)) == 2026
    assert guncel_sezon(date(2027, 3, 1)) == 2026
    assert guncel_sezon(date(2026, 6, 30)) == 2025
    assert guncel_sezon(date(2026, 7, 1)) == 2026


# ---- Uç ----
def kur(monkeypatch, maclar, lig=(71, "Super Lig")):
    kayit = []

    def execute_query(sql, params=None):
        kayit.append((sql, params))
        if "FROM public.leagues" in sql and "public.matches" not in sql:
            return [{"fotmob_league_id": lig[0], "name": lig[1]}] if lig else []
        return maclar

    monkeypatch.setattr(st, "execute_query", execute_query)
    monkeypatch.setattr(st, "bugun", lambda: date(2026, 10, 18))
    return kayit


def test_uc_puan_durumunu_hesaplayip_doner(monkeypatch):
    kur(monkeypatch, [mac("A", 2, 0, "B", ev_id=1, dep_id=2)])
    r = client.get("/api/standings/71?season=2026")
    j = r.json()
    assert r.status_code == 200
    assert j["lig"] == {"fotmob_league_id": 71, "ad": "Super Lig"} and j["sezon"] == "2026/2027"
    assert [x["takim"] for x in j["tablo"]] == ["A", "B"] and j["tablo"][0]["puan"] == 3
    assert j["bitmis_mac_sayisi"] == 1
    assert "kendi" in j["hesap"]["kaynak"].lower()
    assert [x["takim"] for x in j["ic_saha"]] == ["A"] and [x["takim"] for x in j["dis_saha"]] == ["B"]   # A yalnız evinde, B yalnız deplasmanda oynadı


def test_uc_sezon_verilmezse_guncel_sezon_kullanilir(monkeypatch):
    kayit = kur(monkeypatch, [])
    assert client.get("/api/standings/71").json()["sezon"] == "2026/2027"
    params = [p for s, p in kayit if "public.matches" in s][0]
    assert params["bas"] == datetime(2026, 7, 1) and params["bit"] == datetime(2027, 7, 1)


def test_uc_sorgu_parametreli_ve_yalnizca_biten_iptal_olmayan(monkeypatch):
    kayit = kur(monkeypatch, [])
    client.get("/api/standings/71?season=2026")
    sql = [s for s, p in kayit if "public.matches" in s][0]
    assert ":lig" in sql and ":bas" in sql and ":bit" in sql
    assert "m.finished" in sql and "cancelled" in sql
    assert "71" not in sql   # f-string ile gömülmez


def test_uc_lig_yoksa_404(monkeypatch):
    kur(monkeypatch, [], lig=None)
    assert client.get("/api/standings/999999?season=2026").status_code == 404


def test_uc_bitmis_mac_yoksa_bos_tablo(monkeypatch):
    kur(monkeypatch, [])
    j = client.get("/api/standings/71?season=2026").json()
    assert j["tablo"] == [] and j["bitmis_mac_sayisi"] == 0


def test_uc_gecersiz_sezon_422(monkeypatch):
    kur(monkeypatch, [])
    assert client.get("/api/standings/71?season=1999").status_code == 422
    assert client.get("/api/standings/71?season=abc").status_code == 422


def test_hesap_notu_canli_maclarin_sayilmadigini_soyler(monkeypatch):
    """Gerçek veriyle doğrulandı: FotMob tablosu canlı skoru da katar, bizimki yalnızca bitmiş maçları sayar; bu açıkça yazılı olmalı."""
    kur(monkeypatch, [])
    ne = client.get("/api/standings/71?season=2026").json()["hesap"]["not"].lower()
    assert "canlı" in ne or "oynanmakta" in ne


def test_ic_ve_dis_saha_yalniz_o_sahadaki_maclari_sayar():
    maclar = [mac("A", 2, 0, "B"), mac("B", 3, 1, "A", gun=2), mac("A", 1, 1, "C", gun=3)]
    ic = {r["takim"]: r for r in hesapla(maclar, "ev")}
    dis = {r["takim"]: r for r in hesapla(maclar, "dep")}
    assert (ic["A"]["oynanan"], ic["A"]["galibiyet"], ic["A"]["beraberlik"], ic["A"]["puan"]) == (2, 1, 1, 4)   # A'nın evindeki 2 maç
    assert "C" not in ic                                                                                       # C hiç ev sahibi olmadı
    assert (dis["A"]["oynanan"], dis["A"]["maglubiyet"], dis["A"]["puan"]) == (1, 1, 0)                         # A'nın tek deplasmanı: B'ye 1-3
    assert (dis["C"]["oynanan"], dis["C"]["beraberlik"], dis["C"]["puan"]) == (1, 1, 1)
    assert sum(r["oynanan"] for r in ic.values()) == len(maclar) == sum(r["oynanan"] for r in dis.values())   # her maç iki tabloda da bir kez
    assert [r["sira"] for r in hesapla(maclar, "ev")] == list(range(1, len(ic) + 1))
