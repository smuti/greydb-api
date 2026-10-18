"""Takip edilen ligler: Maçlar sayfasının hangi ligleri hangi sırayla göstereceği tek tabloda durur (greydb.tracked_leagues).
Gerçek veritabanına dokunmaz."""
import re

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import tracked_leagues as tl
from app.services.tracked_leagues_seed import SEED

client = TestClient(app)


def satir(**k):
    d = {"anahtar": "super-lig", "ad": "Süper Lig", "oncelik": 1, "aktif": True,
         "fotmob_ids": [71], "ulke": None, "ad_kalibi": None, "logo_id": None}
    d.update(k)
    return d


def test_uc_sadece_aktif_ligleri_oncelik_sirasiyla_ister(monkeypatch):
    sorgular = []

    def sahte(sql, params=None):
        sorgular.append(sql)
        return [satir()]

    monkeypatch.setattr(tl, "execute_query", sahte)
    client.get("/api/tracked-leagues")
    sql = sorgular[0].lower()
    assert "greydb.tracked_leagues" in sql and "aktif" in sql and "order by oncelik" in sql


def test_uc_ligleri_beklenen_bicimde_dondurur(monkeypatch):
    monkeypatch.setattr(tl, "execute_query", lambda sql, params=None: [
        satir(),
        satir(anahtar="avrupa-ligi", ad="Avrupa Ligi", oncelik=3, fotmob_ids=None, ulke="INT", ad_kalibi="^(uefa )?europa league$"),
    ])
    r = client.get("/api/tracked-leagues")
    assert r.status_code == 200
    ligler = r.json()["leagues"]
    assert ligler[0] == {"anahtar": "super-lig", "ad": "Süper Lig", "oncelik": 1, "fotmob_ids": [71], "ulke": None, "ad_kalibi": None, "logo_id": None}
    assert ligler[1]["fotmob_ids"] == []          # null liste yerine boş liste
    assert ligler[1]["ad_kalibi"] == "^(uefa )?europa league$"


def test_uc_tablo_yoksa_bos_liste_ve_503_degil_200_doner(monkeypatch):
    """Tablo henüz oluşturulmadıysa site çökmesin: boş liste döner, frontend yerleşik varsayılana düşer."""
    def patla(sql, params=None):
        raise RuntimeError('relation "greydb.tracked_leagues" does not exist')

    monkeypatch.setattr(tl, "execute_query", patla)
    r = client.get("/api/tracked-leagues")
    assert r.status_code == 200 and r.json() == {"leagues": []}


def test_yeni_uc_eski_lig_ucunu_golgelemez(monkeypatch):
    from app.routers import leagues as eski
    import pandas as pd

    monkeypatch.setattr(tl, "execute_query", lambda sql, params=None: [])
    monkeypatch.setattr(eski, "query_to_df", lambda sql, params=None: pd.DataFrame([{"id": 71, "name": "Super Lig", "country": "Turkey"}]))
    assert client.get("/api/tracked-leagues").json() == {"leagues": []}
    assert client.get("/api/leagues/71").json()["name"] == "Super Lig"


# ---- Başlangıç verisi ----
def test_baslangic_verisi_benzersiz_anahtar_ve_oncelik():
    assert len({s["anahtar"] for s in SEED}) == len(SEED)
    assert len({s["oncelik"] for s in SEED}) == len(SEED)


def test_baslangic_verisi_her_ligin_eslestirme_yolu_var():
    for s in SEED:
        assert s.get("fotmob_ids") or (s.get("ulke") and s.get("ad_kalibi")), s["anahtar"]


def test_baslangic_verisindeki_kaliplar_gecerli_regex():
    for s in SEED:
        if s.get("ad_kalibi"):
            re.compile(s["ad_kalibi"], re.IGNORECASE)


def test_baslangic_verisi_kararlari_icerir():
    adlar = {s["ad"] for s in SEED}
    assert {"Süper Lig", "TFF 1. Lig", "Konferans Ligi", "Türkiye Kupası", "Süper Kupa", "Milli maçlar"} <= adlar
    assert not any("Rusya" in s["ad"] or "Bolivya" in s["ad"] for s in SEED)


def test_baslangic_verisi_beklenen_maclari_eslestirir():
    def tutar(s, ulke, ad, lig_id=0):
        return lig_id in (s.get("fotmob_ids") or []) or (
            s.get("ulke") == ulke and s.get("ad_kalibi") and re.search(s["ad_kalibi"], ad, re.IGNORECASE))

    def bul(ulke, ad, lig_id=0):
        return [s["ad"] for s in SEED if tutar(s, ulke, ad, lig_id)]

    assert bul("TUR", "Super Lig", 71) == ["Süper Lig"]
    assert bul("INT", "Europa League", 943229) == ["Avrupa Ligi"]
    assert bul("TUR", "1. Lig") == ["TFF 1. Lig"]
    assert bul("SUI", "Cup", 164) == []
    assert bul("INT", "CAF Champions League Qualification") == []


def test_baslangic_verisi_gercek_fotmob_adlari_ve_kulup_hazirlik_maclari():
    def bul(ulke, ad, lig_id=0):
        return [s["ad"] for s in SEED if lig_id in (s.get("fotmob_ids") or []) or (
            s.get("ulke") == ulke and s.get("ad_kalibi") and re.search(s["ad_kalibi"], ad, re.IGNORECASE))]

    assert bul("INT", "Friendlies", 917358) == ["Milli maçlar"]
    assert bul("INT", "Club Friendlies") == []
    assert bul("INT", "Champions League", 943230) == ["Şampiyonlar Ligi"]


# ---- Lig logosu: kalıcı FotMob lig kimliği; site logoyu kendi sunucusundan sunar ----
def test_uc_logo_id_doner(monkeypatch):
    monkeypatch.setattr(tl, "execute_query", lambda sql, params=None: [satir(logo_id=71), satir(anahtar="x", ad="X", oncelik=2, logo_id=None)])
    ligler = client.get("/api/tracked-leagues").json()["leagues"]
    assert ligler[0]["logo_id"] == 71 and ligler[1]["logo_id"] is None


def test_uc_logo_id_sutununu_sorgular(monkeypatch):
    sorgular = []
    monkeypatch.setattr(tl, "execute_query", lambda sql, params=None: sorgular.append(sql) or [])
    client.get("/api/tracked-leagues")
    assert "logo_id" in sorgular[0]


def test_baslangic_verisinde_her_ligin_logo_kimligi_var():
    assert all(isinstance(s.get("logo_id"), int) and s["logo_id"] > 0 for s in SEED), [s["anahtar"] for s in SEED if not s.get("logo_id")]
    beklenen = {"super-lig": 71, "sampiyonlar-ligi": 42, "avrupa-ligi": 73, "konferans-ligi": 10216, "premier-lig": 47,
                "turkiye-kupasi": 151, "super-kupa": 166, "tff-1-lig": 165, "milli-maclar": 114}
    mevcut = {s["anahtar"]: s["logo_id"] for s in SEED}
    assert all(mevcut[k] == v for k, v in beklenen.items())


def test_migration_logo_sutununu_ekler_ve_eski_satirlari_doldurur():
    """Tablo logo_id'siz kurulmuş olabilir: betik sütunu ekler ve boş olanları başlangıç verisinden doldurur (elle girilene dokunmaz)."""
    import importlib.util, pathlib
    yol = pathlib.Path(__file__).resolve().parent.parent / "scripts" / "create_tracked_leagues_table.py"
    kaynak = yol.read_text()
    assert "ADD COLUMN IF NOT EXISTS logo_id" in kaynak
    assert "logo_id IS NULL" in kaynak
