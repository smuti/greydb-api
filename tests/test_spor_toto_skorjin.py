"""/api/spor-toto/skorjin-paketi: hafta paketi yükleme (admin) ve herkese açık okuma. Veritabanı sahte."""
import copy
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import spor_toto_skorjin as r

client = TestClient(app)
ADMIN = {"X-Sunucu-Anahtari": "gizli", "X-Yetki": "admin", "X-Kullanici-Email": "fuat@greysports.tv"}
KAPANIS = (datetime.utcnow() + timedelta(days=2)).replace(microsecond=0)


def paket(no=367):
    return {"hafta": {"no": no, "kapanis": KAPANIS.isoformat()}, "dagilim": {"banko": 9, "cift": 2, "uclu": 4},
            "maclar": [{"no": i, "ad": f"A{i}-B{i}", "tip": "banko", "secim": ["1"], "model": {"1": 0.7, "0": 0.2, "2": 0.1}} for i in range(1, 16)],
            "sistemler": [{"hedef": 15, "kolon": 324, "olasilik": 0.036}], "toto_genel": {"uyari": "kesin değildir"}}


DEPO = {}


@pytest.fixture(autouse=True)
def sahte(monkeypatch):
    DEPO.clear()
    monkeypatch.setenv("SUNUCU_ANAHTARI", "gizli")

    def yaz(donem_no, kapanis, p):
        DEPO[donem_no] = {"donem_no": donem_no, "kapanis": kapanis, "paket": p, "guncellendi": datetime(2026, 11, 12, 10, 0)}

    def oku(donem_no=None):
        if donem_no is not None:
            return DEPO.get(donem_no)
        return DEPO[max(DEPO)] if DEPO else None

    monkeypatch.setattr(r, "paket_yaz", yaz)
    monkeypatch.setattr(r, "paket_oku", oku)


def test_yazma_admin_ister():
    assert client.put("/api/spor-toto/skorjin-paketi", json=paket()).status_code == 401
    uye = {"X-Sunucu-Anahtari": "gizli", "X-Yetki": "uye"}
    assert client.put("/api/spor-toto/skorjin-paketi", json=paket(), headers=uye).status_code == 403


def test_yukle_ve_herkese_acik_oku():
    j = client.put("/api/spor-toto/skorjin-paketi", json=paket(), headers=ADMIN).json()
    assert j == {"donem_no": 367, "maclar": 15}
    g = client.get("/api/spor-toto/skorjin-paketi").json()
    assert g["donem_no"] == 367 and g["aktif"] is True and len(g["paket"]["maclar"]) == 15
    assert g["paket"]["sistemler"][0]["kolon"] == 324


def test_aktif_degil_kapanis_gectiyse_ve_donem_secimi():
    eski = paket(366)
    eski["hafta"]["kapanis"] = "2026-11-06T22:40:00"
    client.put("/api/spor-toto/skorjin-paketi", json=eski, headers=ADMIN)
    assert client.get("/api/spor-toto/skorjin-paketi").json()["aktif"] is False
    client.put("/api/spor-toto/skorjin-paketi", json=paket(367), headers=ADMIN)
    assert client.get("/api/spor-toto/skorjin-paketi").json()["donem_no"] == 367          # en yeni dönem
    assert client.get("/api/spor-toto/skorjin-paketi", params={"donem_no": 366}).json()["donem_no"] == 366


def test_paket_yoksa_404():
    assert client.get("/api/spor-toto/skorjin-paketi").status_code == 404
    assert client.get("/api/spor-toto/skorjin-paketi", params={"donem_no": 1}).status_code == 404


@pytest.mark.parametrize("yasak", ["tercih", "yorum", "fuat_tercihi", "kullanici_secimi"])
def test_tercih_ve_yorum_alanlari_reddedilir_icte_bile(yasak):
    p = paket()
    p["maclar"][3][yasak] = "1"
    assert client.put("/api/spor-toto/skorjin-paketi", json=p, headers=ADMIN).status_code == 422
    assert DEPO == {}
    p2 = paket()
    p2["toto_genel"] = {"ic": [{yasak: "x"}]}
    assert client.put("/api/spor-toto/skorjin-paketi", json=p2, headers=ADMIN).status_code == 422


def test_gecersiz_paketler():
    for bozuk in ({k: v for k, v in paket().items() if k != "maclar"},
                  {**paket(), "maclar": paket()["maclar"][:14]},
                  {**paket(), "hafta": {"kapanis": KAPANIS.isoformat()}},
                  {**paket(), "hafta": {"no": 367, "kapanis": "yarın"}}):
        assert client.put("/api/spor-toto/skorjin-paketi", json=bozuk, headers=ADMIN).status_code == 422
    dev = copy.deepcopy(paket())
    dev["toto_genel"] = {"x": "a" * 600_000}
    assert client.put("/api/spor-toto/skorjin-paketi", json=dev, headers=ADMIN).status_code == 413


def test_ayni_donem_tekrar_yuklenince_gunceller():
    client.put("/api/spor-toto/skorjin-paketi", json=paket(), headers=ADMIN)
    p = paket()
    p["dagilim"] = {"banko": 8, "cift": 3, "uclu": 4}
    client.put("/api/spor-toto/skorjin-paketi", json=p, headers=ADMIN)
    assert len(DEPO) == 1 and client.get("/api/spor-toto/skorjin-paketi").json()["paket"]["dagilim"]["banko"] == 8
