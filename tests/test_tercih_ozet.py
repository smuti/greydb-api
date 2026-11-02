"""Tercih izleme birikimli raporu: SQL özet satırlarından tutma oranı ve 50 TL'lik getiri tabloları. Veritabanı yok."""
from fastapi.testclient import TestClient

from app.main import app
from app.routers import tercih_izleme as r
from app.services import tercih_izleme as t

client = TestClient(app)
ANAHTAR = {"X-Cron-Anahtari": "gizli"}


def s(pencere="kapanis", kategori="Maç sonucu", bant=6, deger=False, n=10, tuttu=6, oranli=10, oranli_tuttu=6, getiri=9.0, olasilik_toplam=None):
    """SQL özet satırı: getiri = oranlı ve tutan tercihlerin oranlarının toplamı (her birine 50 TL)."""
    return dict(pencere=pencere, kategori=kategori, bant=bant, deger=deger, n=n, tuttu=tuttu, oranli=oranli, oranli_tuttu=oranli_tuttu, getiri_oran_toplam=getiri, olasilik_toplam=olasilik_toplam)


def test_ozet_tum_satirlar_tutma_ve_getiri():
    o = t.ozet([s(n=10, tuttu=6, oranli=10, oranli_tuttu=6, getiri=9.0), s(kategori="Toplam gol", n=4, tuttu=1, oranli=2, oranli_tuttu=1, getiri=1.6)])
    tum = o["tum"]
    assert (tum["tercih"], tum["tuttu"], tum["tutma_yuzde"]) == (14, 7, 50.0)
    assert (tum["oranli"], tum["yatirilan"], tum["donen"], tum["sonuc"]) == (12, 600.0, 530.0, -70.0)     # 12 × 50; (9.0 + 1.6) × 50


def test_ozet_deger_kurali_yalniz_p_oran_bir_ustu():
    o = t.ozet([s(deger=False, n=10, tuttu=5, oranli=10, oranli_tuttu=5, getiri=7.0), s(deger=True, n=4, tuttu=3, oranli=4, oranli_tuttu=3, getiri=5.1)])
    dk = o["deger_kurali"]
    assert (dk["tercih"], dk["tuttu"], dk["oranli"], dk["yatirilan"], dk["donen"], dk["sonuc"]) == (4, 3, 4, 200.0, 255.0, 55.0)


def test_ozet_pencere_ve_bant_kirilimi():
    o = t.ozet([s(pencere="acilis", bant=5, n=2, tuttu=1, oranli=2, oranli_tuttu=1, getiri=1.5), s(pencere="kapanis", bant=7, n=3, tuttu=3, oranli=3, oranli_tuttu=3, getiri=3.6)])
    assert o["pencere"]["acilis"]["tercih"] == 2 and o["pencere"]["kapanis"]["tutma_yuzde"] == 100.0
    assert [b["bant"] for b in o["bant"]] == ["50-60", "70-80"]
    assert o["bant"][1]["tuttu"] == 3 and o["bant"][1]["ort_olasilik"] is None        # olasılık toplamı yoksa ortalama bilinmez


def test_ozet_pencere_ici_deger_kurali():
    o = t.ozet([s(pencere="acilis", deger=True, n=2, tuttu=2, oranli=2, oranli_tuttu=2, getiri=3.0), s(pencere="acilis", deger=False, n=5, tuttu=1, oranli=5, oranli_tuttu=1, getiri=1.2)])
    assert o["pencere"]["acilis"]["deger_kurali"]["tercih"] == 2 and o["pencere"]["acilis"]["tercih"] == 7


def test_ozet_kategori_kirilimi_ve_bos_veri():
    o = t.ozet([s(kategori="Toplam gol", n=3, tuttu=2, oranli=3, oranli_tuttu=2, getiri=3.0), s(kategori="Maç sonucu", n=5, tuttu=3, oranli=5, oranli_tuttu=3, getiri=4.0)])
    assert [k["kategori"] for k in o["kategori"]] == ["Maç sonucu", "Toplam gol"]            # tercih sayısına göre azalan
    bos = t.ozet([])
    assert bos["tum"]["tercih"] == 0 and bos["tum"]["tutma_yuzde"] is None and bos["tum"]["sonuc"] == 0.0 and bos["bant"] == []


def test_ozet_sql_yalniz_sonuclanmis_ve_oran_bir_ustu():
    assert "sonuc IS NOT NULL" in t.OZET_SQL and "oran > 1.0" in t.OZET_SQL and "olasilik * oran > 1" in t.OZET_SQL
    assert "kickoff >= :bas" in t.OZET_SQL and "GROUP BY" in t.OZET_SQL


def kur(monkeypatch, anahtar="gizli"):
    monkeypatch.setattr(r, "_anahtar", lambda: anahtar)
    monkeypatch.setattr(r.t, "ozet_getir", lambda gun: {"gun": gun, "tum": {"tercih": 0}})


def test_uc_korumali_ve_gun_parametresi(monkeypatch):
    kur(monkeypatch)
    assert client.get("/api/skorjin/tercih-izleme/ozet").status_code == 401
    assert client.get("/api/skorjin/tercih-izleme/ozet", headers=ANAHTAR).json()["gun"] == 30
    assert client.get("/api/skorjin/tercih-izleme/ozet?gun=7", headers=ANAHTAR).json()["gun"] == 7
    assert client.get("/api/skorjin/tercih-izleme/ozet?gun=0", headers=ANAHTAR).status_code == 422
    kur(monkeypatch, anahtar=None)
    assert client.get("/api/skorjin/tercih-izleme/ozet", headers=ANAHTAR).status_code == 503


def test_ozet_bantta_bizim_ortalama_olasilik():
    o = t.ozet([s(bant=7, n=4, tuttu=3, oranli=4, oranli_tuttu=3, getiri=3.6, olasilik_toplam=2.96)])
    assert o["bant"][0]["ort_olasilik"] == 74.0 and o["bant"][0]["tutma_yuzde"] == 75.0           # "%74 dedik, %75 tuttu"


def test_ozet_yuzde_bir_olan_bant_doksan_yuz():
    assert t.ozet([s(bant=10, n=1, tuttu=1, oranli=1, oranli_tuttu=1, getiri=1.1)])["bant"][0]["bant"] == "90-100"
