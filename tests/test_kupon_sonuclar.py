"""Sonuçlar istatistiği ve otomatik sonuçlandırma. Saf fonksiyonlar ve sahte veri."""
from datetime import datetime

import pytest

from app.services.kupon_sonuclandir import sonuclandir
from app.services.kupon_istatistik import sonuc_istatistik

SIMDI = datetime(2026, 10, 20, 12, 0)


def k(sonuc, odeme, oran, zaman, sistem_k=None, nbacak=1, seviye="acik", zamanlama="pre"):
    return {"id": 1, "sonuc": sonuc, "odeme": odeme, "toplam_oran": None if sistem_k else oran, "en_yuksek_odeme": oran, "sistem_k": sistem_k,
            "sonuc_zamani": zaman, "seviye": seviye, "zamanlama": zamanlama, "bacaklar": [{}] * nbacak}


def test_bos_liste():
    s = sonuc_istatistik([], SIMDI)
    assert s["stats"]["settled"] == 0 and s["stats"]["win_rate"] is None and s["stats"]["unit_profit"] == 0 and s["stats"]["form"] == []
    assert s["series"]["monthly"] == [] and s["series"]["last_updated"] is None


def test_temel_sayilar_ve_birim_kar():
    ks = [k("won", 2.0, 2.0, "2026-10-19T10:00:00Z"), k("lost", 0, 3.0, "2026-10-18T10:00:00Z"), k("void", 1.0, 1.5, "2026-10-17T10:00:00Z"),
          k("won", 1.5, 1.5, "2026-10-16T10:00:00Z")]
    st = sonuc_istatistik(ks, SIMDI)["stats"]
    assert (st["settled"], st["decided"], st["won"], st["lost"], st["void"]) == (4, 3, 2, 1, 1)
    assert st["win_rate"] == pytest.approx(2 / 3, abs=1e-4)
    assert st["unit_profit"] == pytest.approx((2.0 - 1) + (0 - 1) + 0 + (1.5 - 1), abs=1e-6)     # tuttu: oran-1, yattı: -1, iade 0
    assert st["avg_odds"] == pytest.approx((2.0 + 3.0 + 1.5) / 3, abs=1e-4)                     # iptal hariç


def test_wilson_araligi_makul():
    st = sonuc_istatistik([k("won", 2, 2, "2026-10-19T10:00:00Z")] * 8 + [k("lost", 0, 2, "2026-10-19T10:00:00Z")] * 2, SIMDI)["stats"]
    assert st["win_rate_low"] < st["win_rate"] < st["win_rate_high"] and 0 <= st["win_rate_low"] and st["win_rate_high"] <= 1
    assert st["win_rate"] == 0.8


def test_sistem_kuponu_kismi_zarar_birim_karda_kesirli():
    st = sonuc_istatistik([k("lost", 0.6825, 5.0, "2026-10-19T10:00:00Z", sistem_k=3, nbacak=4)], SIMDI)["stats"]
    assert st["lost"] == 1 and st["unit_profit"] == pytest.approx(-0.3175, abs=1e-4)


def test_form_en_yeni_basta_en_cok_10_ve_seri():
    ks = [k("won" if i % 3 else "lost", 2 if i % 3 else 0, 2, f"2026-10-{i:02d}T10:00:00Z") for i in range(1, 16)]
    s = sonuc_istatistik(ks, SIMDI)["stats"]
    assert len(s["form"]) == 10
    assert s["form"][0] == ("won" if 15 % 3 else "lost")
    assert s["streak"]["type"] == s["form"][0] and s["streak"]["count"] >= 1


def test_seri_ardisik_sayar():
    ks = [k("lost", 0, 2, "2026-10-01T10:00:00Z"), k("won", 2, 2, "2026-10-02T10:00:00Z"), k("won", 2, 2, "2026-10-03T10:00:00Z"), k("won", 2, 2, "2026-10-04T10:00:00Z")]
    assert sonuc_istatistik(ks, SIMDI)["stats"]["streak"] == {"type": "won", "count": 3}


def test_aylik_seri_ilk_macin_ayina_gore_sayar():
    eski = k("won", 2, 2, "2026-10-19T13:42:00Z")        # Nisan maçı, Ekim'de toplu sonuçlandı
    eski["bacaklar"] = [{"mac_zamani": "2026-04-12T17:00:00Z"}, {"mac_zamani": "2026-04-10T17:00:00Z"}]
    yeni = k("lost", 0, 2, "2026-10-19T13:42:00Z")
    yeni["bacaklar"] = [{"mac_zamani": "2026-10-18T17:00:00Z"}]
    se = sonuc_istatistik([eski, yeni], SIMDI)["series"]
    assert se["monthly"] == [{"month": "2026-04", "won": 1, "lost": 0, "void": 0}, {"month": "2026-10", "won": 0, "lost": 1, "void": 0}]
    assert [d["date"] for d in se["daily"]] == ["2026-10-18"]       # günlük de maç gününe göre, 90 gün penceresi içinde


def test_aylik_ve_gunluk_seri():
    ks = [k("won", 2, 2, "2026-10-19T10:00:00Z"), k("lost", 0, 2, "2026-10-19T12:00:00Z"), k("won", 2, 2, "2026-09-05T10:00:00Z"), k("void", 1, 2, "2026-01-05T10:00:00Z")]
    se = sonuc_istatistik(ks, SIMDI)["series"]
    assert se["monthly"] == [{"month": "2026-01", "won": 0, "lost": 0, "void": 1}, {"month": "2026-09", "won": 1, "lost": 0, "void": 0}, {"month": "2026-10", "won": 1, "lost": 1, "void": 0}]
    assert {"date": "2026-10-19", "won": 1, "lost": 1, "void": 0} in se["daily"]
    assert all(d["date"] >= "2026-07-22" for d in se["daily"])           # son 90 gün
    assert se["last_updated"] == "2026-10-19T12:00:00Z"


# ---- Otomatik sonuçlandırma ----
def bk(i, zaman, secim="1", tur="Maç Sonucu", fotmob=None, iddaa=None, ev="Galatasaray", dep="Fenerbahçe"):
    return {"id": i, "kupon_id": 1, "ev": ev, "dep": dep, "mac_zamani": zaman, "bahis_turu": tur, "secim": secim, "fotmob_match_id": fotmob, "iddaa_event_id": iddaa}


MAC = {"id": 7, "fotmob_match_id": 55, "home_score": 2, "away_score": 1, "ev_ad": "Galatasaray", "dep_ad": "Fenerbahçe"}


def kos(bekleyen, mac_bul=lambda b: MAC, yukle=lambda m: {"home": 2, "away": 1, "home_name": "Galatasaray", "away_name": "Fenerbahçe"}):
    return sonuclandir(bekleyen, mac_bul, yukle, SIMDI)


def test_biten_macin_bacagi_otomatik_sonuclanir():
    guncelleme, rapor = kos([bk(10, datetime(2026, 10, 19, 17, 0)), bk(11, datetime(2026, 10, 19, 17, 0), secim="2")])
    assert guncelleme == [(1, 10, "won"), (1, 11, "lost")]
    assert rapor["sonuclanan"] == 2


def test_mac_henuz_bitmemis_olabilir_2_saat_beklenir():
    guncelleme, rapor = kos([bk(10, datetime(2026, 10, 20, 11, 0))])    # 1 saat önce başladı
    assert guncelleme == [] and rapor["erken"] == 1


def test_mac_bulunamazsa_ve_veri_yoksa_rapor_eder_yazmaz():
    g, r1 = kos([bk(10, datetime(2026, 10, 19, 17, 0))], mac_bul=lambda b: None)
    assert g == [] and r1["mac_yok"] == 1
    g, r2 = kos([bk(10, datetime(2026, 10, 19, 17, 0), tur="1. Yarı Sonucu", secim="1")], yukle=lambda m: {"home": 2, "away": 1})
    assert g == [] and r2["veri_yok"] == 1
    g, r3 = kos([bk(10, datetime(2026, 10, 19, 17, 0), tur="Uydurma Market")])
    assert g == [] and r3["desteklenmiyor"] == 1


def test_rapor_ayrintilari_bacak_kimligiyle():
    _, r = kos([bk(10, datetime(2026, 10, 19, 17, 0), tur="Uydurma Market")])
    assert r["ayrinti"] == [{"kupon_id": 1, "bacak_id": 10, "durum": "desteklenmiyor", "bahis": "Uydurma Market / 1"}]


# ---- Uçlar ----
import copy
from fastapi.testclient import TestClient

from app.main import app
from app.routers import kuponlar as K, kupon_sonuclari as S

istemci = TestClient(app)
GW = {"X-Sunucu-Anahtari": "gizli"}


def kayit(i, seviye, sonuc, odeme, zaman, nbacak=1):
    return {"id": i, "seviye": seviye, "durum": "yayinda", "zamanlama": "pre", "sistem_k": None, "one_cikan": False, "nedenler": ["n"], "ana_risk": "r", "yorum": None,
            "sinyal": None, "risk_seviyesi": None, "toplam_oran": 2.0, "en_yuksek_odeme": 2.0, "kombinasyon_sayisi": 1, "sonuc": sonuc, "odeme": odeme,
            "sonuc_zamani": zaman, "bacaklar": [{"id": i * 10 + j, "ev": "A", "dep": "B", "lig": "L", "mac_zamani": zaman, "bahis_turu": "MS", "secim": "1", "oran": 2.0,
                                                   "sonuc": sonuc, "iddaa_event_id": 1, "fotmob_match_id": None} for j in range(nbacak)]}


def test_sonuclar_ucu_istatistik_ve_kilitli_liste(monkeypatch):
    veri = [kayit(1, "acik", "won", 2.0, "2026-10-19T10:00:00Z"), kayit(2, "elit", "lost", 0, "2026-10-18T10:00:00Z", nbacak=2)]
    monkeypatch.setattr(K, "_anahtar", lambda: "gizli")
    monkeypatch.setattr(K, "db_liste", lambda f: (copy.deepcopy(veri), len(veri)))
    monkeypatch.setattr(S, "_simdi", lambda: datetime(2026, 10, 20, 12, 0))
    j = istemci.get("/api/kuponlar/sonuclar").json()
    assert j["stats"]["won"] == 1 and j["stats"]["lost"] == 1 and j["total"] == 2
    elit = next(i for i in j["items"] if i["seviye"] == "elit")
    assert elit["kilitli"] is True and elit["sonuc"] == "lost" and elit["bacaklar"][0]["secim"] is None     # sonuç açık, seçim gizli
    tek = istemci.get("/api/kuponlar/sonuclar?bacak=single").json()
    assert tek["total"] == 1 and tek["stats"]["settled"] == 1
    kaybeden = istemci.get("/api/kuponlar/sonuclar?sonuc=lost", headers={**GW, "X-Uye-Seviyesi": "elit"}).json()
    assert kaybeden["total"] == 1 and kaybeden["items"][0]["kilitli"] is False
    assert istemci.get("/api/kuponlar/sonuclar?days=1").json()["total"] == 0


def test_sonuclar_yolu_kimlik_sanilmaz(monkeypatch):
    monkeypatch.setattr(K, "db_liste", lambda f: ([], 0))
    assert istemci.get("/api/kuponlar/sonuclar").status_code == 200


def test_sonuclandir_ucu_anahtar_ve_kuru_calisma(monkeypatch):
    yazilan = []
    monkeypatch.setattr(S, "_cron_anahtari", lambda: "cron")
    monkeypatch.setattr(S, "_simdi", lambda: SIMDI)
    monkeypatch.setattr(S, "bekleyen_bacaklar", lambda sinir: [bk(10, datetime(2026, 10, 19, 17, 0))])
    monkeypatch.setattr(S, "mac_bul_bacak", lambda q, b: MAC)
    monkeypatch.setattr(S, "mac_yukle", lambda q, m: {"home": 2, "away": 1, "home_name": "G", "away_name": "F"})
    monkeypatch.setattr(K, "db_bacak_sonucu", lambda *a: yazilan.append(a))
    assert istemci.post("/api/kuponlar/sonuclandir").status_code == 401
    kuru = istemci.post("/api/kuponlar/sonuclandir", headers={"X-Cron-Anahtari": "cron"}).json()
    assert kuru["dry_run"] is True and kuru["sonuclanan"] == 1 and yazilan == []
    gercek = istemci.post("/api/kuponlar/sonuclandir?dry_run=false", headers={"X-Cron-Anahtari": "cron"}).json()
    assert gercek["guncellemeler"] == [{"kupon_id": 1, "bacak_id": 10, "sonuc": "won"}] and yazilan == [(1, 10, "won", "otomatik")]
    monkeypatch.setattr(S, "_cron_anahtari", lambda: None)
    assert istemci.post("/api/kuponlar/sonuclandir", headers={"X-Cron-Anahtari": "cron"}).status_code == 503


def test_sonuclar_listesi_ilk_macin_tarihine_gore_yeniden_eskiye(monkeypatch):
    eski = kayit(1, "acik", "won", 2.0, "2026-10-19T13:42:00Z")      # toplu sonuçlandı ama maçı Nisan'da
    eski["bacaklar"][0]["mac_zamani"] = "2026-04-10T17:00:00Z"
    yeni = kayit(2, "acik", "won", 2.0, "2026-10-19T13:42:00Z")
    yeni["bacaklar"][0]["mac_zamani"] = "2026-10-17T17:00:00Z"
    sonraki = kayit(3, "acik", "lost", 0, "2026-10-18T10:00:00Z")
    sonraki["bacaklar"][0]["mac_zamani"] = "2026-10-21T19:00:00Z"
    veri = [eski, yeni, sonraki]
    monkeypatch.setattr(K, "_anahtar", lambda: "gizli")
    monkeypatch.setattr(K, "db_liste", lambda f: (copy.deepcopy(veri), len(veri)))
    monkeypatch.setattr(S, "_simdi", lambda: datetime(2026, 10, 22, 12, 0))
    j = istemci.get("/api/kuponlar/sonuclar").json()
    assert [x["id"] for x in j["items"]] == [3, 2, 1]
