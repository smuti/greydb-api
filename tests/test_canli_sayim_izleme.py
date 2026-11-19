"""Canlı faul / kart / taç izleme: pazar başına kayıt, sonuçlandırma ve özet; uçlar ve zamanlayıcı. Veritabanı ve ağ yok: kaynaklar sahte."""
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import canli_sayim as r
from app.services import canli_sayim_izleme as c
from app.services import zamanlayici as Z

SIMDI = datetime(2026, 11, 19, 20, 0)


def govde(**o):
    g = {"fotmob_match_id": 5887679, "pazar": "tac", "lig_ad": "Lig", "ev": "Ev", "dep": "Dep", "model_surum": "canli-sayim-1.0",
         "faz": "ikinci", "dakika": 62, "skor": {"ev": 1, "dep": 0}, "sayim": {"ev": 21, "dep": 14}, "kirmizi": {"ev": 0, "dep": 0},
         "onceki": {"ev": 19.1, "dep": 20.4, "kaynak": "sence"},
         "hedefler": {"macSonu": {"ortalama": 45.2, "mevcut": 35, "olasilik": {"44": 0.58, "45": 0.5}}}}
    g.update(o)
    return g


# ---- doğrulama ----
def test_kayit_hazirla_pazar_ve_sayim_alani():
    k = c.kayit_hazirla(govde())
    assert k["pazar"] == "tac" and k["sayim"] == {"ev": 21, "dep": 14} and k["kova"] == 12 and "korner" not in k


def test_takim_basina_50_ustu_taç_kabul_edilir_ama_100_ustu_reddedilir():
    assert c.kayit_hazirla(govde(sayim={"ev": 55, "dep": 3}))["sayim"]["ev"] == 55
    with pytest.raises(ValueError):
        c.kayit_hazirla(govde(sayim={"ev": 101, "dep": 3}))


@pytest.mark.parametrize("bozuk", [{"pazar": "korner"}, {"pazar": None}, {"faz": "x"}, {"sayim": None}, {"hedefler": {}}])
def test_gecersiz_govde_reddedilir(bozuk):
    with pytest.raises(ValueError):
        c.kayit_hazirla(govde(**bozuk))


def test_korner_dogrulamasi_degismedi():
    from app.services import canli_korner_izleme as ck
    g = {**govde(), "korner": {"ev": 3, "dep": 1}}
    g.pop("sayim"); g.pop("pazar")
    k = ck.kayit_hazirla(g)
    assert k["korner"] == {"ev": 3, "dep": 1} and "sayim" not in k
    with pytest.raises(ValueError):
        ck.kayit_hazirla({**g, "korner": {"ev": 51, "dep": 0}})


# ---- matchforge yanıtından sonuç ----
def cift(h, a):
    return {"home": h, "away": a}


YANIT = {"match": {"status": "finished"}, "stats": {"fouls": cift(12, 10), "yellow_cards": cift(2, 1), "red_cards": cift(1, 0)},
         "stats_tum": {"fouls_committed": cift(12, 10), "throws": cift(20, 18), "yellow_cards": cift(2, 1), "red_cards": cift(1, 0)},
         "stats_donem": {"ilk_yari": {"fouls_committed": cift(5, 6), "throws": cift(9, 10), "yellow_cards": cift(1, 0), "red_cards": cift(0, 0)}}}


def test_pazar_sonuclari_kart_olaylardan_oncelikli():
    p = c.pazar_sonuclari({**YANIT, "kart_puani": {"mac": cift(5, 1), "ilk_yari": cift(1, 0)}})
    assert p["faul"] == {"ilk_yari": {"ev": 5, "dep": 6}, "mac": {"ev": 12, "dep": 10}}
    assert p["tac"] == {"ilk_yari": {"ev": 9, "dep": 10}, "mac": {"ev": 20, "dep": 18}}
    assert p["kart"] == {"ilk_yari": {"ev": 1, "dep": 0}, "mac": {"ev": 5, "dep": 1}}


def test_pazar_sonuclari_kart_puani_yoksa_sari_arti_iki_kirmizi_ve_eksik_doku_none():
    p = c.pazar_sonuclari(YANIT)
    assert p["kart"]["mac"] == {"ev": 4, "dep": 1}
    eksik = c.pazar_sonuclari({**YANIT, "stats_donem": {"ilk_yari": {"fouls_committed": cift(5, 6)}}})
    assert eksik["tac"] is None and eksik["kart"] is None and eksik["faul"] is not None


# ---- sonuçlandırma ----
class K:
    def __init__(self, satirlar, sonuclar):
        self.satirlar, self.sonuclar, self.yazilan, self.sorgulanan = satirlar, sonuclar, [], []

    def kaynaklar(self):
        def mac(fid):
            self.sorgulanan.append(fid)
            return self.sonuclar.get(fid)
        return c.Kaynaklar(bekleyen=lambda son: [x for x in self.satirlar if x["kaydedildi"] < son], mac_sonucu=mac,
                           sonuc_yaz=lambda id_, g, an: self.yazilan.append((id_, g)))


def satir(id_, fid, pazar, saat_once=3):
    return {"id": id_, "fotmob_match_id": fid, "pazar": pazar, "kaydedildi": SIMDI - timedelta(hours=saat_once)}


def test_pazar_basina_gerceklesen_yazilir_mac_basina_tek_sorgu():
    veri = {"finished": True, "cancelled": False, "pazarlar": c.pazar_sonuclari(YANIT)}
    k = K([satir(1, 10, "faul"), satir(2, 10, "tac"), satir(3, 10, "kart")], {10: veri})
    rap = c.sonuclandir_bekleyenler(SIMDI, k.kaynaklar(), dry_run=False)
    assert rap["islenen"] == 3 and k.sorgulanan == [10]
    assert dict(k.yazilan) == {1: {"ilkYari": 11, "macSonu": 22, "ev": 12, "dep": 10}, 2: {"ilkYari": 19, "macSonu": 38, "ev": 20, "dep": 18},
                               3: {"ilkYari": 1, "macSonu": 5, "ev": 4, "dep": 1}}


def test_eksik_pazar_beklemede_cok_eskiyse_bos_islenir():
    veri = {"finished": True, "cancelled": False, "pazarlar": {"faul": {"ilk_yari": {"ev": 1, "dep": 1}, "mac": {"ev": 2, "dep": 2}}, "tac": None, "kart": None}}
    k = K([satir(1, 10, "faul"), satir(2, 10, "tac"), satir(3, 11, "tac", saat_once=24 * 5)], {10: veri, 11: veri})
    rap = c.sonuclandir_bekleyenler(SIMDI, k.kaynaklar(), dry_run=False)
    assert (rap["islenen"], rap["eksik"], rap["verisiz"]) == (1, 1, 1)
    assert dict(k.yazilan) == {1: {"ilkYari": 2, "macSonu": 4, "ev": 2, "dep": 2}, 3: None}


def test_bitmemis_bekler_iptal_bos_kuru_calisma_yazmaz():
    k = K([satir(1, 10, "faul")], {10: {"finished": False, "pazarlar": {}}})
    assert c.sonuclandir_bekleyenler(SIMDI, k.kaynaklar(), dry_run=False)["bitmemis"] == 1 and k.yazilan == []
    k2 = K([satir(1, 10, "faul")], {10: {"finished": True, "cancelled": True}})
    c.sonuclandir_bekleyenler(SIMDI, k2.kaynaklar(), dry_run=False)
    assert k2.yazilan == [(1, None)]
    k3 = K([satir(1, 10, "faul")], {10: {"finished": True, "pazarlar": c.pazar_sonuclari(YANIT)}})
    assert c.sonuclandir_bekleyenler(SIMDI, k3.kaynaklar(), dry_run=True)["islenen"] == 1 and k3.yazilan == []


def test_ozet_pazar_basina_ayrilir():
    s = [{"fotmob_match_id": 1, "pazar": "faul", "faz": "devre", "dakika": 45, "hedefler": {"macSonu": {"ortalama": 24.0, "mevcut": 12, "olasilik": {"24": 0.5}}}, "gerceklesen": {"macSonu": 26}},
         {"fotmob_match_id": 1, "pazar": "kart", "faz": "devre", "dakika": 45, "hedefler": {"macSonu": {"ortalama": 4.0, "mevcut": 1, "olasilik": {"5": 0.4}}}, "gerceklesen": {"macSonu": 3}}]
    o = c.ozet(s)
    assert set(o) == {"faul", "kart", "tac"}
    assert o["faul"]["hedefler"]["macSonu"]["sapma"] == -2.0 and o["kart"]["hedefler"]["macSonu"]["sapma"] == 1.0 and o["tac"]["kayit"] == 0


# ---- uçlar ve zamanlayıcı ----
client = TestClient(app)
GW = {"X-Sunucu-Anahtari": "gizli"}
ADMIN = {**GW, "X-Yetki": "admin", "X-Kullanici-Email": "editor@greysports.tv"}


def test_kayit_yalniz_admin(monkeypatch):
    monkeypatch.setenv("SUNUCU_ANAHTARI", "gizli")
    yazilan = []
    monkeypatch.setattr(r, "db_ekle", lambda k: yazilan.append(k) or 7)
    assert client.post("/api/skorjin/canli-sayim/kayit", json=govde()).status_code == 401
    assert client.post("/api/skorjin/canli-sayim/kayit", json=govde(), headers=GW).status_code == 403
    o = client.post("/api/skorjin/canli-sayim/kayit", json=govde(), headers=ADMIN)
    assert o.status_code == 200 and o.json() == {"yazildi": True} and yazilan[0]["pazar"] == "tac"
    assert client.post("/api/skorjin/canli-sayim/kayit", json=govde(pazar="x"), headers=ADMIN).status_code == 422


def test_ozet_ucu_pazarlar(monkeypatch):
    monkeypatch.setenv("SUNUCU_ANAHTARI", "gizli")
    monkeypatch.setattr(r, "db_ozet_satirlari", lambda gun: [])
    assert client.get("/api/skorjin/canli-sayim/ozet", headers=GW).status_code == 403
    o = client.get("/api/skorjin/canli-sayim/ozet?gun=14", headers=ADMIN).json()
    assert o["gun"] == 14 and set(o["pazarlar"]) == {"faul", "kart", "tac"}


def test_sonuc_ucu_cron_anahtari_ister_varsayilan_kuru(monkeypatch):
    monkeypatch.setenv("CRON_ANAHTARI", "cron")
    cagri = []
    monkeypatch.setattr(c, "calistir", lambda dry_run: cagri.append(dry_run) or {"islenen": 0})
    assert client.post("/api/skorjin/canli-sayim/sonuc").status_code == 401
    client.post("/api/skorjin/canli-sayim/sonuc", headers={"X-Cron-Anahtari": "cron"})
    client.post("/api/skorjin/canli-sayim/sonuc?dry_run=false", headers={"X-Cron-Anahtari": "cron"})
    assert cagri == [True, False]


def test_zamanlayicida_sonuc_isi_ve_sql():
    t = {ad: kw for ad, kw, _ in Z.isler()}
    assert t["canli-sayim-sonuc"] == dict(hour="*/2", minute="57")
    assert "ON CONFLICT (fotmob_match_id, pazar, faz, kova) DO NOTHING RETURNING id" in c.YAZ_SQL
