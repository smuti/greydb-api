"""Canlı korner izleme: panelin gösterdiği tahminler kaydedilir, maç bitince gerçekleşen sayılar yazılır, kalibrasyon özeti çıkar.
Yalnız iç analiz (yönetici). Veritabanı ve ağ yok: kaynaklar sahte."""
from datetime import datetime, timedelta

import pytest

from app.services import canli_korner_izleme as c

SIMDI = datetime(2026, 11, 8, 20, 0)


def govde(**o):
    g = {"fotmob_match_id": 5887679, "lig_ad": "Liga Portugal", "ev": "Sporting CP", "dep": "Moreirense", "model_surum": "canli-korner-1.0",
         "faz": "ilk", "dakika": 44, "skor": {"ev": 0, "dep": 0}, "korner": {"ev": 6, "dep": 3}, "kirmizi": {"ev": 0, "dep": 0},
         "onceki": {"ev": 6.97, "dep": 2.78, "kaynak": "yerel"},
         "hedefler": {"macSonu": {"ortalama": 14.88, "mevcut": 9, "olasilik": {"13": 0.82, "14": 0.69, "15": 0.53}},
                      "ev": {"ortalama": 10.5, "mevcut": 6, "olasilik": {"9": 0.77, "10": 0.61}}}}
    g.update(o)
    return g


# ---- girdi doğrulama ----
def test_kayit_hazirla_gecerli_govde_ve_kova():
    k = c.kayit_hazirla(govde())
    assert (k["fotmob_match_id"], k["faz"], k["dakika"], k["kova"]) == (5887679, "ilk", 44, 8)
    assert k["hedefler"]["macSonu"]["olasilik"]["13"] == 0.82


@pytest.mark.parametrize("duzelt", [
    dict(fotmob_match_id=0), dict(fotmob_match_id="x"), dict(faz="uzatma"), dict(dakika=-1), dict(dakika=200), dict(hedefler={}),
    dict(hedefler={"bilinmeyen": {"ortalama": 1, "mevcut": 0, "olasilik": {"1": 0.5}}}),
    dict(hedefler={"ev": {"ortalama": -1, "mevcut": 0, "olasilik": {"1": 0.5}}}),
    dict(hedefler={"ev": {"ortalama": 5, "mevcut": 0, "olasilik": {"1": 1.5}}}),
    dict(hedefler={"ev": {"ortalama": 5, "mevcut": 0, "olasilik": {"x": 0.5}}}),
    dict(hedefler={"ev": {"ortalama": 5, "mevcut": 0, "olasilik": {str(i): 0.5 for i in range(1, 30)}}}),
    dict(korner={"ev": -1, "dep": 0}), dict(skor={"ev": "a", "dep": 0}), dict(model_surum=""),
])
def test_kayit_hazirla_gecersiz_govde_reddedilir(duzelt):
    with pytest.raises(ValueError):
        c.kayit_hazirla(govde(**duzelt))


def test_kova_faz_ve_5_dakikalik_dilim():
    assert c.kayit_hazirla(govde(faz="ikinci", dakika=47))["kova"] == 9
    assert c.kayit_hazirla(govde(faz="devre", dakika=45))["kova"] == 9
    assert c.kayit_hazirla(govde(faz="ilk", dakika=4))["kova"] == 0


# ---- sonuçlandırma ----
class K:
    def __init__(self, bekleyen, sonuclar):
        self.bekleyen_satirlar, self.sonuclar, self.yazilan, self.sorgulanan = bekleyen, sonuclar, [], []

    def kaynaklar(self):
        def mac_sonucu(fid):
            self.sorgulanan.append(fid)
            return self.sonuclar.get(fid)
        return c.Kaynaklar(bekleyen=lambda son: [r for r in self.bekleyen_satirlar if r["kaydedildi"] < son],
                           mac_sonucu=mac_sonucu, sonuc_yaz=lambda id_, g, an: self.yazilan.append((id_, g, an)))


def satir(id_, fid, saat_once=3):
    return {"id": id_, "fotmob_match_id": fid, "kaydedildi": SIMDI - timedelta(hours=saat_once)}


BITTI = {"finished": True, "cancelled": False, "ilk_yari": {"ev": 3, "dep": 2}, "mac": {"ev": 7, "dep": 4}}


def test_biten_maca_gerceklesen_yazilir():
    k = K([satir(1, 10), satir(2, 10)], {10: BITTI})
    r = c.sonuclandir_bekleyenler(SIMDI, k.kaynaklar(), dry_run=False)
    assert r["islenen"] == 2 and k.sorgulanan == [10]          # maç başına tek sorgu
    assert k.yazilan[0][1] == {"ilkYari": 5, "macSonu": 11, "ev": 7, "dep": 4}


def test_bitmemis_mac_beklemede_kalir_kuru_calisma_yazmaz():
    k = K([satir(1, 10)], {10: {**BITTI, "finished": False}})
    r = c.sonuclandir_bekleyenler(SIMDI, k.kaynaklar(), dry_run=False)
    assert (r["islenen"], r["bitmemis"], k.yazilan) == (0, 1, [])
    k2 = K([satir(1, 10)], {10: BITTI})
    assert c.sonuclandir_bekleyenler(SIMDI, k2.kaynaklar(), dry_run=True)["islenen"] == 1 and k2.yazilan == []


def test_iptal_edilen_islenir_gerceklesen_bos():
    k = K([satir(1, 10)], {10: {**BITTI, "cancelled": True}})
    c.sonuclandir_bekleyenler(SIMDI, k.kaynaklar(), dry_run=False)
    assert k.yazilan[0][1] is None


def test_cok_eski_ve_verisi_olmayan_islenir_yeni_olan_beklemeye_alinir():
    k = K([satir(1, 11, saat_once=24 * 5), satir(2, 12, saat_once=5)], {})
    r = c.sonuclandir_bekleyenler(SIMDI, k.kaynaklar(), dry_run=False)
    assert [y[0] for y in k.yazilan] == [1] and r["verisiz"] == 1 and len(r["hata"]) == 1


def test_hata_diger_maclari_durdurmaz():
    k = K([satir(1, 10), satir(2, 20)], {20: BITTI})
    k.sonuclar = {20: BITTI}
    orijinal = k.kaynaklar()
    def mac(fid):
        if fid == 10:
            raise RuntimeError("ağ")
        return orijinal.mac_sonucu(fid)
    kay = c.Kaynaklar(bekleyen=orijinal.bekleyen, mac_sonucu=mac, sonuc_yaz=orijinal.sonuc_yaz)
    r = c.sonuclandir_bekleyenler(SIMDI, kay, dry_run=False)
    assert r["islenen"] == 1 and len(r["hata"]) == 1


# ---- kalibrasyon özeti ----
def kayit(fid, faz, dakika, ort, olas, gerc, hedef="macSonu", mevcut=0):
    return {"fotmob_match_id": fid, "faz": faz, "dakika": dakika, "hedefler": {hedef: {"ortalama": ort, "mevcut": mevcut, "olasilik": olas}}, "gerceklesen": gerc}


def test_ozet_sapma_ve_bantlar():
    satirlar = [
        kayit(1, "ikinci", 70, 11.0, {"11": 0.8, "12": 0.5}, {"macSonu": 12}),
        kayit(2, "ikinci", 70, 11.0, {"11": 0.8, "12": 0.5}, {"macSonu": 10}),
    ]
    o = c.ozet(satirlar)
    assert o["maclar"] == 2 and o["kayit"] == 2
    h = o["hedefler"]["macSonu"]
    assert h["n"] == 2 and h["sapma"] == 0.0 and h["mae"] == 1.0         # (11-12) ve (11-10)
    b80 = [b for b in h["bantlar"] if b["bant"] == "80-90"][0]
    assert (b80["n"], b80["ort_olasilik"], b80["tutma_yuzde"]) == (2, 80.0, 50.0)       # 11+ : 12>=11 evet, 10>=11 hayır
    b50 = [b for b in h["bantlar"] if b["bant"] == "50-60"][0]
    assert (b50["n"], b50["tutma_yuzde"]) == (2, 50.0)


def test_ozet_dakika_dilimi_ve_bos_girdi():
    satirlar = [kayit(1, "ilk", 10, 9.0, {"9": 0.6}, {"macSonu": 11}), kayit(2, "devre", 45, 9.0, {"9": 0.6}, {"macSonu": 9}), kayit(3, "ikinci", 80, 9.0, {"9": 0.6}, {"macSonu": 9})]
    d = c.ozet(satirlar)["hedefler"]["macSonu"]["dilimler"]
    assert {x["dilim"] for x in d} == {"0-15", "devre", "75+"}
    assert c.ozet([])["kayit"] == 0 and c.ozet([])["hedefler"] == {}


def test_ozet_gerceklesen_olmayan_hedefi_atlar():
    s = kayit(1, "ilk", 10, 5.0, {"5": 0.5}, {"ev": 5}, hedef="macSonu")      # gerceklesen'de macSonu yok
    assert c.ozet([s])["hedefler"] == {}
