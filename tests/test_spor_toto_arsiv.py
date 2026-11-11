"""Spor Toto arşivi: resmî API'den hafta + maç sonuçları, sezon özetleri, zorluk tablosu, benzer haftalar, senkron. Gerçek API cevapları fixtures'ta; veritabanı sahte."""
import json
from datetime import datetime
from pathlib import Path

import pytest

from app.services import spor_toto_arsiv as a

FIX = json.loads((Path(__file__).parent / "fixtures" / "sporoto_arsiv.json").read_text())


# ---------------- ayrıştırma ----------------
def test_oyun_no_ve_sezon():
    assert a.oyun_no("14. Oyun Programı") == 14
    assert a.oyun_no("7. Hafta") == 7
    assert a.oyun_no("bilinmeyen") is None


def test_hafta_kaydi_gercek_cevaptan():
    h, m = a.hafta_kaydi(1541, "2026/2027", "14. Oyun Programı", FIX["sonuc_1541"]["object"], FIX["maclar_1541"]["object"])
    assert h["round_id"] == 1541 and h["sezon"] == "2026/2027" and h["oyun_no"] == 14
    assert (h["bilen_15"], h["bilen_14"], h["bilen_13"], h["bilen_12"]) == (1031, 20811, 168466, 718133)
    assert h["ikramiye_15"] == 44261.24 and h["kapanis"] == "2026-11-06T19:55:00"
    assert (h["ev"], h["beraberlik"], h["deplasman"]) == (6, 2, 7)
    assert h["mac_sayisi"] == 15 and h["noter_sayisi"] == 0 and h["temiz"] is True
    assert len(m) == 15 and [x["sira"] for x in m] == list(range(1, 16))
    assert m[0] == {"round_id": 1541, "sira": 1, "tarih": "2026-11-06T20:00:00", "ev": "Gaziantep FK", "dep": "Kasımpaşa", "skor_ev": 0, "skor_dep": 2, "sonuc": "2", "noter": False}


def test_devir_haftasi_ve_noterli_mac_temiz_sayilmaz():
    """2026/27 1. oyun: 15 bilen yok (devir), bir maç noter çekilişiyle sonuçlanmış."""
    h, m = a.hafta_kaydi(1528, "2026/2027", "1. Oyun Programı", FIX["sonuc_1528"]["object"], FIX["maclar_1528"]["object"])
    assert h["bilen_15"] == 0 and h["ikramiye_15"] == 30149380.57
    assert h["noter_sayisi"] == 1 and h["temiz"] is False and sum(1 for x in m if x["noter"]) == 1


def test_sonuc_yayinlanmadiysa_sayilar_bos_ve_temiz_degil():
    h, m = a.hafta_kaydi(1542, "2026/2027", "15. Oyun Programı", None, FIX["maclar_1541"]["object"])
    assert h["bilen_15"] is None and h["temiz"] is False and len(m) == 15


def test_eksik_ya_da_noterli_hafta_temiz_degil():
    mac = lambda s, noter=0: {"match": {"date": "2026-01-01T10:00:00", "fullTimeWin": s, "noterWin": noter, "score": {"homeRegular": 1, "awayRegular": 0},
                                      "homeTeam": {"name": "A"}, "awayTeam": {"name": "B"}}}
    sonuc = FIX["sonuc_1541"]["object"]
    h14, _ = a.hafta_kaydi(1, "s", "1. Hafta", sonuc, [mac(1)] * 14)
    assert h14["mac_sayisi"] == 14 and not h14["temiz"]
    h, m = a.hafta_kaydi(1, "s", "1. Hafta", sonuc, [mac(1)] * 14 + [mac(1, noter=1)])
    assert h["noter_sayisi"] == 1 and not h["temiz"] and m[-1]["noter"] is True
    hn, mn = a.hafta_kaydi(1, "s", "1. Hafta", sonuc, [mac(1)] * 14 + [mac(None, noter=None)])
    assert hn["ev"] == 14 and not hn["temiz"] and mn[-1]["sonuc"] is None


# ---------------- istatistik ----------------
def hafta(i, sezon, ev, ber, dep, f15, f14=100, temiz=True):
    return {"round_id": i, "sezon": sezon, "oyun_no": i, "ev": ev, "beraberlik": ber, "deplasman": dep, "bilen_15": f15, "bilen_14": f14, "mac_sayisi": 15, "noter_sayisi": 0, "temiz": temiz}


HAFTALAR = [hafta(1, "A", 8, 3, 4, 10), hafta(2, "A", 7, 4, 4, 0), hafta(3, "A", 9, 2, 4, 40), hafta(4, "B", 6, 5, 4, 2), hafta(5, "B", 8, 3, 4, 0), hafta(6, "B", 8, 3, 4, 7, temiz=False)]


def test_sezon_ozeti_yuzdeler_medyan_devir():
    o = {x["sezon"]: x for x in a.sezon_ozeti(HAFTALAR)}
    assert o["A"]["hafta"] == 3 and o["A"]["oran"] == {"ev": 53, "beraberlik": 20, "deplasman": 27}        # 24/45, 9/45, 12/45
    assert o["A"]["medyan_15"] == 10 and o["A"]["devir_hafta"] == 1
    assert o["B"]["hafta"] == 2                                                                         # temiz olmayan hafta sayılmaz
    assert o["B"]["en_cok_15"] == {"oyun_no": 4, "sayi": 2}


def test_zorluk_tablosu_beraberlige_gore():
    t = {x["beraberlik"]: x for x in a.zorluk_tablosu(HAFTALAR)}
    assert t[3]["hafta"] == 2 and t[3]["medyan_15"] == 5 and t[3]["devir_yuzde"] == 50
    assert t[2]["hafta"] == 1 and t[5]["hafta"] == 1


def test_zorluk_tablosu_7_ve_uzeri_birlesir():
    h = [hafta(i, "A", 4, 7 + (i % 3), 4, i) for i in range(1, 7)]
    t = a.zorluk_tablosu(h)
    assert [x["beraberlik"] for x in t] == [7] and t[0]["etiket"] == "7+"


def test_benzer_haftalar_uzakliga_gore_ve_ozet():
    r = a.benzer_haftalar(HAFTALAR, ev=8, ber=3, dep=4, k=3)
    assert [x["round_id"] for x in r["haftalar"]][:2] == [5, 1] and len(r["haftalar"]) == 3      # aynı uzaklıkta yeni olan önce
    assert r["haftalar"][0]["uzaklik"] == 0.0
    assert r["ozet"]["medyan_15"] == 10 and r["genel"]["medyan_15"] == 2 and r["ozet"]["k"] == 3
    assert r["genel"]["hafta"] == 5                                                                  # temiz olmayan hariç


def test_benzer_haftalar_toplam_15_olmayan_istek_hata():
    with pytest.raises(ValueError):
        a.benzer_haftalar(HAFTALAR, ev=8, ber=3, dep=3)


def test_korelasyon_ev_ve_beraberlik():
    c = a.korelasyon(HAFTALAR)
    assert -1 <= c["beraberlik"] <= 1 and -1 <= c["ev"] <= 1 and c["beraberlik"] < 0


# ---------------- senkron ----------------
class SahteIstemci:
    def __init__(self):
        self.cagri = []

    def yillar(self):
        return ["2026/2027"]

    def adlar(self, sezon):
        self.cagri.append(("adlar", sezon))
        return [{"id": 1541, "name": "14. Oyun Programı"}, {"id": 1542, "name": "15. Oyun Programı"}]

    def sonuc(self, rid):
        self.cagri.append(("sonuc", rid))
        return FIX["sonuc_1541"]["object"] if rid == 1541 else None

    def maclar(self, rid):
        self.cagri.append(("maclar", rid))
        return FIX["maclar_1541"]["object"]


class SahteDepo:
    def __init__(self, mevcut=None):
        self.mevcut = mevcut or {}
        self.yazilan = {}

    def mevcut_haftalar(self):
        return dict(self.mevcut)

    def kaydet(self, hafta, maclar):
        self.yazilan[hafta["round_id"]] = (hafta, maclar)


def test_senkron_yeni_haftalari_yazar_ve_sonuc_yoksa_temiz_degil_isaretler():
    d, i = SahteDepo(), SahteIstemci()
    r = a.senkronla(i, d)
    assert sorted(d.yazilan) == [1541, 1542]
    assert d.yazilan[1541][0]["temiz"] and not d.yazilan[1542][0]["temiz"]
    assert r == {"sezon": 1, "hafta": 2, "yazilan": 2, "atlanan": 0, "hata": 0}


def test_senkron_tamamlanmis_haftayi_tekrar_cekmez_ama_sonucsuzu_yeniler():
    d, i = SahteDepo({1541: True, 1542: False}), SahteIstemci()
    r = a.senkronla(i, d)
    assert ("sonuc", 1541) not in i.cagri and ("sonuc", 1542) in i.cagri
    assert r["yazilan"] == 1 and r["atlanan"] == 1


def test_senkron_tek_hafta_hatasi_digerlerini_durdurmaz():
    class Bozuk(SahteIstemci):
        def sonuc(self, rid):
            if rid == 1541:
                raise RuntimeError("522")
            return None
    d = SahteDepo()
    r = a.senkronla(Bozuk(), d)
    assert r["hata"] == 1 and 1542 in d.yazilan and 1541 not in d.yazilan


def test_senkron_sezon_secimi():
    d, i = SahteDepo(), SahteIstemci()
    a.senkronla(i, d, sezonlar=["2025/2026"])
    assert ("adlar", "2025/2026") in i.cagri and ("adlar", "2026/2027") not in i.cagri
