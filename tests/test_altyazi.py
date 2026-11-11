"""/api/altyazi: CapCut projelerine otomatik altyazı (admin ↔ işçi Mac). Yetki, proje listesi, iş akışı durumları, kuyruk, sözlük. Veritabanı sahte."""
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import altyazi as r

client = TestClient(app)
ADMIN = {"X-Sunucu-Anahtari": "gizli", "X-Yetki": "admin", "X-Kullanici-Email": "fuat@greysports.tv"}
GW = {"X-Sunucu-Anahtari": "gizli"}
ISCI = {"X-Isci-Anahtari": "isci-gizli"}
MAC = "FUAT MacBook Pro"
KALAN = "mac-sabri"
AKTIF = ("sirada", "calisiyor", "capcut_acik_bekliyor")


class Sahte:
    def __init__(self):
        self.projeler, self.isler, self.sozluk, self.isciler, self.onizlemeler = {}, {}, {}, {}, {}
        self.n = self.sn = 0

    def projeleri_yenile(self, isci, projeler):
        self.projeler[isci] = list(projeler)
        self.isciler.setdefault(isci, None)      # gerçek sürüm altyazi_isciler'i de günceller

    def projeler_listele(self, isci):
        return self.projeler.get(isci, [])

    def isciler_listele(self):
        return [{"ad": a, "son_gorulme": datetime(2026, 11, 11, 12), "surum": s} for a, s in self.isciler.items()]

    def isci_gor(self, ad, surum):
        self.isciler[ad] = surum

    def is_ekle(self, j):
        if any(x["isci"] == j["isci"] and x["draft_id"] == j["draft_id"] and x["durum"] in AKTIF for x in self.isler.values()):
            return None
        self.n += 1
        self.isler[self.n] = {**j, "id": self.n, "asama": "dokum", "durum": "sirada", "alt_adim": None, "kelimeler": None, "parcalar": None, "imza": None, "tuval": None,
                              "sure": None, "uyarilar": None, "yedek_yolu": None, "hata": None, "deneme": 0, "olusturuldu": datetime(2026, 11, 11, 12), "guncellendi": datetime(2026, 11, 11, 12)}
        return {"id": self.n}

    def isler_listele(self, isci=None, draft_id=None, limit=50):
        return [x for x in self.isler.values() if (isci is None or x["isci"] == isci) and (draft_id is None or x["draft_id"] == draft_id)]

    def is_getir(self, i):
        return self.isler.get(i)

    def kuyruga_geri_al(self):
        self.geri = True

    def is_al(self, isci):
        for j in sorted(self.isler.values(), key=lambda x: x["id"]):
            if j["isci"] == isci and j["durum"] == "sirada":
                j.update({"durum": "calisiyor", "deneme": j["deneme"] + 1})
                return dict(j)
        return None

    def _benim(self, i, isci, durumlar=("calisiyor", "capcut_acik_bekliyor")):
        j = self.isler.get(i)
        return j if j and j["isci"] == isci and j["durum"] in durumlar else None

    def kalp_yaz(self, i, isci, alt_adim):
        j = self._benim(i, isci)
        if j and alt_adim: j["alt_adim"] = alt_adim
        return bool(j)

    def dokum_yaz(self, i, isci, d):
        j = self._benim(i, isci, ("calisiyor",))
        if j: j.update({"durum": "dokum_hazir", **d})
        return bool(j)

    def onizleme_yaz(self, i, isci, b):
        j = self._benim(i, isci)
        if j: self.onizlemeler[i] = b
        return bool(j)

    def onizleme_getir(self, i):
        return self.onizlemeler.get(i)

    def durum_yaz(self, i, isci, durum, alt_adim):
        j = self._benim(i, isci)
        if j: j.update({"durum": durum, "alt_adim": alt_adim})
        return bool(j)

    def bitti_yaz(self, i, isci, yedek_yolu, parca_sayisi):
        j = self._benim(i, isci)
        if j:
            j.update({"durum": "yazildi" if j["asama"] == "yaz" else "geri_alindi", "yedek_yolu": yedek_yolu or j["yedek_yolu"], "alt_adim": None})
            self.onizlemeler.pop(i, None)
        return bool(j)

    def hata_yaz(self, i, isci, hata):
        j = self._benim(i, isci)
        if j: j.update({"durum": "hata", "hata": hata})
        return bool(j)

    def parcalar_kaydet(self, i, parcalar):
        j = self.isler.get(i)
        if j and j["durum"] in r.YAZILABILIR and j["kelimeler"] is not None:
            j.update({"parcalar": parcalar, "durum": "onay_bekliyor"}); return True
        return False

    def asama_baslat(self, i, asama):
        j = self.isler.get(i)
        izinli = {"yaz": r.YAZILABILIR, "geri_al": ("yazildi",)}[asama]
        if not j or j["durum"] not in izinli or (asama == "yaz" and j["parcalar"] is None) or (asama == "geri_al" and not j["yedek_yolu"]):
            return False
        if any(x["isci"] == j["isci"] and x["draft_id"] == j["draft_id"] and x["durum"] in AKTIF and x["id"] != i for x in self.isler.values()):
            return False
        j.update({"asama": asama, "durum": "sirada", "hata": None, "deneme": 0}); return True

    def is_iptal_et(self, i):
        j = self.isler.get(i)
        if j and j["durum"] in AKTIF:
            j["durum"] = "iptal"; return True
        return False

    def sozluk_listele(self):
        return list(self.sozluk.values())

    def sozluk_ekle(self, yanlis, dogru, tur, ekleyen):
        self.sn += 1
        self.sozluk[self.sn] = {"id": self.sn, "yanlis": yanlis, "dogru": dogru, "tur": tur, "ekleyen": ekleyen, "eklendi": datetime(2026, 11, 11)}
        return self.sozluk[self.sn]

    def sozluk_sil(self, i):
        return self.sozluk.pop(i, None) is not None


@pytest.fixture(autouse=True)
def sahte(monkeypatch):
    s = Sahte()
    monkeypatch.setenv("SUNUCU_ANAHTARI", "gizli")
    monkeypatch.setenv("SAYAC_ISCI_ANAHTARI", "isci-gizli")
    for ad in ("projeleri_yenile", "projeler_listele", "isciler_listele", "isci_gor", "is_ekle", "isler_listele", "is_getir", "kuyruga_geri_al", "is_al", "kalp_yaz", "dokum_yaz",
               "onizleme_yaz", "onizleme_getir", "durum_yaz", "bitti_yaz", "hata_yaz", "parcalar_kaydet", "asama_baslat", "is_iptal_et", "sozluk_listele", "sozluk_ekle", "sozluk_sil"):
        monkeypatch.setattr(r, ad, getattr(s, ad))
    return s


PROJE = {"draft_id": "D1", "ad": "Fransa Turkiye", "klasor": "Fransa Turkiye", "degisme": "2026-11-11T10:00:00Z", "sure_sn": 29.08, "tuval": {"w": 3840, "h": 2160, "fps": 50.0},
         "surum": "9.5.0", "medya": [{"ad": "1.mp4", "var_mi": True}], "altyazi_var": False}
DOKUM = {"kelimeler": [{"w": "Merhaba", "s": 0.1, "e": 0.5, "p": 0.99, "yol": "/m/1.mp4"}], "parcalar": [{"t": "Merhaba", "s": 0.1, "e": 0.8, "dusuk": [], "kelimeler": []}],
         "imza": "abc", "tuval": {"w": 3840, "h": 2160, "fps": 50.0}, "sure": 29.08, "uyarilar": []}


def is_ac(isci=MAC, draft="D1", ayarlar=None):
    return client.post("/api/altyazi/isler", json={"isci": isci, "draft_id": draft, "proje_ad": "Fransa", "ayarlar": ayarlar or {"ayar": "yatay"}}, headers=ADMIN)


def al(isci=MAC):
    return client.post("/api/altyazi/isci/al", json={"isci": isci, "surum": "t1"}, headers=ISCI).json()["is"]


def dokumlu(sahte):
    """Döküm hazır duruma getirir, işin id'sini verir."""
    iid = is_ac().json()["id"]; al()
    assert client.post(f"/api/altyazi/isci/isler/{iid}/dokum", json={"isci": MAC, **DOKUM}, headers=ISCI).status_code == 200
    return iid


# ---- yetki
def test_admin_uclari_yetkisiz():
    assert client.get("/api/altyazi/projeler?isci=x").status_code == 401
    assert client.get("/api/altyazi/projeler?isci=x", headers=GW).status_code == 403
    assert is_ac().status_code == 201


def test_isci_uclari_anahtarsiz_reddedilir(monkeypatch):
    assert client.post("/api/altyazi/isci/al", json={"isci": MAC}).status_code == 401
    assert client.post("/api/altyazi/isci/al", json={"isci": MAC}, headers={"X-Isci-Anahtari": "yanlis"}).status_code == 401
    monkeypatch.delenv("SAYAC_ISCI_ANAHTARI")
    assert client.post("/api/altyazi/isci/al", json={"isci": MAC}, headers=ISCI).status_code == 503


def test_admin_isci_ucunu_kullanamaz_ve_tersi():
    assert client.post("/api/altyazi/isci/projeler", json={"isci": MAC, "projeler": []}, headers=ADMIN).status_code == 401
    assert client.get("/api/altyazi/projeler?isci=x", headers=ISCI).status_code in (401, 403)


# ---- proje listesi
def test_isci_proje_listesini_gonderir_admin_kendi_isciisinin_listesini_gorur(sahte):
    assert client.post("/api/altyazi/isci/projeler", json={"isci": MAC, "projeler": [PROJE]}, headers=ISCI).status_code == 200
    client.post("/api/altyazi/isci/projeler", json={"isci": KALAN, "projeler": [{**PROJE, "draft_id": "S1", "ad": "Sabri'nin"}]}, headers=ISCI)
    fuat = client.get(f"/api/altyazi/projeler?isci={MAC}", headers=ADMIN).json()["projeler"]
    assert [p["ad"] for p in fuat] == ["Fransa Turkiye"]
    assert [p["ad"] for p in client.get(f"/api/altyazi/projeler?isci={KALAN}", headers=ADMIN).json()["projeler"]] == ["Sabri'nin"]


def test_proje_listesi_gonderen_isci_cevrimici_gorunur(sahte):
    client.post("/api/altyazi/isci/projeler", json={"isci": MAC, "projeler": []}, headers=ISCI)
    assert MAC in [x["ad"] for x in client.get("/api/altyazi/isciler", headers=ADMIN).json()["isciler"]]


def test_proje_listesi_dogrulanir():
    kotu = {**PROJE}; del kotu["draft_id"]
    assert client.post("/api/altyazi/isci/projeler", json={"isci": MAC, "projeler": [kotu]}, headers=ISCI).status_code == 422
    assert client.post("/api/altyazi/isci/projeler", json={"isci": MAC, "projeler": [PROJE] * 501}, headers=ISCI).status_code == 422


# ---- iş akışı
def test_is_acma_ve_ayni_projede_ikinci_aktif_is_409():
    assert is_ac().status_code == 201
    assert is_ac().status_code == 409
    assert is_ac(draft="D2").status_code == 201                    # başka proje
    assert is_ac(isci=KALAN).status_code == 201                    # başka Mac aynı kimlikli proje


def test_isci_yalniz_kendi_isini_alir(sahte):
    is_ac(isci=KALAN)
    assert al(MAC) is None
    j = al(KALAN)
    assert j["asama"] == "dokum" and j["draft_id"] == "D1" and j["id"] == 1


def test_tam_dongu_dokum_duzeltme_yaz_bitti(sahte):
    iid = dokumlu(sahte)
    j = client.get(f"/api/altyazi/isler/{iid}", headers=ADMIN).json()
    assert j["durum"] == "dokum_hazir" and j["parcalar"][0]["t"] == "Merhaba" and j["kelimeler"][0]["w"] == "Merhaba"
    # düzeltme
    yeni = [{"t": "Merhaba dünya", "s": 0.1, "e": 0.9}]
    assert client.put(f"/api/altyazi/isler/{iid}/parcalar", json={"parcalar": yeni}, headers=ADMIN).status_code == 200
    assert client.get(f"/api/altyazi/isler/{iid}", headers=ADMIN).json()["durum"] == "onay_bekliyor"
    # yaz
    assert client.post(f"/api/altyazi/isler/{iid}/yaz", headers=ADMIN).status_code == 200
    g = al()
    assert g["asama"] == "yaz" and g["parcalar"] == yeni and g["imza"] == "abc" and g["kelimeler"][0]["w"] == "Merhaba"
    assert client.post(f"/api/altyazi/isci/isler/{iid}/durum", json={"isci": MAC, "durum": "capcut_acik_bekliyor", "alt_adim": "CapCut açık"}, headers=ISCI).status_code == 200
    assert client.post(f"/api/altyazi/isci/isler/{iid}/bitti", json={"isci": MAC, "yedek_yolu": "/y/p_1", "parca_sayisi": 1}, headers=ISCI).status_code == 200
    s = client.get(f"/api/altyazi/isler/{iid}", headers=ADMIN).json()
    assert s["durum"] == "yazildi" and s["yedek_yolu"] == "/y/p_1"


def test_dokum_olmadan_yaz_olmaz(sahte):
    iid = is_ac().json()["id"]
    assert client.post(f"/api/altyazi/isler/{iid}/yaz", headers=ADMIN).status_code == 409
    assert client.put(f"/api/altyazi/isler/{iid}/parcalar", json={"parcalar": []}, headers=ADMIN).status_code == 409


def test_geri_al_yalniz_yazilmis_ve_yedekli_projede(sahte):
    iid = dokumlu(sahte)
    assert client.post(f"/api/altyazi/isler/{iid}/geri-al", headers=ADMIN).status_code == 409
    client.post(f"/api/altyazi/isler/{iid}/yaz", headers=ADMIN); al()
    client.post(f"/api/altyazi/isci/isler/{iid}/bitti", json={"isci": MAC, "yedek_yolu": "/y/p_1", "parca_sayisi": 1}, headers=ISCI)
    assert client.post(f"/api/altyazi/isler/{iid}/geri-al", headers=ADMIN).status_code == 200
    g = al()
    assert g["asama"] == "geri_al" and g["yedek_yolu"] == "/y/p_1"
    client.post(f"/api/altyazi/isci/isler/{iid}/bitti", json={"isci": MAC}, headers=ISCI)
    assert client.get(f"/api/altyazi/isler/{iid}", headers=ADMIN).json()["durum"] == "geri_alindi"


def test_geri_aldiktan_sonra_ayni_dokum_yeniden_yazilabilir(sahte):
    iid = dokumlu(sahte)
    client.post(f"/api/altyazi/isler/{iid}/yaz", headers=ADMIN); al()
    client.post(f"/api/altyazi/isci/isler/{iid}/bitti", json={"isci": MAC, "yedek_yolu": "/y/p_1"}, headers=ISCI)
    client.post(f"/api/altyazi/isler/{iid}/geri-al", headers=ADMIN); al()
    client.post(f"/api/altyazi/isci/isler/{iid}/bitti", json={"isci": MAC}, headers=ISCI)
    assert client.get(f"/api/altyazi/isler/{iid}", headers=ADMIN).json()["durum"] == "geri_alindi"
    assert client.put(f"/api/altyazi/isler/{iid}/parcalar", json={"parcalar": [{"t": "Yeni", "s": 0, "e": 1}]}, headers=ADMIN).status_code == 200
    assert client.post(f"/api/altyazi/isler/{iid}/yaz", headers=ADMIN).status_code == 200


def test_isci_baskasinin_isine_dokunamaz(sahte):
    iid = is_ac().json()["id"]; al()
    for yol, g in ((f"/api/altyazi/isci/isler/{iid}/kalp", {"isci": KALAN}), (f"/api/altyazi/isci/isler/{iid}/hata", {"isci": KALAN, "hata": "x"}),
                   (f"/api/altyazi/isci/isler/{iid}/bitti", {"isci": KALAN}), (f"/api/altyazi/isci/isler/{iid}/durum", {"isci": KALAN, "durum": "capcut_acik_bekliyor"})):
        assert client.post(yol, json=g, headers=ISCI).status_code == 409, yol


def test_hata_ve_tekrar_yaz(sahte):
    iid = dokumlu(sahte)
    client.post(f"/api/altyazi/isler/{iid}/yaz", headers=ADMIN); al()
    assert client.post(f"/api/altyazi/isci/isler/{iid}/hata", json={"isci": MAC, "hata": "tanınmayan CapCut sürümü 9.9.0"}, headers=ISCI).status_code == 200
    j = client.get(f"/api/altyazi/isler/{iid}", headers=ADMIN).json()
    assert j["durum"] == "hata" and "9.9.0" in j["hata"]
    assert client.post(f"/api/altyazi/isler/{iid}/yaz", headers=ADMIN).status_code == 200       # yeniden denenebilir


def test_dokum_hatali_govde_422(sahte):
    iid = is_ac().json()["id"]; al()
    assert client.post(f"/api/altyazi/isci/isler/{iid}/dokum", json={"isci": MAC}, headers=ISCI).status_code == 422


def test_iptal(sahte):
    iid = is_ac().json()["id"]
    assert client.post(f"/api/altyazi/isler/{iid}/iptal", headers=ADMIN).status_code == 200
    assert client.post(f"/api/altyazi/isler/{iid}/iptal", headers=ADMIN).status_code == 409
    assert is_ac().status_code == 201                              # iptalden sonra yeniden açılır


def test_is_listesi_projeye_gore_suzulur(sahte):
    is_ac(draft="D1"); is_ac(draft="D2")
    assert len(client.get(f"/api/altyazi/isler?isci={MAC}&draft_id=D1", headers=ADMIN).json()["isler"]) == 1
    sade = client.get(f"/api/altyazi/isler?isci={MAC}", headers=ADMIN).json()["isler"][0]
    assert "kelimeler" not in sade and "parcalar" not in sade        # liste hafif


# ---- önizleme
def test_onizleme_yukle_ve_oku(sahte):
    iid = is_ac().json()["id"]; al()
    mp4 = b"\x00\x00\x00\x18ftypmp42" + b"x" * 100
    assert client.post(f"/api/altyazi/isci/isler/{iid}/onizleme?isci={MAC}", content=mp4, headers=ISCI).status_code == 200
    g = client.get(f"/api/altyazi/isler/{iid}/onizleme", headers=ADMIN)
    assert g.status_code == 200 and g.headers["content-type"] == "video/mp4" and g.content == mp4


def test_onizleme_mp4_degilse_ya_da_cok_buyukse_422(sahte):
    iid = is_ac().json()["id"]; al()
    assert client.post(f"/api/altyazi/isci/isler/{iid}/onizleme?isci={MAC}", content=b"<html>", headers=ISCI).status_code == 422
    assert client.post(f"/api/altyazi/isci/isler/{iid}/onizleme?isci={MAC}", content=b"\x00\x00\x00\x18ftyp" + b"x" * (r.ONIZLEME_MAKS_BAYT + 1), headers=ISCI).status_code == 422


def test_onizleme_yoksa_404():
    assert client.get("/api/altyazi/isler/1/onizleme", headers=ADMIN).status_code == 404


def test_yazildiktan_sonra_onizleme_silinir(sahte):
    iid = dokumlu(sahte)
    client.post(f"/api/altyazi/isci/isler/{iid}/onizleme?isci={MAC}", content=b"\x00\x00\x00\x18ftypmp42", headers=ISCI)
    client.post(f"/api/altyazi/isler/{iid}/yaz", headers=ADMIN); al()
    client.post(f"/api/altyazi/isci/isler/{iid}/bitti", json={"isci": MAC, "yedek_yolu": "/y"}, headers=ISCI)
    assert client.get(f"/api/altyazi/isler/{iid}/onizleme", headers=ADMIN).status_code == 404


# ---- sözlük
def test_sozluk_ekle_listele_sil():
    s = client.post("/api/altyazi/sozluk", json={"yanlis": "Kökçü", "dogru": "Kökcü", "tur": "oyuncu"}, headers=ADMIN)
    assert s.status_code == 201
    liste = client.get("/api/altyazi/sozluk", headers=ADMIN).json()["sozluk"]
    assert liste[0]["dogru"] == "Kökcü" and liste[0]["ekleyen"] == "fuat@greysports.tv"
    assert [x["dogru"] for x in client.get("/api/altyazi/isci/sozluk", headers=ISCI).json()["sozluk"]] == ["Kökcü"]
    assert client.delete(f"/api/altyazi/sozluk/{s.json()['id']}", headers=ADMIN).status_code == 200
    assert client.delete("/api/altyazi/sozluk/99", headers=ADMIN).status_code == 404


def test_sozluk_dogrulama():
    assert client.post("/api/altyazi/sozluk", json={"yanlis": "x", "dogru": ""}, headers=ADMIN).status_code == 422
    assert client.post("/api/altyazi/sozluk", json={"yanlis": "x", "dogru": "y", "tur": "bilinmeyen"}, headers=ADMIN).status_code == 422
    assert client.post("/api/altyazi/sozluk", json={"dogru": "Hoffenheim"}, headers=ADMIN).status_code == 201     # yanlışsız = yalnız ipucu


# ---- tablo betiği ile kod aynı değerleri kullanır (1-2 olayı: CHECK kısıtı kodda yeni durum eklenince kayıt koparmasın)
def test_tablo_betigi_butun_durum_ve_asamalari_kapsar():
    from scripts import create_altyazi_tables as t
    ddl = " ".join(t.DDL)
    for d in r.DURUMLAR:
        assert f"'{d}'" in ddl, d
    for a in r.ASAMALAR:
        assert f"'{a}'" in ddl, a
