"""/api/sayac: yetki (admin ağ geçidi / işçi anahtarı), görsel doğrulama, 'Üret' kuralları, kuyruk (atomik alma, kalp atışı, iptal). Veritabanı sahte."""
import io
from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from PIL import Image

from app.main import app
from app.routers import sayac as r

client = TestClient(app)
ADMIN = {"X-Sunucu-Anahtari": "gizli", "X-Yetki": "admin", "X-Kullanici-Email": "editor@greysports.tv"}
GW = {"X-Sunucu-Anahtari": "gizli"}
ISCI = {"X-Isci-Anahtari": "isci-gizli"}


def png(w=400, h=600, seffaf=True) -> bytes:
    im = Image.new("RGBA", (w, h), (200, 30, 30, 0 if seffaf else 255))
    for x in range(w // 3, 2 * w // 3):           # ortada opak gövde
        for y in range(h // 4, h):
            im.putpixel((x, y), (200, 30, 30, 255))
    b = io.BytesIO(); im.save(b, "PNG"); return b.getvalue()


AKTIF = ("onizleme_bekliyor", "onizleme_calisiyor", "onizleme_hazir", "bekliyor", "calisiyor")


class Sahte:
    def __init__(self):
        self.oyuncular, self.isler, self.isciler, self.sayac, self.isayac, self.gorseller = {}, {}, {}, 0, 0, {}
        self.onizlemeler = {}
        self.renkler = {}

    def oyuncu_ekle(self, o):
        for x in self.oyuncular.values():
            if x["fotmob_player_id"] == o["fotmob_player_id"] and x["poz"] == o["poz"]:
                x.update({k: v for k, v in o.items() if k != "gorsel"}); self.gorseller[x["id"]] = o["gorsel"]; return x
        self.sayac += 1
        x = {**{k: v for k, v in o.items() if k != "gorsel"}, "id": self.sayac, "gorsel_tur": "image/png", "yildiz": False, "aktif": True, "olusturma": datetime(2026, 10, 22, 12)}
        self.oyuncular[x["id"]] = x; self.gorseller[x["id"]] = o["gorsel"]
        return x

    def oyuncular_listele(self, takim):
        return [x for x in self.oyuncular.values() if x["aktif"] and (takim is None or x["fotmob_team_id"] == takim)]

    def oyuncu_getir(self, oid):
        return self.oyuncular.get(oid)

    def oyuncu_gorseli(self, oid):
        x = self.oyuncular.get(oid)
        return {"gorsel": self.gorseller[oid], "gorsel_tur": "image/png"} if x else None

    def takim_yildizi(self, t):
        return next((x for x in self.oyuncular.values() if x["fotmob_team_id"] == t and x["yildiz"] and x["aktif"]), None)

    def oyuncu_guncelle(self, oid, g):
        x = self.oyuncular.get(oid)
        if not x:
            return False
        if g.get("yildiz"):
            for y in self.oyuncular.values():
                if y["fotmob_team_id"] == x["fotmob_team_id"]:
                    y["yildiz"] = False
        x.update(g)
        if g.get("aktif") is False:
            x["yildiz"] = False
        return True

    def takim_renkleri(self, idler):
        return {i: self.renkler[i] for i in idler if i in self.renkler}

    def is_ekle(self, j):
        if any(x["fotmob_match_id"] == j["fotmob_match_id"] and x["durum"] in AKTIF for x in self.isler.values()):
            return None
        self.isayac += 1
        self.isler[self.isayac] = {**j, "id": self.isayac, "durum": "onizleme_bekliyor", "isci": None, "deneme": 0, "hata": None, "dosya_yolu": None, "olusturma": datetime(2026, 10, 22, 12), "baslama": None, "bitis": None, "kalp_atisi": None}
        return {"id": self.isayac}

    def isler_listele(self, limit=50):
        return list(self.isler.values())

    def is_getir(self, i):
        return self.isler.get(i)

    def is_iptal_et(self, i):
        j = self.isler.get(i)
        if j and j["durum"] in AKTIF:
            j["durum"] = "iptal"; return True
        return False

    def isciler_listele(self):
        return [{"ad": a, "son_gorulme": datetime(2026, 10, 22, 12), "surum": s} for a, s in self.isciler.items()]

    def isci_gor(self, ad, surum):
        self.isciler[ad] = surum

    def kuyruga_geri_al(self):
        self.geri_alindi = True

    def is_al(self, isci):
        for d in ("onizleme_bekliyor", "bekliyor"):
            for j in sorted(self.isler.values(), key=lambda x: x["id"]):
                if j["durum"] == d:
                    j.update({"durum": "onizleme_calisiyor" if d == "onizleme_bekliyor" else "calisiyor", "isci": isci, "deneme": j["deneme"] + 1})
                    return {**j, "onizleme_asamasi": d == "onizleme_bekliyor"}
        return None

    def onizleme_yaz(self, i, isci, jpeg):
        j = self._benim(i, isci, ("onizleme_calisiyor",))
        if j: j.update({"durum": "onizleme_hazir"}); self.onizlemeler[i] = jpeg
        return bool(j)

    def onizleme_getir(self, i):
        return self.onizlemeler.get(i)

    def is_onayla(self, i):
        j = self.isler.get(i)
        if j and j["durum"] == "onizleme_hazir":
            j.update({"durum": "bekliyor", "isci": None, "deneme": 0}); return True
        return False

    def _benim(self, i, isci, durumlar=("calisiyor", "onizleme_calisiyor")):
        j = self.isler.get(i)
        return j if j and j["isci"] == isci and j["durum"] in durumlar else None

    def kalp_yaz(self, i, isci):
        return bool(self._benim(i, isci))

    def bitti_yaz(self, i, isci, yol):
        j = self._benim(i, isci, ("calisiyor",))
        if j: j.update({"durum": "hazir", "dosya_yolu": yol})
        return bool(j)

    def hata_yaz(self, i, isci, h):
        j = self._benim(i, isci)
        if j: j.update({"durum": "hata", "hata": h})
        return bool(j)


@pytest.fixture(autouse=True)
def sahte(monkeypatch):
    s = Sahte()
    monkeypatch.setenv("SUNUCU_ANAHTARI", "gizli")
    monkeypatch.setenv("SAYAC_ISCI_ANAHTARI", "isci-gizli")
    for ad in ("oyuncu_ekle", "oyuncular_listele", "oyuncu_getir", "oyuncu_gorseli", "takim_yildizi", "oyuncu_guncelle", "takim_renkleri", "is_ekle", "isler_listele",
               "is_getir", "onizleme_yaz", "onizleme_getir", "is_onayla", "is_iptal_et", "isciler_listele", "isci_gor", "kuyruga_geri_al", "is_al", "kalp_yaz", "bitti_yaz", "hata_yaz"):
        monkeypatch.setattr(r, ad, getattr(s, ad))
    return s


def yukle(pid=1, takim=10, ad="Oyuncu", veri=None, poz="varsayilan", baslik=ADMIN):
    return client.put(f"/api/sayac/oyuncular?fotmob_player_id={pid}&fotmob_team_id={takim}&ad={ad}&poz={poz}", content=veri if veri is not None else png(), headers=baslik)


def hazirla(sahte):
    sahte.renkler = {10: "#1a2b3c"}
    a = yukle(1, 10, "Ev Yıldız").json()["id"]; b = yukle(2, 20, "Dep Yıldız").json()["id"]
    client.patch(f"/api/sayac/oyuncular/{a}", json={"yildiz": True}, headers=ADMIN)
    client.patch(f"/api/sayac/oyuncular/{b}", json={"yildiz": True}, headers=ADMIN)
    return a, b


MAC = {"fotmob_match_id": 4711, "ev_team_id": 10, "dep_team_id": 20, "ev_ad": "Trabzonspor", "dep_ad": "Hearts", "lig": "UEFA Konferans Ligi", "mac_zamani": "2026-10-22T16:45:00.000Z"}


# ---- saf yardımcılar
def test_turkce_buyuk_ve_kunye():
    assert r.tr_buyuk("UEFA Şampiyonlar Ligi") == "UEFA ŞAMPİYONLAR LİGİ"
    assert r.tr_buyuk("Süper Lig") == "SÜPER LİG"
    k = r.kunye(datetime(2026, 10, 22, 16, 45), "UEFA Konferans Ligi")
    assert k == {"lig": "UEFA KONFERANS LİGİ", "tarih": "22 EKİM", "saat": "19:45"}     # UTC+3
    assert r.kunye(datetime(2026, 10, 22, 22, 30), "x")["tarih"] == "23 EKİM"            # gün TSİ'ye göre kayar


def test_tohum_maca_ozgu_ve_kararli():
    assert r.tohumlar(4711) == r.tohumlar(4711)
    assert r.tohumlar(4711) != r.tohumlar(4712)
    assert r.tohumlar(4711)["evSeed"] != r.tohumlar(4711)["depSeed"]


def test_parilti():
    assert r.parilti("#1a2b3c", "x") == "rgba(26,43,60,0.6)"
    assert r.parilti(None, "x") == "x" and r.parilti("kırmızı", "x") == "x"


# ---- yetki
def test_oyuncu_yukle_yetkisiz():
    assert yukle(baslik={}).status_code == 401
    assert yukle(baslik=GW).status_code == 403
    assert client.get("/api/sayac/isler").status_code == 401
    assert client.post("/api/sayac/isci/al", json={"isci": "mac1"}).status_code == 401
    assert client.post("/api/sayac/isci/al", json={"isci": "mac1"}, headers=ADMIN).status_code == 401   # admin anahtarı işçi anahtarı yerine geçmez


def test_isci_anahtari_tanimsizsa_kapali(monkeypatch):
    monkeypatch.delenv("SAYAC_ISCI_ANAHTARI")
    assert client.post("/api/sayac/isci/al", json={"isci": "mac1"}, headers=ISCI).status_code == 503


# ---- görsel doğrulama
def test_gorsel_dogrulama():
    assert yukle().status_code == 200
    assert yukle(pid=2, veri=png(seffaf=False)).status_code == 422          # şeffaf değil
    assert yukle(pid=3, veri=png(w=300, h=500)).status_code == 422          # çok dar
    assert yukle(pid=4, veri=b"JFIF-degil-png").status_code == 422
    assert "şeffaf" in yukle(pid=5, veri=png(seffaf=False)).json()["detail"]
    assert yukle(pid=6, veri=b"\x89PNG\r\n\x1a\n" + b"0" * (9 * 1024 * 1024)).status_code == 422


def test_yukleme_aspect_ve_ayni_poz_gunceller(sahte):
    j = yukle(1, 10, "Ali").json()
    assert j["genislik"] == 400 and j["yukseklik"] == 600 and j["aspect"] == pytest.approx(0.6667, abs=1e-3)
    yukle(1, 10, "Ali", png(500, 500))
    assert len(sahte.oyuncular) == 1 and sahte.oyuncular[1]["genislik"] == 500
    assert yukle(1, 10, "Ali", poz="kutlama").json()["id"] == 2              # yeni poz, yeni kayıt


def test_tek_yildiz_kurali(sahte):
    a = yukle(1, 10).json()["id"]; b = yukle(2, 10).json()["id"]
    client.patch(f"/api/sayac/oyuncular/{a}", json={"yildiz": True}, headers=ADMIN)
    client.patch(f"/api/sayac/oyuncular/{b}", json={"yildiz": True}, headers=ADMIN)
    assert not sahte.oyuncular[a]["yildiz"] and sahte.oyuncular[b]["yildiz"]
    assert client.patch("/api/sayac/oyuncular/99", json={"yildiz": True}, headers=ADMIN).status_code == 404
    assert client.patch(f"/api/sayac/oyuncular/{a}", json={"kafa_orani": 2}, headers=ADMIN).status_code == 422


# ---- "Üret"
def test_uret_yildizlari_otomatik_secer(sahte):
    a, b = hazirla(sahte)
    res = client.post("/api/sayac/isler", json=MAC, headers=ADMIN)
    assert res.status_code == 201, res.text
    j = sahte.isler[res.json()["id"]]
    assert (j["ev_oyuncu_id"], j["dep_oyuncu_id"]) == (a, b) and j["olusturan"] == "editor@greysports.tv"
    g = j["girdi"]
    assert (g["lig"], g["tarih"], g["saat"]) == ("UEFA KONFERANS LİGİ", "22 EKİM", "19:45")
    assert g["evGlow"] == "rgba(26,43,60,0.6)" and g["depGlow"] == r.VARSAYILAN_PARILTI[1]      # takım rengi yoksa varsayılan


def test_uret_rakip_gorseli_yoksa_reddeder(sahte):
    sahte.renkler = {}
    a = yukle(1, 10).json()["id"]
    client.patch(f"/api/sayac/oyuncular/{a}", json={"yildiz": True}, headers=ADMIN)
    res = client.post("/api/sayac/isler", json=MAC, headers=ADMIN)
    assert res.status_code == 422 and "deplasman" in res.json()["detail"]
    assert sahte.isler == {}


def test_uret_yanlis_takim_ve_ayni_takim(sahte):
    a, b = hazirla(sahte)
    assert client.post("/api/sayac/isler", json={**MAC, "ev_oyuncu_id": b}, headers=ADMIN).status_code == 422     # ev için rakip oyuncusu
    assert client.post("/api/sayac/isler", json={**MAC, "dep_team_id": 10}, headers=ADMIN).status_code == 422


def test_uret_cift_tik_409(sahte):
    hazirla(sahte)
    assert client.post("/api/sayac/isler", json=MAC, headers=ADMIN).status_code == 201
    assert client.post("/api/sayac/isler", json=MAC, headers=ADMIN).status_code == 409


# ---- kuyruk
JPEG = b"\xff\xd8\xff\xe0" + b"0" * 100


def onayla(sahte, iid, isci="mac1"):
    """Önizleme aşamasını geç: al → önizleme yükle → admin onayı."""
    al = client.post("/api/sayac/isci/al", json={"isci": isci}, headers=ISCI).json()["is"]
    assert al["asama"] == "onizleme"
    assert client.post(f"/api/sayac/isci/isler/{iid}/onizleme?isci={isci}", content=JPEG, headers=ISCI).status_code == 200
    assert client.post(f"/api/sayac/isler/{iid}/onayla", headers=ADMIN).status_code == 200


def test_onizleme_akisi(sahte):
    a, b = hazirla(sahte)
    iid = client.post("/api/sayac/isler", json=MAC, headers=ADMIN).json()["id"]
    assert sahte.isler[iid]["durum"] == "onizleme_bekliyor"
    assert client.post(f"/api/sayac/isler/{iid}/onayla", headers=ADMIN).status_code == 409           # önizleme yokken onaylanamaz
    al = client.post("/api/sayac/isci/al", json={"isci": "mac1"}, headers=ISCI).json()["is"]
    assert al["asama"] == "onizleme" and al["id"] == iid
    assert client.post(f"/api/sayac/isci/isler/{iid}/onizleme?isci=mac2", content=JPEG, headers=ISCI).status_code == 409   # başkasının işi
    assert client.post(f"/api/sayac/isci/isler/{iid}/onizleme?isci=mac1", content=b"png degil", headers=ISCI).status_code == 422
    assert client.post(f"/api/sayac/isci/isler/{iid}/onizleme?isci=mac1", content=JPEG, headers=ISCI).status_code == 200
    assert client.get(f"/api/sayac/isler/{iid}/onizleme", headers=ADMIN).headers["content-type"] == "image/jpeg"
    assert client.get(f"/api/sayac/isler/{iid}/onizleme", headers=ISCI).status_code == 401
    assert client.post("/api/sayac/isci/al", json={"isci": "mac2"}, headers=ISCI).json()["is"] is None   # onay gelmeden video başlamaz
    assert client.post(f"/api/sayac/isler/{iid}/onayla", headers=ADMIN).status_code == 200
    v = client.post("/api/sayac/isci/al", json={"isci": "mac2"}, headers=ISCI).json()["is"]
    assert v["asama"] == "video" and v["id"] == iid


def test_isci_atomik_alir_ve_bitirir(sahte):
    a, b = hazirla(sahte)
    iid = client.post("/api/sayac/isler", json=MAC, headers=ADMIN).json()["id"]
    onayla(sahte, iid)
    al = client.post("/api/sayac/isci/al", json={"isci": "mac1", "surum": "abc"}, headers=ISCI).json()["is"]
    assert al["id"] == iid and al["ev"]["oyuncu"]["id"] == a and al["dep"]["oyuncu"]["aspect"] == pytest.approx(0.6667, abs=1e-3)
    assert al["girdi"]["tarih"] == "22 EKİM" and sahte.isciler["mac1"] == "abc"
    assert client.post("/api/sayac/isci/al", json={"isci": "mac2"}, headers=ISCI).json()["is"] is None          # ikinci Mac aynı işi alamaz
    assert client.post(f"/api/sayac/isci/isler/{iid}/kalp", json={"isci": "mac2"}, headers=ISCI).status_code == 409   # başkasının işi
    assert client.post(f"/api/sayac/isci/isler/{iid}/kalp", json={"isci": "mac1"}, headers=ISCI).status_code == 200
    assert client.post(f"/api/sayac/isci/isler/{iid}/bitti", json={"isci": "mac1", "dosya_yolu": "/Users/x/sayac/trabzonspor-hearts.mp4"}, headers=ISCI).status_code == 200
    j = sahte.isler[iid]
    assert j["durum"] == "hazir" and j["dosya_yolu"].endswith(".mp4")
    assert client.post(f"/api/sayac/isci/isler/{iid}/bitti", json={"isci": "mac1", "dosya_yolu": "/x.mp4"}, headers=ISCI).status_code == 409   # çift bildirim
    # bitince aynı maç için yeniden üretilebilir
    assert client.post("/api/sayac/isler", json=MAC, headers=ADMIN).status_code == 201


def test_isci_hata_bildirir(sahte):
    hazirla(sahte)
    iid = client.post("/api/sayac/isler", json=MAC, headers=ADMIN).json()["id"]
    onayla(sahte, iid)
    client.post("/api/sayac/isci/al", json={"isci": "mac1"}, headers=ISCI)
    assert client.post(f"/api/sayac/isci/isler/{iid}/hata", json={"isci": "mac1", "hata": "ffmpeg bulunamadı"}, headers=ISCI).status_code == 200
    assert sahte.isler[iid]["durum"] == "hata" and sahte.isler[iid]["hata"] == "ffmpeg bulunamadı"


def test_iptal_edilen_isi_isci_bitiremez(sahte):
    hazirla(sahte)
    iid = client.post("/api/sayac/isler", json=MAC, headers=ADMIN).json()["id"]
    onayla(sahte, iid)
    client.post("/api/sayac/isci/al", json={"isci": "mac1"}, headers=ISCI)
    assert client.post(f"/api/sayac/isler/{iid}/iptal", headers=ADMIN).status_code == 200
    assert client.post(f"/api/sayac/isci/isler/{iid}/bitti", json={"isci": "mac1", "dosya_yolu": "/x.mp4"}, headers=ISCI).status_code == 409
    assert client.post(f"/api/sayac/isler/{iid}/iptal", headers=ADMIN).status_code == 409


def test_liste_ve_isci_gorsel(sahte):
    a, _ = hazirla(sahte)
    client.post("/api/sayac/isler", json=MAC, headers=ADMIN)
    client.post("/api/sayac/isci/al", json={"isci": "mac1"}, headers=ISCI)
    l = client.get("/api/sayac/isler", headers=ADMIN).json()
    assert len(l["isler"]) == 1 and l["isciler"][0]["ad"] == "mac1"
    assert client.get(f"/api/sayac/oyuncular/{a}/gorsel", headers=ISCI).status_code == 200      # işçi görseli indirebilir
    assert client.get(f"/api/sayac/oyuncular/{a}/gorsel", headers=ADMIN).headers["content-type"] == "image/png"
    assert client.get(f"/api/sayac/oyuncular/{a}/gorsel").status_code == 401
