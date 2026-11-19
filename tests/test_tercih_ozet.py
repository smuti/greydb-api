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


# ---- oran kapsamı: hangi ligde tercihler oransız yazılıyor (LaLiga örneği) ----
def lig(ad, mac, n, oransiz):
    return dict(lig_ad=ad, mac=mac, n=n, oransiz=oransiz)


def test_oran_kapsami_oransiz_yuzde_ve_siralama():
    k = t.oran_kapsami([lig("Premier League", 8, 800, 40), lig("LaLiga", 4, 400, 400), lig("Serie A", 5, 500, 100)])
    assert [x["lig"] for x in k] == ["LaLiga", "Serie A", "Premier League"]                     # oransız yüzdesi azalan
    assert k[0]["oransiz_yuzde"] == 100.0 and k[0]["oran_yok"] is True
    assert k[1]["oransiz_yuzde"] == 20.0 and k[1]["oran_yok"] is False
    assert k[2]["mac"] == 8 and k[2]["tercih"] == 800


def test_oran_kapsami_bos_ve_adsiz_lig():
    assert t.oran_kapsami([]) == []
    assert t.oran_kapsami([lig(None, 1, 10, 0)])[0]["lig"] == "bilinmiyor"


def test_oran_kapsami_sql_oransiz_null_ya_da_bir_ve_alti():
    assert "oran IS NULL OR oran <= 1.0" in t.ORAN_KAPSAMI_SQL and "GROUP BY lig_ad" in t.ORAN_KAPSAMI_SQL and "kickoff >= :bas" in t.ORAN_KAPSAMI_SQL


def test_ozet_getir_oran_kapsamini_icerir(monkeypatch):
    cevaplar = {"OZET": [s()], "KAPSAM": [lig("LaLiga", 4, 400, 400)]}

    def sorgu(sql, params=None):
        if "pencere = 'kapanis'" in sql:
            return [onay_satiri()]
        return cevaplar["KAPSAM"] if "GROUP BY lig_ad" in sql else cevaplar["OZET"]
    monkeypatch.setattr("app.services.db.execute_query", sorgu)
    o = t.ozet_getir(7)
    assert o["oran_kapsami"][0]["lig"] == "LaLiga" and o["tum"]["tercih"] == 10
    assert o["piyasa_onayi"]["etiket"]["onay"]["tercih"] == 1


# ---- piyasa onayı: açılıştan kapanışa oran modelin yönüne mi gitti ----
def onay_satiri(kategori="Toplam gol", secim="Gol Üst 2.5", olasilik_ac=0.60, oran_ac=2.00, oran_ka=1.80, sonuc=True):
    return dict(kategori=kategori, secim=secim, olasilik_ac=olasilik_ac, oran_ac=oran_ac, oran_ka=oran_ka, sonuc=sonuc)


def test_piyasa_onayi_ima_marjin_yarisini_duser():
    assert t.ima(2.0) == 1 / (2.0 * (1 + t.MARJ_YARI))
    assert t.ima(1.0) > t.ima(2.0)


def test_piyasa_onayi_onay_ret_sabit_model_yakin():
    # açılış 2,00 → ima ~%46,7; model %60 → model yukarıda
    assert t.piyasa_onayi(0.60, 2.00, 1.80)["etiket"] == "onay"            # oran düştü: piyasa modele doğru
    assert t.piyasa_onayi(0.60, 2.00, 2.30)["etiket"] == "ret"             # oran çıktı: modelden uzaklaştı
    assert t.piyasa_onayi(0.60, 2.00, 2.02)["etiket"] == "sabit"           # ~0,5 puan: hareket sayılmaz
    assert t.piyasa_onayi(0.48, 2.00, 1.50)["etiket"] == "model_yakin"     # model açılışa yakın: yön yok
    # model aşağıda: oran çıkarsa onay
    e = t.piyasa_onayi(0.30, 2.00, 2.40)
    assert e["etiket"] == "onay" and e["model_yon"] == "asagi" and e["hareket"] < 0


def test_piyasa_onayi_ozet_grup_etiket_ve_model_yonunde_fark():
    satirlar = [onay_satiri(sonuc=True), onay_satiri(sonuc=False), onay_satiri(kategori="Maç sonucu", secim="1", oran_ka=2.40, sonuc=True),
                onay_satiri(kategori="Toplam korner", secim="Korner Üst 8.5", olasilik_ac=0.47, oran_ka=1.90, sonuc=True)]
    o = t.piyasa_onayi_ozet(satirlar)
    h = {(x["grup"], x["etiket"], x["model_yon"]): x for x in o["hucreler"]}
    gol = h[("gol", "onay", "yukari")]
    assert (gol["tercih"], gol["tuttu"], gol["tutma_yuzde"]) == (2, 1, 50.0)
    assert gol["kapanis_ima_ort"] == round(100 * t.ima(1.80), 1)
    assert gol["model_yonunde_fark"] == round(100 * (0.5 - t.ima(1.80)), 1)
    assert gol["yatirilan"] == 100.0 and gol["donen"] == 90.0                # kapanış oranından eşit tutar: 1,80 × 50
    assert h[("taraf", "ret", "yukari")]["tuttu"] == 1
    assert h[("korner", "model_yakin", None)]["model_yonunde_fark"] == 0.0
    assert set(o["etiket"]) == {"onay", "ret", "model_yakin"} and o["etiket"]["onay"]["tercih"] == 2
    assert t.piyasa_onayi_ozet([])["hucreler"] == []
    assert t.piyasa_onayi_ozet([onay_satiri(oran_ac=float("nan")), onay_satiri(oran_ka=1.0)])["hucreler"] == []      # NaN ve kapalı oran sayılmaz


def test_piyasa_onayi_sql_acilis_kapanis_esler_ve_sonuclanmis():
    q = t.ONAY_SQL
    assert "a.pencere = 'acilis'" in q and "k.pencere = 'kapanis'" in q and "k.sonuc IS NOT NULL" in q
    assert "a.oran > 1.0" in q and "k.oran > 1.0" in q and "a.kickoff >= :bas" in q
    assert "k.kategori = a.kategori AND k.secim = a.secim" in q
    assert "a.oran <> 'NaN'" in q and "k.oran <> 'NaN'" in q


def test_ozet_sql_nan_oran_para_hesabina_girmez():
    assert t.OZET_SQL.count("oran <> 'NaN'") == 4 and "oran = 'NaN'" in t.ORAN_KAPSAMI_SQL
