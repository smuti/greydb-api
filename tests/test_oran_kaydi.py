"""Oran geçmişi: ön maç işi iddaa'dan oranı çektiğinde, sırası geldiyse ham olayı greydb.odds_snapshots'a yeni satır olarak yazar (otomatik kayıt).
Sıklık maça kalan süreye göre: >24 saat 12 saatte bir, 6–24 saat 3 saatte bir, son 6 saat saatte bir. Kuru çalıştırmada yazılmaz."""
from datetime import datetime, timedelta

from app.services import skorjin_prematch as P
from app.services.skorjin_prematch import Kaynaklar, hazirla, oran_kaydi_gerekli
from tests.test_skorjin_prematch_hazirla import SIMDI, Sahte, mac

K = datetime(2026, 10, 25, 17, 0)


def test_oran_kaydi_gerekli_araliklar():
    assert oran_kaydi_gerekli(K, K - timedelta(hours=40), None)                                   # hiç kayıt yok
    assert not oran_kaydi_gerekli(K, K - timedelta(hours=40), K - timedelta(hours=45))             # >24 saat: 12 saatte bir
    assert oran_kaydi_gerekli(K, K - timedelta(hours=30), K - timedelta(hours=42))
    assert not oran_kaydi_gerekli(K, K - timedelta(hours=10), K - timedelta(hours=12))             # 6–24 saat: 3 saatte bir
    assert oran_kaydi_gerekli(K, K - timedelta(hours=10), K - timedelta(hours=13))
    assert not oran_kaydi_gerekli(K, K - timedelta(hours=2), K - timedelta(minutes=150))           # son 6 saat: saatte bir
    assert oran_kaydi_gerekli(K, K - timedelta(hours=2), K - timedelta(hours=3))
    assert oran_kaydi_gerekli(K, K - timedelta(hours=2), K - timedelta(hours=2, minutes=57))       # 30 dk zamanlama oynaması: 5 dk tolerans (57 dk önce)
    assert not oran_kaydi_gerekli(K, K + timedelta(minutes=1), None)                               # başlamış maç


def _kaynak(sahte, son=None):
    yazilan = []
    k = sahte.kaynaklar()
    return Kaynaklar(**{**k.__dict__, "mevcut": lambda ids: {1001},
                        "son_oran_kaydi": lambda eid: son, "oran_kaydet": lambda m, olay: yazilan.append((m["iddaa_event_id"], olay))}), yazilan


def test_oran_kapsaminda_kayit_yazilir():
    k, yazilan = _kaynak(Sahte(maclar=[mac()]))
    r = hazirla("oran", False, k, SIMDI, "harman-v1")
    assert len(yazilan) == 1 and yazilan[0][0] == 555 and "m" in yazilan[0][1] and r["oran_kaydi"] == 1


def test_kuru_calismada_yazilmaz():
    k, yazilan = _kaynak(Sahte(maclar=[mac()]))
    r = hazirla("oran", True, k, SIMDI, "harman-v1")
    assert yazilan == [] and r["oran_kaydi"] == 0


def test_yakin_zamanda_kayit_varsa_yazilmaz():
    k, yazilan = _kaynak(Sahte(maclar=[mac()]), son=SIMDI - timedelta(hours=1))                  # maça ~77 saat var: 12 saat aralık
    assert hazirla("oran", False, k, SIMDI, "harman-v1")["oran_kaydi"] == 0 and yazilan == []


def test_iddaa_hatasinda_kayit_yok_ve_is_surer():
    def hata(eid):
        raise RuntimeError("iddaa yok")
    s = Sahte(maclar=[mac()])
    k, yazilan = _kaynak(s)
    k = Kaynaklar(**{**k.__dict__, "iddaa": hata})
    r = hazirla("oran", False, k, SIMDI, "harman-v1")
    assert yazilan == [] and r["iddaa_hatasi"] == 1


def test_kayit_hatasi_isi_durdurmaz():
    k, _ = _kaynak(Sahte(maclar=[mac()]))
    def patla(m, olay):
        raise RuntimeError("db")
    k = Kaynaklar(**{**k.__dict__, "oran_kaydet": patla})
    r = hazirla("oran", False, k, SIMDI, "harman-v1")
    assert r["hazirlanan"] == 1 and r["oran_kaydi"] == 0 and any("oran_kaydi" in h["hata"] for h in r["hatalar"])


def test_gunluk_kapsamda_da_yazilir():
    k, yazilan = _kaynak(Sahte(maclar=[mac()]))
    hazirla("gunluk", False, k, SIMDI, "harman-v1")
    assert len(yazilan) == 1


def test_sql_otomatik_isaret():
    assert "greydb.odds_snapshots" in P.ORAN_KAYDI_SQL and P.ORAN_KAYDI_KAYNAK == "otomatik:prematch"
    assert "created_by_email = :kaynak" in P.SON_ORAN_KAYDI_SQL


def test_gercek_kaynaklarda_oran_kaydi_bagli(monkeypatch):
    import app.services.db as db
    sorgular = []
    monkeypatch.setattr(db, "execute_query", lambda sql, p=None: sorgular.append(("q", sql, p)) or [{"son": datetime(2026, 10, 25, 9, 0, tzinfo=__import__("datetime").timezone.utc)}])
    monkeypatch.setattr(db, "execute_insert", lambda sql, p=None: sorgular.append(("i", sql, p)))
    k = P.gercek_kaynaklar()
    assert k.son_oran_kaydi(555) == datetime(2026, 10, 25, 9, 0)                                   # UTC, naive
    k.oran_kaydet(mac(), {"m": [{"st": 1}, {"st": 101}]})
    tur, sql, p = sorgular[-1]
    assert tur == "i" and "odds_snapshots" in sql and p["eid"] == 555 and p["n"] == 2 and p["kaynak"] == "otomatik:prematch" and p["ev"] == "Yıldız FK"
