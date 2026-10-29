"""Kupon liste önbelleği: art arda aynı istek veritabanına bir kez gider, yazma önbelleği boşaltır."""
import pytest

from app.routers import kuponlar as K


@pytest.fixture(autouse=True)
def temiz():
    K.liste_onbellegini_bosalt()
    yield
    K.liste_onbellegini_bosalt()


def _sahte(monkeypatch, sayac):
    def sorgu(sql, params=None):
        sayac.append(sql)
        if "count(*)" in sql:
            return [{"n": 0}]
        return []
    monkeypatch.setattr(K, "execute_query", sorgu)


def test_ikinci_cagri_veritabanina_gitmez(monkeypatch):
    sayac = []
    _sahte(monkeypatch, sayac)
    f = {"durum": "yayinda", "limit": 20, "offset": 0}
    K.db_liste(f)
    ilk = len(sayac)
    assert ilk > 0
    K.db_liste(dict(f))
    assert len(sayac) == ilk


def test_farkli_filtre_ayri_onbellek(monkeypatch):
    sayac = []
    _sahte(monkeypatch, sayac)
    K.db_liste({"durum": "yayinda", "limit": 20, "offset": 0})
    ilk = len(sayac)
    K.db_liste({"durum": "taslak", "limit": 20, "offset": 0})
    assert len(sayac) > ilk


def test_yazma_onbellegi_bosaltir(monkeypatch):
    sayac = []
    _sahte(monkeypatch, sayac)
    f = {"durum": "yayinda", "limit": 20, "offset": 0}
    K.db_liste(f)
    ilk = len(sayac)

    @K._yazma
    def yaz():
        return 1

    yaz()
    K.db_liste(f)
    assert len(sayac) > ilk


def test_onbellekten_gelen_veri_kopyadir(monkeypatch):
    def sorgu(sql, params=None):
        if "count(*)" in sql:
            return [{"n": 1}]
        if "kupon_bacaklari" in sql:
            return []
        return [{"id": 1}]
    monkeypatch.setattr(K, "execute_query", sorgu)
    monkeypatch.setattr(K, "_kupon_satiri", lambda s, b: {"id": s["id"], "bacaklar": b})
    f = {"limit": 20, "offset": 0}
    a, _ = K.db_liste(f)
    a[0]["bozuk"] = True
    b, _ = K.db_liste(f)
    assert "bozuk" not in b[0]
