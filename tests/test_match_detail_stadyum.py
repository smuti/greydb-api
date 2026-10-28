"""Maç detayında stadyum: match_context'ten ad ve kapasite; kayıt/ad yoksa ya da sorgu hata verirse None (sayfa çökmez)."""
from app.routers import match_detail as md


def test_stadyum_ad_ve_kapasite(monkeypatch):
    monkeypatch.setattr(md, "execute_query", lambda sql, p=None: [{"stadium": " Ramón Sánchez-Pizjuán ", "stadium_capacity": 43883}])
    assert md._stadyum({"id": 1}) == {"ad": "Ramón Sánchez-Pizjuán", "kapasite": 43883}


def test_stadyum_yoksa_none(monkeypatch):
    monkeypatch.setattr(md, "execute_query", lambda sql, p=None: [])
    assert md._stadyum({"id": 1}) is None
    monkeypatch.setattr(md, "execute_query", lambda sql, p=None: [{"stadium": "", "stadium_capacity": None}])
    assert md._stadyum({"id": 1}) is None


def test_stadyum_sorgu_hatasi_none(monkeypatch):
    def hata(sql, p=None):
        raise RuntimeError("tablo yok")
    monkeypatch.setattr(md, "execute_query", hata)
    assert md._stadyum({"id": 1}) is None
