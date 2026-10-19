"""Toplayıcı: istatistik servisine all_comps=1 gider (tüm turnuvalar); iç/dış saha sorguları maçın turnuvasıyla sınırlıdır."""
from app.services import yayin_toplayici as T


def test_takim_istatistigi_tum_turnuvalar_ister(monkeypatch):
    cagrilar = []
    monkeypatch.setattr(T, "_get", lambda url, **p: cagrilar.append((url.rsplit("/", 1)[-1], p)) or {"ok": 1})
    T.takim_istatistigi(37)
    assert cagrilar == [("stats", {"all_comps": 1}), ("type", {"all_comps": 1}), ("goal-timing", {"all_comps": 1})]


def test_ic_dis_sorgulari_turnuvaya_sinirli_form_ve_kart_degil():
    for sql in (T._SEZON_EV, T._SEZON_DEP):
        assert "l.fotmob_league_id = :lig" in sql and "JOIN public.leagues l" in sql
    assert ":lig" not in T._FORM and ":lig" not in T._H2H          # form ve karşılıklı geçmiş tüm turnuvalar
