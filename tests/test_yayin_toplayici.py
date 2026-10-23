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


# ---- lig adı eşleme: FotMob'un adı ile analiz servisindeki ad farklı olabiliyor (yayın sayfasında analiz kartı boş kalıyordu) ----
LIGLER = [{"id": 21, "name": "First Division A"}, {"id": 20, "name": "Primeira Liga"}, {"id": 28, "name": "Turkish 1. Lig"},
          {"id": 6, "name": "LaLiga"}, {"id": 5, "name": "Premier League"}, {"id": 13, "name": "Premier League"}]
TAKIMLAR = {
    21: [{"id": 326, "name": "Kortrijk"}, {"id": 317, "name": "KV Mechelen"}],
    20: [{"id": 301, "name": "Vitoria de Guimaraes"}, {"id": 308, "name": "Maritimo"}],
    28: [{"id": 416, "name": "Boluspor"}, {"id": 425, "name": "Van Spor Kulübü"}],
    6: [{"id": 70, "name": "Deportivo Alaves"}, {"id": 340, "name": "Málaga"}],
    5: [{"id": 1, "name": "Arsenal"}, {"id": 2, "name": "Chelsea"}],
    13: [{"id": 91, "name": "Celtic"}, {"id": 92, "name": "Rangers"}],
}


def _sahte(monkeypatch):
    monkeypatch.setattr(T, "_analiz_ligleri", lambda: LIGLER)
    monkeypatch.setattr(T, "_lig_takimlari", lambda i: TAKIMLAR[i])


def _ids(lig, ev, dep):
    e, d = T.analiz_takimlari(lig, ev, dep)
    return (e["id"], d["id"]) if e and d else None


def test_fotmob_lig_adlari_analiz_ligine_cozulur(monkeypatch):
    _sahte(monkeypatch)
    assert _ids("Belgian Pro League", "Kortrijk", "KV Mechelen") == (326, 317)
    assert _ids("Liga Portugal", "Vitória de Guimarães", "Marítimo") == (301, 308)
    assert _ids("1. Lig", "Boluspor", "Van Spor Kulübü") == (416, 425)
    assert _ids("LaLiga", "Deportivo Alavés", "Málaga") == (70, 340)          # aksanlı takım adı


def test_eski_adlar_ve_cok_ligli_ad_bozulmadi(monkeypatch):
    _sahte(monkeypatch)
    assert _ids("First Division A", "Kortrijk", "KV Mechelen") == (326, 317)
    assert _ids("Premier League", "Celtic", "Rangers") == (91, 92)             # aynı adlı iki lig: takımları içereni seçer


def test_bilinmeyen_lig_veya_takim_bos_doner(monkeypatch):
    _sahte(monkeypatch)
    assert _ids("Eerste Divisie", "Vitesse", "FC Emmen") is None
    assert _ids("Belgian Pro League", "Kortrijk", "Bilinmeyen FK") is None
    assert _ids("First Division B", "Kortrijk", "KV Mechelen") is None         # Belçika 2. ligi 1. ligin takma adı sayılmaz
