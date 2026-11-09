"""Detay doldurma (refill'in paralel yerel karşılığı): seçim sorgusu uç ile aynı süzgeçler, işçi bölümü id % n = k, keyset ilerleme, sayımlar."""
from app.services import detay_doldur as dd


def test_secim_sorgusu_ucla_ayni_suzgecler_ve_bolum():
    sql = " ".join(dd.SECIM_SQL.split())
    for parca in ("m.raw_match_details IS NOT NULL", "ms.match_id IS NULL", "m.finished = true", "jsonb_typeof(m.raw_match_details->'content'->'stats') = 'object'",
                  "m.raw_match_details->'content'->'stats' <> '{}'::jsonb", "m.id % :n = :k", "m.id > :sonra", "ORDER BY m.id", "LIMIT :limit"):
        assert parca in sql, parca


def test_secim_sorgusu_donemsel_istatistigi_bos_maci_secmez():
    """FotMob 'stats' nesnesi var ama Periods.All null/nesne değil (veri yok): kayıt kodu patlıyordu, bu maçlar hep 'eksik' görünüp hata sayacını şişiriyordu."""
    sql = " ".join(dd.SECIM_SQL.split())
    assert "jsonb_typeof(m.raw_match_details->'content'->'stats'->'Periods'->'All') = 'object'" in sql


class Sahte:
    """Sahte veri: id'ler 1..N, eksik olanlar; kaydet() bazılarında match_stats oluşturmaz (atlanan) ya da hata verir."""
    def __init__(self, idler, atlanan=(), hatali=()):
        self.idler, self.atlanan, self.hatali, self.kaydedilen, self.sorgular = list(idler), set(atlanan), set(hatali), [], []

    def getir(self, n, k, sonra, limit):
        self.sorgular.append((n, k, sonra, limit))
        return [{"id": i, "fotmob_match_id": 1000 + i, "raw_match_details": {"x": i}} for i in self.idler if i % n == k and i > sonra][:limit]

    def kaydet(self, ham):
        i = ham["x"]
        if i in self.hatali:
            raise RuntimeError("boom")
        self.kaydedilen.append(i)
        return i not in self.atlanan


def test_isci_kendi_dilimini_id_sirasiyla_isler_baska_dilime_dokunmaz():
    s = Sahte(range(1, 41))
    r = dd.isle(s.getir, s.kaydet, n=4, k=1, limit=3)
    assert s.kaydedilen == [i for i in range(1, 41) if i % 4 == 1]
    assert r == {"islenen": 10, "dolan": 10, "atlanan": 0, "hata": 0}


def test_keyset_ilerleme_atlanan_ve_hatali_maclar_donguye_sokmaz():
    s = Sahte(range(1, 21), atlanan={4, 8}, hatali={12})
    r = dd.isle(s.getir, s.kaydet, n=4, k=0, limit=2)                  # 4, 8, 12, 16, 20
    assert r == {"islenen": 5, "dolan": 2, "atlanan": 2, "hata": 1}
    sonralar = [q[2] for q in s.sorgular]
    assert sonralar == sorted(sonralar) and len(set(sonralar)) == len(sonralar)      # her sorgu bir öncekinden ileri: aynı maç tekrar seçilmez


def test_bos_dilim_ve_ilerleme_cagrisi():
    s = Sahte([])
    gorulen = []
    r = dd.isle(s.getir, s.kaydet, n=8, k=3, limit=5, ilerleme=lambda toplam: gorulen.append(toplam))
    assert r == {"islenen": 0, "dolan": 0, "atlanan": 0, "hata": 0} and gorulen == []
    s2 = Sahte(range(1, 30))
    dd.isle(s2.getir, s2.kaydet, n=1, k=0, limit=10, ilerleme=lambda t: gorulen.append(t), ilerleme_adim=10)
    assert gorulen == [10, 20]


def test_gecersiz_bolum_reddedilir():
    import pytest
    for n, k in ((0, 0), (4, 4), (4, -1)):
        with pytest.raises(ValueError):
            dd.isle(lambda *a: [], lambda x: True, n=n, k=k, limit=1)


def test_eksik_sutunlar_bilgi_semasindan_bulunur():
    var = {"rating", "xg"}
    sorgu = lambda sql, p=None: [{"column_name": c} for c in var]      # noqa: E731
    assert dd.eksik_sutunlar(sorgu, ["rating", "xg", "xgot", "goals_prevented"]) == ["xgot", "goals_prevented"]
    assert dd.eksik_sutunlar(sorgu, ["rating", "xg"]) == []


def test_sutun_sorgusu_yalniz_okur_ddl_icermez():
    sql = dd.SUTUN_SQL.upper()
    assert "INFORMATION_SCHEMA.COLUMNS" in sql and "ALTER" not in sql and "match_player_stats" in dd.SUTUN_SQL
