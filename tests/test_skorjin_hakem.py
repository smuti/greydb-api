"""Skorjin `referee` grubu: FotMob'dan hakem adı + bizim geçmişten hakem istatistiği (saf hesap, SQL metni, orkestrasyon). Ağ/DB yok."""
import json
from datetime import datetime

import pytest

from app.services import skorjin_hakem as h
from app.services import skorjin_prematch as sp
from app.services.skorjin_prematch import Kaynaklar, hazirla

SIMDI = datetime(2026, 10, 22, 12, 0)
MAC_GUNU = datetime(2026, 10, 25, 17, 0)
IST = {"n": 177, "sari": 3.5084745762711864, "kirmizi": 0.14124293785310734, "faul": 22.29378531, "gol": 2.89265536, "ev_galibiyet": 0.42372881, "penalti": 0.22033898}
LIG = {"n": 1969, "sari": 3.8202133062468258, "kirmizi": 0.12188928, "faul": 21.58913, "gol": 2.7, "ev_galibiyet": 0.45, "penalti": 0.2}


def detay(ad="Michael Oliver", ulke="England", lineup=False):
    ref = {"imgUrl": "x", "text": ad, "countryCode": "ENG", "country": ulke} if ad is not None else None
    icerik = {"matchFacts": {"infoBox": {"Referee": ref, "Stadium": {"name": "S"}}}}
    if lineup:
        icerik["lineup"] = {"homeTeam": {"unavailable": [{"name": "A", "unavailability": {"type": "injury"}}]}, "awayTeam": {"unavailable": []}}
    return {"general": {}, "content": icerik}


# ---------------------------------------------------------------- FotMob'dan hakem
def test_hakem_bilgisi_adi_ve_ulkeyi_alir():
    assert h.hakem_bilgisi(detay()) == {"name": "Michael Oliver", "country": "England"}


@pytest.mark.parametrize("d", [None, {}, {"content": {}}, {"content": {"matchFacts": {}}}, {"content": {"matchFacts": {"infoBox": {"Referee": None}}}},
                               {"content": {"matchFacts": {"infoBox": {"Referee": {"text": "  "}}}}}, {"content": {"matchFacts": {"infoBox": {"Referee": "x"}}}}])
def test_hakem_yoksa_none(d):
    assert h.hakem_bilgisi(d) is None


def test_ad_kirpilir_ulke_yoksa_none():
    assert h.hakem_bilgisi({"content": {"matchFacts": {"infoBox": {"Referee": {"text": " Ali Veli "}}}}}) == {"name": "Ali Veli", "country": None}


# ---------------------------------------------------------------- grup biçimi
def test_hakem_alani_istatistik_ve_lig_ortalamasi_yuvarlanir():
    g = h.hakem_alani({"name": "Michael Oliver", "country": "England"}, IST, LIG)
    assert g["name"] == "Michael Oliver" and g["country"] == "England" and g["source"] == "FotMob"
    assert (g["n"], g["yellow_avg"], g["red_avg"], g["foul_avg"], g["penalty_avg"], g["goals_avg"], g["home_win_rate"]) == (177, 3.51, 0.14, 22.29, 0.22, 2.89, 0.424)
    assert g["league"] == {"n": 1969, "yellow_avg": 3.82, "red_avg": 0.12, "foul_avg": 21.59, "penalty_avg": 0.2, "goals_avg": 2.7, "home_win_rate": 0.45}


def test_az_maci_olan_hakemde_istatistik_yok_yalniz_ad_ve_n():
    g = h.hakem_alani({"name": "Yeni Hakem", "country": None}, {**IST, "n": 4}, LIG)
    assert g == {"name": "Yeni Hakem", "country": None, "source": "FotMob", "n": 4}
    assert h.hakem_alani({"name": "X", "country": None}, None, LIG) == {"name": "X", "country": None, "source": "FotMob"}


def test_lig_ortalamasi_yetersizse_ya_yoksa_league_alani_yok():
    assert "league" not in h.hakem_alani({"name": "X", "country": None}, IST, None)
    assert "league" not in h.hakem_alani({"name": "X", "country": None}, IST, {**LIG, "n": 10})


def test_bos_istatistik_alani_none_kalir_uydurulmaz():
    g = h.hakem_alani({"name": "X", "country": None}, {**IST, "kirmizi": None, "faul": None}, LIG)
    assert g["red_avg"] is None and g["foul_avg"] is None and g["yellow_avg"] == 3.51


def test_hakem_alani_bilgi_yoksa_none():
    assert h.hakem_alani(None, IST, LIG) is None


# ---------------------------------------------------------------- SQL ve tablo
def test_hakem_sorgusu_bitmis_skorlu_maclar_maca_kadar_hakem_adina_gore():
    q = h.HAKEM_SQL
    assert "c.referee = :ad" in q and "m.finished" in q and "NOT COALESCE(m.cancelled, false)" in q and "m.match_date < :tarih" in q
    assert "JOIN public.match_stats" in q and "is_penalty" in q and "MISSEDPENALTY" in q
    assert ":lig" not in q


def test_lig_sorgusu_ligin_maclari_hakem_filtresi_yok():
    q = h.LIG_SQL
    assert "m.league_id = :lig" in q and "m.match_date < :tarih" in q and "c.referee" not in q and "match_context" not in q


def test_tablo_ddl_ve_yazma_hakem_kolonlari():
    for parca in ("referee JSONB", "referee_at TIMESTAMPTZ", "ADD COLUMN IF NOT EXISTS referee JSONB", "ADD COLUMN IF NOT EXISTS referee_at TIMESTAMPTZ"):
        assert parca in sp.TABLO_DDL
    satir = {"fotmob_match_id": 1, "referee": {"name": "X"}, "referee_at": SIMDI}
    for kapsam in ("gunluk", "oran"):
        sql, params = sp.yazma_sorgusu(kapsam, satir)
        assert "referee" in sql and json.loads(params["referee"]) == {"name": "X"}
    sql, _ = sp.yazma_sorgusu("gunluk", {"fotmob_match_id": 1})
    assert "referee" not in sql                                                  # satırda yoksa eski veri korunur


# ---------------------------------------------------------------- orkestrasyon
def mac():
    return {"fotmob_match_id": 1001, "iddaa_event_id": None, "home_team_id": 1, "away_team_id": 2, "home_name": "Yıldız FK",
            "away_name": "Kuzey SK", "league_id": 4, "league_name": "Süper Lig", "match_date": MAC_GUNU}


class Kosu:
    def __init__(self, fotmob, hakem="varsayilan"):
        self.yazilan, self.fotmob_cagri, self.hakem_cagri = [], [], []
        self.fotmob_fn, self.hakem_fn = fotmob, hakem

    def kaynaklar(self):
        def fotmob(fid):
            self.fotmob_cagri.append(fid)
            r = self.fotmob_fn(fid) if callable(self.fotmob_fn) else self.fotmob_fn
            if isinstance(r, Exception):
                raise r
            return r

        def hakem(ad, lig, tarih):
            self.hakem_cagri.append((ad, lig, tarih))
            return IST, LIG

        return Kaynaklar(maclar=lambda b, s: [mac()], form=lambda t, d: [], h2h=lambda a, b, d: [], model=lambda ms: {},
                         iddaa=lambda e: (None, {}, {}), mevcut=lambda ids: {1001}, yaz=lambda k, s: self.yazilan.append((k, s)),
                         fotmob=fotmob, hakem=hakem if self.hakem_fn == "varsayilan" else self.hakem_fn)

    def calistir(self, kapsam="gunluk"):
        return hazirla(kapsam, False, self.kaynaklar(), SIMDI, "harman-v1")


def test_gunluk_satiri_hakem_grubunu_yazar_ve_fotmob_tek_kez_cagrilir():
    k = Kosu(detay(lineup=True))
    r = k.calistir()
    satir = k.yazilan[0][1]
    assert satir["referee"]["name"] == "Michael Oliver" and satir["referee"]["yellow_avg"] == 3.51 and satir["referee_at"] == SIMDI
    assert satir["absences"] is not None                                          # eksik oyuncu da aynı çağrıdan
    assert k.fotmob_cagri == [1001]                                                # FotMob tek istek
    assert k.hakem_cagri == [("Michael Oliver", 4, MAC_GUNU)]
    assert "referee" in r["maclar"][0]["alanlar"]


def test_oran_kapsami_hakemi_de_gunceller_hakem_son_gunlerde_aciklanir():
    k = Kosu(detay())
    k.calistir("oran")
    kapsam, satir = k.yazilan[0]
    assert kapsam == "oran" and satir["referee"]["name"] == "Michael Oliver" and satir["referee_at"] == SIMDI


def test_fotmob_hakem_yoksa_grup_none_zaman_damgasi_none():
    k = Kosu(detay(ad=None))
    k.calistir()
    s = k.yazilan[0][1]
    assert s["referee"] is None and s["referee_at"] is None and k.hakem_cagri == []


def test_fotmob_detayi_gelmezse_none_yazilir():
    k = Kosu(None)
    k.calistir()
    assert k.yazilan[0][1]["referee"] is None


def test_fotmob_hatasinda_hakem_anahtarlari_olmaz_eski_veri_korunur():
    k = Kosu(RuntimeError("fotmob"))
    r = k.calistir()
    s = k.yazilan[0][1]
    assert "referee" not in s and "referee_at" not in s and r["fotmob_hatasi"] == 1


def test_istatistik_sorgusu_hata_verirse_yalniz_ad_yazilir_ve_sayilir():
    def patlar(ad, lig, tarih):
        raise RuntimeError("db")
    k = Kosu(detay(), hakem=patlar)
    r = k.calistir()
    g = k.yazilan[0][1]["referee"]
    assert g == {"name": "Michael Oliver", "country": "England", "source": "FotMob"} and r["hakem_hatasi"] == 1 and r["hazirlanan"] == 1


def test_hakem_kaynagi_verilmezse_yalniz_ad_yazilir():
    k = Kosu(detay(), hakem=None)
    k.calistir()
    assert k.yazilan[0][1]["referee"] == {"name": "Michael Oliver", "country": "England", "source": "FotMob"}


# ---------------------------------------------------------------- gerçek kaynak
def test_gercek_kaynak_hakemi_ve_ligi_bir_kez_sorgular_bellekler(monkeypatch):
    cagrilar = []

    def sorgu(sql, p):
        cagrilar.append((sql is h.HAKEM_SQL, dict(p)))
        return [{"n": 177, "sari": 3.5, "kirmizi": 0.1, "faul": 22.0, "gol": 2.9, "ev_galibiyet": 0.4, "penalti": 0.2}]
    monkeypatch.setattr(h, "_sorgu", sorgu)
    fn = h.hakem_kaynagi()
    a = fn("Michael Oliver", 5, MAC_GUNU)
    b = fn("Michael Oliver", 5, datetime(2026, 10, 26))
    c = fn("Anthony Taylor", 5, MAC_GUNU)
    assert len(cagrilar) == 3                                                    # Oliver 1 kez, lig 1 kez, Taylor 1 kez
    assert a[0]["n"] == 177 and a == b and c[1] == a[1]
    assert sum(1 for hak, _ in cagrilar if hak) == 2


def test_gercek_kaynak_satir_yoksa_none_doner(monkeypatch):
    monkeypatch.setattr(h, "_sorgu", lambda sql, p: [])
    assert h.hakem_kaynagi()("X", 5, MAC_GUNU) == (None, None)
