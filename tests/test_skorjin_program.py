"""Skorjin `schedule` (dinlenme, yoğun program, Avrupa) ve `rotation` (ilk 11 değişimi) grupları: saf hesap, SQL, orkestrasyon. Veritabanı yok."""
from datetime import datetime, timedelta

import pytest

from app.services import skorjin_prematch as sp
from app.services import skorjin_program as p
from app.services.skorjin_prematch import Kaynaklar, hazirla

MAC_GUNU = datetime(2026, 10, 25, 17, 0)
T = 1


def kadro(degisim: int, ayri: str = "X"):
    ortak = [f"P{i}" for i in range(11 - degisim)]
    return sorted(ortak + [f"{ayri}{i}" for i in range(degisim)])


def mac(gun, ay=10, lig=4, lig_ad="Super Lig", ev=True, gf=1, ga=0, rakip="R", ilk11=None, saat=17, mid=None, dt=None):
    return {"mac_id": mid or gun + ay * 100, "tarih": dt or datetime(2026, ay, gun, saat, 0), "league_id": lig, "lig_ad": lig_ad,
            "home_team_id": T if ev else 9, "away_team_id": 9 if ev else T, "home_score": gf if ev else ga, "away_score": ga if ev else gf,
            "rakip": rakip, "ilk11": ilk11}


def zincir(degisimler, araliklar=None):
    """Referans maç (1 Eylül) + her değişim için bir maç; araliklar[i] önceki maça uzaklık (gün, varsayılan 7). Liste en yeniden eskiye döner."""
    araliklar = araliklar or [7] * len(degisimler)
    t, onceki = datetime(2026, 9, 1, 17, 0), kadro(0)
    rows = [mac(1, ay=9, ilk11=onceki, mid=1, dt=t)]
    for i, (d, a) in enumerate(zip(degisimler, araliklar), start=1):
        t = t + timedelta(days=a)
        yeni = sorted(onceki[: 11 - d] + [f"N{i}_{j}" for j in range(d)])
        rows.append(mac(1, ay=9, ilk11=yeni, mid=10 + i, dt=t))
        onceki = yeni
    return list(reversed(rows))


# ---------------------------------------------------------------- program
def test_dinlenme_gunu_son_mac_ve_yogunluk():
    rows = [mac(22, lig=11, lig_ad="Europa League", ev=False, gf=2, ga=2, rakip="Roma"), mac(19), mac(15), mac(5)]
    s = p.program(rows, T, MAC_GUNU)
    assert s["days_since_last"] == 3
    assert s["last"] == {"d": "2026-10-22", "competition": "Europa League", "competition_id": 11, "venue": "A", "opp": "Roma", "gf": 2, "ga": 2}
    assert (s["matches_7d"], s["matches_14d"]) == (2, 3)                       # 22 ve 19 (6 gün); 15'i 10 gün önce → 14 günde 3
    assert s["european_7d"] is True


def test_avrupa_kupa_yedi_gunden_eskiyse_isaretlenmez():
    s = p.program([mac(14, lig=10, lig_ad="Champions League"), mac(20)], T, MAC_GUNU)
    assert s["european_7d"] is False and s["days_since_last"] == 5


def test_sayim_sinirlari_tam_yedi_ve_on_dort_gun_dahil_gelecek_maclar_haric():
    rows = [mac(18), mac(11), mac(26), mac(4)]                                  # 7 gün, 14 gün, gelecek, 21 gün
    s = p.program(rows, T, MAC_GUNU)
    assert (s["matches_7d"], s["matches_14d"]) == (1, 2) and s["days_since_last"] == 7


def test_mac_yoksa_ya_da_son_mac_cok_eskiyse_none():
    assert p.program([], T, MAC_GUNU) is None
    assert p.program([mac(1, ay=7)], T, MAC_GUNU) is None                       # 100+ gün: sezon başı, anlamsız


def test_program_girdiyi_degistirmez_sira_bagimsiz():
    rows = [mac(22), mac(19), mac(15)]
    before = [dict(r) for r in rows]
    assert p.program(list(reversed(rows)), T, MAC_GUNU) == p.program(rows, T, MAC_GUNU) and rows == before


# ---------------------------------------------------------------- rotasyon
def test_degisim_sayisi_oncekine_gore_kac_oyuncu_yeni():
    r = p.rotasyon(zincir([2, 5, 0, 3]), T, MAC_GUNU)
    assert [m["changes"] for m in r["matches"]] == [3, 0, 5, 2]               # en yeniden eskiye
    assert r["n"] == 4 and r["last_changes"] == 3 and r["avg_changes"] == pytest.approx((2 + 5 + 0 + 3) / 4)


def test_ortalama_icin_en_az_uc_gecis_gerekir():
    r = p.rotasyon(zincir([2, 4]), T, MAC_GUNU)
    assert r["avg_changes"] is None and r["last_changes"] == 4 and r["n"] == 2


def test_kisa_dinlenme_sonrasi_ve_normal_dinlenme_ayri_ortalanir():
    r = p.rotasyon(zincir([5, 6, 1, 2], [3, 4, 7, 7]), T, MAC_GUNU)           # aralıklar 3, 4, 7, 7 gün
    assert r["short_rest"] == {"n": 2, "avg_changes": 5.5} and r["normal_rest"] == {"n": 2, "avg_changes": 1.5}
    assert [m["days_rest"] for m in r["matches"]] == [7, 7, 4, 3]


def test_dinlenme_grubunda_iki_gecis_yoksa_none():
    r = p.rotasyon(zincir([2, 4, 1, 0]), T, MAC_GUNU)                           # hepsi 7 gün arayla
    assert r["short_rest"] is None and r["normal_rest"]["n"] == 4


def test_eksik_ya_da_bozuk_kadro_gecise_girmez_atlanan_maclar_dinlenmeyi_uzatir():
    rows = zincir([3, 3, 3])
    rows[1]["ilk11"] = None                                                      # maç kadrosu yok
    rows[2]["ilk11"] = ["tek"]                                                   # 11 kişi değil
    r = p.rotasyon(rows, T, MAC_GUNU)
    assert r["n"] == 1 and r["matches"][0]["days_rest"] == 21


def test_kadro_hic_yoksa_rotasyon_none():
    assert p.rotasyon([mac(20), mac(13)], T, MAC_GUNU) is None
    assert p.rotasyon([], T, MAC_GUNU) is None


# ---------------------------------------------------------------- SQL
def test_takim_sorgusu_son_12_bitmis_maci_tum_musabakalardan_ve_ilk_11i_getirir():
    q = p.TAKIM_SQL
    assert "m.finished" in q and "NOT COALESCE(m.cancelled, false)" in q and "m.match_date < :tarih" in q and "LIMIT 12" in q
    assert "ml.is_starter" in q and "match_lineups" in q and "ml.team_id = :t" in q and "public.leagues" in q
    assert "league_id =" not in q                                                  # lig filtresi yok: Avrupa maçları da gelir


# ---------------------------------------------------------------- orkestrasyon
def ev_mac():
    return {"fotmob_match_id": 1001, "iddaa_event_id": None, "home_team_id": 1, "away_team_id": 2, "home_name": "Yıldız FK", "away_name": "Kuzey SK",
            "league_id": 4, "league_name": "Süper Lig", "match_date": MAC_GUNU}


def kaynak_kur(program, yazilan):
    return Kaynaklar(maclar=lambda b, s: [ev_mac()], form=lambda t, d: [], h2h=lambda a, b, d: [], model=lambda ms: {},
                     iddaa=lambda e: (None, {}, {}), mevcut=lambda ids: set(), yaz=lambda k, s: yazilan.append(s), fotmob=lambda f: None, program=program)


SIMDI = datetime(2026, 10, 22, 12, 0)


def test_gunluk_satiri_schedule_ve_rotation_iki_takim_icin_zaman_damgasiyla():
    S = {"days_since_last": 3, "last": None, "matches_7d": 2, "matches_14d": 3, "european_7d": True}
    R = {"n": 5, "avg_changes": 3.2, "last_changes": 4, "short_rest": None, "normal_rest": None, "matches": []}
    cagrilar, yazilan = [], []

    def program(takim, tarih):
        cagrilar.append((takim, tarih))
        return (S, R) if takim == 1 else (None, R)
    r = hazirla("gunluk", False, kaynak_kur(program, yazilan), SIMDI, "harman-v1")
    s = yazilan[0]
    assert cagrilar == [(1, MAC_GUNU), (2, MAC_GUNU)]
    assert s["schedule"] == {"home": S, "away": None} and s["schedule_at"] == SIMDI
    assert s["rotation"] == {"home": R, "away": R} and s["rotation_at"] == SIMDI
    assert {"schedule", "rotation"} <= set(r["maclar"][0]["alanlar"])


def test_iki_takim_da_veri_yoksa_gruplar_null():
    yazilan = []
    hazirla("gunluk", False, kaynak_kur(lambda t, d: (None, None), yazilan), SIMDI, "harman-v1")
    s = yazilan[0]
    assert s["schedule"] is None and s["schedule_at"] is None and s["rotation"] is None and s["rotation_at"] is None


def test_kaynak_hatasinda_anahtarlar_olmaz_is_surer_ve_sayilir():
    def patlar(t, d):
        raise RuntimeError("db")
    yazilan = []
    r = hazirla("gunluk", False, kaynak_kur(patlar, yazilan), SIMDI, "harman-v1")
    s = yazilan[0]
    assert "schedule" not in s and "rotation" not in s and r["program_hatasi"] == 1 and r["hazirlanan"] == 1


def test_kaynak_verilmezse_ve_oran_kapsaminda_cagrilmaz():
    yazilan = []
    hazirla("gunluk", False, kaynak_kur(None, yazilan), SIMDI, "harman-v1")
    assert "schedule" not in yazilan[0]
    cagri = []
    k = kaynak_kur(lambda t, d: cagri.append(t) or (None, None), yazilan)
    hazirla("oran", False, Kaynaklar(**{**k.__dict__, "mevcut": lambda ids: {1001}}), SIMDI, "harman-v1")
    assert cagri == []


def test_tablo_ddl_ve_yazma_yeni_gruplar():
    for kolon in ("schedule", "rotation", "style", "timing"):
        assert f"{kolon} JSONB" in sp.TABLO_DDL and f"{kolon}_at TIMESTAMPTZ" in sp.TABLO_DDL
        assert f"ADD COLUMN IF NOT EXISTS {kolon} JSONB" in sp.TABLO_DDL
        sql, params = sp.yazma_sorgusu("gunluk", {"fotmob_match_id": 1, kolon: {"x": 1}, f"{kolon}_at": SIMDI})
        assert f"{kolon} = EXCLUDED.{kolon}" in sql
        sql, _ = sp.yazma_sorgusu("oran", {"fotmob_match_id": 1, "odds": None, "odds_at": None, kolon: {"x": 1}})
        assert kolon not in sql


def test_gercek_kaynak_takim_basina_tek_sorgu_ve_bellek(monkeypatch):
    cagrilar = []
    monkeypatch.setattr(p, "_sorgu", lambda sql, prm: cagrilar.append(dict(prm)) or [mac(22), mac(15)])
    fn = p.program_kaynagi()
    a = fn(T, MAC_GUNU)
    b = fn(T, MAC_GUNU)
    fn(2, MAC_GUNU)
    assert len(cagrilar) == 2 and a == b and a[0]["days_since_last"] == 3
