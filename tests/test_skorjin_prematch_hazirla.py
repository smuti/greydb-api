"""Skorjin anlık görüntü işi: orkestrasyon (sahte kaynaklarla; veritabanı, ağ yok)."""
from datetime import datetime

import pytest

from app.services.skorjin_prematch import Kaynaklar, hazirla

SIMDI = datetime(2026, 10, 22, 12, 0)
CFG = {"1_1": {"n": "Maç Sonucu"}, "1_101": {"n": "Alt/Üst {0}"}}
OLAY = {"m": [
    {"i": 100, "t": 1, "st": 1, "o": [{"no": 1, "odd": 2.0, "n": "1"}, {"no": 2, "odd": 3.5, "n": "0"}, {"no": 3, "odd": 4.0, "n": "2"}]},
    {"i": 102, "t": 1, "st": 101, "sov": "2.5", "o": [{"no": 1, "odd": 1.95, "n": "Alt"}, {"no": 2, "odd": 1.85, "n": "Üst"}]},
]}
YUZDE = {"100": {"1": 10.0, "2": 6.0, "3": 4.0}}
MATRIS = [[0.5, 0.2], [0.2, 0.1]]


def mac(fid=1001, lig=4, iddaa=555, ev=1, dep=2):
    return {"fotmob_match_id": fid, "iddaa_event_id": iddaa, "home_team_id": ev, "away_team_id": dep, "home_name": "Yıldız FK",
            "away_name": "Kuzey SK", "league_id": lig, "league_name": "Süper Lig", "match_date": datetime(2026, 10, 25, 17, 0)}


def form_satiri(gun, ev, dep, hs, as_):
    return {"tarih": datetime(2026, 9, gun, 17, 0), "home_team_id": ev, "away_team_id": dep, "home_score": hs, "away_score": as_,
            "ev_ad": "Yıldız FK" if ev == 1 else "Rakip", "dep_ad": "Rakip" if ev == 1 else "Yıldız FK"}


class Sahte:
    def __init__(self, maclar=None, mevcut=(), model=None, iddaa=None, form_hata=None):
        self.maclar_ = maclar if maclar is not None else [mac()]
        self.mevcut_ = set(mevcut)
        self.model_ = model if model is not None else {1001: {"p1": 0.5, "p0": 0.3, "p2": 0.2, "score_matrix": MATRIS,
                                                              "new_team_home": False, "new_team_away": False}}
        self.iddaa_ = iddaa if iddaa is not None else (OLAY, YUZDE, CFG)
        self.form_hata = form_hata
        self.yazilan, self.model_cagrilari, self.iddaa_cagrilari = [], [], []

    def kaynaklar(self):
        def form(takim, tarih):
            if self.form_hata == takim:
                raise RuntimeError("db")
            return [form_satiri(17, takim, 9, 2, 0)]
        return Kaynaklar(
            maclar=lambda bas, son: self.maclar_,
            form=form,
            h2h=lambda a, b, tarih: [{"tarih": datetime(2026, 4, 3), "home_team_id": a, "away_team_id": b, "home_score": 2, "away_score": 1,
                                      "ev_ad": "Yıldız FK", "dep_ad": "Kuzey SK"}],
            model=lambda ms: (self.model_cagrilari.append([m["fotmob_match_id"] for m in ms]) or self.model_),
            iddaa=self._iddaa,
            mevcut=lambda ids: self.mevcut_ & set(ids),
            yaz=lambda kapsam, satir: self.yazilan.append((kapsam, satir)),
            fotmob=self._fotmob_yok,
        )

    @staticmethod
    def _fotmob_yok(fid):
        raise RuntimeError("fotmob kapalı")

    def _iddaa(self, event_id):
        self.iddaa_cagrilari.append(event_id)
        if isinstance(self.iddaa_, Exception):
            raise self.iddaa_
        return self.iddaa_


def calistir(sahte, kapsam="gunluk", dry_run=False):
    return hazirla(kapsam, dry_run, sahte.kaynaklar(), SIMDI, model_surumu="harman-v1")


def test_gunluk_satiri_butun_alanlari_doldurur():
    s = Sahte()
    r = calistir(s)
    kapsam, satir = s.yazilan[0]
    assert kapsam == "gunluk" and r["hazirlanan"] == 1 and r["hatalar"] == []
    assert satir["fotmob_match_id"] == 1001 and satir["home_name"] == "Yıldız FK" and satir["kickoff"] == datetime(2026, 10, 25, 17, 0)
    assert satir["form"]["home"] == [{"d": "2026-09-17", "v": "H", "gf": 2, "ga": 0, "opp": "Rakip"}] and satir["form_at"] == SIMDI
    assert satir["h2h"][0]["home"] == "Yıldız FK" and satir["h2h_at"] == SIMDI
    assert satir["scoreline"] == {"matrix": MATRIS, "blend": {"home": 0.5, "draw": 0.3, "away": 0.2}, "model_version": "harman-v1"}
    assert satir["odds"] == {"home": 2.0, "draw": 3.5, "away": 4.0, "over25": 1.85, "under25": 1.95} and satir["odds_at"] == SIMDI
    assert satir["crowd"] == {"home": 0.5, "draw": 0.3, "away": 0.2} and satir["crowd_at"] == SIMDI


def test_kuru_calistirma_yazmaz_ama_raporlar():
    s = Sahte()
    r = calistir(s, dry_run=True)
    assert s.yazilan == [] and r["dry_run"] is True and r["hazirlanan"] == 1
    assert r["maclar"][0]["mac"] == "Yıldız FK - Kuzey SK" and "scoreline" in r["maclar"][0]["alanlar"]


def test_model_kapsami_disindaki_lig_icin_matris_yok_ve_model_cagrilmaz():
    s = Sahte(maclar=[mac(lig=99)])
    r = calistir(s)
    assert s.yazilan[0][1]["scoreline"] is None and r["model_yok"] == 1
    assert s.model_cagrilari == []


def test_model_yalniz_kapsamdaki_maclar_icin_toplu_cagrilir():
    s = Sahte(maclar=[mac(1001), mac(1002, lig=99), mac(1003, lig=8)], model={1001: {"p1": .5, "p0": .3, "p2": .2, "score_matrix": MATRIS},
                                                                           1003: {"p1": .5, "p0": .3, "p2": .2, "score_matrix": MATRIS}})
    calistir(s)
    assert s.model_cagrilari == [[1001, 1003]]


def test_yeni_takim_isaretli_modelde_matris_yazilmaz():
    s = Sahte(model={1001: {"p1": .5, "p0": .3, "p2": .2, "score_matrix": MATRIS, "new_team_home": True, "new_team_away": False}})
    r = calistir(s)
    assert s.yazilan[0][1]["scoreline"] is None and r["model_yok"] == 1


def test_iddaa_hatasi_oranlari_bos_birakir_gerisini_yazar():
    s = Sahte(iddaa=RuntimeError("kapalı"))
    r = calistir(s)
    satir = s.yazilan[0][1]
    assert not any(k in satir for k in ("odds", "odds_at", "crowd", "crowd_at")) and satir["scoreline"] is not None   # eski oran korunur
    assert r["iddaa_hatasi"] == 1 and r["hazirlanan"] == 1


def test_iddaa_kimligi_olmayan_mac_icin_iddaa_cagrilmaz():
    s = Sahte(maclar=[mac(iddaa=None)])
    calistir(s)
    assert s.iddaa_cagrilari == [] and s.yazilan[0][1]["odds"] is None


def test_bir_macin_hatasi_digerlerini_durdurmaz():
    s = Sahte(maclar=[mac(1001, ev=1), mac(1002, ev=3)], form_hata=1)
    r = calistir(s)
    assert [y[1]["fotmob_match_id"] for y in s.yazilan] == [1002]
    assert r["hazirlanan"] == 1 and len(r["hatalar"]) == 1 and r["hatalar"][0]["fotmob_match_id"] == 1001


def test_bos_form_ve_h2h_null_yazilir_uydurulmaz():
    s = Sahte()
    k = s.kaynaklar()
    k = Kaynaklar(maclar=k.maclar, form=lambda t, d: [], h2h=lambda a, b, d: [], model=k.model, iddaa=k.iddaa, mevcut=k.mevcut, yaz=k.yaz, fotmob=k.fotmob)
    hazirla("gunluk", False, k, SIMDI, model_surumu="v")
    satir = s.yazilan[0][1]
    assert satir["form"] is None and satir["form_at"] is None and satir["h2h"] is None and satir["h2h_at"] is None


# ------------------------------------------------------------------ oran kapsamı
def test_oran_yalniz_satiri_olan_ve_iddaa_kimlikli_maclari_gunceller():
    s = Sahte(maclar=[mac(1001), mac(1002, iddaa=None), mac(1003)], mevcut={1001, 1002})
    r = calistir(s, kapsam="oran")
    assert [(k, y["fotmob_match_id"]) for k, y in s.yazilan] == [("oran", 1001)]
    assert set(s.yazilan[0][1]) == {"fotmob_match_id", "odds", "odds_at", "crowd", "crowd_at"}
    assert r["hazirlanan"] == 1 and r["satiri_yok"] == 1 and r["iddaa_kimligi_yok"] == 1
    assert s.model_cagrilari == []


def test_oran_iddaada_piyasa_kalmadiysa_null_yazar_hata_olursa_dokunmaz():
    kapali = Sahte(iddaa=({"m": []}, {}, CFG), mevcut={1001})
    calistir(kapali, kapsam="oran")
    assert kapali.yazilan[0][1]["odds"] is None and kapali.yazilan[0][1]["odds_at"] is None
    hata = Sahte(iddaa=RuntimeError("kapalı"), mevcut={1001})
    r = calistir(hata, kapsam="oran")
    assert hata.yazilan == [] and r["iddaa_hatasi"] == 1


def test_gecersiz_kapsam():
    with pytest.raises(ValueError):
        calistir(Sahte(), kapsam="saatlik")


# ------------------------------------------------------------------ eksik oyuncular (FotMob)
def fm_detay(ev=1, dep=0):
    p = lambda ad: {"name": ad, "marketValue": 1000, "unavailability": {"type": "injury", "expectedReturn": "Day to day"}}   # noqa: E731
    return {"content": {"lineup": {"homeTeam": {"unavailable": [p(f"E{i}") for i in range(ev)]},
                                   "awayTeam": {"unavailable": [p(f"D{i}") for i in range(dep)]}}}}


def kaynak_fotmob(sahte, detay, hata=False):
    k = sahte.kaynaklar()

    def fotmob(fid):
        sahte.fotmob_cagrilari = getattr(sahte, "fotmob_cagrilari", []) + [fid]
        if hata:
            raise RuntimeError("fotmob")
        return detay
    return Kaynaklar(**{**k.__dict__, "fotmob": fotmob})


def test_gunluk_satiri_eksikleri_kaynak_ve_zamanla_yazar():
    s = Sahte()
    hazirla("gunluk", False, kaynak_fotmob(s, fm_detay(2, 0)), SIMDI, model_surumu="v")
    satir = s.yazilan[0][1]
    assert satir["absences"]["source"] == "FotMob" and satir["absences_at"] == SIMDI and satir["absences_source"] == "FotMob"
    assert [t["name"] for t in satir["absences"]["teams"]] == ["Yıldız FK", "Kuzey SK"]


def test_fotmob_kapsami_yoksa_eksik_null_yazilir_bilgi_yok():
    s = Sahte()
    hazirla("gunluk", False, kaynak_fotmob(s, fm_detay(0, 0)), SIMDI, model_surumu="v")
    satir = s.yazilan[0][1]
    assert satir["absences"] is None and satir["absences_at"] is None and satir["absences_source"] is None


def test_fotmob_hatasinda_eksik_anahtarlari_yok_eski_veri_korunur():
    s = Sahte()
    r = hazirla("gunluk", False, kaynak_fotmob(s, None, hata=True), SIMDI, model_surumu="v")
    satir = s.yazilan[0][1]
    assert not any(k in satir for k in ("absences", "absences_at", "absences_source")) and r["fotmob_hatasi"] == 1 and r["hazirlanan"] == 1


def test_oran_kapsami_eksikleri_de_yeniler():
    s = Sahte(mevcut={1001})
    hazirla("oran", False, kaynak_fotmob(s, fm_detay(1, 1)), SIMDI, model_surumu="v")
    satir = s.yazilan[0][1]
    assert satir["absences"] is not None and set(satir) >= {"odds", "crowd", "absences", "absences_at", "absences_source"}


def test_fotmob_cagrisi_maç_basina_bir_kez():
    s = Sahte(maclar=[mac(1001), mac(1002, ev=3)], model={})
    hazirla("gunluk", False, kaynak_fotmob(s, fm_detay(1, 0)), SIMDI, model_surumu="v")
    assert s.fotmob_cagrilari == [1001, 1002]
