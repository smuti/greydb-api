"""Oyuncu maç istatistiği: FotMob'un bütün oyuncu alanları (Önemli, Hücum, Pas, Savunma, Mücadele, Kaleci sekmeleri) anahtarlarıyla ayıklanır,
bitmiş maç kaydı hepsini yazar, geçmiş ham maç verisinden onarılır. Veritabanı ve ağ yok."""
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services import oyuncu_stat as o

client = TestClient(app)


def stat(tip, deger, toplam=None):
    s = {"type": tip, "value": deger}
    if toplam is not None:
        s["total"] = toplam
    return {"stat": s}


def oyuncu(ad="Isco", pid=101, takim=9, kaleci=False, konum=2, **kv):
    """kv: ham anahtar -> (tip, değer[, toplam]). Gerçek yapı gibi iki grupta (top_stats ve alt grup)."""
    ust, alt = {}, {}
    for i, (k, v) in enumerate(kv.items()):
        e = {"key": k, **stat(*v)}
        (ust if i % 2 == 0 else alt)[f"Başlık {k}"] = e
    return {"id": pid, "name": ad, "teamId": takim, "isGoalkeeper": kaleci, "usualPosition": konum,
            "stats": [{"key": "top_stats", "title": "Top stats", "stats": ust}, {"key": "attack", "title": "Attack", "stats": alt}]}


TAM = dict(rating_title=("double", 7.4), minutes_played=("integer", 90), goals=("integer", 1), assists=("integer", 1),
           expected_goals=("double", 0.42), expected_assists=("double", 0.25), xg_and_xa=("double", 0.67), expected_goals_on_target_variant=("double", 0.8),
           defensive_actions=("integer", 5), total_shots=("integer", 4), ShotsOnTarget=("integer", 2), ShotsOffTarget=("integer", 1), blocked_shots=("integer", 1),
           touches_opp_box=("integer", 7), dribbles_succeeded=("fractionWithPercentage", 2, 4), Offsides=("integer", 1), touches=("integer", 60),
           accurate_passes=("fractionWithPercentage", 40, 50), chances_created=("integer", 3), passes_into_final_third=("integer", 6),
           accurate_crosses=("fractionWithPercentage", 1, 3), long_balls_accurate=("fractionWithPercentage", 2, 5), recoveries=("integer", 4),
           **{"matchstats.headers.tackles": ("integer", 2)}, interceptions=("integer", 1), shot_blocks=("integer", 1), clearances=("integer", 2),
           headed_clearance=("integer", 1), dribbled_past=("integer", 2), duel_won=("integer", 8), duel_lost=("integer", 4),
           ground_duels_won=("fractionWithPercentage", 5, 8), aerials_won=("fractionWithPercentage", 3, 4), fouls=("integer", 2), was_fouled=("integer", 3),
           dispossessed=("integer", 2), big_chance_created_team_title=("integer", 1), big_chance_missed_title=("integer", 1), line_breaking_passes=("integer", 2))


def test_butun_sekmelerin_alanlari_dogru_sutuna_gider():
    r = o.ayikla(oyuncu(**TAM))
    assert (r["rating"], r["minutes_played"], r["goals"], r["assists"]) == (7.4, 90, 1, 1)
    assert (r["xg"], r["xa"], r["xg_plus_xa"], r["xgot"]) == (0.42, 0.25, 0.67, 0.8)
    assert (r["defensive_actions"], r["total_shots"], r["shots_on_target"], r["shots_off_target"], r["blocked_shots"]) == (5, 4, 2, 1, 1)
    assert (r["touches_in_box"], r["successful_dribbles"], r["dribbles_total"], r["offsides"], r["touches"]) == (7, 2, 4, 1, 60)
    assert (r["accurate_passes"], r["total_passes"], r["chances_created"], r["passes_into_final_third"]) == (40, 50, 3, 6)
    assert (r["accurate_crosses"], r["crosses_total"], r["accurate_long_balls"], r["long_balls_total"]) == (1, 3, 2, 5)
    assert (r["recoveries"], r["tackles"], r["interceptions"], r["blocks"], r["clearances"], r["aerial_clearances"], r["was_dribbled"]) == (4, 2, 1, 1, 2, 1, 2)
    assert (r["duels_won"], r["duels_lost"], r["ground_duels_won"], r["ground_duels_total"], r["aerial_duels_won"], r["aerial_duels_total"]) == (8, 4, 5, 8, 3, 4)
    assert (r["fouls_committed"], r["fouls_won"], r["dispossessed"], r["big_chances_created"], r["big_chances_missed"], r["line_breaking_passes"]) == (2, 3, 2, 1, 1, 2)


def test_turetilen_alanlar():
    r = o.ayikla(oyuncu(**TAM))
    assert r["duels_total"] == 12 and r["duels_won_pct"] == pytest.approx(66.7, abs=0.05)
    assert r["pass_accuracy"] == pytest.approx(80.0)
    assert r["key_passes"] == r["chances_created"] == 3          # FotMob'da "kilit pas" ayrı alan değil: karşılığı yaratılan gol fırsatı


def test_pozisyon_ve_kimlik():
    assert (o.ayikla(oyuncu(konum=0, kaleci=True))["position"], o.ayikla(oyuncu(konum=1))["position"], o.ayikla(oyuncu(konum=2))["position"], o.ayikla(oyuncu(konum=3))["position"]) == ("GK", "DF", "MF", "FW")
    r = o.ayikla(oyuncu(pid=555, konum=3))
    assert (r["fotmob_player_id"], r["usual_position"]) == (555, 3)
    assert o.ayikla(oyuncu(konum=None))["position"] is None and o.ayikla(oyuncu(konum=7))["position"] is None


def test_eski_varsayilanlar_korunur_olcum_olmayan_bos_kalir():
    r = o.ayikla(oyuncu())
    assert all(r[k] == 0 for k in ("goals", "assists", "total_shots", "shots_on_target", "touches", "total_passes", "accurate_passes", "tackles", "interceptions", "clearances", "duels_won", "duels_lost", "fouls_committed", "fouls_won"))
    assert all(r[k] is None for k in ("xg", "xa", "xgot", "rating", "xgot_faced", "goals_prevented", "pass_accuracy", "duels_won_pct", "recoveries", "touches_in_box", "chances_created", "offsides"))
    assert r["duels_total"] == 0 and r["key_passes"] == 0


def test_kaleci_alanlari():
    r = o.ayikla(oyuncu(ad="Vallés", kaleci=True, konum=0, saves=("integer", 5), saves_inside_box=("integer", 4), goals_conceded=("integer", 1), expected_goals_on_target_faced=("double", 1.7),
                        goals_prevented=("double", 0.7), keeper_high_claim=("integer", 2), punches=("integer", 1), keeper_sweeper=("integer", 3), keeper_diving_save=("integer", 2), player_throws=("integer", 9)))
    assert (r["saves"], r["saves_inside_box"], r["goals_conceded"], r["xgot_faced"], r["goals_prevented"]) == (5, 4, 1, 1.7, 0.7)
    assert (r["high_claims"], r["punches"], r["keeper_sweeper"], r["keeper_diving_saves"], r["throws"]) == (2, 1, 3, 2, 9)


def test_oyuncuda_kaleci_alani_yoksa_saves_bos():
    assert o.ayikla(oyuncu())["saves"] is None and o.ayikla(oyuncu())["goals_conceded"] is None


def test_fiziksel_ve_diger():
    r = o.ayikla(oyuncu(physical_metrics_topspeed=("speed", 24.8), physical_metrics_distance_covered=("distance", 5799), physical_metrics_running=("distanceWithPercentage", 153, 5799),
                        physical_metrics_sprinting=("distanceWithPercentage", 144, 11676), physical_metrics_number_of_sprints=("integer", 4), fantasy_points=("fantasyPoints", 12),
                        shots_woodwork=("integer", 1), corners=("integer", 3), errors_led_to_goal=("integer", 1), conceded_penalties=("integer", 1), penalties_won=("integer", 1),
                        missed_penalty=("integer", 1), saved_penalties=("integer", 1), owngoal=("integer", 1), last_man_tackle=("integer", 1), clearance_off_the_line=("integer", 1),
                        expected_goals_non_penalty=("double", 0.3)))
    assert (r["top_speed"], r["distance_covered"], r["running_distance"], r["sprinting_distance"], r["number_of_sprints"], r["fantasy_points"]) == (24.8, 5799, 153, 144, 4, 12)
    assert (r["shots_woodwork"], r["corners_taken"], r["errors_led_to_goal"], r["conceded_penalties"], r["penalties_won"], r["missed_penalty"], r["saved_penalties"], r["own_goals"], r["last_man_tackles"], r["clearance_off_the_line"], r["xg_non_penalty"]) == (1, 3, 1, 1, 1, 1, 1, 1, 1, 1, 0.3)


def test_bozuk_girdi_cokmez():
    assert o.ayikla({"id": 1, "name": "X", "stats": None})["goals"] == 0
    assert o.ayikla({"id": 1, "name": "X", "stats": [{"stats": None}, {"stats": {"a": {"key": "goals", "stat": None}}}]})["goals"] == 0
    assert o.ayikla({"name": "X"})["fotmob_player_id"] is None


def test_tum_sutun_adlari_listelenir_ve_ayiklananla_ayni():
    assert set(o.ayikla(oyuncu())) == set(o.TUM_SUTUNLAR)
    assert len(o.TUM_SUTUNLAR) == len(set(o.TUM_SUTUNLAR))


def test_yeni_sutunlar_eklenir_ve_eskiler_yeniden_tanimlanmaz():
    assert o.YENI_SUTUNLAR and set(o.YENI_SUTUNLAR) <= set(o.TUM_SUTUNLAR)
    assert not {"goals", "xg", "rating", "tackles", "duels_won", "position"} & set(o.YENI_SUTUNLAR)
    assert "offsides" in o.YENI_SUTUNLAR and "passes_into_final_third" in o.YENI_SUTUNLAR and "fotmob_player_id" in o.YENI_SUTUNLAR


# ---------------- bitmiş maç kaydı ----------------
def maclar_icerik(*oyuncular):
    return {"content": {"playerStats": {str(p["id"]): p for p in oyuncular}, "lineup": {"homeTeam": {"id": 9}, "awayTeam": {"id": 8}}}}


def test_mac_kaydi_butun_alanlari_yazar(monkeypatch):
    from app.services import match_saver as ms
    yazilan, sorgu = [], []
    monkeypatch.setattr(ms, "execute_query", lambda sql, params=None: sorgu.append(sql) or [{"cnt": 0}])
    monkeypatch.setattr(ms, "execute_insert", lambda sql, params=None: yazilan.append((sql, params)))
    monkeypatch.setattr(o, "sutunlari_hazirla", lambda: sorgu.append("HAZIRLA"))
    ms.save_match_player_stats(77, maclar_icerik(oyuncu(**TAM), oyuncu(ad="Dep", pid=202, takim=8)), 1, 2)
    assert "HAZIRLA" in sorgu
    ekler = [y for y in yazilan if "INSERT INTO public.match_player_stats" in y[0]]
    assert len(ekler) == 2
    sql, p = ekler[0]
    assert p["match_id"] == 77 and p["team_id"] == 1 and p["player_name"] == "Isco"
    assert p["chances_created"] == 3 and p["touches_in_box"] == 7 and p["recoveries"] == 4 and p["position"] == "MF" and p["fotmob_player_id"] == 101
    assert ekler[1][1]["team_id"] == 2
    for s in o.TUM_SUTUNLAR:
        assert f":{s}" in sql, s


def test_mac_kaydi_kayit_varsa_atlar(monkeypatch):
    from app.services import match_saver as ms
    monkeypatch.setattr(ms, "execute_query", lambda sql, params=None: [{"cnt": 5}])
    monkeypatch.setattr(ms, "execute_insert", lambda *a, **k: pytest.fail("yazılmamalı"))
    ms.save_match_player_stats(77, maclar_icerik(oyuncu()), 1, 2)


# ---------------- geçmiş onarımı ----------------
class K:
    def __init__(self, maclar, mevcut):
        self.maclar_, self.mevcut_, self.guncel, self.eklenen, self.hazir = maclar, mevcut, [], [], 0
        self.islemler = []

    def islem(self):
        from contextlib import contextmanager

        @contextmanager
        def _i():
            self.islemler.append("ac")
            yield
            self.islemler.append("kapat")
        return _i()

    def kaynaklar(self):
        return o.Kaynaklar(maclar=lambda sonra, n, kadar=None, eksik=False: [m for m in self.maclar_ if m["id"] > sonra and (kadar is None or m["id"] <= kadar)][:n], mevcut=lambda mid: self.mevcut_.get(mid, []),
                           guncelle=lambda id_, v: self.guncel.append((id_, v)), ekle=lambda mid, tid, v: self.eklenen.append((mid, tid, v)), islem=self.islem)


def mac(id_, *oyuncular, ev=1, dep=2):
    return {"id": id_, "home_team_id": ev, "away_team_id": dep, "raw": maclar_icerik(*oyuncular)}


def test_onarim_mevcut_satiri_gunceller_eksik_satiri_ekler():
    k = K([mac(10, oyuncu(**TAM), oyuncu(ad="Yeni", pid=303, takim=8))], {10: [{"id": 500, "team_id": 1, "player_name": "Isco"}]})
    r = o.geri_doldur(0, 50, False, k.kaynaklar())
    assert r["islenen_mac"] == 1 and r["guncellenen"] == 1 and r["eklenen"] == 1 and r["sonraki"] == 10
    assert k.guncel[0][0] == 500 and k.guncel[0][1]["chances_created"] == 3 and k.guncel[0][1]["recoveries"] == 4
    assert k.eklenen[0][0:2] == (10, 2) and k.eklenen[0][2]["player_name"] == "Yeni"
    assert "id" not in k.guncel[0][1] and "match_id" not in k.guncel[0][1]


def test_onarim_kuru_calisma_yazmaz_ama_sayar():
    k = K([mac(10, oyuncu())], {10: [{"id": 500, "team_id": 1, "player_name": "Isco"}]})
    r = o.geri_doldur(0, 50, True, k.kaynaklar())
    assert r["dry_run"] is True and r["guncellenen"] == 1 and k.guncel == [] and k.eklenen == []


def test_onarim_imleci_ve_sinir():
    k = K([mac(i, oyuncu()) for i in (10, 11, 12, 13)], {})
    r = o.geri_doldur(10, 2, True, k.kaynaklar())
    assert (r["islenen_mac"], r["sonraki"], r["bitti"]) == (2, 12, False)
    r2 = o.geri_doldur(12, 5, True, k.kaynaklar())
    assert (r2["islenen_mac"], r2["sonraki"], r2["bitti"]) == (1, 13, True)
    r3 = o.geri_doldur(13, 5, True, k.kaynaklar())
    assert (r3["islenen_mac"], r3["bitti"]) == (0, True)


def test_onarim_ayni_adli_birden_cok_satir_belirsiz_atlanir_hata_digerini_durdurmaz():
    k = K([mac(10, oyuncu()), {"id": 11, "home_team_id": 1, "away_team_id": 2, "raw": {"content": None}}, mac(12, oyuncu(ad="B", pid=2))],
          {10: [{"id": 1, "team_id": 1, "player_name": "Isco"}, {"id": 2, "team_id": 1, "player_name": "Isco"}], 12: [{"id": 3, "team_id": 1, "player_name": "B"}]})
    r = o.geri_doldur(0, 10, False, k.kaynaklar())
    assert r["belirsiz"] == 1 and r["guncellenen"] == 1 and r["islenen_mac"] == 3 and k.guncel[0][0] == 3
    assert r["oyuncusuz_mac"] == 1


def test_onarim_takim_eslemesi_ev_dep():
    k = K([mac(10, oyuncu(takim=9), oyuncu(ad="D", pid=2, takim=8), ev=11, dep=22)], {})
    o.geri_doldur(0, 10, False, k.kaynaklar())
    assert {(e[2]["player_name"], e[1]) for e in k.eklenen} == {("Isco", 11), ("D", 22)}


# ---------------- uç ----------------
def test_uc_cron_anahtari_ister_varsayilan_kuru(monkeypatch):
    monkeypatch.setenv("CRON_ANAHTARI", "cron")
    cagri = []
    monkeypatch.setattr(o, "calistir", lambda sonra, n, dry_run, kadar=None, yalniz_eksik=False: cagri.append((sonra, n, dry_run, kadar)) or {"bitti": True})
    yol = "/api/skorjin/oyuncu-stat/geri-doldur"
    assert client.post(yol).status_code == 401
    assert client.post(yol, headers={"X-Cron-Anahtari": "cron"}).status_code == 200
    client.post(yol + "?dry_run=false&sonra=500&limit=50", headers={"X-Cron-Anahtari": "cron"})
    assert cagri == [(0, 100, True, None), (500, 50, False, None)]
    client.post(yol + "?kadar=900", headers={"X-Cron-Anahtari": "cron"})
    assert cagri[-1] == (0, 100, True, 900)
    assert client.post(yol + "?limit=0", headers={"X-Cron-Anahtari": "cron"}).status_code == 422


# ---------------- onarım betiği ----------------
def test_onarim_betigi_bitene_kadar_cagirir_toplar_ve_yazar_bayragini_iletir():
    import importlib.util, pathlib
    yol = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "oyuncu_stat_onar.py"
    spec = importlib.util.spec_from_file_location("oyuncu_stat_onar", yol)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    cevaplar = iter([dict(islenen_mac=100, guncellenen=4000, eklenen=0, belirsiz=1, oyuncusuz_mac=0, hata=[], sonraki=100, bitti=False),
                     dict(islenen_mac=30, guncellenen=900, eklenen=20, belirsiz=0, oyuncusuz_mac=2, hata=["x: y"], sonraki=130, bitti=True)])
    url_ler, ciktilar = [], []
    sonuc = b.onar(True, 0, 100, "gizli", "https://x", post=lambda url, a: url_ler.append((url, a)) or next(cevaplar), yaz_cikti=ciktilar.append)
    assert sonuc["islenen_mac"] == 130 and sonuc["guncellenen"] == 4900 and sonuc["eklenen"] == 20 and sonuc["belirsiz"] == 1 and sonuc["hata"] == 1 and sonuc["bitti"] is True and sonuc["dry_run"] is False
    assert url_ler[0][0].endswith("dry_run=false&sonra=0&limit=100") and url_ler[1][0].endswith("dry_run=false&sonra=100&limit=100") and url_ler[0][1] == "gizli"
    assert len(ciktilar) == 2


def test_onarim_betigi_varsayilan_kuru_ve_anahtarsiz_cikis(monkeypatch):
    import importlib.util, pathlib
    yol = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "oyuncu_stat_onar.py"
    spec = importlib.util.spec_from_file_location("oyuncu_stat_onar", yol)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    url_ler = []
    b.onar(False, 5, 50, "k", "https://x", post=lambda url, a: url_ler.append(url) or dict(sonraki=5, bitti=True), yaz_cikti=lambda *_: None)
    assert "dry_run=true" in url_ler[0] and "sonra=5" in url_ler[0]
    monkeypatch.delenv("CRON_ANAHTARI", raising=False)
    assert b.main([]) == 2


def test_onarim_her_macin_yazmalari_tek_islemde_kuru_calismada_islem_acilmaz():
    k = K([mac(10, oyuncu(), oyuncu(ad="B", pid=2)), mac(11, oyuncu())], {10: [{"id": 1, "team_id": 1, "player_name": "Isco"}, {"id": 2, "team_id": 1, "player_name": "B"}]})
    o.geri_doldur(0, 10, False, k.kaynaklar())
    assert k.islemler == ["ac", "kapat", "ac", "kapat"]
    k2 = K([mac(10, oyuncu())], {})
    o.geri_doldur(0, 10, True, k2.kaynaklar())
    assert k2.islemler == []


def test_onarim_ust_sinir_kadar():
    k = K([mac(i, oyuncu()) for i in (10, 11, 12, 13)], {})
    r = o.geri_doldur(10, 10, True, k.kaynaklar(), kadar=12)
    assert (r["islenen_mac"], r["sonraki"], r["bitti"]) == (2, 12, True)


def test_onarim_maci_hata_verirse_sayaclar_yarim_eklenmez():
    class Patlayan(K):
        def kaynaklar(self):
            k = super().kaynaklar()
            def guncelle(id_, v):
                raise RuntimeError("yazma hatası")
            return o.Kaynaklar(maclar=k.maclar, mevcut=k.mevcut, guncelle=guncelle, ekle=k.ekle, islem=k.islem)
    k = Patlayan([mac(10, oyuncu())], {10: [{"id": 1, "team_id": 1, "player_name": "Isco"}]})
    r = o.geri_doldur(0, 10, False, k.kaynaklar())
    assert r["guncellenen"] == 0 and len(r["hata"]) == 1


# ---------------- teamId'siz oyuncular (kadrodan takım bulma) ----------------
def kadrolu(*oyuncular, ev_oyuncular=(), dep_oyuncular=()):
    """Oyuncuların teamId'si yok; takım kadro listesinden (ilk 11 + yedekler) bulunur."""
    return {"content": {"playerStats": {str(p["id"]): {k: v for k, v in p.items() if k != "teamId"} for p in oyuncular},
                        "lineup": {"homeTeam": {"id": 9, "starters": [{"id": i} for i in ev_oyuncular], "subs": []},
                                   "awayTeam": {"id": 8, "starters": [], "subs": [{"id": i} for i in dep_oyuncular]}}}}


def test_teamid_yoksa_takim_kadrodan_bulunur():
    icerik = kadrolu(oyuncu(pid=1), oyuncu(ad="Dep", pid=2), oyuncu(ad="Kimsesiz", pid=3), ev_oyuncular=(1,), dep_oyuncular=(2,))["content"]
    esle = o.takim_eslemesi(icerik, 11, 22)
    assert o.oyuncu_takimi({"id": 1, "name": "A"}, icerik, esle) == 11          # ilk 11
    assert o.oyuncu_takimi({"id": 2, "name": "B"}, icerik, esle) == 22          # yedek
    assert o.oyuncu_takimi({"id": 3, "name": "C"}, icerik, esle) is None        # kadroda yok: atlanır
    assert o.oyuncu_takimi({"id": 4, "name": "D", "teamId": 9}, icerik, esle) == 11   # teamId varsa o kullanılır


def test_onarim_teamidsiz_maci_kadrodan_esler_ve_sayar():
    m = {"id": 10, "home_team_id": 11, "away_team_id": 22, "raw": kadrolu(oyuncu(pid=1), oyuncu(ad="Dep", pid=2), oyuncu(ad="Kimsesiz", pid=3), ev_oyuncular=(1,), dep_oyuncular=(2,))}
    k = K([m], {})
    r = o.geri_doldur(0, 10, False, k.kaynaklar())
    assert r["eklenen"] == 2 and r["takimsiz"] == 1
    assert {(e[2]["player_name"], e[1]) for e in k.eklenen} == {("Isco", 11), ("Dep", 22)}


def test_mac_kaydi_teamidsiz_oyuncuyu_da_yazar(monkeypatch):
    from app.services import match_saver as ms
    yazilan = []
    monkeypatch.setattr(ms, "execute_query", lambda sql, params=None: [{"cnt": 0}])
    monkeypatch.setattr(ms, "execute_insert", lambda sql, params=None: yazilan.append(params))
    monkeypatch.setattr(o, "sutunlari_hazirla", lambda: None)
    ms.save_match_player_stats(77, kadrolu(oyuncu(pid=1), oyuncu(ad="Dep", pid=2), ev_oyuncular=(1,), dep_oyuncular=(2,)), 1, 2)
    assert sorted((p["player_name"], p["team_id"]) for p in yazilan if p) == [("Dep", 2), ("Isco", 1)]


def test_yalniz_eksik_mod_ayniyla_aktarilir(monkeypatch):
    k = K([mac(10, oyuncu()), mac(11, oyuncu())], {})
    gorulen = []
    def maclar(sonra, n, kadar=None, eksik=False):
        gorulen.append(eksik)
        return k.maclar_[:n]
    kay = o.Kaynaklar(maclar=maclar, mevcut=k.kaynaklar().mevcut, guncelle=k.kaynaklar().guncelle, ekle=k.kaynaklar().ekle, islem=k.islem)
    o.geri_doldur(0, 10, True, kay, yalniz_eksik=True)
    o.geri_doldur(0, 10, True, kay)
    assert gorulen == [True, False]


def test_uc_yalniz_eksik_parametresi(monkeypatch):
    monkeypatch.setenv("CRON_ANAHTARI", "cron")
    cagri = []
    monkeypatch.setattr(o, "calistir", lambda sonra, n, dry_run, kadar=None, yalniz_eksik=False: cagri.append((dry_run, yalniz_eksik)) or {"bitti": True})
    client.post("/api/skorjin/oyuncu-stat/geri-doldur?yalniz_eksik=true", headers={"X-Cron-Anahtari": "cron"})
    client.post("/api/skorjin/oyuncu-stat/geri-doldur", headers={"X-Cron-Anahtari": "cron"})
    assert cagri == [(True, True), (True, False)]


def test_betik_yalniz_eksik_ve_yeniden_deneme():
    import importlib.util, pathlib, urllib.error
    yol = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "oyuncu_stat_onar.py"
    spec = importlib.util.spec_from_file_location("oyuncu_stat_onar2", yol)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    url_ler = []
    b.onar(False, 0, 50, "k", "https://x", post=lambda url, a: url_ler.append(url) or dict(sonraki=0, bitti=True), yaz_cikti=lambda *_: None, yalniz_eksik=True)
    assert "yalniz_eksik=true" in url_ler[0]
    # bağlantı kopunca yeniden dener, 3. denemede başarır; bekleme süreleri artar
    cagri, beklemeler = [], []
    class Y:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b'{"bitti": true}'
    def acik(r, timeout=0):
        cagri.append(1)
        if len(cagri) < 3:
            raise urllib.error.URLError(ConnectionResetError("koptu"))
        return Y()
    import urllib.request
    orijinal = urllib.request.urlopen
    urllib.request.urlopen = acik
    try:
        assert b.istek("https://x", "k", bekle=beklemeler.append) == {"bitti": True}
    finally:
        urllib.request.urlopen = orijinal
    assert len(cagri) == 3 and beklemeler == [10, 20]
