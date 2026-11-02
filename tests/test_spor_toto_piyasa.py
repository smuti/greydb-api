"""Farklı ülke ligleri maçı: model yerine iddaa Maç Sonucu oranı (komisyonsuz), kart ligi, önerilen kart yedeği."""
import pytest

import app.routers.spor_toto as router
from app.services import spor_toto_fetch as stf
from app.services import spor_toto_piyasa as pz


def olay(hn, an, ms=(4.99, 4.34, 1.41)):
    return {"i": 1, "hn": hn, "an": an, "m": [
        {"t": 2, "st": 100, "o": [{"n": "1", "odd": 1.43}, {"n": "0", "odd": 4.31}, {"n": "2", "odd": 3.75}]},   # handikaplı MS: karışmamalı
        {"t": 1, "st": 1, "o": [{"n": "1", "odd": ms[0]}, {"n": "0", "odd": ms[1]}, {"n": "2", "odd": ms[2]}]},
    ]}


def test_ms_pazari_handikapli_degil_gercek_pazari_secer():
    assert pz.ms_pazari(olay("a", "b")) == {"1": 4.99, "0": 4.34, "2": 1.41}


def test_ms_pazari_yoksa_none():
    assert pz.ms_pazari({"m": [{"t": 2, "st": 100, "o": []}]}) is None


def test_ihtimaller_komisyonsuz_ve_toplam_bir():
    ih = pz.ihtimaller({"1": 4.99, "0": 4.34, "2": 1.41})
    assert sum(ih.values()) == pytest.approx(1.0)
    assert ih["2"] == pytest.approx(0.62, abs=0.01) and ih["1"] == pytest.approx(0.18, abs=0.01)


def test_fenerbahce_liverpool_deplasman_favori_banko():
    t = pz.piyasa_tahmini([olay("Fenerbahçe", "Liverpool"), olay("Barcelona", "Aston Villa")], "Fenerbahçe", "Liverpool")
    assert t["decision"]["senaryo"] == "2" and t["decision"]["tier"] == "tek"
    assert t["facts"]["favori_taraf"] == "deplasman" and t["facts"]["model_version"] == "piyasa-iddaa"
    assert [s["senaryo"] for s in t["facts"]["senaryolar"] if s["onerilen"]] == ["2"]


def test_belirsiz_ya_da_yok_maci_baglamaz():
    assert pz.piyasa_tahmini([olay("Barcelona", "Aston Villa")], "Fenerbahçe", "Liverpool") is None
    assert pz.piyasa_tahmini([], "Fenerbahçe", "Liverpool") is None


def test_ayni_maci_oransiz_bos_kayitla_iki_kez_listeleyen_iddaa():
    bos = {"i": 2, "hn": "Fenerbahçe", "an": "Liverpool", "m": []}
    assert pz.olay_bul([bos, olay("Fenerbahçe", "Liverpool")], "Fenerbahçe", "Liverpool")["i"] == 1


def test_oranlari_farkli_iki_aday_belirsiz_none():
    assert pz.olay_bul([olay("A", "B"), olay("A", "B", ms=(2.0, 3.0, 4.0))], "A", "B") is None


def test_karar_dengeli_ise_uclu_ve_beraberlik_favori_olmaz():
    assert pz.karar(0.34, 0.33, 0.33)["senaryo"] == "1-0-2"
    assert pz.karar(0.30, 0.45, 0.25)["senaryo"] == "1-0-2"
    assert pz.karar(0.46, 0.30, 0.24)["senaryo"] == "1-0"


def test_turnuva_adi_bilinmeyende_uydurmaz():
    assert pz.turnuva_adi(218) == "Şampiyonlar Ligi"
    assert pz.turnuva_adi(237) == "Avrupa Ligi" and pz.turnuva_adi(236) == "Konferans Ligi"
    assert pz.turnuva_adi(999) is None and pz.turnuva_adi(None) is None


def test_onerilen_yedegi_model_yokken_1_0_2():
    sc = [{"senaryo": c, "metin": "m", "onerilen": False} for c in ["1", "0", "2", "1-0", "0-2", "1-0-2"]]
    assert [s["senaryo"] for s in stf.apply_model_choice(sc, None) if s["onerilen"]] == ["1-0-2"]


def test_onerilen_tek_ise_dokunulmaz():
    sc = [{"senaryo": c, "metin": "m", "onerilen": c == "0-2"} for c in ["1", "0", "2", "1-0", "0-2", "1-0-2"]]
    assert stf.apply_model_choice(sc, None) == sc


# ---- üretim akışı ----
HOME, AWAY = {"team_id": 1, "name": "Fenerbahçe"}, {"team_id": 2, "name": "Liverpool"}


@pytest.fixture
def akis(monkeypatch):
    log = {"sql": [], "md": None, "model_cagri": 0}
    ligler = {1: {"league_id": 4, "league_name": "Super Lig"}, 2: {"league_id": 5, "league_name": "Premier League"}}
    monkeypatch.setattr(router.stf, "get_team_league", lambda t: ligler[t])
    monkeypatch.setattr(router.stf, "build_match_data", lambda h, a, l: {"ev_sahibi": "Fenerbahçe", "deplasman": "Liverpool", "lig": "Super Lig"})
    monkeypatch.setattr(router.stf, "fetch_model_prediction", lambda *a, **k: log.__setitem__("model_cagri", log["model_cagri"] + 1))
    monkeypatch.setattr(router.stf, "fetch_piyasa_tahmini", lambda h, a: pz.piyasa_tahmini([olay("Fenerbahçe", "Liverpool")], "Fenerbahçe", "Liverpool"))
    def gen(md):
        log["md"] = md
        return [{"senaryo": c, "metin": "m", "onerilen": c == "1-0-2"} for c in ["1", "0", "2", "1-0", "0-2", "1-0-2"]]
    monkeypatch.setattr(router.stf, "generate_scenarios", gen)
    monkeypatch.setattr(router, "execute_insert", lambda sql, p=None: log["sql"].append((" ".join(sql.split()), p)))
    return log


def test_farkli_lig_modeli_cagirmaz_piyasayi_kullanir(akis):
    router._generate_for_match(5, HOME, AWAY, "4. Hafta", "2026-11-04T20:45:00")
    assert akis["model_cagri"] == 0
    assert akis["md"]["lig"] == "Avrupa Kupası" and akis["md"]["model_tahmini"]["kaynak"] == "piyasa"
    ent = [p for s, p in akis["sql"] if "INSERT INTO greydb.spor_toto_entries" in s]
    assert [e["senaryo"] for e in ent if e["onerilen"]] == ["2"]                  # piyasa kararı kaydedildi
    kayit = [p for s, p in akis["sql"] if "model_p1" in s and "NULL" not in s][0]
    assert kayit["p2"] == pytest.approx(0.62, abs=0.01) and kayit["version"] == "piyasa-iddaa"


def test_farkli_lig_ligi_ic_lige_cevirmez(akis):
    router._generate_for_match(5, HOME, AWAY, "4. Hafta", "2026-11-04T20:45:00")
    lig_yazilar = [(s, p) for s, p in akis["sql"] if "SET lig" in s]
    assert len(lig_yazilar) == 1 and "lig IS NULL OR lig IN" in lig_yazilar[0][0]
    assert lig_yazilar[0][1]["lig"] == "Avrupa Kupası"


def test_farkli_lig_oran_yoksa_model_yok_yedek_kart(akis, monkeypatch):
    monkeypatch.setattr(router.stf, "fetch_piyasa_tahmini", lambda h, a: None)
    router._generate_for_match(5, HOME, AWAY, "4. Hafta", "2026-11-04T20:45:00")
    assert "model_tahmini" not in akis["md"]
    ent = [p for s, p in akis["sql"] if "INSERT INTO greydb.spor_toto_entries" in s]
    assert [e["senaryo"] for e in ent if e["onerilen"]] == ["1-0-2"]


def test_ayni_lig_eski_akis_modeli_kullanir(akis, monkeypatch):
    monkeypatch.setattr(router.stf, "get_team_league", lambda t: {"league_id": 4, "league_name": "Super Lig"})
    router._generate_for_match(5, HOME, AWAY, "4. Hafta", "2026-11-04T20:45:00")
    assert akis["model_cagri"] == 1 and akis["md"]["lig"] == "Super Lig"
