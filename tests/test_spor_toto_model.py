"""Spor Toto + MS modeli: matchforge çağrısı, hata toleransı, önerilen kartın modele bağlanması, DB kaydı."""
import json

import pandas as pd
import pytest

import app.routers.spor_toto as router
from app.services import spor_toto_fetch as stf

RESULT = {
    "home_team_id": 1, "away_team_id": 2, "p1": 0.634, "p0": 0.226, "p2": 0.140,
    "decision": {"tier": "tek", "senaryo": "1", "secimler": ["1"], "kolon": 1, "banko": True},
    "facts": {"ev": "Juventus", "deplasman": "Lazio", "ihtimaller": {"p1": 0.634, "p0": 0.226, "p2": 0.14}, "onerilen": "1", "kademe": "tek",
              "banko": True, "senaryolar": [{"senaryo": "1", "ihtimal": 0.634, "guven_dili": "ağırlıklı ihtimal", "onerilen": True}],
              "uyarilar": ["Kadro, sakatlık, hakem ve hava durumu hesaba katılmadı; tahmin maçtan günler önce üretilir."],
              "model_version": "harman-v1-2026-10", "as_of": "2026-10-18"},
}


class FakeResp:
    def __init__(self, status=200, body=None): self.status_code, self._b, self.text = status, body, str(body)
    def json(self): return self._b


class FakeClient:
    calls = []
    behavior = None
    def __init__(self, *a, **k): pass
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def post(self, url, json=None, timeout=None):
        FakeClient.calls.append((url, json, timeout))
        if isinstance(FakeClient.behavior, Exception): raise FakeClient.behavior
        return FakeClient.behavior


@pytest.fixture(autouse=True)
def fake_http(monkeypatch):
    FakeClient.calls, FakeClient.behavior = [], FakeResp(200, {"results": [RESULT], "kolon": 1})
    monkeypatch.setattr(stf.httpx, "Client", FakeClient)


HOME, AWAY = {"team_id": 1, "name": "Juventus"}, {"team_id": 2, "name": "Lazio"}


# ---------------- matchforge çağrısı ----------------
def test_fetch_model_prediction_sends_ids_date_and_league():
    r = stf.fetch_model_prediction(HOME, AWAY, "2026-10-18T21:45:00", league_id=8)
    url, body, _ = FakeClient.calls[0]
    assert url.endswith("/api/ms/predict")
    m = body["matches"][0]
    assert (m["home_team_id"], m["away_team_id"], m["league_id"], m["match_date"]) == (1, 2, 8, "2026-10-18T21:45:00")
    assert r["p1"] == 0.634 and r["facts"]["onerilen"] == "1"


@pytest.mark.parametrize("behavior", [FakeResp(500, {"error": "x"}), FakeResp(400, {"error": "as_of"}), FakeResp(200, {"results": []}), RuntimeError("timeout")])
def test_fetch_model_prediction_returns_none_on_any_failure(behavior):
    FakeClient.behavior = behavior
    assert stf.fetch_model_prediction(HOME, AWAY, "2026-10-18T21:45:00", league_id=8) is None


@pytest.mark.parametrize("date", [None, "", float("nan"), pd.NaT])
def test_no_valid_date_skips_model_without_calling_matchforge(date):
    assert stf.fetch_model_prediction(HOME, AWAY, date, league_id=8) is None
    assert FakeClient.calls == []


def test_missing_team_id_skips_model():
    assert stf.fetch_model_prediction({"team_id": None, "name": "X"}, AWAY, "2026-10-18", league_id=8) is None
    assert FakeClient.calls == []


# ---------------- önerilen kart modele bağlı (Skorjin eski sürümde kalsa bile) ----------------
def scen(pick): return [{"senaryo": c, "metin": "m", "onerilen": c == pick} for c in ["1", "0", "2", "1-0", "0-2", "1-0-2"]]


def test_apply_model_choice_overrides_llm():
    out = stf.apply_model_choice(scen("1-0-2"), RESULT)
    assert [s["senaryo"] for s in out if s["onerilen"]] == ["1"]


def test_apply_model_choice_without_model_unchanged():
    s = scen("1-0-2"); assert stf.apply_model_choice(s, None) == s


# ---------------- _generate_for_match akışı ----------------
@pytest.fixture
def pipeline(monkeypatch):
    log = {"sql": [], "match_data": None}
    monkeypatch.setattr(router.stf, "build_match_data", lambda h, a, l: {"ev_sahibi": "Juventus", "deplasman": "Lazio", "lig": "Serie A"})
    monkeypatch.setattr(router.stf, "get_team_league", lambda tid: {"league_id": 8, "league_name": "Serie A"})
    def gen(md): log["match_data"] = md; return scen("1-0-2")
    monkeypatch.setattr(router.stf, "generate_scenarios", gen)
    monkeypatch.setattr(router, "execute_insert", lambda sql, params=None: log["sql"].append((" ".join(sql.split()), params)))
    return log


def test_generate_passes_model_to_skorjin_and_saves_it(pipeline):
    router._generate_for_match(10, HOME, AWAY, "8. Hafta", "2026-10-18T21:45:00")
    assert pipeline["match_data"]["model_tahmini"]["onerilen"] == "1"
    saves = [(s, p) for s, p in pipeline["sql"] if "model_p1" in s]
    assert len(saves) == 1
    p = saves[0][1]
    assert p["id"] == 10 and p["p1"] == 0.634 and p["kademe"] == "tek" and p["onerilen"] == "1" and p["version"] == "harman-v1-2026-10"
    assert json.loads(p["facts"])["ev"] == "Juventus"
    entries = [p for s, p in pipeline["sql"] if "INSERT INTO greydb.spor_toto_entries" in s]
    assert [e["senaryo"] for e in entries if e["onerilen"]] == ["1"]          # model kararı kaydedildi (LLM 1-0-2 demişti)


def test_generate_without_model_keeps_old_flow_and_clears_stale_model(pipeline):
    FakeClient.behavior = RuntimeError("matchforge down")
    router._generate_for_match(10, HOME, AWAY, "8. Hafta", "2026-10-18T21:45:00")
    assert "model_tahmini" not in pipeline["match_data"]
    clears = [(s, p) for s, p in pipeline["sql"] if "model_p1 = NULL" in s]
    assert len(clears) == 1                                                   # eski model değeri kalmasın
    entries = [p for s, p in pipeline["sql"] if "INSERT INTO greydb.spor_toto_entries" in s]
    assert [e["senaryo"] for e in entries if e["onerilen"]] == ["1-0-2"]      # LLM'in kendi seçimi


def test_every_caller_passes_match_date_to_generate():
    """Model, maç tarihi olmadan çalışmaz; tüm çağıranlar (üretim + manuel takım bağlama sonrası) tarihi geçirmeli."""
    import ast, inspect
    tree = ast.parse(inspect.getsource(router))
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "_generate_for_match"]
    assert len(calls) >= 2
    assert all(len(c.args) + len(c.keywords) >= 5 for c in calls), "bir çağıran mac_tarihi geçirmiyor"
