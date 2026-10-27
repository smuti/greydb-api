"""Form özeti: LLM'in ev/deplasman ve 'son maç' yorumunu yanlış yapmaması için kod hesaplar (gerçek 1537 verisiyle)."""
import pytest

from app.services.spor_toto_fetch import summarize_form

# Skorjin'e giden gerçek son_5_mac_formu.maclar (en yeni başta; skor = takımın kendi golü önce; saha H=ev, A=deplasman)
RIZESPOR = [
    {"rakip": "Fenerbahçe", "skor": "0-3", "sonuc": "L", "saha": "H"},
    {"rakip": "Göztepe", "skor": "2-2", "sonuc": "D", "saha": "A"},
    {"rakip": "Eyüpspor", "skor": "2-0", "sonuc": "W", "saha": "A"},
    {"rakip": "Alanyaspor", "skor": "0-1", "sonuc": "L", "saha": "H"},
    {"rakip": "Gaziantep FK", "skor": "2-1", "sonuc": "W", "saha": "A"},
]
CORUM = [
    {"rakip": "Gaziantep FK", "skor": "3-0", "sonuc": "W", "saha": "A"},
    {"rakip": "Alanyaspor", "skor": "1-2", "sonuc": "L", "saha": "H"},
    {"rakip": "Samsunspor", "skor": "5-1", "sonuc": "W", "saha": "A"},
    {"rakip": "Eyüpspor", "skor": "3-0", "sonuc": "W", "saha": "H"},
    {"rakip": "Beşiktaş", "skor": "2-6", "sonuc": "L", "saha": "A"},
]


def test_rizespor_away_form_is_strong_not_weak():
    o = summarize_form(RIZESPOR)
    assert o["deplasman"] == "2 galibiyet, 1 beraberlik"          # LLM "deplasmanda zorlanıyor" demişti
    assert o["evde"] == "2 yenilgi"
    assert o["son_5"] == "2 galibiyet, 1 beraberlik, 2 yenilgi"


def test_corum_last_match_is_alanyaspor_loss_not_eyupspor_win():
    o = summarize_form(CORUM)
    assert o["son_mac"] == {"rakip": "Gaziantep FK", "saha": "deplasman", "skor": "3-0", "sonuc": "galibiyet", "atilan": 3, "yenilen": 0}
    assert o["evde_son_mac"] == {"rakip": "Alanyaspor", "skor": "1-2", "sonuc": "yenilgi"}   # "son iç saha maçı" = Alanyaspor, Eyüpspor değil
    assert o["deplasman"] == "2 galibiyet, 1 yenilgi"


def test_sentences_are_ready_to_quote_and_avoid_inflection():
    c = summarize_form(CORUM)["cumleler"]
    assert "Son 5 lig maçı: 3 galibiyet, 2 yenilgi." in c
    assert "Deplasmanda oynadığı son 3 maç: 2 galibiyet, 1 yenilgi." in c
    assert "Evinde oynadığı son 2 maç: 1 galibiyet, 1 yenilgi." in c
    assert any(s.startswith("Evinde oynadığı en son maç: Alanyaspor 1-2 yenilgi") for s in c)
    assert any(s.startswith("Son maçı: Gaziantep FK (deplasman) 3-0 galibiyet") for s in c)


def test_streak_reported_only_when_two_or_more():
    losing = [{"rakip": "A", "skor": "0-1", "sonuc": "L", "saha": "H"}, {"rakip": "B", "skor": "0-2", "sonuc": "L", "saha": "A"}, {"rakip": "C", "skor": "1-0", "sonuc": "W", "saha": "H"}]
    assert "Üst üste 2 yenilgi." in summarize_form(losing)["cumleler"]
    assert not any("Üst üste" in s for s in summarize_form(CORUM)["cumleler"])      # galibiyet-yenilgi dönüşümlü


def test_no_home_or_no_away_matches():
    only_away = [m for m in CORUM if m["saha"] == "A"]
    o = summarize_form(only_away)
    assert o["evde"] is None and "evde_son_mac" not in o and all("Evinde" not in s for s in o["cumleler"])


@pytest.mark.parametrize("empty", [None, [], ()])
def test_empty_returns_none(empty):
    assert summarize_form(empty) is None


def test_unknown_result_codes_are_ignored_not_crashing():
    o = summarize_form([{"rakip": "X", "skor": "1-1", "sonuc": "?", "saha": "H"}, {"rakip": "Y", "skor": "2-0", "sonuc": "W", "saha": "A"}])
    assert o["son_5"] == "1 galibiyet"


# ---------------- bağlantı: build_match_data özeti gerçekten ekliyor mu? ----------------
from app.services import spor_toto_fetch as stf  # noqa: E402


@pytest.fixture
def wired(monkeypatch):
    monkeypatch.setattr(stf, "get_team_league", lambda tid: {"league_id": 99999, "league_name": "Test Ligi"})          # zengin katman dışı -> fallback
    monkeypatch.setattr(stf, "_fetch_fallback_team_data", lambda tid: {"son_5_mac_formu": {"maclar": CORUM if tid == 334 else RIZESPOR}})
    monkeypatch.setattr(stf, "_fetch_rich_team_data", lambda tid, name: {})


def test_build_match_data_attaches_form_ozeti_for_both_teams(wired):
    md = stf.build_match_data({"team_id": 334, "name": "Çorum"}, {"team_id": 14, "name": "Rizespor"}, "7. Hafta")
    assert md["ev_sahibi_verisi"]["form_ozeti"]["evde_son_mac"]["rakip"] == "Alanyaspor"
    assert md["deplasman_verisi"]["form_ozeti"]["deplasman"] == "2 galibiyet, 1 beraberlik"
    assert "Üst üste" not in " ".join(md["ev_sahibi_verisi"]["form_ozeti"]["cumleler"])


def test_build_match_data_without_form_data_has_no_form_ozeti(monkeypatch):
    monkeypatch.setattr(stf, "get_team_league", lambda tid: {"league_id": 99999, "league_name": "Test Ligi"})
    monkeypatch.setattr(stf, "_fetch_fallback_team_data", lambda tid: {"sezon_istatistikleri": {"x": 1}})
    md = stf.build_match_data({"team_id": 1, "name": "A"}, {"team_id": 2, "name": "B"}, "7. Hafta")
    assert "form_ozeti" not in md["ev_sahibi_verisi"]


def test_takim_adi_verilince_her_cumle_ozneli_ve_skor_yonu_acik():
    c = summarize_form(CORUM, "Çorum FK")["cumleler"]
    assert all(s.startswith("Çorum FK: ") for s in c)
    assert any("Son maçı: Gaziantep FK (deplasman) 3-0 galibiyet (3 gol attı, 0 gol yedi)" in s for s in c)
    assert any("Evinde oynadığı en son maç: Alanyaspor 1-2 yenilgi (1 gol attı, 2 gol yedi)" in s for s in c)


def test_son_mac_atilan_yenilen_sayilari():
    o = summarize_form([{"rakip": "Atalanta", "skor": "3-2", "sonuc": "W", "saha": "H"}], "Milan")
    assert o["son_mac"]["atilan"] == 3 and o["son_mac"]["yenilen"] == 2          # "evinde Atalanta'dan 3 gol yedi" hatası: 3 attı, 2 yedi
    assert "Milan: Son maçı: Atalanta (ev) 3-2 galibiyet (3 gol attı, 2 gol yedi)." in o["cumleler"]


def test_okunamayan_skorda_gol_notu_eklenmez():
    o = summarize_form([{"rakip": "X", "skor": None, "sonuc": "W", "saha": "H"}], "Milan")
    assert "atilan" not in o["son_mac"] and all("gol attı" not in s for s in o["cumleler"])
