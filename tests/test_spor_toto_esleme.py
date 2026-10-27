"""Spor Toto takım adı eşleştirme: 'St' = 'Saint', tire boşluk; eşleşmemiş eski kayıt yeniden denenir, elle bağlanmış kayıt korunur."""
import pandas as pd

from app.services import spor_toto_fetch as stf


def test_normalize_st_saint_ve_tire():
    assert stf._normalize("Paris St Germain") == stf._normalize("Paris Saint-Germain") == "paris saint germain"
    assert stf._normalize("St. Pauli") == "saint pauli"
    assert stf._normalize("Çaykur Rizespor") == "caykur rizespor"
    assert stf._normalize("Stuttgart") == "stuttgart"          # 'st' yalnızca tek başına sözcükse


TAKIMLAR = pd.DataFrame([{"id": 152, "fotmob_team_id": 9847, "name": "Paris Saint-Germain"}, {"id": 142, "fotmob_team_id": 6379, "name": "Paris FC"}])


def _sahte(monkeypatch, onbellek):
    yazilan = []
    def sorgu(sql, params=None):
        return pd.DataFrame(onbellek) if "spor_toto_team_map" in sql else TAKIMLAR
    monkeypatch.setattr(stf, "query_to_df", sorgu)
    monkeypatch.setattr(stf, "execute_insert", lambda sql, p=None: yazilan.append((sql, p)))
    return yazilan


def test_eslesmemis_onbellek_yeniden_denenir_ve_bulunur(monkeypatch):
    yazilan = _sahte(monkeypatch, [{"matchforge_team_id": None}])
    assert stf.resolve_team(1, "Paris St Germain")["team_id"] == 152
    assert "COALESCE(greydb.spor_toto_team_map.matchforge_team_id" in yazilan[0][0]      # dolu kaydı ezmez
    assert yazilan[0][1]["matchforge_team_id"] == 152


def test_dolu_onbellek_aynen_kullanilir(monkeypatch):
    yazilan = _sahte(monkeypatch, [{"matchforge_team_id": 142}])
    assert stf.resolve_team(1, "Paris St Germain") == {"team_id": 142}
    assert yazilan == []


def test_turkce_medya_adlari_veritabani_adina_baglanir(monkeypatch):
    takimlar = pd.DataFrame([{"id": 76, "fotmob_team_id": 8315, "name": "Athletic Club"}, {"id": 90, "fotmob_team_id": 9823, "name": "Bayern München"}])
    yazilan = []
    monkeypatch.setattr(stf, "query_to_df", lambda sql, params=None: pd.DataFrame([{"matchforge_team_id": None}]) if "spor_toto_team_map" in sql else takimlar)
    monkeypatch.setattr(stf, "execute_insert", lambda sql, p=None: yazilan.append(p))
    assert stf.resolve_team(112, "Athletic Bilbao")["team_id"] == 76          # canlıda eşleşmeyen ikili
    assert stf.resolve_team(7, "Bayern Münih")["team_id"] == 90
    assert [y["matchforge_team_id"] for y in yazilan] == [76, 90]


# ---------------- eşleşme önerisi ve kalıcılık ----------------
def test_oneri_en_yakin_takimi_ilk_verir(monkeypatch):
    takimlar = pd.DataFrame([
        {"id": 76, "name": "Athletic Club", "league_name": "LaLiga"}, {"id": 73, "name": "Real Sociedad", "league_name": "LaLiga"},
        {"id": 5, "name": "Athletic Bilbao B", "league_name": None}, {"id": 9, "name": "Fenerbahçe", "league_name": "Süper Lig"},
    ])
    monkeypatch.setattr(stf, "query_to_df", lambda sql, params=None: takimlar)
    o = stf.suggest_teams("Athletic Bilbao")
    assert o[0]["team_id"] in (76, 5) and all(x["team_id"] != 9 for x in o)          # alakasız takım önerilmez
    assert stf.suggest_teams("Xyzzy Qwerty") == []


def test_oneri_alias_adi_kullanir(monkeypatch):
    takimlar = pd.DataFrame([{"id": 90, "name": "Bayern München", "league_name": "Bundesliga"}, {"id": 3, "name": "Bayer Leverkusen", "league_name": "Bundesliga"}])
    monkeypatch.setattr(stf, "query_to_df", lambda sql, params=None: takimlar)
    assert stf.suggest_teams("Bayern Münih")[0]["team_id"] == 90


def test_otomatik_eslesme_basarisizken_elle_baglanan_taraf_korunur(monkeypatch):
    from app.routers import spor_toto as st
    olaylar = []
    monkeypatch.setattr(st, "execute_insert", lambda sql, p=None: olaylar.append((" ".join(sql.split()), p)))
    st._set_match_team_ids_korunur(7, None, 73)
    sql, p = olaylar[0]
    assert "home_team_id = COALESCE(:h, home_team_id)" in sql and "CASE WHEN" in sql and p == {"h": None, "a": 73, "id": 7}


def test_elle_baglama_kalici_haritaya_yazilir(monkeypatch):
    import asyncio
    from app.routers import spor_toto as st
    olaylar = []
    maclar = iter([pd.DataFrame([{"id": 7, "ev_takim": "Athletic Bilbao", "deplasman_takim": "Real Sociedad", "home_team_id": None, "away_team_id": 73, "lig_haftasi": None, "mac_tarihi": None}]),
                   pd.DataFrame([{"id": 7, "ev_takim": "Athletic Bilbao", "deplasman_takim": "Real Sociedad", "home_team_id": 76, "away_team_id": 73, "lig_haftasi": None, "mac_tarihi": None}])])
    monkeypatch.setattr(st, "query_to_df", lambda sql, params=None: next(maclar))
    monkeypatch.setattr(st, "execute_insert", lambda sql, p=None: olaylar.append((" ".join(sql.split()), p)))
    monkeypatch.setattr(st, "_generate_for_match", lambda *a, **k: None)
    asyncio.run(st.manual_team_link(7, st.ManualTeamLinkRequest(side="home", matchforge_team_id=76)))
    harita = [o for o in olaylar if "spor_toto_team_map" in o[0]]
    assert harita and harita[0][1] == {"tid": 76, "ad": "Athletic Bilbao"}
