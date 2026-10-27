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
