"""Spor Toto'daki Türkçe milli takım adları (Uluslar Ligi 15. oyun programı) veritabanındaki İngilizce takım adına bağlanır."""
import pandas as pd
import pytest

from app.services import spor_toto_fetch as stf

# (Spor Toto adı [webapi.sportoto.gov.tr, 15. Oyun Programı], veritabanı takım adı [public.teams])
CIFTLER = [
    ("Fransa", "France"), ("Türkiye", "Turkiye"), ("Bulgaristan", "Bulgaria"), ("İzlanda", "Iceland"), ("Lihtenştayn", "Liechtenstein"),
    ("Azerbaycan", "Azerbaijan"), ("Lüksemburg", "Luxembourg"), ("Estonya", "Estonia"), ("Slovakya", "Slovakia"), ("Faroe Adaları", "Faroe Islands"),
    ("Gürcistan", "Georgia"), ("Macaristan", "Hungary"), ("Norveç", "Norway"), ("Galler", "Wales"), ("Portekiz", "Portugal"), ("Danimarka", "Denmark"),
    ("Belçika", "Belgium"), ("İtalya", "Italy"), ("Hırvatistan", "Croatia"), ("Çekya", "Czechia"), ("İspanya", "Spain"), ("İngiltere", "England"),
    ("Almanya", "Germany"), ("Hollanda", "Netherlands"), ("Kuzey Makedonya Cumhuriyeti", "North Macedonia"), ("Slovenya", "Slovenia"),
    ("Yunanistan", "Greece"), ("Sırbistan", "Serbia"), ("İsviçre", "Switzerland"), ("İskoçya", "Scotland"),
    # Uluslar Ligi'nin kalan takımları: sözlükte olup Spor Toto'da henüz görülmeyenler de aynı kuralla bağlanır
    ("İrlanda", "Ireland"), ("Kuzey İrlanda", "Northern Ireland"), ("Bosna Hersek", "Bosnia and Herzegovina"), ("İsveç", "Sweden"), ("Kosova", "Kosovo"),
]
TAKIMLAR = pd.DataFrame([{"id": 1000 + i, "fotmob_team_id": 5000 + i, "name": en} for i, (_, en) in enumerate(CIFTLER)]
                        + [{"id": 1, "fotmob_team_id": 1, "name": "Irlanda FC"}, {"id": 2, "fotmob_team_id": 2, "name": "Republic of Ireland"}])


@pytest.fixture
def depo(monkeypatch):
    yazilan = []
    monkeypatch.setattr(stf, "query_to_df", lambda sql, params=None: pd.DataFrame([{"matchforge_team_id": None}]) if "spor_toto_team_map" in sql else TAKIMLAR)
    monkeypatch.setattr(stf, "execute_insert", lambda sql, p=None: yazilan.append(p))
    return yazilan


@pytest.mark.parametrize("tr_ad,en_ad", CIFTLER)
def test_milli_takim_adi_veritabani_adina_baglanir(depo, tr_ad, en_ad):
    beklenen = int(TAKIMLAR[TAKIMLAR.name == en_ad].id.iloc[0])
    assert stf.resolve_team(7000, tr_ad)["team_id"] == beklenen
    assert depo[-1]["matchforge_team_id"] == beklenen


def test_irlanda_cumhuriyeti_degil_veritabanindaki_irlandaya_gider(depo):
    # sözlükte "Republic of Ireland" ve "Ireland" ikisi de "İrlanda"; veritabanında takım "Ireland"
    assert TAKIMLAR[TAKIMLAR.name == "Ireland"].id.iloc[0] == stf.resolve_team(1, "İrlanda")["team_id"]


def test_kulup_adlari_etkilenmez(monkeypatch):
    takimlar = pd.DataFrame([{"id": 76, "fotmob_team_id": 1, "name": "Athletic Club"}, {"id": 12, "fotmob_team_id": 2, "name": "Fenerbahçe"}])
    monkeypatch.setattr(stf, "query_to_df", lambda sql, params=None: pd.DataFrame([{"matchforge_team_id": None}]) if "spor_toto_team_map" in sql else takimlar)
    monkeypatch.setattr(stf, "execute_insert", lambda sql, p=None: None)
    assert stf.resolve_team(112, "Athletic Bilbao")["team_id"] == 76
    assert stf.resolve_team(9, "Fenerbahçe")["team_id"] == 12
