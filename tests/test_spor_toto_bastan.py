"""Spor Toto baştan üretim: onaylı maç varsayılan 409; bastan=true ile yeniden üretilir, onay yeni taslak yazıldıktan SONRA silinir; üretim patlarsa onay korunur."""
import asyncio

import pandas as pd
import pytest
from fastapi import HTTPException

from app.routers import spor_toto as st


def _mac(onayli: bool):
    return pd.DataFrame([{"id": 7, "final_metin": "Ana kupon tercihim 2 olacak." if onayli else None, "home_team_id": 1, "away_team_id": 2,
                          "ev_takim": "A", "deplasman_takim": "B", "lig_haftasi": "8. Hafta", "mac_tarihi": None}])


def _kur(monkeypatch, onayli, uretim_patlar=False):
    olaylar = []
    monkeypatch.setattr(st, "query_to_df", lambda sql, params=None: _mac(onayli))
    monkeypatch.setattr(st, "execute_insert", lambda sql, p=None: olaylar.append(("sql", " ".join(sql.split()), p)))
    def uret(*a, **k):
        if uretim_patlar:
            raise RuntimeError("model yok")
        olaylar.append(("uretildi",))
    monkeypatch.setattr(st, "_generate_for_match", uret)
    return olaylar


def test_onayli_maca_varsayilan_dokunulmaz(monkeypatch):
    olaylar = _kur(monkeypatch, onayli=True)
    with pytest.raises(HTTPException) as e:
        asyncio.run(st.generate_match(7))
    assert e.value.status_code == 409 and olaylar == []


def test_bastan_onayli_maci_yeniden_uretir_ve_onayi_sonra_siler(monkeypatch):
    olaylar = _kur(monkeypatch, onayli=True)
    r = asyncio.run(st.generate_match(7, bastan=True))
    assert r["message"] == "Baştan üretildi"
    assert olaylar[0] == ("uretildi",) and olaylar[1][0] == "sql"          # önce üretim, sonra onay silme
    sql = olaylar[1][1]
    assert sql.startswith("UPDATE greydb.spor_toto_matches SET final_metin = NULL, secilen_senaryo = NULL") and "video" not in sql and olaylar[1][2] == {"id": 7}


def test_bastan_uretim_patlarsa_onay_korunur(monkeypatch):
    olaylar = _kur(monkeypatch, onayli=True, uretim_patlar=True)
    with pytest.raises(HTTPException) as e:
        asyncio.run(st.generate_match(7, bastan=True))
    assert e.value.status_code == 502 and not any(o[0] == "sql" for o in olaylar)


def test_taslagi_hazir_mac_normal_akis_degismez(monkeypatch):
    olaylar = _kur(monkeypatch, onayli=False)
    r = asyncio.run(st.generate_match(7))
    assert r["message"] == "Üretildi" and olaylar == [("uretildi",)]
