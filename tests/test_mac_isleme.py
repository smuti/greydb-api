"""Maç işleme: tek bağlantı/tek işlem, olay döngüsünü kilitlememe, eşzamanlı çalıştırmayı reddetme.
Gerçek veritabanına dokunmaz: bellek içi SQLite ve sahte bağımlılıklar kullanır."""
import asyncio
import threading
import time

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from app.routers import match_data as md
from app.services import db


@pytest.fixture
def sqlite(monkeypatch):
    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    with eng.begin() as c:
        c.execute(text("create table t (v int)"))

    class Sayac:
        baglanti = 0

        def connect(self):
            Sayac.baglanti += 1
            return eng.connect()

    monkeypatch.setattr(db, "engine", Sayac())
    Sayac.baglanti = 0
    return Sayac


def say():
    return db.execute_query("select count(*) as n from t")[0]["n"]


def test_islem_basarili_olursa_hepsi_yazilir(sqlite):
    with db.transaction():
        db.execute_insert("insert into t values (:v)", {"v": 1})
        db.execute_insert("insert into t values (:v)", {"v": 2})
    assert say() == 2


def test_islemde_hata_olursa_hicbir_sey_yazilmaz(sqlite):
    with pytest.raises(RuntimeError):
        with db.transaction():
            db.execute_insert("insert into t values (:v)", {"v": 1})
            raise RuntimeError("yarıda kesildi")
    assert say() == 0


def test_islem_icindeki_tum_sorgular_tek_baglantiyi_kullanir(sqlite):
    with db.transaction():
        for i in range(10):
            db.execute_insert("insert into t values (:v)", {"v": i})
            db.execute_query("select count(*) from t")
    assert sqlite.baglanti == 1


def test_islem_disinda_davranis_eskisi_gibi(sqlite):
    db.execute_insert("insert into t values (:v)", {"v": 1})
    db.execute_insert("insert into t values (:v)", {"v": 2})
    assert sqlite.baglanti == 2 and say() == 2


def test_islem_bitince_baglanti_serbest_ve_sonraki_sorgu_islem_disi(sqlite):
    with db.transaction():
        pass
    before = sqlite.baglanti
    db.execute_query("select 1")
    assert sqlite.baglanti == before + 1


# ---- match_data router ----
_GERCEK_SLEEP = asyncio.sleep
DATA = {"general": {}}
INFO = {"finished": True, "home_score": 1, "away_score": 0, "home_team_name": "A", "away_team_name": "B"}


def kur(monkeypatch, kayit, save=None, lig_adi="Ligue 1"):
    sorgular = []

    def execute_query(sql, params=None):
        sorgular.append(sql)
        if "DISTINCT l.id" in sql:
            return [{"id": 7, "name": lig_adi, "fotmob_league_id": 53}]
        return [{"id": 1, "fotmob_match_id": 100, "match_url": "u", "match_date": None, "home_team_name": "A",
                 "away_team_name": "B", "round": 1, "league_name": lig_adi, "fotmob_league_id": 53, "league_id": 7}]

    async def fetch(_):
        return DATA

    class Tx:
        def __enter__(self):
            kayit.append("enter")

        def __exit__(self, *a):
            kayit.append("exit")
            return False

    monkeypatch.setattr(md, "execute_query", execute_query)
    monkeypatch.setattr(md, "fetch_match_details", fetch)
    monkeypatch.setattr(md, "parse_match_info", lambda d: INFO)
    monkeypatch.setattr(md, "save_full_match_data", save or (lambda d: kayit.append("save")))
    monkeypatch.setattr(md, "execute_insert", lambda sql, p=None: kayit.append("mark"))
    monkeypatch.setattr(md, "transaction", lambda: Tx())
    monkeypatch.setattr(md.asyncio, "sleep", lambda s: _GERCEK_SLEEP(0))
    return sorgular


def test_maci_kaydet_ve_isaretle_tek_islemde(monkeypatch):
    kayit = []
    kur(monkeypatch, kayit)
    r = asyncio.run(md.process_finished_matches())
    assert kayit == ["enter", "save", "mark", "exit"]
    assert r.processed_count == 1 and r.error_count == 0


def test_kuru_calistirmada_hicbir_sey_yazilmaz(monkeypatch):
    kayit = []
    kur(monkeypatch, kayit)
    r = asyncio.run(md.process_finished_matches(dry_run=True))
    assert kayit == [] and r.finished_count == 1 and r.processed_count == 0


def test_kayit_olay_dongusunu_kilitlemez(monkeypatch):
    kayit, iplikler = [], []

    def yavas(d):
        iplikler.append(threading.current_thread())
        time.sleep(0.4)

    kur(monkeypatch, kayit, save=yavas)

    async def dene():
        vurus = 0

        async def nabiz():
            nonlocal vurus
            while True:
                await asyncio.sleep(0.02)
                vurus += 1

        t = asyncio.create_task(nabiz())
        await md.process_finished_matches()
        t.cancel()
        return vurus

    vurus = asyncio.run(dene())
    assert iplikler[0] is not threading.main_thread()
    assert vurus >= 5, f"olay döngüsü kilitlendi (vuruş={vurus})"


def test_hata_olursa_maç_hata_olarak_sayilir_diger_islemler_surer(monkeypatch):
    kayit = []

    def patla(d):
        raise ValueError("kaydedilemedi")

    kur(monkeypatch, kayit, save=patla)
    r = asyncio.run(md.process_finished_matches())
    assert r.error_count == 1 and r.processed_count == 0 and "kaydedilemedi" in r.errors[0]
    assert "mark" not in kayit


def test_ayni_anda_ikinci_calistirma_409(monkeypatch):
    kayit = []

    def yavas(d):
        time.sleep(0.3)

    kur(monkeypatch, kayit, save=yavas)

    async def dene():
        ilk = asyncio.create_task(md.process_finished_matches())
        await asyncio.sleep(0.1)
        with pytest.raises(HTTPException) as e:
            await md.process_finished_matches()
        await ilk
        return e.value.status_code

    assert asyncio.run(dene()) == 409
    # ilk iş bitince kilit açılır: yeni çalıştırma kabul edilir
    assert asyncio.run(md.process_finished_matches()).processed_count == 1


def test_lig_adindaki_tirnak_sorguyu_bozmaz(monkeypatch):
    kayit = []
    sorgular = kur(monkeypatch, kayit, lig_adi="Côte d'Ivoire Ligue 1")
    asyncio.run(md.process_finished_matches(dry_run=True))
    assert not any("d'Ivoire" in s for s in sorgular)


def test_refill_islem_basina_atomik_ve_olay_dongusunu_kilitlemez(monkeypatch):
    kayit = []
    monkeypatch.setattr(md, "execute_query", lambda sql, p=None: [{"id": 1, "fotmob_match_id": 5, "raw_match_details": {"a": 1}},
                                                                   {"id": 2, "fotmob_match_id": 6, "raw_match_details": "{\"a\": 2}"}])
    iplik = []

    class Tx:
        def __enter__(self):
            kayit.append("enter")

        def __exit__(self, *a):
            kayit.append("exit")
            return False

    def save(d):
        iplik.append(threading.current_thread())
        kayit.append(("save", d))
        if d == {"a": 2}:
            raise ValueError("kötü veri")

    monkeypatch.setattr(md, "save_full_match_data", save)
    monkeypatch.setattr(md, "transaction", lambda: Tx())
    r = asyncio.run(md.refill_missing_data(limit=2))
    assert r["total_found"] == 2 and r["filled"] == 1 and len(r["errors"]) == 1
    assert iplik[0] is not threading.main_thread()
    assert kayit.count("enter") == 2 and kayit.count("exit") == 2   # her maç ayrı işlem


def test_refill_ve_process_ayni_kilidi_paylasir(monkeypatch):
    kayit = []

    def yavas(d):
        time.sleep(0.3)

    kur(monkeypatch, kayit, save=yavas)

    async def dene():
        ilk = asyncio.create_task(md.process_finished_matches())
        await asyncio.sleep(0.1)
        with pytest.raises(HTTPException) as e:
            await md.refill_missing_data(limit=1)
        await ilk
        return e.value.status_code

    assert asyncio.run(dene()) == 409
