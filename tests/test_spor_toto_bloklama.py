"""Spor Toto üretimi (Skorjin'e senkron 90 sn'ye kadar süren çağrı) API'nin olay döngüsünü bloklamamalı: üretim sürerken kalp atışı, işçi sorgusu, sitemap verisi gibi diğer istekler cevap verebilmeli."""
import asyncio
import inspect
import time

import httpx
import pandas as pd

from app.main import app
from app.routers import spor_toto as r


def test_bloklayan_uclar_async_def_degil():
    """async def içinde senkron ağ/DB çağrısı olay döngüsünü tutar; düz def FastAPI'de iş parçacığı havuzunda çalışır."""
    for ad in ("generate_match", "fetch_week", "get_week", "list_weeks", "update_selection"):
        assert not inspect.iscoroutinefunction(getattr(r, ad)), f"{ad} async def olmamalı"


def test_uretim_surerken_diger_istekler_bekletilmez(monkeypatch):
    satir = pd.DataFrame([{"final_metin": None, "home_team_id": 1, "away_team_id": 2, "ev_takim": "A", "deplasman_takim": "B", "lig_haftasi": "1. Hafta", "mac_tarihi": None}])
    monkeypatch.setattr(r, "query_to_df", lambda *a, **k: satir)
    basladi = []

    def yavas_uretim(*a, **k):
        basladi.append(time.monotonic()); time.sleep(0.8)                              # yavaş üretim (Skorjin çağrısını taklit eder)

    monkeypatch.setattr(r, "_generate_for_match", yavas_uretim)

    async def dene():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
            t0 = time.monotonic()                                                          # ölçüm üretimin BAŞLANGICINDAN: döngü bloklanırsa hızlı istek ancak üretim bitince cevap alır
            yavas = asyncio.create_task(c.post("/api/spor-toto/matches/1/generate"))
            while not basladi:                                                             # üretim gerçekten başlasın; döngü bloklandıysa bu bekleme ancak üretim bitince döner
                await asyncio.sleep(0.01)
            hizli = await c.get("/openapi.json")
            gecen = time.monotonic() - t0
            assert (await yavas).status_code == 200
            return hizli.status_code, gecen

    kod, gecen = asyncio.run(dene())
    assert kod == 200 and gecen < 0.4, f"hızlı istek üretimi bekledi ({gecen:.2f} sn)"
