"""iddaa maç kimliğini (iddaa_event_id) fotmob maçlarına bağlar. Zamanlayıcı (GitHub Actions) çağırır.

Yazan bir uç olduğu için X-Cron-Anahtari başlığıyla korunur; sunucuda CRON_ANAHTARI tanımlı değilse uç tamamen kapalıdır (503).
Varsayılan KURU çalışmadır: yalnızca raporlar. Yazmak için ?dry_run=false.
"""
import hmac
import os
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, Header, HTTPException

from app.services.db import execute_query, transaction, execute_insert
from app.services.iddaa_eslestir import eslestir

router = APIRouter(tags=["iddaa"])

_LISTE = "https://sportsbookv2.iddaa.com/sportsbook/events"
_BASLIK = {"User-Agent": "Mozilla/5.0", "Origin": "https://www.iddaa.com", "Referer": "https://www.iddaa.com/"}
_ALTER = [
    "ALTER TABLE public.matches ADD COLUMN IF NOT EXISTS iddaa_event_id BIGINT",
    "ALTER TABLE public.upcoming_matches ADD COLUMN IF NOT EXISTS iddaa_event_id BIGINT",
    "CREATE INDEX IF NOT EXISTS idx_matches_iddaa_event_id ON public.matches (iddaa_event_id)",
    "CREATE INDEX IF NOT EXISTS idx_upcoming_matches_iddaa_event_id ON public.upcoming_matches (iddaa_event_id)",
]
# Pencere: biten son 6 saat ile gelecek 4 gün (iddaa listesi ~3 gün; canlı liste oynanmakta olanları da verir)
_MACLAR = """
SELECT m.fotmob_match_id, ev.name AS home, dep.name AS away, m.match_date
FROM public.matches m JOIN public.teams ev ON ev.id = m.home_team_id JOIN public.teams dep ON dep.id = m.away_team_id
WHERE m.fotmob_match_id IS NOT NULL AND m.match_date BETWEEN :bas AND :son
UNION
SELECT u.fotmob_match_id, u.home_team_name, u.away_team_name, u.match_date
FROM public.upcoming_matches u
WHERE u.fotmob_match_id IS NOT NULL AND u.match_date BETWEEN :bas AND :son
"""


def _anahtar():
    return os.environ.get("CRON_ANAHTARI") or None


def olaylari_cek():
    olaylar = []
    with httpx.Client(headers=_BASLIK, timeout=30) as istemci:
        for tip in (0, 1):   # 0: maç önü program, 1: canlı
            yanit = istemci.get(_LISTE, params={"st": 1, "type": tip})
            yanit.raise_for_status()
            olaylar += yanit.json()["data"]["events"]
    return olaylar


def yakin_maclar(bas, son):
    return execute_query(_MACLAR, {"bas": bas, "son": son})


def iddaa_yaz(eslesen):
    with transaction():
        for q in _ALTER:
            execute_insert(q)
        for fid, i in eslesen.items():
            for tablo in ("public.matches", "public.upcoming_matches"):
                execute_insert(f"UPDATE {tablo} SET iddaa_event_id = :i WHERE fotmob_match_id = :fid", {"i": i, "fid": fid})


@router.post("/iddaa/eslestir")
async def iddaa_eslestir(dry_run: bool = True, x_cron_anahtari: str = Header(default="")):
    anahtar = _anahtar()
    if not anahtar:
        raise HTTPException(status_code=503, detail="Uç kapalı: sunucuda CRON_ANAHTARI tanımlı değil")
    if not hmac.compare_digest(x_cron_anahtari.encode(), anahtar.encode()):
        raise HTTPException(status_code=401, detail="Geçersiz anahtar")
    try:
        olaylar = olaylari_cek()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"iddaa listesi alınamadı: {type(e).__name__}")
    simdi = datetime.now(timezone.utc).replace(tzinfo=None)
    satirlar = yakin_maclar(simdi - timedelta(hours=6), simdi + timedelta(days=4))
    maclar = [
        {"fotmob_match_id": s["fotmob_match_id"], "home": s["home"], "away": s["away"],
         "kickoff_ts": s["match_date"].replace(tzinfo=timezone.utc).timestamp()}
        for s in satirlar
    ]
    sonuc = eslestir(olaylar, maclar)
    if not dry_run and sonuc["eslesen"]:
        iddaa_yaz(sonuc["eslesen"])
    ad = lambda m: {"fotmob_match_id": m["fotmob_match_id"], "mac": f"{m['home']} - {m['away']}"}   # noqa: E731
    return {
        "dry_run": dry_run, "iddaa_olay": len(olaylar), "toplam_mac": len(maclar), "eslesen": len(sonuc["eslesen"]),
        "belirsiz": [ad(m) for m in sonuc["belirsiz"]], "eslesmeyen": [ad(m) for m in sonuc["eslesmeyen"]],
    }
