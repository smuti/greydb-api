"""iddaa maç kimliğini (iddaa_event_id) fotmob maçlarına bağlar. Zamanlayıcı (GitHub Actions) çağırır.

Yazan bir uç olduğu için X-Cron-Anahtari başlığıyla korunur; sunucuda CRON_ANAHTARI tanımlı değilse uç tamamen kapalıdır (503).
Varsayılan KURU çalışmadır: yalnızca raporlar. Yazmak için ?dry_run=false.
"""
import hmac
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, Header, HTTPException

from app.services.db import execute_query, transaction, execute_insert
from app.services.iddaa_dagilim import dagilim_olustur
from app.services.iddaa_eslestir import eslestir, takim_benzer, TOLERANS_SN

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


def _simdi():
    return datetime.now(timezone.utc).replace(tzinfo=None)


_ACIKLAMA = {
    "basladi": "Maç başlamış ya da bitmiş: iddaa maç önü listesinde artık yok (beklenen).",
    "pencere_disi": "iddaa bu maçı henüz programa almadı (listesi yaklaşık 3 gün ilerisine kadar). Yaklaşınca sonraki çalışmada eşleşir.",
    "ad_sorunu_olasi": "DİKKAT: aynı saatte iddaa'da benzer adlı bir maç var ama iki takım adı birden benzemiyor. Takım adı yazımı sorunu olabilir.",
    "iddaa_de_yok": "iddaa bu maçı listelemiyor (aynı saatte benzer adlı maç yok). Lig iddaa'da olmayabilir.",
}


def _neden(m, olaylar, simdi, ufuk):
    """Eşleşmeyen maçın nedeni. (neden, aday listesi) döner."""
    kick = m["kickoff_ts"]
    if m["match_date"] < simdi:
        return "basladi", []
    if ufuk is not None and kick > ufuk + TOLERANS_SN:
        return "pencere_disi", []
    aday = [
        f"{e['hn'].strip()} - {e['an'].strip()}" for e in olaylar
        if abs(e["d"] - kick) <= TOLERANS_SN and (takim_benzer(m["home"], e["hn"]) or takim_benzer(m["away"], e["an"]))
    ]
    return ("ad_sorunu_olasi", aday[:3]) if aday else ("iddaa_de_yok", [])


def olaylari_cek():
    olaylar = []
    with httpx.Client(headers=_BASLIK, timeout=30) as istemci:
        for tip in (0, 1):   # 0: maç önü program, 1: canlı
            yanit = istemci.get(_LISTE, params={"st": 1, "type": tip})
            yanit.raise_for_status()
            olaylar += [{**e, "_liste": tip} for e in yanit.json()["data"]["events"]]
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
def iddaa_eslestir(dry_run: bool = True, x_cron_anahtari: str = Header(default="")):
    anahtar = _anahtar()
    if not anahtar:
        raise HTTPException(status_code=503, detail="Uç kapalı: sunucuda CRON_ANAHTARI tanımlı değil")
    if not hmac.compare_digest(x_cron_anahtari.encode(), anahtar.encode()):
        raise HTTPException(status_code=401, detail="Geçersiz anahtar")
    try:
        return eslestir_calistir(dry_run)
    except IddaaListeHatasi as e:
        raise HTTPException(status_code=502, detail=str(e))


class IddaaListeHatasi(Exception):
    pass


def eslestir_calistir(dry_run: bool = True) -> Dict[str, Any]:
    """Eşleştirmenin kendisi (uç ve uygulama içi zamanlayıcı ortak kullanır)."""
    try:
        olaylar = olaylari_cek()
    except Exception as e:
        raise IddaaListeHatasi(f"iddaa listesi alınamadı: {type(e).__name__}")
    simdi = _simdi()
    satirlar = yakin_maclar(simdi - timedelta(hours=6), simdi + timedelta(days=4))
    # Aynı maç hem matches hem upcoming_matches'ta (farklı saat/yazımla) olabilir: kimlik başına en güncel (son saatli) satır
    tekil = {}
    for s_ in satirlar:
        if s_["fotmob_match_id"] not in tekil or s_["match_date"] > tekil[s_["fotmob_match_id"]]["match_date"]:
            tekil[s_["fotmob_match_id"]] = s_
    maclar = [
        {"fotmob_match_id": s_["fotmob_match_id"], "home": s_["home"], "away": s_["away"], "match_date": s_["match_date"],
         "kickoff_ts": s_["match_date"].replace(tzinfo=timezone.utc).timestamp()}
        for s_ in tekil.values()
    ]
    sonuc = eslestir(olaylar, maclar)
    if not dry_run and sonuc["eslesen"]:
        iddaa_yaz(sonuc["eslesen"])
    ad = lambda m: f"{m['home']} - {m['away']}"   # noqa: E731
    saat = lambda m: m["match_date"].isoformat() + "Z"   # noqa: E731
    ufuk = max((e["d"] for e in olaylar if e.get("_liste", 0) == 0), default=None)   # canlı listede çok ileri tek tük olay olabilir
    eslesmeyen = []
    for m in sorted(sonuc["eslesmeyen"], key=lambda m: m["match_date"]):
        neden, aday = _neden(m, olaylar, simdi, ufuk)
        eslesmeyen.append({"fotmob_match_id": m["fotmob_match_id"], "mac": ad(m), "saat": saat(m), "neden": neden,
                           "aciklama": _ACIKLAMA[neden], "aday": aday})
    por_id = {m["fotmob_match_id"]: m for m in maclar}
    return {
        "dry_run": dry_run, "iddaa_olay": len(olaylar), "toplam_mac": len(maclar), "eslesen": len(sonuc["eslesen"]),
        "iddaa_ufku": datetime.fromtimestamp(ufuk, timezone.utc).isoformat() if ufuk else None,
        "eslesenler": [{"fotmob_match_id": fid, "mac": ad(por_id[fid]), "iddaa_event_id": i} for fid, i in sonuc["eslesen"].items()],
        # İkinci geçiş: yalnızca bir takım adı benzeyen ama aynı saatte tek aday olan maçlar (incelemek için ayrıca listelenir)
        "tek_taraf_eslesen": [{"fotmob_match_id": fid, "mac": ad(por_id[fid]), "iddaa_event_id": sonuc["eslesen"][fid]} for fid in sonuc["tek_taraf"]],
        "belirsiz": [{"fotmob_match_id": m["fotmob_match_id"], "mac": ad(m), "saat": saat(m)} for m in sonuc["belirsiz"]],
        "eslesmeyen": eslesmeyen,
    }




_ONBELLEK: Dict[str, Any] = {}
_TTL_SN = 120


def _iddaa_json(yol: str, params: Optional[Dict[str, Any]] = None):
    anahtar = yol + str(sorted((params or {}).items()))
    simdi = time.time()
    if anahtar in _ONBELLEK and simdi - _ONBELLEK[anahtar][0] < _TTL_SN:
        return _ONBELLEK[anahtar][1]
    with httpx.Client(headers=_BASLIK, timeout=30) as istemci:
        yanit = istemci.get(f"https://sportsbookv2.iddaa.com/sportsbook/{yol}", params=params)
        yanit.raise_for_status()
        veri = yanit.json().get("data")
    _ONBELLEK[anahtar] = (simdi, veri)
    return veri


def iddaa_dagilim_verisi(event_id: int):
    """(maç detayı, bu maçın seçim yüzdeleri, maçın tüm kuponlardaki payı, market tablosu). Kısa süreli önbellekli."""
    olay = _iddaa_json(f"event/{event_id}")
    yuzdeler = (_iddaa_json("outcome-play-percentages", {"sportType": 1}) or {}).get(str(event_id), {})
    pay = (_iddaa_json("played-event-percentage", {"sportType": 1}) or {}).get(str(event_id))
    cfg = (_iddaa_json("get_market_config") or {}).get("m", {})
    return olay, yuzdeler, pay, cfg


@router.get("/iddaa/dagilim/{event_id}")
def iddaa_dagilim(event_id: int):
    """Maçın iddaa'daki oynanma dağılımı: en çok oynanan 10 seçim ve market içi yüzdeler (iddaa'nın kendi açık verisi)."""
    try:
        olay, yuzdeler, pay, cfg = iddaa_dagilim_verisi(event_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"iddaa verisi alınamadı: {type(e).__name__}")
    if not olay:
        raise HTTPException(status_code=404, detail="iddaa'da böyle bir maç yok")
    return {"iddaa_event_id": event_id, "ev": olay.get("hn"), "dep": olay.get("an"), **dagilim_olustur(olay, yuzdeler, pay, cfg)}
