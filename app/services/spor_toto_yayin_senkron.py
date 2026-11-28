"""Spor Toto haftalık analiz videosunun bağlantısını YouTube'daki "GREYSPORTS | SPOR TOTO MASASI" oynatma listesinden otomatik doldurur (web /yayinlar).

Listenin herkese açık RSS akışı (API anahtarı gerekmez) okunur: kaynak liste elle seçilmiştir, kanalın kısa videoları ve oyunları içinde yoktur. Başlığında
"SPOR TOTO N. HAFTA" ya da "SPOR TOTO N. OYUN PROGRAMI" geçen video o haftanındır; ve o haftanın yayın bağlantısı BOŞSA yazılır. Elle girilmiş bağlantıya asla dokunulmaz.
Aynı hafta için birden çok video varsa: "15 Maç Analizi" olan, sonra canlı yayın tekrarı olmayan, sonra en eski yayın (ana video önce yüklenir).
"""
import logging
import re
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("spor-toto-yayin-senkron")

LISTE_ID = "PLettuYyGVIcI"      # GREYSPORTS | SPOR TOTO MASASI
AKIS_URL = f"https://www.youtube.com/feeds/videos.xml?playlist_id={LISTE_ID}"
_BASLIK = re.compile(r"spor\s*toto\s+(\d{1,2})\s*\.\s*(?:hafta|oyun\s+program)", re.IGNORECASE)
_ANALIZ = re.compile(r"15\s*ma[çc]\s*anal[iİı]z", re.IGNORECASE)
_CANLI = re.compile(r"canl[iİı]", re.IGNORECASE)
_VIDEO_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")
_ENTRY = re.compile(r"<entry>[\s\S]*?</entry>")


def akis_oku(xml: str) -> List[Dict[str, str]]:
    """RSS akışından [{id, baslik, tarih}] (yeniden eskiye, akış sırası)."""
    sonuc = []
    for e in _ENTRY.findall(xml or ""):
        vid = re.search(r"<yt:videoId>([^<]*)</yt:videoId>", e)
        baslik = re.search(r"<title>([^<]*)</title>", e)
        tarih = re.search(r"<published>([^<]*)</published>", e)
        if vid and baslik and _VIDEO_ID.match(vid.group(1).strip()):
            ad = baslik.group(1).replace("&amp;", "&").replace("&#39;", "'").replace("&quot;", '"')
            sonuc.append({"id": vid.group(1).strip(), "baslik": ad, "tarih": tarih.group(1) if tarih else ""})
    return sonuc


def _oncelik(v: Dict[str, str]) -> tuple:
    """Küçük olan kazanır: 15 Maç Analizi, canlı tekrarı değil, en eski yayın."""
    return (0 if _ANALIZ.search(v["baslik"]) else 1, 1 if _CANLI.search(v["baslik"]) else 0, v["tarih"])


def hafta_videolari(videolar: List[Dict[str, str]]) -> Dict[int, Dict[str, str]]:
    """Başlığında "SPOR TOTO N. HAFTA/OYUN PROGRAMI" geçen videolar, hafta numarasına göre (aynı hafta için _oncelik)."""
    sonuc: Dict[int, Dict[str, str]] = {}
    for v in videolar:
        m = _BASLIK.search(v["baslik"])
        if not m:
            continue
        n = int(m.group(1))
        if 1 <= n <= 60 and (n not in sonuc or _oncelik(v) < _oncelik(sonuc[n])):
            sonuc[n] = v
    return sonuc


def _haftalar() -> List[Dict[str, Any]]:
    from app.services.db import execute_query
    return [dict(r) for r in execute_query("SELECT hafta_no, hafta_etiketi, youtube_linki FROM greydb.spor_toto_weeks ORDER BY hafta_no DESC")]


def _akis_getir() -> str:
    import httpx
    r = httpx.get(AKIS_URL, timeout=15.0, follow_redirects=True)
    r.raise_for_status()
    return r.text


def _yaz(hafta_no: int, link: str) -> int:
    from app.services.db import execute_insert
    s = execute_insert("UPDATE greydb.spor_toto_weeks SET youtube_linki = :l WHERE hafta_no = :h AND COALESCE(youtube_linki, '') = '' RETURNING id", {"l": link, "h": hafta_no})
    return 1 if s else 0


def calistir(dry_run: bool = True, akis: Callable[[], str] = _akis_getir, haftalar: Callable[[], List[Dict[str, Any]]] = _haftalar,
             yaz: Callable[[int, str], int] = _yaz) -> Dict[str, Any]:
    """Bağlantısı boş haftalar için kanalda ana analiz videosu varsa bağlantıyı yazar. Rapor: {dry_run, yazilan: [{hafta, video_id, baslik}], bekleyen: [hafta...]}."""
    from app.routers.spor_toto_yayin import hafta_numarasi
    videolar = hafta_videolari(akis_oku(akis()))
    yazilan, bekleyen = [], []
    for h in haftalar():
        if (h.get("youtube_linki") or "").strip():
            continue                                  # elle girilmiş ya da daha önce doldurulmuş: dokunma
        n = hafta_numarasi(h.get("hafta_etiketi"), int(h["hafta_no"]))
        v = videolar.get(n)
        if not v:
            bekleyen.append(n)
            continue
        link = f"https://www.youtube.com/watch?v={v['id']}"
        if not dry_run and not yaz(int(h["hafta_no"]), link):
            continue
        yazilan.append({"hafta": n, "video_id": v["id"], "baslik": v["baslik"]})
    return {"dry_run": dry_run, "yazilan": yazilan, "bekleyen": bekleyen}
