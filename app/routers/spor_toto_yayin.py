"""
Spor Toto yayın sayfaları (web /yayinlar): haftanın YouTube yayın bağlantısı ve sitede gösterilen güvenli maç bilgisi.

Önemli: bu uçlar herkese açık sayfaları besler; tercih (secilen_senaryo), yorum metni ve model alanları BURADAN ASLA DÖNMEZ.
Yalnız hafta, maç sırası, takım adları ve fotmob takım kimlikleri, lig, tarih ve yayın bağlantısı.
"""
import re
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.db import execute_insert, query_to_df

router = APIRouter(tags=["spor-toto-yayin"])


class YoutubeLinki(BaseModel):
    youtube_linki: str


_YT = re.compile(r"^https?://(?:www\.|m\.)?(?:youtube\.com/(?:watch\?(?:[^#]*&)?v=|live/|shorts/|embed/)|youtu\.be/)([A-Za-z0-9_-]{11})(?:[?&#/].*)?$")


def youtube_id(link: str) -> Optional[str]:
    """YouTube bağlantısından 11 karakterlik video kimliği; geçersizse None."""
    m = _YT.match((link or "").strip())
    return m.group(1) if m else None


def hafta_numarasi(etiket: Optional[str], hafta_no: int) -> int:
    """Sitedeki hafta numarası: etiketteki ilk sayı ("12. Hafta" -> 12); etiket yoksa ya da sayı içermiyorsa hafta_no."""
    m = re.search(r"\d+", etiket or "")
    return int(m.group()) if m else int(hafta_no)


@router.put("/spor-toto/weeks/{hafta_no}/youtube")
async def youtube_linki_kaydet(hafta_no: int, govde: YoutubeLinki):
    """Haftanın yayın bağlantısını kaydeder; boş gönderilirse bağlantıyı kaldırır."""
    link = govde.youtube_linki.strip()
    if link and not youtube_id(link):
        raise HTTPException(status_code=400, detail="Geçerli bir YouTube bağlantısı girin")
    satir = execute_insert(
        "UPDATE greydb.spor_toto_weeks SET youtube_linki = :link WHERE hafta_no = :h RETURNING id",
        {"link": link or None, "h": hafta_no},
    )
    if not satir:
        raise HTTPException(status_code=404, detail="Hafta bulunamadı")
    return {"hafta_no": hafta_no, "youtube_linki": link or None}


def _haftalar():
    """Yayını olan haftalar. Hafta için bağlantı: önce haftanın kendi youtube_linki; yoksa maçlarına önceden girilmiş ilk YouTube bağlantısı."""
    df = query_to_df(
        """
        SELECT w.id, w.hafta_no, w.hafta_etiketi,
               COALESCE(NULLIF(w.youtube_linki, ''),
                        (SELECT m.video_linki FROM greydb.spor_toto_matches m
                         WHERE m.week_id = w.id AND m.video_linki ~* 'youtu' ORDER BY m.mac_no LIMIT 1)) AS youtube_linki,
               (SELECT COUNT(*) FROM greydb.spor_toto_matches m WHERE m.week_id = w.id) AS mac_sayisi
        FROM greydb.spor_toto_weeks w
        ORDER BY w.hafta_no DESC
        """
    )
    sonuc = []
    for _, r in df.iterrows():
        vid = youtube_id(r["youtube_linki"]) if isinstance(r["youtube_linki"], str) else None
        if not vid:
            continue
        sonuc.append({"id": int(r["id"]), "hafta_no": int(r["hafta_no"]), "hafta": hafta_numarasi(r["hafta_etiketi"], int(r["hafta_no"])),
                      "etiket": r["hafta_etiketi"], "video_id": vid, "mac_sayisi": int(r["mac_sayisi"])})
    return sonuc


@router.get("/spor-toto/yayinlar")
async def yayin_listesi():
    """Yayını girilmiş haftalar, yeniden eskiye (aynı hafta numarası tekrar ederse en yeni kayıt kalır)."""
    goruldu, liste = set(), []
    for h in _haftalar():
        if h["hafta"] in goruldu:
            continue
        goruldu.add(h["hafta"])
        liste.append({k: h[k] for k in ("hafta", "etiket", "video_id", "mac_sayisi")})
    return {"haftalar": liste}


@router.get("/spor-toto/yayinlar/{hafta}")
async def yayin_haftasi(hafta: int):
    secili = next((h for h in _haftalar() if h["hafta"] == hafta), None)
    if not secili:
        raise HTTPException(status_code=404, detail="Bu hafta için yayın yok")
    df = query_to_df(
        """
        SELECT m.mac_no, m.ev_takim, m.deplasman_takim, m.lig, m.mac_tarihi,
               th.fotmob_team_id AS ev_fotmob_id, ta.fotmob_team_id AS dep_fotmob_id
        FROM greydb.spor_toto_matches m
        LEFT JOIN public.teams th ON th.id = m.home_team_id
        LEFT JOIN public.teams ta ON ta.id = m.away_team_id
        WHERE m.week_id = %s ORDER BY m.mac_no
        """,
        (secili["id"],),
    )
    maclar = []
    for _, r in df.iterrows():
        maclar.append({
            "mac_no": int(r["mac_no"]), "ev": r["ev_takim"], "dep": r["deplasman_takim"], "lig": r["lig"] if r["lig"] == r["lig"] else None,
            "tarih": r["mac_tarihi"].isoformat() if hasattr(r["mac_tarihi"], "isoformat") else None,
            "ev_fotmob_id": int(r["ev_fotmob_id"]) if r["ev_fotmob_id"] == r["ev_fotmob_id"] and r["ev_fotmob_id"] is not None else None,
            "dep_fotmob_id": int(r["dep_fotmob_id"]) if r["dep_fotmob_id"] == r["dep_fotmob_id"] and r["dep_fotmob_id"] is not None else None,
        })
    return {"hafta": secili["hafta"], "etiket": secili["etiket"], "video_id": secili["video_id"], "maclar": maclar}
