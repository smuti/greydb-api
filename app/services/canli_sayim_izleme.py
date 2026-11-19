"""Canlı faul / kart puanı / taç izleme: yönetici panelinin gösterdiği tahminler (ilk yarı / maç sonu / ev / deplasman beklentisi ve "N ve üstü"
olasılıkları) pazar başına kaydedilir, maç bitince gerçekleşen sayılar yazılır ve kalibrasyon özeti çıkarılır. Yalnız iç analiz; sitede gösterilmez.
Canlı korner izlemesiyle aynı düzen (canli_korner_izleme): doğrulama, gerçekleşen ve özet oradan; tablo ve sonuç kaynağı pazar başınadır.

Kayıt eklemelidir: maç, pazar ve yarı fazı başına 5 dakikalık dilimde en çok bir kayıt (UNIQUE fotmob_match_id, pazar, faz, kova).
Kart = iddaa kart puanı (sarı 1, ikinci sarıdan kırmızı 2, kırmızı 2); sonuç matchforge'un olaylardan saydığı kart_puani'ndan, yoksa sarı + 2 × kırmızı.
"""
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

from app.services import canli_korner_izleme as ck

log = logging.getLogger("canli-sayim-izleme")

PAZARLAR = ("faul", "kart", "tac")
SAYIM_UST = 100

TABLO_DDL = """
CREATE TABLE IF NOT EXISTS greydb.canli_sayim_kayit (
    id BIGSERIAL PRIMARY KEY,
    fotmob_match_id BIGINT NOT NULL,
    pazar TEXT NOT NULL,
    lig_ad TEXT,
    ev TEXT,
    dep TEXT,
    model_surum TEXT NOT NULL,
    faz TEXT NOT NULL,
    dakika INTEGER NOT NULL,
    kova INTEGER NOT NULL,
    skor JSONB NOT NULL,
    sayim JSONB NOT NULL,
    kirmizi JSONB NOT NULL,
    onceki JSONB,
    hedefler JSONB NOT NULL,
    kaydedildi TIMESTAMPTZ NOT NULL DEFAULT now(),
    gerceklesen JSONB,
    sonuclandi TIMESTAMPTZ,
    UNIQUE (fotmob_match_id, pazar, faz, kova)
);
CREATE INDEX IF NOT EXISTS idx_canli_sayim_bekleyen ON greydb.canli_sayim_kayit (kaydedildi) WHERE sonuclandi IS NULL;
"""


def kayit_hazirla(g: Any) -> Dict[str, Any]:
    """Web'den gelen kaydı doğrular; geçersizse ValueError."""
    if not isinstance(g, dict) or g.get("pazar") not in PAZARLAR:
        raise ValueError("pazar faul, kart ya da tac olmalı")
    k = ck.kayit_hazirla(g, alan="sayim", ust=SAYIM_UST)
    k["pazar"] = g["pazar"]
    return k


# ---------------- sonuçlandırma ----------------
@dataclass(frozen=True)
class Kaynaklar:
    bekleyen: Callable[[datetime], List[Dict[str, Any]]]                      # sonuçlanmamış kayıtlar: id, fotmob_match_id, pazar, kaydedildi
    mac_sonucu: Callable[[int], Optional[Dict[str, Any]]]                     # {finished, cancelled, pazarlar: {pazar: {ilk_yari, mac} | None}} ya da None
    sonuc_yaz: Callable[[int, Optional[Dict[str, int]], datetime], None]


def sonuclandir_bekleyenler(simdi: datetime, k: Kaynaklar, dry_run: bool = True) -> Dict[str, Any]:
    """Bitmiş maçların kayıtlarına pazar başına gerçekleşen sayıları yazar. İptal ve çok eski verisiz kayıtlar gerçekleşen=NULL ile işlenmiş sayılır;
    bitmiş ama bir pazarın dökümü (ilk yarı dahil) henüz yoksa o pazarın kayıtları bir sonraki koşuya kalır."""
    rapor: Dict[str, Any] = dict(dry_run=dry_run, islenen=0, bitmemis=0, iptal=0, verisiz=0, eksik=0, hata=[])
    gruplar: Dict[int, List[Dict[str, Any]]] = {}
    for r in k.bekleyen(simdi - timedelta(minutes=ck.SONUC_BEKLEME_DK)):
        gruplar.setdefault(int(r["fotmob_match_id"]), []).append(r)
    yaz = (lambda id_, g: k.sonuc_yaz(id_, g, simdi)) if not dry_run else (lambda id_, g: None)
    eski = lambda r: simdi - r["kaydedildi"].replace(tzinfo=None) > timedelta(days=ck.VAZGEC_GUN)
    for fid, satirlar in gruplar.items():
        try:
            veri = k.mac_sonucu(fid)
        except Exception as e:  # noqa: BLE001 — bir maç hata verse de diğerleri denenir
            log.warning("canlı sayım sonucu: maç verisi alınamadı (%s): %s", fid, e)
            rapor["hata"].append(f"{fid}: {str(e)[:150]}")
            continue
        if not veri:
            if all(eski(r) for r in satirlar):
                rapor["verisiz"] += len(satirlar)
                for r in satirlar:
                    yaz(r["id"], None)
            else:
                rapor["hata"].append(f"{fid}: maç verisi yok")
            continue
        if veri.get("cancelled"):
            rapor["iptal"] += len(satirlar)
            for r in satirlar:
                yaz(r["id"], None)
            continue
        if not veri.get("finished"):
            rapor["bitmemis"] += 1
            continue
        for r in satirlar:
            p = (veri.get("pazarlar") or {}).get(r["pazar"])
            if p:
                rapor["islenen"] += 1
                yaz(r["id"], ck.gerceklesen(p))
            elif eski(r):
                rapor["verisiz"] += 1
                yaz(r["id"], None)
            else:
                rapor["eksik"] += 1
    return rapor


def ozet(satirlar: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Pazar başına kalibrasyon özeti (canlı korner özetiyle aynı ölçüler)."""
    return {p: ck.ozet([s for s in satirlar if s.get("pazar") == p]) for p in PAZARLAR}


# ---------------- matchforge yanıtından sonuç (saf) ----------------
def _cift(c: Any) -> Optional[Dict[str, int]]:
    if not isinstance(c, dict) or c.get("home") is None or c.get("away") is None:
        return None
    return {"ev": int(c["home"]), "dep": int(c["away"])}


def _kart(sari: Any, kirmizi: Any) -> Optional[Dict[str, int]]:
    s, k = _cift(sari), _cift(kirmizi)
    if not s:
        return None
    k = k or {"ev": 0, "dep": 0}
    return {"ev": s["ev"] + 2 * k["ev"], "dep": s["dep"] + 2 * k["dep"]}


def pazar_sonuclari(d: Dict[str, Any]) -> Dict[str, Optional[Dict[str, Dict[str, int]]]]:
    """matchforge canlı yanıtından pazar başına {ilk_yari, mac}; dökümü eksik pazar None (uydurma yok)."""
    st, tum = d.get("stats") or {}, d.get("stats_tum") or {}
    iy = (d.get("stats_donem") or {}).get("ilk_yari") or {}
    kp = d.get("kart_puani") or {}
    cift_ya = lambda iy_, mac_: {"ilk_yari": iy_, "mac": mac_} if iy_ and mac_ else None
    if kp.get("mac") and kp.get("ilk_yari"):
        kart = cift_ya(_cift(kp["ilk_yari"]), _cift(kp["mac"]))
    else:
        kart = cift_ya(_kart(iy.get("yellow_cards"), iy.get("red_cards")), _kart(tum.get("yellow_cards") or st.get("yellow_cards"), tum.get("red_cards") or st.get("red_cards")))
    return {
        "faul": cift_ya(_cift(iy.get("fouls_committed")), _cift(tum.get("fouls_committed") or st.get("fouls"))),
        "kart": kart,
        "tac": cift_ya(_cift(iy.get("throws")), _cift(tum.get("throws"))),
    }


# ---------------- gerçek kaynaklar (DB ve matchforge) ----------------
YAZ_SQL = """INSERT INTO greydb.canli_sayim_kayit (fotmob_match_id, pazar, lig_ad, ev, dep, model_surum, faz, dakika, kova, skor, sayim, kirmizi, onceki, hedefler)
VALUES (:fotmob_match_id, :pazar, :lig_ad, :ev, :dep, :model_surum, :faz, :dakika, :kova, CAST(:skor AS JSONB), CAST(:sayim AS JSONB), CAST(:kirmizi AS JSONB), CAST(:onceki AS JSONB), CAST(:hedefler AS JSONB))
ON CONFLICT (fotmob_match_id, pazar, faz, kova) DO NOTHING RETURNING id"""
BEKLEYEN_SQL = ("SELECT id, fotmob_match_id, pazar, kaydedildi FROM greydb.canli_sayim_kayit "
                "WHERE sonuclandi IS NULL AND kaydedildi < :son ORDER BY kaydedildi LIMIT 5000")
SONUC_SQL = "UPDATE greydb.canli_sayim_kayit SET gerceklesen = CAST(:gerceklesen AS JSONB), sonuclandi = :an WHERE id = :id"
OZET_SQL = ("SELECT fotmob_match_id, pazar, faz, dakika, hedefler, gerceklesen FROM greydb.canli_sayim_kayit "
            "WHERE gerceklesen IS NOT NULL AND kaydedildi >= :bas ORDER BY kaydedildi DESC LIMIT 50000")

_HAZIR = False


def _hazirla() -> None:
    global _HAZIR
    if _HAZIR:
        return
    from app.services.db import execute_insert
    execute_insert(TABLO_DDL, {})
    _HAZIR = True


def db_ekle(k: Dict[str, Any]) -> Optional[int]:
    """Kaydı ekler; aynı dilimde kayıt varsa None (atlandı)."""
    from app.services.db import execute_insert
    _hazirla()
    j = lambda v: None if v is None else json.dumps(v, ensure_ascii=False)
    s = execute_insert(YAZ_SQL, {**k, "skor": j(k["skor"]), "sayim": j(k["sayim"]), "kirmizi": j(k["kirmizi"]), "onceki": j(k["onceki"]), "hedefler": j(k["hedefler"])})
    return None if not s else s["id"]


def _bekleyen(son: datetime) -> List[Dict[str, Any]]:
    from app.services.db import execute_query
    _hazirla()
    return execute_query(BEKLEYEN_SQL, {"son": ck._utc(son)})


def _sonuc_yaz(id_: int, g: Optional[Dict[str, int]], an: datetime) -> None:
    from app.services.db import execute_insert
    execute_insert(SONUC_SQL, {"id": id_, "gerceklesen": None if g is None else json.dumps(g), "an": ck._utc(an)})


def _mac_sonucu(fotmob_match_id: int) -> Optional[Dict[str, Any]]:
    import httpx
    from app.services.skorjin_match_analysis import MATCHFORGE_BASE
    yanit = httpx.get(f"{MATCHFORGE_BASE}/api/matches/live", params={"match_id": fotmob_match_id}, timeout=40.0)
    if yanit.status_code == 404:
        return None
    yanit.raise_for_status()
    d = yanit.json()
    if d.get("error") or not d.get("match"):
        return None
    return {"finished": d["match"].get("status") == "finished", "cancelled": False, "pazarlar": pazar_sonuclari(d)}


def gercek_kaynaklar() -> Kaynaklar:
    return Kaynaklar(bekleyen=_bekleyen, mac_sonucu=_mac_sonucu, sonuc_yaz=_sonuc_yaz)


def calistir(dry_run: bool = True) -> Dict[str, Any]:
    """Zamanlayıcı ve uç buradan çağırır."""
    from datetime import timezone
    return sonuclandir_bekleyenler(datetime.now(timezone.utc).replace(tzinfo=None), gercek_kaynaklar(), dry_run=dry_run)


def db_ozet_satirlari(gun: int) -> List[Dict[str, Any]]:
    from datetime import timezone
    from app.services.db import execute_query
    _hazirla()
    return execute_query(OZET_SQL, {"bas": datetime.now(timezone.utc) - timedelta(days=gun)})
