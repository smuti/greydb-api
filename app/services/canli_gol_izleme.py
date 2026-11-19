"""Canlı gol olasılığı izleme: yönetici panelinin gösterdiği olasılıklar (ev / deplasman / herhangi gol; sonraki 10 dk ve maç sonuna kadar)
5 dakikalık dilim başına kaydedilir, maç bitince gerçekleşen yazılır ve kalibrasyon özeti çıkarılır. Yalnız iç analiz; sitede gösterilmez.
Canlı korner / sayım izlemesiyle aynı düzen (doğrulama yardımcıları, dakika dilimi ve bekleme süreleri canli_korner_izleme'den).

Gerçekleşen: maç sonu = son skor kayıttaki skordan büyük mü; 10 dk = dakikası (etiket) kayıt dakikası + 10'a kadar olan gol sayısı kayıttaki skoru
aşıyor mu (aynı dakikada kayıttan sonra gelen gol de sayılır; 90+ uzatmada pencere kalan süredir). Gol listesi son skorla uyuşmazsa 10 dk yazılmaz (None).
"""
import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

from app.services import canli_korner_izleme as ck

log = logging.getLogger("canli-gol-izleme")

TARAFLAR = ("ev", "dep", "herhangi")
PENCERELER = ("on", "kalan")
PENCERE_DK = 10

TABLO_DDL = """
CREATE TABLE IF NOT EXISTS greydb.canli_gol_kayit (
    id BIGSERIAL PRIMARY KEY,
    fotmob_match_id BIGINT NOT NULL,
    lig_ad TEXT,
    ev TEXT,
    dep TEXT,
    model_surum TEXT NOT NULL,
    faz TEXT NOT NULL,
    dakika INTEGER NOT NULL,
    kova INTEGER NOT NULL,
    skor JSONB NOT NULL,
    kirmizi JSONB NOT NULL,
    onceki JSONB,
    olasilik JSONB NOT NULL,
    kaydedildi TIMESTAMPTZ NOT NULL DEFAULT now(),
    gerceklesen JSONB,
    sonuclandi TIMESTAMPTZ,
    UNIQUE (fotmob_match_id, kova)
);
CREATE INDEX IF NOT EXISTS idx_canli_gol_bekleyen ON greydb.canli_gol_kayit (kaydedildi) WHERE sonuclandi IS NULL;
"""


def kayit_hazirla(g: Any) -> Dict[str, Any]:
    """Web'den gelen kaydı doğrular ve sadeleştirir; geçersizse ValueError."""
    if not isinstance(g, dict):
        raise ValueError("gövde nesne olmalı")
    surum = g.get("model_surum")
    if not isinstance(surum, str) or not surum.strip() or len(surum) > 40:
        raise ValueError("model_surum gerekli")
    if g.get("faz") not in ck.FAZLAR:
        raise ValueError("faz ilk, devre ya da ikinci olmalı")
    dakika = ck._tamsayi(g.get("dakika"), "dakika", 0, 130)
    ham = g.get("olasilik")
    if not isinstance(ham, dict) or set(ham) != set(TARAFLAR):
        raise ValueError("olasilik ev, dep ve herhangi içermeli")
    olasilik = {}
    for t in TARAFLAR:
        o = ham[t]
        if not isinstance(o, dict) or set(o) != set(PENCERELER):
            raise ValueError(f"olasilik.{t} on ve kalan içermeli")
        olasilik[t] = {p: ck._sayi(o[p], f"olasilik.{t}.{p}", 0.0, 1.0) for p in PENCERELER}
    gb = g.get("golBeklentisi")
    onceki = None
    if gb is not None:
        if not isinstance(gb, dict):
            raise ValueError("golBeklentisi nesne olmalı")
        onceki = {"ev": ck._sayi(gb.get("ev"), "golBeklentisi.ev", 0.0, 10.0), "dep": ck._sayi(gb.get("dep"), "golBeklentisi.dep", 0.0, 10.0)}
    txt = lambda k: (str(g[k])[:80] if g.get(k) else None)
    return dict(fotmob_match_id=ck._tamsayi(g.get("fotmob_match_id"), "fotmob_match_id", 1, 10**12), lig_ad=txt("lig_ad"), ev=txt("ev"), dep=txt("dep"),
                model_surum=surum.strip(), faz=g["faz"], dakika=dakika, kova=dakika // ck.KOVA_DK,
                skor=ck._cift(g.get("skor"), "skor"), kirmizi=ck._cift(g.get("kirmizi"), "kirmizi"), onceki=onceki, olasilik=olasilik)


def gerceklesen(kayit: Dict[str, Any], sonuc: Dict[str, Any]) -> Dict[str, Dict[str, Optional[int]]]:
    """kayit: {dakika, skor:{ev,dep}}; sonuc: {skor:{ev,dep}, goller:[{team, minute}]} → taraf başına {on, kalan} (0/1; 10 dk belirsizse None)."""
    sk = kayit["skor"] if isinstance(kayit["skor"], dict) else json.loads(kayit["skor"])
    son, goller = sonuc["skor"], sonuc.get("goller") or []
    t = int(kayit["dakika"])
    takim = {"ev": "home", "dep": "away"}
    tutarli = all(sum(1 for x in goller if x["team"] == takim[k]) == int(son[k]) for k in ("ev", "dep"))
    out: Dict[str, Dict[str, Optional[int]]] = {}
    for k in ("ev", "dep"):
        kalan = int(int(son[k]) > int(sk[k]))
        on = int(sum(1 for x in goller if x["team"] == takim[k] and x["minute"] <= t + PENCERE_DK) > int(sk[k])) if tutarli else None
        out[k] = {"on": on, "kalan": kalan}
    out["herhangi"] = {p: (None if out["ev"][p] is None else max(out["ev"][p], out["dep"][p])) for p in PENCERELER}
    return out


# ---------------- sonuçlandırma ----------------
@dataclass(frozen=True)
class Kaynaklar:
    bekleyen: Callable[[datetime], List[Dict[str, Any]]]                      # sonuçlanmamış kayıtlar: id, fotmob_match_id, dakika, skor, kaydedildi
    mac_sonucu: Callable[[int], Optional[Dict[str, Any]]]                     # {finished, cancelled, skor, goller} ya da None
    sonuc_yaz: Callable[[int, Optional[Dict[str, Any]], datetime], None]


def sonuclandir_bekleyenler(simdi: datetime, k: Kaynaklar, dry_run: bool = True) -> Dict[str, Any]:
    """Bitmiş maçların kayıtlarına gerçekleşeni yazar. İptal ve çok eski verisiz kayıtlar gerçekleşen=NULL ile işlenmiş sayılır."""
    rapor: Dict[str, Any] = dict(dry_run=dry_run, islenen=0, bitmemis=0, iptal=0, verisiz=0, hata=[])
    gruplar: Dict[int, List[Dict[str, Any]]] = {}
    for r in k.bekleyen(simdi - timedelta(minutes=ck.SONUC_BEKLEME_DK)):
        gruplar.setdefault(int(r["fotmob_match_id"]), []).append(r)
    yaz = (lambda id_, g: k.sonuc_yaz(id_, g, simdi)) if not dry_run else (lambda id_, g: None)
    eski = lambda r: simdi - r["kaydedildi"].replace(tzinfo=None) > timedelta(days=ck.VAZGEC_GUN)
    for fid, satirlar in gruplar.items():
        try:
            veri = k.mac_sonucu(fid)
        except Exception as e:  # noqa: BLE001 — bir maç hata verse de diğerleri denenir
            log.warning("canlı gol sonucu: maç verisi alınamadı (%s): %s", fid, e)
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
            rapor["islenen"] += 1
            yaz(r["id"], gerceklesen(r, veri))
    return rapor


def ozet(satirlar: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Hedef (taraf_pencere) başına: n, ortalama tahmin, gerçekleşen oran (yüzde), Brier, log-kayıp, 10 puanlık bantlar ve dakika dilimleri."""
    toplam: Dict[str, Dict[str, Any]] = {}
    maclar = set()
    for s in satirlar:
        ol = s["olasilik"] if isinstance(s["olasilik"], dict) else json.loads(s["olasilik"])
        ger = s.get("gerceklesen") or {}
        ger = ger if isinstance(ger, dict) else json.loads(ger)
        for t in TARAFLAR:
            for p in PENCERELER:
                y = (ger.get(t) or {}).get(p)
                if y is None:
                    continue
                maclar.add(int(s["fotmob_match_id"]))
                q = float(ol[t][p])
                h = toplam.setdefault(f"{t}_{p}", dict(n=0, p=0.0, y=0, brier=0.0, ll=0.0, bant={}, dilim={}))
                qq = min(max(q, 1e-6), 1 - 1e-6)
                h["n"] += 1; h["p"] += q; h["y"] += y; h["brier"] += (q - y) ** 2; h["ll"] += -math.log(qq if y else 1 - qq)
                for anahtar, kap in ((min(int(round(q * 100, 6) // 10), 9), "bant"), (ck.dilim(s["faz"], int(s["dakika"])), "dilim")):
                    x = h[kap].setdefault(anahtar, dict(n=0, p=0.0, y=0))
                    x["n"] += 1; x["p"] += q; x["y"] += y
    yuz = lambda a, n: round(100 * a / n, 1)
    hedefler = {}
    for ad, h in toplam.items():
        hedefler[ad] = dict(
            n=h["n"], ort_tahmin=yuz(h["p"], h["n"]), gercek=yuz(h["y"], h["n"]), brier=round(h["brier"] / h["n"], 4), log_kayip=round(h["ll"] / h["n"], 4),
            bantlar=[dict(bant=f"{b * 10}-{b * 10 + 10}", n=x["n"], ort_tahmin=yuz(x["p"], x["n"]), gercek=yuz(x["y"], x["n"])) for b, x in sorted(h["bant"].items())],
            dilimler=[dict(dilim=k, n=x["n"], ort_tahmin=yuz(x["p"], x["n"]), gercek=yuz(x["y"], x["n"]))
                      for k, x in sorted(h["dilim"].items(), key=lambda kv: ck.DILIM_SIRA.index(kv[0]))])
    return dict(kayit=len(satirlar), maclar=len(maclar), hedefler=hedefler)


# ---------------- matchforge yanıtından sonuç (saf) ----------------
def sonuc_cozumle(d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    m = d.get("match") or {}
    sk = m.get("score") or {}
    if sk.get("home") is None or sk.get("away") is None:
        return None
    goller = [{"team": e["team"], "minute": int(e["minute"])} for e in (d.get("events") or [])
              if isinstance(e, dict) and "Card" not in str(e.get("type") or "") and e.get("team") in ("home", "away") and isinstance(e.get("minute"), (int, float))]
    return {"finished": m.get("status") == "finished", "cancelled": False, "skor": {"ev": int(sk["home"]), "dep": int(sk["away"])}, "goller": goller}


# ---------------- gerçek kaynaklar (DB ve matchforge) ----------------
YAZ_SQL = """INSERT INTO greydb.canli_gol_kayit (fotmob_match_id, lig_ad, ev, dep, model_surum, faz, dakika, kova, skor, kirmizi, onceki, olasilik)
VALUES (:fotmob_match_id, :lig_ad, :ev, :dep, :model_surum, :faz, :dakika, :kova, CAST(:skor AS JSONB), CAST(:kirmizi AS JSONB), CAST(:onceki AS JSONB), CAST(:olasilik AS JSONB))
ON CONFLICT (fotmob_match_id, kova) DO NOTHING RETURNING id"""
BEKLEYEN_SQL = ("SELECT id, fotmob_match_id, dakika, skor, kaydedildi FROM greydb.canli_gol_kayit "
                "WHERE sonuclandi IS NULL AND kaydedildi < :son ORDER BY kaydedildi LIMIT 5000")
SONUC_SQL = "UPDATE greydb.canli_gol_kayit SET gerceklesen = CAST(:gerceklesen AS JSONB), sonuclandi = :an WHERE id = :id"
OZET_SQL = ("SELECT fotmob_match_id, faz, dakika, olasilik, gerceklesen FROM greydb.canli_gol_kayit "
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
    s = execute_insert(YAZ_SQL, {**k, "skor": j(k["skor"]), "kirmizi": j(k["kirmizi"]), "onceki": j(k["onceki"]), "olasilik": j(k["olasilik"])})
    return None if not s else s["id"]


def _bekleyen(son: datetime) -> List[Dict[str, Any]]:
    from app.services.db import execute_query
    _hazirla()
    return execute_query(BEKLEYEN_SQL, {"son": ck._utc(son)})


def _sonuc_yaz(id_: int, g: Optional[Dict[str, Any]], an: datetime) -> None:
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
    return sonuc_cozumle(d)


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
