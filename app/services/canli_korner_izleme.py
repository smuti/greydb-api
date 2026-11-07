"""Canlı korner izleme: yönetici panelinin gösterdiği tahminler (ilk yarı / maç sonu / ev / deplasman beklentisi ve "N ve üstü" olasılıkları) kaydedilir,
maç bitince gerçekleşen korner sayıları yazılır ve kalibrasyon özeti çıkarılır. Yalnız iç analiz; sitede gösterilmez.

Kayıt eklemelidir: bir maç için her yarı fazında 5 dakikalık dilim başına en çok bir kayıt (UNIQUE fotmob_match_id, faz, kova); aynı dilimde ikinci kayıt atlanır.
"""
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("canli-korner-izleme")

HEDEFLER = ("ilkYari", "macSonu", "ev", "dep")
FAZLAR = ("ilk", "devre", "ikinci")
KOVA_DK = 5
EN_FAZLA_BASAMAK = 12
SONUC_BEKLEME_SAAT = 2          # kayıttan bu kadar sonra sonuçlandırma denenir
VAZGEC_GUN = 4                  # bu kadar eski kayıt için maç verisi hâlâ yoksa işlenmiş sayılır (sonsuza kadar denenmez)

TABLO_DDL = """
CREATE TABLE IF NOT EXISTS greydb.canli_korner_kayit (
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
    korner JSONB NOT NULL,
    kirmizi JSONB NOT NULL,
    onceki JSONB,
    hedefler JSONB NOT NULL,
    kaydedildi TIMESTAMPTZ NOT NULL DEFAULT now(),
    gerceklesen JSONB,
    sonuclandi TIMESTAMPTZ,
    UNIQUE (fotmob_match_id, faz, kova)
);
CREATE INDEX IF NOT EXISTS idx_canli_korner_bekleyen ON greydb.canli_korner_kayit (kaydedildi) WHERE sonuclandi IS NULL;
"""


# ---------------- girdi doğrulama (saf) ----------------
def _tamsayi(v: Any, ad: str, alt: int = 0, ust: int = 1000) -> int:
    if isinstance(v, bool) or not isinstance(v, int) or not alt <= v <= ust:
        raise ValueError(f"{ad} {alt}-{ust} arasında tam sayı olmalı")
    return v


def _cift(v: Any, ad: str) -> Dict[str, int]:
    if not isinstance(v, dict):
        raise ValueError(f"{ad} nesne olmalı")
    return {"ev": _tamsayi(v.get("ev"), f"{ad}.ev", 0, 50), "dep": _tamsayi(v.get("dep"), f"{ad}.dep", 0, 50)}


def _sayi(v: Any, ad: str, alt: float, ust: float) -> float:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not alt <= float(v) <= ust:
        raise ValueError(f"{ad} {alt}-{ust} arasında sayı olmalı")
    return float(v)


def kayit_hazirla(g: Any) -> Dict[str, Any]:
    """Web'den gelen kaydı doğrular ve sadeleştirir; geçersizse ValueError."""
    if not isinstance(g, dict):
        raise ValueError("gövde nesne olmalı")
    surum = g.get("model_surum")
    if not isinstance(surum, str) or not surum.strip() or len(surum) > 40:
        raise ValueError("model_surum gerekli")
    if g.get("faz") not in FAZLAR:
        raise ValueError("faz ilk, devre ya da ikinci olmalı")
    dakika = _tamsayi(g.get("dakika"), "dakika", 0, 130)
    hedefler_ham = g.get("hedefler")
    if not isinstance(hedefler_ham, dict) or not hedefler_ham:
        raise ValueError("hedefler gerekli")
    hedefler = {}
    for ad, h in hedefler_ham.items():
        if ad not in HEDEFLER or not isinstance(h, dict):
            raise ValueError(f"bilinmeyen hedef: {ad}")
        olasilik = h.get("olasilik")
        if not isinstance(olasilik, dict) or not 1 <= len(olasilik) <= EN_FAZLA_BASAMAK:
            raise ValueError(f"{ad}.olasilik 1-{EN_FAZLA_BASAMAK} basamak olmalı")
        temiz = {}
        for n, p in olasilik.items():
            if not isinstance(n, str) or not n.isdigit() or int(n) > 100:
                raise ValueError(f"{ad}.olasilik anahtarı sayı olmalı")
            temiz[str(int(n))] = _sayi(p, f"{ad}.olasilik", 0.0, 1.0)
        hedefler[ad] = {"ortalama": _sayi(h.get("ortalama"), f"{ad}.ortalama", 0.0, 100.0), "mevcut": _tamsayi(h.get("mevcut"), f"{ad}.mevcut", 0, 100), "olasilik": temiz}
    onceki = g.get("onceki")
    if onceki is not None:
        if not isinstance(onceki, dict):
            raise ValueError("onceki nesne olmalı")
        onceki = {"ev": _sayi(onceki.get("ev"), "onceki.ev", 0.0, 50.0), "dep": _sayi(onceki.get("dep"), "onceki.dep", 0.0, 50.0), "kaynak": str(onceki.get("kaynak") or "")[:10]}
    txt = lambda k: (str(g[k])[:80] if g.get(k) else None)
    return dict(fotmob_match_id=_tamsayi(g.get("fotmob_match_id"), "fotmob_match_id", 1, 10**12), lig_ad=txt("lig_ad"), ev=txt("ev"), dep=txt("dep"),
                model_surum=surum.strip(), faz=g["faz"], dakika=dakika, kova=dakika // KOVA_DK,
                skor=_cift(g.get("skor"), "skor"), korner=_cift(g.get("korner"), "korner"), kirmizi=_cift(g.get("kirmizi"), "kirmizi"), onceki=onceki, hedefler=hedefler)


# ---------------- sonuçlandırma ----------------
@dataclass(frozen=True)
class Kaynaklar:
    bekleyen: Callable[[datetime], List[Dict[str, Any]]]                      # kaydedildi < son olan sonuçlanmamış kayıtlar
    mac_sonucu: Callable[[int], Optional[Dict[str, Any]]]                     # {finished, cancelled, ilk_yari:{ev,dep}, mac:{ev,dep}} ya da None
    sonuc_yaz: Callable[[int, Optional[Dict[str, int]], datetime], None]      # (kayıt id, gerçekleşen ya da None, zaman)


def gerceklesen(veri: Dict[str, Any]) -> Dict[str, int]:
    iy, mac = veri["ilk_yari"], veri["mac"]
    return {"ilkYari": int(iy["ev"]) + int(iy["dep"]), "macSonu": int(mac["ev"]) + int(mac["dep"]), "ev": int(mac["ev"]), "dep": int(mac["dep"])}


def sonuclandir_bekleyenler(simdi: datetime, k: Kaynaklar, dry_run: bool = True) -> Dict[str, Any]:
    """Bitmiş maçların kayıtlarına gerçekleşen sayıları yazar. İptal ve çok eski verisiz kayıtlar gerçekleşen=NULL ile işlenmiş sayılır."""
    rapor: Dict[str, Any] = dict(dry_run=dry_run, islenen=0, bitmemis=0, iptal=0, verisiz=0, hata=[])
    gruplar: Dict[int, List[Dict[str, Any]]] = {}
    for r in k.bekleyen(simdi - timedelta(hours=SONUC_BEKLEME_SAAT)):
        gruplar.setdefault(int(r["fotmob_match_id"]), []).append(r)
    for fid, satirlar in gruplar.items():
        eski = all(simdi - r["kaydedildi"].replace(tzinfo=None) > timedelta(days=VAZGEC_GUN) for r in satirlar)
        try:
            veri = k.mac_sonucu(fid)
        except Exception as e:  # noqa: BLE001 — bir maç hata verse de diğerleri denenir
            log.warning("canlı korner sonucu: maç verisi alınamadı (%s): %s", fid, e)
            rapor["hata"].append(f"{fid}: {str(e)[:150]}")
            continue
        if not veri:
            if eski:
                rapor["verisiz"] += len(satirlar)
                if not dry_run:
                    for r in satirlar:
                        k.sonuc_yaz(r["id"], None, simdi)
            else:
                rapor["hata"].append(f"{fid}: maç verisi yok")
            continue
        if veri.get("cancelled"):
            sonuc: Optional[Dict[str, int]] = None
            rapor["iptal"] += len(satirlar)
        elif not veri.get("finished"):
            rapor["bitmemis"] += 1
            continue
        else:
            sonuc = gerceklesen(veri)
            rapor["islenen"] += len(satirlar)
        if not dry_run:
            for r in satirlar:
                k.sonuc_yaz(r["id"], sonuc, simdi)
    return rapor


# ---------------- kalibrasyon özeti (saf) ----------------
def dilim(faz: str, dakika: int) -> str:
    if faz == "devre":
        return "devre"
    if faz == "ilk":
        return "0-15" if dakika < 15 else "15-30" if dakika < 30 else "30-45+"
    return "45-60" if dakika < 60 else "60-75" if dakika < 75 else "75+"


DILIM_SIRA = ["0-15", "15-30", "30-45+", "devre", "45-60", "60-75", "75+"]


def ozet(satirlar: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Sonuçlanmış kayıtlardan hedef başına sapma (beklenen − gerçek), MAE, olasılık bantlarında tutma ve dakika dilimi kırılımı."""
    toplam: Dict[str, Dict[str, Any]] = {}
    maclar = set()
    for s in satirlar:
        ger = s.get("gerceklesen") or {}
        h_all = s["hedefler"] if isinstance(s["hedefler"], dict) else json.loads(s["hedefler"])
        for ad, h in h_all.items():
            if ad not in ger:
                continue
            maclar.add(int(s["fotmob_match_id"]))
            gercek = ger[ad]
            t = toplam.setdefault(ad, dict(n=0, hata=0.0, mutlak=0.0, bant={}, dilim={}))
            sapma = float(h["ortalama"]) - gercek
            t["n"] += 1
            t["hata"] += sapma
            t["mutlak"] += abs(sapma)
            d = t["dilim"].setdefault(dilim(s["faz"], int(s["dakika"])), dict(n=0, hata=0.0))
            d["n"] += 1
            d["hata"] += sapma
            for n, p in h["olasilik"].items():
                b = min(int(round(float(p) * 100, 6) // 10), 9)
                x = t["bant"].setdefault(b, dict(n=0, p=0.0, tuttu=0))
                x["n"] += 1
                x["p"] += float(p)
                x["tuttu"] += 1 if gercek >= int(n) else 0
    hedefler = {}
    for ad, t in toplam.items():
        hedefler[ad] = dict(
            n=t["n"], sapma=round(t["hata"] / t["n"], 3), mae=round(t["mutlak"] / t["n"], 3),
            bantlar=[dict(bant=f"{b * 10}-{b * 10 + 10}", n=x["n"], ort_olasilik=round(100 * x["p"] / x["n"], 1), tutma_yuzde=round(100 * x["tuttu"] / x["n"], 1)) for b, x in sorted(t["bant"].items())],
            dilimler=[dict(dilim=k, n=v["n"], sapma=round(v["hata"] / v["n"], 3)) for k, v in sorted(t["dilim"].items(), key=lambda kv: DILIM_SIRA.index(kv[0]))])
    return dict(kayit=len(satirlar), maclar=len(maclar), hedefler=hedefler)


# ---------------- gerçek kaynaklar (DB ve matchforge) ----------------
YAZ_SQL = """INSERT INTO greydb.canli_korner_kayit (fotmob_match_id, lig_ad, ev, dep, model_surum, faz, dakika, kova, skor, korner, kirmizi, onceki, hedefler)
VALUES (:fotmob_match_id, :lig_ad, :ev, :dep, :model_surum, :faz, :dakika, :kova, CAST(:skor AS JSONB), CAST(:korner AS JSONB), CAST(:kirmizi AS JSONB), CAST(:onceki AS JSONB), CAST(:hedefler AS JSONB))
ON CONFLICT (fotmob_match_id, faz, kova) DO NOTHING RETURNING id"""
BEKLEYEN_SQL = ("SELECT id, fotmob_match_id, kaydedildi FROM greydb.canli_korner_kayit "
                "WHERE sonuclandi IS NULL AND kaydedildi < :son ORDER BY kaydedildi LIMIT 5000")
SONUC_SQL = "UPDATE greydb.canli_korner_kayit SET gerceklesen = CAST(:gerceklesen AS JSONB), sonuclandi = :an WHERE id = :id"
OZET_SQL = ("SELECT fotmob_match_id, faz, dakika, hedefler, gerceklesen FROM greydb.canli_korner_kayit "
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
    s = execute_insert(YAZ_SQL, {**k, "skor": j(k["skor"]), "korner": j(k["korner"]), "kirmizi": j(k["kirmizi"]), "onceki": j(k["onceki"]), "hedefler": j(k["hedefler"])})
    return None if not s else s["id"]


def _utc(d: datetime) -> datetime:
    from datetime import timezone
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _bekleyen(son: datetime) -> List[Dict[str, Any]]:
    from app.services.db import execute_query
    _hazirla()
    return execute_query(BEKLEYEN_SQL, {"son": _utc(son)})


def _sonuc_yaz(id_: int, g: Optional[Dict[str, int]], an: datetime) -> None:
    from app.services.db import execute_insert
    execute_insert(SONUC_SQL, {"id": id_, "gerceklesen": None if g is None else json.dumps(g), "an": _utc(an)})


def _mac_sonucu(fotmob_match_id: int) -> Optional[Dict[str, Any]]:
    """matchforge canlı ucu bitmiş maç için de tam maç ve ilk yarı korner sayılarını verir."""
    import httpx
    from app.services.skorjin_match_analysis import MATCHFORGE_BASE
    yanit = httpx.get(f"{MATCHFORGE_BASE}/api/matches/live", params={"match_id": fotmob_match_id}, timeout=40.0)
    if yanit.status_code == 404:
        return None
    yanit.raise_for_status()
    d = yanit.json()
    if d.get("error") or not d.get("match"):
        return None
    st = d["match"].get("status")
    kt = (d.get("stats") or {}).get("corners") or {}
    ik = ((d.get("stats_donem") or {}).get("ilk_yari") or {}).get("corners") or {}
    if st == "finished" and not ik:
        return None                                   # ilk yarı dökümü yoksa uydurma: sonuçlanmaz, yeniden denenir
    return {"finished": st == "finished", "cancelled": False, "ilk_yari": {"ev": ik.get("home", 0), "dep": ik.get("away", 0)}, "mac": {"ev": kt.get("home", 0), "dep": kt.get("away", 0)}}


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
