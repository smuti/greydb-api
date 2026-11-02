"""Tercih izleme: matchforge günlük listesindeki her tercihi (olasılık + o andaki iddaa oranı) maçtan önce kaydeder, maç bitince sonucu yazar.

Kayıtlar eklemelidir (hiçbiri ezilmez): bir maç için kickoff'a ~24, ~6 ve ~1 saat kala olmak üzere en çok üç pencere yazılır; ilki "açılış", sonuncusu "kapanış" oranıdır.
Yalnızca iç analiz içindir; site ve postlarda gösterilmez.
"""
import logging
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from app.services.tercih_sonuc import sonuclandir as _hesapla

log = logging.getLogger("tercih-izleme")

# (pencere adı, kickoff'a kalan saat): bir maç, kalan süre bu değerin altına indiğinde o pencerenin kaydını bir kez alır
PENCERELER = (("acilis", 24.0), ("orta", 6.0), ("kapanis", 1.0))
GRUP = 30                         # matchforge /api/liste tek çağrıda en çok bu kadar maç alır
SONUC_BEKLEME_SAAT = 2            # kickoff'tan bu kadar sonra sonuçlandırma denenir (bitmemişse bir sonraki koşuda)

TABLO_DDL = """
CREATE TABLE IF NOT EXISTS greydb.tercih_izleme (
    id BIGSERIAL PRIMARY KEY,
    fotmob_match_id BIGINT NOT NULL,
    iddaa_match_id BIGINT,
    kickoff TIMESTAMPTZ NOT NULL,
    lig_ad TEXT,
    ev TEXT NOT NULL,
    dep TEXT NOT NULL,
    pencere TEXT NOT NULL,
    kategori TEXT NOT NULL,
    secim TEXT NOT NULL,
    olasilik DOUBLE PRECISION NOT NULL,
    oran DOUBLE PRECISION,
    guvenilirlik TEXT,
    model_surum TEXT,
    kaydedildi TIMESTAMPTZ NOT NULL DEFAULT now(),
    sonuc BOOLEAN,
    sonuclandi TIMESTAMPTZ,
    UNIQUE (fotmob_match_id, pencere, kategori, secim)
);
CREATE INDEX IF NOT EXISTS idx_tercih_izleme_bekleyen
    ON greydb.tercih_izleme (kickoff)
    WHERE sonuc IS NULL;
CREATE INDEX IF NOT EXISTS idx_tercih_izleme_mac
    ON greydb.tercih_izleme (fotmob_match_id, pencere);
"""


@dataclass(frozen=True)
class Kaynaklar:
    maclar: Optional[Callable[[datetime, datetime], List[Dict[str, Any]]]]        # (bas, son) -> yaklaşan maçlar (upcoming_matches satırları)
    mevcut: Optional[Callable[[List[int]], Dict[int, Set[str]]]]                  # fotmob_match_id -> daha önce yazılmış pencereler
    liste: Optional[Callable[[List[int]], List[Dict[str, Any]]]]                  # matchforge /api/liste (iddaa=true) satırları
    yaz: Optional[Callable[[List[Dict[str, Any]]], int]]                          # satırları ekler (çakışanı atlar), yazılan sayısı
    bekleyen: Optional[Callable[[datetime], List[Dict[str, Any]]]]                # kickoff < son olan sonuçlanmamış kayıtlar
    sonuc_yaz: Optional[Callable[[int, Optional[bool], datetime], None]]          # (kayıt id, sonuç, zaman)
    mac_sonucu: Optional[Callable[[int], Optional[Dict[str, Any]]]]               # fotmob_match_id -> {finished, cancelled, score, stats} (yoksa None)


def pencere_sec(kalan_saat: float, yazilmis: Set[str]) -> Optional[str]:
    """Kickoff'a kalan süreye göre şimdiki pencere (24 saat içinde: açılış; 6'da: orta; 1'de: kapanış). Yazılmışsa ya da maç başlamış/uzaksa None.
    Kaçırılan önceki pencereler geri doldurulmaz: kayıt yalnızca o andaki oranı yansıtır."""
    if kalan_saat <= 0 or kalan_saat > PENCERELER[0][1]:
        return None
    p = "kapanis" if kalan_saat <= PENCERELER[2][1] else "orta" if kalan_saat <= PENCERELER[1][1] else "acilis"
    return None if p in yazilmis else p


def _n(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def _benzer(a: str, b: str) -> bool:
    return bool(a) and bool(b) and (a == b or a in b or b in a)


def satirlari_esle(maclar: List[Dict[str, Any]], pencere: Dict[int, str], satirlar: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """matchforge liste satırlarını ("Ev – Dep" etiketiyle) maça eşler. Eşleşmeyen etiketler ayrıca döner (sessizce düşürülmez)."""
    aday = [m for m in maclar if m["fotmob_match_id"] in pencere]
    yazilan, eslesmeyen = [], []
    for s in satirlar:
        parca = [p.strip() for p in re.split(r"\s[–-]\s", s["mac"], maxsplit=1)]
        a, b = (_n(parca[0]), _n(parca[1])) if len(parca) == 2 else ("", "")
        uygun = [m for m in aday if _benzer(a, _n(m["home_name"])) and _benzer(b, _n(m["away_name"]))]
        if len(uygun) > 1:                                         # birden çok aday: tam ad eşleşeni al
            uygun = [m for m in uygun if _n(m["home_name"]) == a and _n(m["away_name"]) == b] or uygun
        if len(uygun) != 1:
            if s["mac"] not in eslesmeyen:
                eslesmeyen.append(s["mac"])
            continue
        m = uygun[0]
        yazilan.append(dict(fotmob_match_id=m["fotmob_match_id"], iddaa_match_id=m.get("iddaa_event_id"), kickoff=m["match_date"], lig_ad=m.get("league_name"),
                            ev=m["home_name"], dep=m["away_name"], pencere=pencere[m["fotmob_match_id"]], kategori=s["kategori"], secim=s["secim"],
                            olasilik=float(s["olasilik"]), oran=None if s.get("oran") is None else float(s["oran"]), guvenilirlik=s.get("guvenilirlik"), model_surum=None))
    return yazilan, eslesmeyen


def kaydet(simdi: datetime, k: Kaynaklar, dry_run: bool = True) -> Dict[str, Any]:
    """Kickoff'a 24 saatten az kalan maçlardan, şimdiki penceresi henüz yazılmamış olanların listesini matchforge'dan oranlı alıp ekler."""
    rapor: Dict[str, Any] = dict(dry_run=dry_run, aday=0, yazilan=0, eslesmeyen=[], hata=[])
    maclar = k.maclar(simdi, simdi + timedelta(hours=PENCERELER[0][1]))
    yazilmis = k.mevcut([m["fotmob_match_id"] for m in maclar]) if maclar else {}
    secilen: Dict[int, str] = {}
    for m in maclar:
        p = pencere_sec((m["match_date"] - simdi).total_seconds() / 3600, yazilmis.get(m["fotmob_match_id"], set()))
        if p:
            secilen[m["fotmob_match_id"]] = p
    rapor["aday"] = len(secilen)
    ids = list(secilen)
    for i in range(0, len(ids), GRUP):
        grup = ids[i:i + GRUP]
        try:
            satirlar = k.liste(grup)
        except Exception as e:  # noqa: BLE001 — bir grup hata verse de diğerleri denenir
            log.warning("tercih kaydı: liste alınamadı (%s maç): %s", len(grup), e)
            rapor["hata"].append(str(e)[:200])
            continue
        esle, eslesmeyen = satirlari_esle([m for m in maclar if m["fotmob_match_id"] in grup], {g: secilen[g] for g in grup}, satirlar)
        rapor["eslesmeyen"] += eslesmeyen
        if esle and not dry_run:
            rapor["yazilan"] += k.yaz(esle)
    return rapor


def sonuclandir_bekleyenler(simdi: datetime, k: Kaynaklar, dry_run: bool = True) -> Dict[str, Any]:
    """Bitmiş maçların kayıtlarına tuttu/tutmadı yazar. Karar verilemeyenler (faul sayısı yok, iptal) sonuç NULL ama işlenmiş olarak işaretlenir."""
    rapor: Dict[str, Any] = dict(dry_run=dry_run, islenen=0, tuttu=0, tutmadi=0, sonuclanamayan=0, bitmemis=0, hata=[])
    gruplar: Dict[int, List[Dict[str, Any]]] = {}
    for r in k.bekleyen(simdi - timedelta(hours=SONUC_BEKLEME_SAAT)):
        gruplar.setdefault(r["fotmob_match_id"], []).append(r)
    for fid, satirlar in gruplar.items():
        try:
            veri = k.mac_sonucu(fid)
        except Exception as e:  # noqa: BLE001
            log.warning("tercih sonucu: maç verisi alınamadı (%s): %s", fid, e)
            rapor["hata"].append(f"{fid}: {str(e)[:150]}")
            continue
        if not veri:
            rapor["hata"].append(f"{fid}: maç verisi yok")
            continue
        iptal = bool(veri.get("cancelled"))
        if not iptal and not veri.get("finished"):
            rapor["bitmemis"] += 1
            continue
        for r in satirlar:
            sonuc = None if iptal else _hesapla(r["kategori"], r["secim"], r["ev"], r["dep"], veri.get("score"), veri.get("stats"))
            rapor["islenen"] += 1
            rapor["tuttu" if sonuc is True else "tutmadi" if sonuc is False else "sonuclanamayan"] += 1
            if not dry_run:
                k.sonuc_yaz(r["id"], sonuc, simdi)
    return rapor
