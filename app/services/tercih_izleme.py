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
GRUP = 6                          # matchforge /api/liste tek çağrıda en çok bu kadar maç alır: matchforge tek işçili, büyük çağrı Skorjin'in model (sence) isteklerini dakikalarca bekletir
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


# ---- gerçek kaynaklar (DB ve matchforge); iş mantığı yukarıda sahte kaynaklarla test edilir ----
YAZ_SQL = """INSERT INTO greydb.tercih_izleme (fotmob_match_id, iddaa_match_id, kickoff, lig_ad, ev, dep, pencere, kategori, secim, olasilik, oran, guvenilirlik, model_surum)
VALUES (:fotmob_match_id, :iddaa_match_id, :kickoff, :lig_ad, :ev, :dep, :pencere, :kategori, :secim, :olasilik, :oran, :guvenilirlik, :model_surum)
ON CONFLICT (fotmob_match_id, pencere, kategori, secim) DO NOTHING RETURNING id"""
MEVCUT_SQL = "SELECT fotmob_match_id, pencere FROM greydb.tercih_izleme WHERE fotmob_match_id = ANY(:ids) GROUP BY fotmob_match_id, pencere"
BEKLEYEN_SQL = ("SELECT id, fotmob_match_id, ev, dep, kategori, secim, kickoff FROM greydb.tercih_izleme "
                "WHERE sonuclandi IS NULL AND kickoff < :son ORDER BY kickoff LIMIT 5000")
SONUC_SQL = "UPDATE greydb.tercih_izleme SET sonuc = :sonuc, sonuclandi = :an WHERE id = :id"


def _utc(d: datetime) -> datetime:
    from datetime import timezone
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _yaklasan(bas: datetime, son: datetime) -> List[Dict[str, Any]]:
    from app.services.skorjin_prematch import _yaklasan as yaklasan
    return yaklasan(bas, son)


def _mevcut(ids: List[int]) -> Dict[int, Set[str]]:
    from app.services.db import execute_query
    sonuc: Dict[int, Set[str]] = {}
    for r in execute_query(MEVCUT_SQL, {"ids": list(ids)}):
        sonuc.setdefault(int(r["fotmob_match_id"]), set()).add(r["pencere"])
    return sonuc


def _liste(ids: List[int]) -> List[Dict[str, Any]]:
    """matchforge /api/liste: günün tercih listesi, iddaa oranıyla (maç henüz başlamadığı için canlı oran gelir)."""
    import httpx
    from app.services.skorjin_match_analysis import MATCHFORGE_BASE
    yanit = httpx.post(f"{MATCHFORGE_BASE}/api/liste", json={"fotmob_match_ids": ids, "iddaa": True}, timeout=170.0)
    yanit.raise_for_status()
    return yanit.json().get("results", [])


def _yaz(satirlar: List[Dict[str, Any]]) -> int:
    from app.services.db import execute_insert
    n = 0
    for s in satirlar:
        if execute_insert(YAZ_SQL, {**s, "kickoff": _utc(s["kickoff"])}):
            n += 1
    return n


def _bekleyen(son: datetime) -> List[Dict[str, Any]]:
    from app.services.db import execute_query
    return execute_query(BEKLEYEN_SQL, {"son": _utc(son)})


def _sonuc_yaz(id_: int, sonuc: Optional[bool], an: datetime) -> None:
    from app.services.db import execute_insert
    execute_insert(SONUC_SQL, {"id": id_, "sonuc": sonuc, "an": _utc(an)})


def _mac_sonucu(fotmob_match_id: int) -> Optional[Dict[str, Any]]:
    import httpx
    from app.services.skorjin_match_analysis import MATCHFORGE_BASE
    yanit = httpx.get(f"{MATCHFORGE_BASE}/api/matches/db/{fotmob_match_id}", timeout=40.0)
    if yanit.status_code == 404:
        return None
    yanit.raise_for_status()
    d = yanit.json()
    return {"finished": bool(d.get("finished")), "cancelled": bool(d.get("cancelled")), "score": d.get("score"), "stats": d.get("stats")}


def gercek_kaynaklar() -> Kaynaklar:
    return Kaynaklar(maclar=_yaklasan, mevcut=_mevcut, liste=_liste, yaz=_yaz, bekleyen=_bekleyen, sonuc_yaz=_sonuc_yaz, mac_sonucu=_mac_sonucu)


def calistir(is_adi: str, dry_run: bool = True) -> Dict[str, Any]:
    """Zamanlayıcı ve uç buradan çağırır. is_adi: "kaydet" (maçtan önce liste+oran) ya da "sonuc" (biten maçların sonucu)."""
    from datetime import timezone
    if is_adi not in ("kaydet", "sonuc"):
        raise ValueError(f"bilinmeyen iş: {is_adi}")
    simdi = datetime.now(timezone.utc).replace(tzinfo=None)
    k = gercek_kaynaklar()
    return kaydet(simdi, k, dry_run=dry_run) if is_adi == "kaydet" else sonuclandir_bekleyenler(simdi, k, dry_run=dry_run)


# ---- birikimli rapor: SQL gruplanmış satırları döndürür, toplamlar saf fonksiyonda ----
BAHIS_TUTARI = 50.0         # varsayımsal eşit tutar (TL); rapor "her tercihe 50 TL olsaydı" hesabıdır
OZET_SQL = """SELECT pencere, kategori, LEAST(floor(olasilik * 10)::int, 9) AS bant, COALESCE(olasilik * oran > 1, false) AS deger,
    count(*) AS n, count(*) FILTER (WHERE sonuc) AS tuttu, count(*) FILTER (WHERE oran > 1.0) AS oranli,
    count(*) FILTER (WHERE oran > 1.0 AND sonuc) AS oranli_tuttu, COALESCE(sum(oran) FILTER (WHERE oran > 1.0 AND sonuc), 0) AS getiri_oran_toplam,
    sum(olasilik) AS olasilik_toplam
FROM greydb.tercih_izleme
WHERE sonuc IS NOT NULL AND kickoff >= :bas
GROUP BY 1, 2, 3, 4"""


def _topla(satirlar: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = sum(int(s["n"]) for s in satirlar)
    tuttu = sum(int(s["tuttu"]) for s in satirlar)
    oranli = sum(int(s["oranli"]) for s in satirlar)
    yatirilan = round(oranli * BAHIS_TUTARI, 2)
    donen = round(sum(float(s["getiri_oran_toplam"]) for s in satirlar) * BAHIS_TUTARI, 2)
    ort = None
    if n and all(s.get("olasilik_toplam") is not None for s in satirlar):
        ort = round(100 * sum(float(s["olasilik_toplam"]) for s in satirlar) / n, 1)
    return dict(tercih=n, tuttu=tuttu, tutma_yuzde=round(100 * tuttu / n, 1) if n else None, ort_olasilik=ort, oranli=oranli, yatirilan=yatirilan, donen=donen,
                sonuc=round(donen - yatirilan, 2), getiri_yuzde=round(100 * (donen - yatirilan) / yatirilan, 1) if yatirilan else None)


def ozet(satirlar: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Sonuçlanmış tercihlerin özeti. Para hesabı yalnızca oranı olan (oran > 1,00) tercihleri sayar; `deger_kurali`: bizim olasılık × oran > 1 olanlar."""
    sirali = {"acilis": 0, "orta": 1, "kapanis": 2}
    pencere = {}
    for p in sorted({s["pencere"] for s in satirlar}, key=lambda x: sirali.get(x, 9)):
        ps = [s for s in satirlar if s["pencere"] == p]
        pencere[p] = {**_topla(ps), "deger_kurali": _topla([s for s in ps if s["deger"]])}
    bant_no = lambda s: min(int(s["bant"]), 9)      # %100 olasılık da "90-100" bandına girer
    bant = [{"bant": f"{b * 10}-{b * 10 + 10}", **_topla([s for s in satirlar if bant_no(s) == b])} for b in sorted({bant_no(s) for s in satirlar})]
    kategoriler = sorted({s["kategori"] for s in satirlar})
    kategori = sorted(({"kategori": k, **_topla([s for s in satirlar if s["kategori"] == k])} for k in kategoriler), key=lambda x: (-x["tercih"], x["kategori"]))
    return dict(tum=_topla(satirlar), deger_kurali=_topla([s for s in satirlar if s["deger"]]), pencere=pencere, bant=bant, kategori=kategori)


def ozet_getir(gun: int = 30) -> Dict[str, Any]:
    from datetime import timezone
    from app.services.db import execute_query
    simdi = datetime.now(timezone.utc)
    bas = simdi - timedelta(days=gun)
    satirlar = execute_query(OZET_SQL, {"bas": bas})
    kapsam = oran_kapsami(execute_query(ORAN_KAPSAMI_SQL, {"bas": bas}))
    return {"gun": gun, "oran_kapsami": kapsam, "uretildi": simdi.isoformat(timespec="seconds"), "bahis_tutari": BAHIS_TUTARI,
            "not": "Her tercihe eşit tutar yatırılmış varsayımıdır; oranı olmayan ya da 1,00 olan tercihler para hesabına girmez. Yalnızca iç analiz.", **ozet(satirlar)}


# ---- oran kapsamı: oransız yazılan tercihler lig bazında (iddaa olayı eşleşmeyen ligler hemen görünsün) ----
ORAN_KAPSAMI_SQL = """SELECT lig_ad, count(DISTINCT fotmob_match_id) AS mac, count(*) AS n, count(*) FILTER (WHERE oran IS NULL OR oran <= 1.0) AS oransiz
FROM greydb.tercih_izleme
WHERE kickoff >= :bas
GROUP BY lig_ad"""


def oran_kapsami(satirlar: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cikti = []
    for s in satirlar:
        n, oransiz = int(s["n"]), int(s["oransiz"])
        cikti.append(dict(lig=s["lig_ad"] or "bilinmiyor", mac=int(s["mac"]), tercih=n, oransiz=oransiz, oransiz_yuzde=round(100 * oransiz / n, 1) if n else 0.0,
                          oran_yok=bool(n) and oransiz == n))
    return sorted(cikti, key=lambda x: (-x["oransiz_yuzde"], x["lig"]))
