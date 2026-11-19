"""Tercih izleme: matchforge günlük listesindeki her tercihi (olasılık + o andaki iddaa oranı) maçtan önce kaydeder, maç bitince sonucu yazar.

Kayıtlar eklemelidir (hiçbiri ezilmez): bir maç için kickoff'a ~24, ~6 ve ~1 saat kala olmak üzere en çok üç pencere yazılır; ilki "açılış", sonuncusu "kapanış" oranıdır.
Yalnızca iç analiz içindir; site ve postlarda gösterilmez.
"""
import logging
import math
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
OZET_SQL = """SELECT pencere, kategori, LEAST(floor(olasilik * 10)::int, 9) AS bant, COALESCE(oran <> 'NaN' AND olasilik * oran > 1, false) AS deger,
    count(*) AS n, count(*) FILTER (WHERE sonuc) AS tuttu, count(*) FILTER (WHERE oran > 1.0 AND oran <> 'NaN') AS oranli,
    count(*) FILTER (WHERE oran > 1.0 AND oran <> 'NaN' AND sonuc) AS oranli_tuttu, COALESCE(sum(oran) FILTER (WHERE oran > 1.0 AND oran <> 'NaN' AND sonuc), 0) AS getiri_oran_toplam,
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
    onay = piyasa_onayi_ozet(execute_query(ONAY_SQL, {"bas": bas}))
    return {"gun": gun, "oran_kapsami": kapsam, "piyasa_onayi": onay, "uretildi": simdi.isoformat(timespec="seconds"), "bahis_tutari": BAHIS_TUTARI,
            "not": "Her tercihe eşit tutar yatırılmış varsayımıdır; oranı olmayan ya da 1,00 olan tercihler para hesabına girmez. Yalnızca iç analiz.", **ozet(satirlar)}


# ---- oran kapsamı: oransız yazılan tercihler lig bazında (iddaa olayı eşleşmeyen ligler hemen görünsün) ----
ORAN_KAPSAMI_SQL = """SELECT lig_ad, count(DISTINCT fotmob_match_id) AS mac, count(*) AS n, count(*) FILTER (WHERE oran IS NULL OR oran <= 1.0 OR oran = 'NaN') AS oransiz
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


# ---- piyasa onayı: açılıştan kapanışa iddaa oranı modelin yönüne mi gitti? ----
# Ölçüm (youtube-faceless analiz/IDDAA-ORAN-HAREKETI-2026-11-20.md, 200 maç): model ilk orandan ≥4 puan farklıyken gol marketlerinde piyasa
# modele doğru giderse ("onay") sonuç son orandan da öte model yönünde ~8 puan saptı; taraf marketlerinde piyasa modelden uzaklaşırsa ("ret") yine model haklı çıktı.
# Bu bölüm o sinyali ileriye dönük izler. Tabloda yalnız seçilen tercihin oranı var: marj tam ayıklanamaz, iki seçimli iddaa marketlerinin
# ~%14-15 marjının yarısı kadar yaklaşık düzeltme yapılır (MARJ_YARI). Hareket yönü için düzeltme gerekmez (fark alınınca marj sadeleşir).
MARJ_YARI = 0.07            # 1/oran ≈ adil olasılık × (1 + MARJ_YARI)
MODEL_ESIK = 0.04           # model, açılış oranının ima ettiğinden en az bu kadar farklıysa yönü vardır
HAREKET_ESIK = 0.01         # kapanışta ima edilen olasılık açılıştan en az bu kadar oynadıysa piyasa hareket etmiştir
KATEGORI_GRUBU = {"Toplam gol": "gol", "Takım golü": "gol", "Karşılıklı gol": "gol", "Maç sonucu": "taraf", "Çifte şans": "taraf",
                  "Toplam korner": "korner", "Takım korneri": "korner", "Korner handikap": "korner", "Korner: kim daha çok": "korner",
                  "Toplam kart": "kart", "Kim daha çok kart": "kart"}
ETIKETLER = ("onay", "ret", "sabit", "model_yakin")

ONAY_SQL = """SELECT a.kategori, a.secim, a.olasilik AS olasilik_ac, a.oran AS oran_ac, k.oran AS oran_ka, k.sonuc
FROM greydb.tercih_izleme a
JOIN greydb.tercih_izleme k ON k.fotmob_match_id = a.fotmob_match_id AND k.kategori = a.kategori AND k.secim = a.secim AND k.pencere = 'kapanis'
WHERE a.pencere = 'acilis' AND k.sonuc IS NOT NULL AND a.oran > 1.0 AND k.oran > 1.0 AND a.oran <> 'NaN' AND k.oran <> 'NaN' AND a.kickoff >= :bas"""


def ima(oran: float) -> float:
    """Tek seçimin oranından yaklaşık adil olasılık (marjın yarısı düşülür)."""
    return 1.0 / (oran * (1.0 + MARJ_YARI))


def piyasa_onayi(olasilik_ac: float, oran_ac: float, oran_ka: float) -> Dict[str, Any]:
    """Etiket: "onay" (piyasa modele doğru gitti), "ret" (modelden uzaklaştı), "sabit" (kıpırdamadı), "model_yakin" (model açılışa yakın: yön yok).
    model_fark ve hareket olasılık puanı (0-1) cinsindendir."""
    p_ac, p_ka = ima(oran_ac), ima(oran_ka)
    fark, hareket = olasilik_ac - p_ac, p_ka - p_ac
    if abs(fark) < MODEL_ESIK:
        etiket = "model_yakin"
    elif abs(hareket) < HAREKET_ESIK:
        etiket = "sabit"
    else:
        etiket = "onay" if fark * hareket > 0 else "ret"
    return dict(etiket=etiket, model_yon="yukari" if fark >= MODEL_ESIK else "asagi" if fark <= -MODEL_ESIK else None,
                model_fark=round(fark, 4), hareket=round(hareket, 4), ima_ac=round(p_ac, 4), ima_ka=round(p_ka, 4))


def piyasa_onayi_ozet(satirlar: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Açılış ve kapanış penceresi olan sonuçlanmış tercihler: grup (gol, taraf, korner, kart, diğer) × etiket × model yönü.
    Her hücrede tutma, ortalama model olasılığı (açılış), kapanış oranının ima ettiği olasılık ve kapanış oranından eşit tutarlı getiri.
    "model_yonunde_fark": model yönünde (gerçek − kapanış ima) puan; artıysa model kapanış oranından daha haklı çıkmış."""
    hucre: Dict[Tuple[str, str, Optional[str]], List[Dict[str, Any]]] = {}
    for r in satirlar:
        sayilar = [float(r[k]) for k in ("olasilik_ac", "oran_ac", "oran_ka")]
        if not all(math.isfinite(x) for x in sayilar) or sayilar[1] <= 1.0 or sayilar[2] <= 1.0:
            continue                                               # NaN/eksik oran: PostgreSQL NaN'ı her sayıdan büyük sayar, SQL süzgeci kaçırabilir
        e = piyasa_onayi(*sayilar)
        grup = KATEGORI_GRUBU.get(r["kategori"], "diğer")
        hucre.setdefault((grup, e["etiket"], e["model_yon"]), []).append({**r, **e})

    def ozetle(v: List[Dict[str, Any]]) -> Dict[str, Any]:
        n = len(v)
        tuttu = sum(1 for x in v if x["sonuc"])
        yon = lambda x: 1.0 if x["model_yon"] == "yukari" else -1.0 if x["model_yon"] == "asagi" else 0.0
        satir = dict(n=n, tuttu=tuttu, oranli=n, getiri_oran_toplam=sum(float(x["oran_ka"]) for x in v if x["sonuc"]),
                     olasilik_toplam=sum(float(x["olasilik_ac"]) for x in v))
        return {**_topla([satir]), "kapanis_ima_ort": round(100 * sum(x["ima_ka"] for x in v) / n, 1) if n else None,
                "model_yonunde_fark": round(100 * sum(yon(x) * ((1.0 if x["sonuc"] else 0.0) - x["ima_ka"]) for x in v) / n, 1) if n else None}

    sira = {e: i for i, e in enumerate(ETIKETLER)}
    hucreler = [{"grup": g, "etiket": e, "model_yon": y, **ozetle(v)} for (g, e, y), v in hucre.items()]
    hucreler.sort(key=lambda h: (h["grup"], sira[h["etiket"]], h["model_yon"] or ""))
    etiket = {e: ozetle([x for (g, ee, y), v in hucre.items() if ee == e for x in v]) for e in ETIKETLER if any(k[1] == e for k in hucre)}
    return dict(tanim=dict(marj_yari=MARJ_YARI, model_esik=MODEL_ESIK, hareket_esik=HAREKET_ESIK,
                           not_="Açılış = kickoff'a ~24 saat kala kayıt, kapanış = ~1 saat kala. onay: piyasa modelin yönüne gitti; ret: ters yöne; "
                                "sabit: oynamadı; model_yakin: model açılış oranına yakın (yön yok)."),
                etiket=etiket, hucreler=hucreler)


# ---- maç bazında piyasa onayı (Günün Maçları paneli, deneysel): sonuç beklenmeden, eldeki ilk ve son pencereden ----
MAC_SQL = """SELECT pencere, kategori, secim, olasilik, oran, kaydedildi, sonuc, kickoff
FROM greydb.tercih_izleme WHERE fotmob_match_id = :id ORDER BY kaydedildi"""
PENCERE_SIRASI = {"acilis": 0, "orta": 1, "kapanis": 2}


def _gecerli_oran(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) and v > 1.0 else None


def mac_onaylari(satirlar: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Bir maçın tercihleri: (kategori, seçim) başına ilk ve son kayıt; ikisi farklı pencereyse ve oranlar geçerliyse etiket.
    Tek kayıt varsa etiket "bekliyor" (ikinci pencere ~6 saat kala alınır). Model olasılığı ilk kayıttan (açılış) alınır."""
    gruplar: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for r in satirlar:
        gruplar.setdefault((r["kategori"], r["secim"]), []).append(r)
    secimler = []
    for (kategori, secim), v in gruplar.items():
        v = sorted(v, key=lambda r: PENCERE_SIRASI.get(r["pencere"], 9))
        ilk, son = v[0], v[-1]
        o_ilk, o_son = _gecerli_oran(ilk["oran"]), _gecerli_oran(son["oran"])
        kayit = dict(kategori=kategori, secim=secim, grup=KATEGORI_GRUBU.get(kategori, "diğer"), olasilik=round(float(ilk["olasilik"]), 4),
                     olasilik_son=round(float(son["olasilik"]), 4), pencere_ilk=ilk["pencere"], pencere_son=son["pencere"], oran_ilk=o_ilk, oran_son=o_son,
                     sonuc=son.get("sonuc"))
        if ilk["pencere"] != son["pencere"] and o_ilk is not None and o_son is not None:
            kayit.update(piyasa_onayi(float(ilk["olasilik"]), o_ilk, o_son))
        else:
            kayit.update(etiket="oran_yok" if o_ilk is None or o_son is None else "bekliyor", model_yon=None, model_fark=None, hareket=None,
                         ima_ac=None if o_ilk is None else round(ima(o_ilk), 4), ima_ka=None if o_son is None else round(ima(o_son), 4))
        secimler.append(kayit)
    secimler.sort(key=lambda k: (-float(k["olasilik"]), k["kategori"], k["secim"]))
    pencereler = sorted({r["pencere"] for r in satirlar}, key=lambda p: PENCERE_SIRASI.get(p, 9))
    return dict(pencereler=pencereler, secimler=secimler)


def mac_getir(fotmob_match_id: int) -> Dict[str, Any]:
    from app.services.db import execute_query
    return {"fotmob_match_id": fotmob_match_id, **mac_onaylari(execute_query(MAC_SQL, {"id": fotmob_match_id}))}
