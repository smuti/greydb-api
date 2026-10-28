"""Gol modeli geri testi: geçmiş maçları üretimdeki gibi günlük yeniden tahmin eder ve karne biçiminde (kaynak=geri_test) satır üretir.

Her maç günü için model o günün başında (as_of = gün 00:00) kurulur; matchforge `_prior` yalnız `date < as_of` maçları kullandığından
tahmin hiçbir zaman kendi gününün ve sonrasının sonuçlarını görmez. Model dışarıdan verilir (`model_kur(as_of) -> tahmin(ev, dep, lig)`):
bu modül matchforge'a bağlı değildir. Son dönem "sınav" olarak ayrılır: adaylar yalnız geliştirme döneminde ayarlanır, sınavda bir kez ölçülür.
"""
import json
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Dict, List, Tuple

from app.services.gol_karne import rapor, tahmin_satiri
from app.services.skorjin_prematch import MODEL_LIGLERI, _model_yok_nedeni

KAYNAK = "geri_test"
Tahmin = Callable[[Any, Any, Any], Dict[str, Any]]


def gun_pencereleri(bas: date, son: date) -> List[date]:
    """bas..son (ikisi dahil) her gün."""
    return [bas + timedelta(days=i) for i in range((son - bas).days + 1)]


def uret(maclar: List[Dict[str, Any]], model_kur: Callable[[datetime], Tahmin]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Maçları günlere böler, her gün için tek model kurar, karne satırlarını (gerçek skorla) üretir."""
    gunler: Dict[date, List[Dict[str, Any]]] = defaultdict(list)
    for m in maclar:
        gunler[m["match_date"].date()].append(m)
    satirlar: List[Dict[str, Any]] = []
    ozet: Dict[str, Any] = {"gun": 0, "mac": 0, "hata_gun": 0,
                            "atlanan": {"kapsam_disi": 0, "yeni_takim": 0, "matris_yok": 0, "tahmin_hatasi": 0}}
    for gun in sorted(gunler):
        as_of = datetime.combine(gun, time.min)
        try:
            tahmin = model_kur(as_of)
        except Exception:  # noqa: BLE001 — tek günün kurulum hatası geri testi durdurmaz
            ozet["hata_gun"] += 1
            continue
        ozet["gun"] += 1
        for m in sorted(gunler[gun], key=lambda x: (x["match_date"], x["fotmob_match_id"])):
            if m["league_id"] not in MODEL_LIGLERI:
                ozet["atlanan"]["kapsam_disi"] += 1
                continue
            try:
                sonuc = tahmin(m["home_team_id"], m["away_team_id"], m["league_id"])
            except Exception:  # noqa: BLE001
                ozet["atlanan"]["tahmin_hatasi"] += 1
                continue
            satir = tahmin_satiri(m, sonuc, as_of - timedelta(seconds=1), kaynak=KAYNAK)   # gece yarısı başlayan maç da kayıt alır
            if satir is None:
                ozet["atlanan"][_model_yok_nedeni(m, sonuc)] += 1
                continue
            satirlar.append({**satir, "home_score": m["home_score"], "away_score": m["away_score"]})
            ozet["mac"] += 1
    return satirlar, ozet


def sinav_ayir(satirlar: List[Dict[str, Any]], sinav_bas: datetime) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """(geliştirme: kickoff < sinav_bas, sınav: kickoff >= sinav_bas)."""
    return [s for s in satirlar if s["kickoff"] < sinav_bas], [s for s in satirlar if s["kickoff"] >= sinav_bas]


def bolumlu_rapor(satirlar: List[Dict[str, Any]], sinav_bas: datetime, min_n: int = 40, esik_z: float = 2.0) -> Dict[str, Any]:
    gelistirme, sinav = sinav_ayir(satirlar, sinav_bas)
    return {"sinav_bas": sinav_bas.isoformat(), "gelistirme": rapor(gelistirme, min_n, esik_z), "sinav": rapor(sinav, min_n, esik_z)}


_TARIHLER = ("kickoff",)


def satir_json(satir: Dict[str, Any]) -> str:
    return json.dumps({k: (v.isoformat() if k in _TARIHLER else v) for k, v in satir.items()}, ensure_ascii=False)


def satir_oku(metin: str) -> Dict[str, Any]:
    s = json.loads(metin)
    for k in _TARIHLER:
        if s.get(k) is not None:
            s[k] = datetime.fromisoformat(s[k])
    return s
