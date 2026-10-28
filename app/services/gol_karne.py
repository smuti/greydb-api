"""Gol modeli karnesi: skor matrisinden pazar olayları, puanlama ve sapma uyarıları.

Maçtan önce kaydedilen 11x11 skor matrisi (P[ev i gol, dep j gol]) maç bitince gerçek skorla puanlanır. Pazar olasılıkları
matristen sonradan türetilir; her olay tek yönüyle tutulur (Üst, KG Var, MS 1 ...). Alt/Yok yönü 1-p olduğundan Brier ve log loss
iki yön için aynıdır, ayrıca saklamaya gerek yoktur. Saf fonksiyonlar: I/O yok.
"""
import math
from collections import defaultdict
from typing import Any, Dict, List

BOYUT = 11
TOPLAM_CIZGILERI = (0.5, 1.5, 2.5, 3.5, 4.5)
TAKIM_CIZGILERI = (0.5, 1.5, 2.5)
_EPS = 1e-6      # log loss için olasılığı uçlardan uzak tut


def _dogrula(matris: Any) -> None:
    if not (isinstance(matris, (list, tuple)) and len(matris) == BOYUT and all(isinstance(s, (list, tuple)) and len(s) == BOYUT for s in matris)):
        raise ValueError("skor matrisi 11x11 olmalı")
    toplam = sum(sum(s) for s in matris)
    if abs(toplam - 1.0) > 1e-6:
        raise ValueError(f"skor matrisi toplamı 1 olmalı: {toplam}")


def pazar_olaylari(matris: List[List[float]], ev_gol: Any, dep_gol: Any) -> List[Dict[str, Any]]:
    """Matristen her pazarın olay olasılığı (p) ve gerçekleşen sonuç (y: 0/1) listesi."""
    _dogrula(matris)
    if ev_gol is None or dep_gol is None:
        raise ValueError("gerçek skor yok")
    ev_gol, dep_gol = int(ev_gol), int(dep_gol)
    hucreler = [(i, j, matris[i][j]) for i in range(BOYUT) for j in range(BOYUT)]

    def p_of(kosul) -> float:
        return sum(p for i, j, p in hucreler if kosul(i, j))

    cikti: List[Dict[str, Any]] = []

    def ekle(ad: str, p: float, y: bool) -> None:
        cikti.append({"pazar": ad, "p": p, "y": 1 if y else 0})

    for c in TOPLAM_CIZGILERI:
        ekle(f"toplam_ust_{c}", p_of(lambda i, j, c=c: i + j > c), ev_gol + dep_gol > c)
    ekle("kg_var", p_of(lambda i, j: i > 0 and j > 0), ev_gol > 0 and dep_gol > 0)
    for c in TAKIM_CIZGILERI:
        ekle(f"ev_ust_{c}", p_of(lambda i, j, c=c: i > c), ev_gol > c)
    for c in TAKIM_CIZGILERI:
        ekle(f"dep_ust_{c}", p_of(lambda i, j, c=c: j > c), dep_gol > c)
    p1, p0, p2 = p_of(lambda i, j: i > j), p_of(lambda i, j: i == j), p_of(lambda i, j: i < j)
    ekle("ms_1", p1, ev_gol > dep_gol)
    ekle("ms_0", p0, ev_gol == dep_gol)
    ekle("ms_2", p2, ev_gol < dep_gol)
    ekle("cs_10", p1 + p0, ev_gol >= dep_gol)
    ekle("cs_02", p0 + p2, dep_gol >= ev_gol)
    ekle("cs_12", p1 + p2, ev_gol != dep_gol)
    return cikti


def puanla(satirlar: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """(pazar, model_version, kaynak) başına n, ortalama olasılık, gerçekleşen oran, Brier, log loss ve z.

    z = (gerçekleşen - ortalama olasılık) / standart hata, standart hata = sqrt(sum p(1-p)) / n. Negatif: fazla iddialı, pozitif: fazla temkinli.
    """
    gruplar: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
    for s in satirlar:
        gruplar[(s["pazar"], s["model_version"], s["kaynak"])].append(s)
    cikti = []
    for (pazar, surum, kaynak), grup in gruplar.items():
        n = len(grup)
        ps = [min(max(float(s["p"]), _EPS), 1 - _EPS) for s in grup]
        ys = [int(s["y"]) for s in grup]
        ort_p, gercek = sum(ps) / n, sum(ys) / n
        brier = sum((p - y) ** 2 for p, y in zip(ps, ys)) / n
        logloss = -sum(y * math.log(p) + (1 - y) * math.log(1 - p) for p, y in zip(ps, ys)) / n
        se = math.sqrt(sum(p * (1 - p) for p in ps)) / n
        cikti.append({"pazar": pazar, "model_version": surum, "kaynak": kaynak, "n": n, "ort_p": ort_p, "gercek": gercek,
                      "fark": gercek - ort_p, "brier": brier, "logloss": logloss, "z": (gercek - ort_p) / se})
    return sorted(cikti, key=lambda r: (r["kaynak"], r["model_version"], r["pazar"]))


def uyarilar(tablo: List[Dict[str, Any]], min_n: int = 40, esik_z: float = 2.0) -> List[Dict[str, Any]]:
    """Yeterli örnekte kalibrasyonu belirgin bozuk pazarları işaretler (|z| >= eşik)."""
    cikti = []
    for r in tablo:
        if r["n"] >= min_n and abs(r["z"]) >= esik_z:
            cikti.append({**r, "yon": "fazla_iddiali" if r["z"] < 0 else "fazla_temkinli"})
    return cikti
