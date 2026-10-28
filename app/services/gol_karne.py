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
        ham = [float(s["p"]) for s in grup]
        ps = [min(max(p, _EPS), 1 - _EPS) for p in ham]          # kırpılmış: yalnız log loss ve standart hata için (sıfıra bölme/log(0) olmasın)
        ys = [int(s["y"]) for s in grup]
        ort_p, gercek = sum(ham) / n, sum(ys) / n
        brier = sum((p - y) ** 2 for p, y in zip(ham, ys)) / n
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


# ---------------------------------------------------------------- tablo, kayıt ve rapor
import json  # noqa: E402
from datetime import datetime  # noqa: E402
from typing import Optional, Tuple  # noqa: E402

TABLO_DDL = """
CREATE TABLE IF NOT EXISTS greydb.gol_karne_tahmin (
    fotmob_match_id BIGINT NOT NULL,
    model_version TEXT NOT NULL,
    kaynak TEXT NOT NULL,
    league_id INTEGER,
    home_team_id INTEGER, away_team_id INTEGER,
    home_name TEXT, away_name TEXT,
    kickoff TIMESTAMPTZ NOT NULL,
    as_of DATE,
    matrix JSONB NOT NULL,
    lam_h DOUBLE PRECISION, lam_a DOUBLE PRECISION,
    p1 DOUBLE PRECISION, p0 DOUBLE PRECISION, p2 DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (fotmob_match_id, model_version, kaynak)
);
CREATE INDEX IF NOT EXISTS idx_gol_karne_kickoff ON greydb.gol_karne_tahmin (kickoff)
"""

_KOLONLAR = ("league_id", "home_team_id", "away_team_id", "home_name", "away_name", "kickoff", "as_of", "matrix",
             "lam_h", "lam_a", "p1", "p0", "p2")


def tahmin_satiri(m: Dict[str, Any], sonuc: Optional[Dict[str, Any]], simdi: datetime, kaynak: str = "canli") -> Optional[Dict[str, Any]]:
    """Karne kaydı: matris yoksa, yeni takımsa, matris geçersizse ya da maç başlamışsa None."""
    if not sonuc or not sonuc.get("score_matrix") or sonuc.get("new_team_home") or sonuc.get("new_team_away"):
        return None
    if m["match_date"] <= simdi:
        return None
    try:
        _dogrula(sonuc["score_matrix"])
    except ValueError:
        return None
    return {"fotmob_match_id": m["fotmob_match_id"], "model_version": sonuc.get("model_version"), "kaynak": kaynak,
            "league_id": m.get("league_id"), "home_team_id": m.get("home_team_id"), "away_team_id": m.get("away_team_id"),
            "home_name": m.get("home_name"), "away_name": m.get("away_name"), "kickoff": m["match_date"], "as_of": sonuc.get("as_of"),
            "matrix": sonuc["score_matrix"], "lam_h": sonuc.get("lam_h"), "lam_a": sonuc.get("lam_a"),
            "p1": sonuc.get("p1"), "p0": sonuc.get("p0"), "p2": sonuc.get("p2")}


def yazma_sorgusu(satir: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Maç başlayana kadar günceller; başlayınca satır donar (WHERE kickoff > now())."""
    params = {c: satir.get(c) for c in ("fotmob_match_id", "model_version", "kaynak") + _KOLONLAR}
    params["matrix"] = json.dumps(satir["matrix"])
    sutunlar = ", ".join(("fotmob_match_id", "model_version", "kaynak") + _KOLONLAR)
    degerler = ", ".join(f"CAST(:{c} AS jsonb)" if c == "matrix" else f":{c}" for c in ("fotmob_match_id", "model_version", "kaynak") + _KOLONLAR)
    guncelle = ", ".join(f"{c} = EXCLUDED.{c}" for c in _KOLONLAR)
    return (f"INSERT INTO greydb.gol_karne_tahmin ({sutunlar}, updated_at) VALUES ({degerler}, now()) "
            f"ON CONFLICT (fotmob_match_id, model_version, kaynak) DO UPDATE SET {guncelle}, updated_at = now() "
            f"WHERE greydb.gol_karne_tahmin.kickoff > now()"), params


SONUC_SQL = """
SELECT t.fotmob_match_id, t.model_version, t.kaynak, t.matrix, t.kickoff, m.home_score, m.away_score
FROM greydb.gol_karne_tahmin t
JOIN public.matches m ON m.fotmob_match_id = t.fotmob_match_id
WHERE m.finished AND NOT COALESCE(m.cancelled, false) AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
  AND t.kickoff >= :bas AND t.kickoff < :son
ORDER BY t.kickoff
"""


def rapor(satirlar: List[Dict[str, Any]], min_n: int = 40, esik_z: float = 2.0) -> Dict[str, Any]:
    """Bitmiş maç satırlarını (matris + gerçek skor) pazarlara açıp puanlar. Bozuk matris atlanır ve sayılır."""
    olaylar, mac, atlanan = [], 0, 0
    for s in satirlar:
        try:
            ol = pazar_olaylari(s["matrix"], s["home_score"], s["away_score"])
        except ValueError:
            atlanan += 1
            continue
        mac += 1
        olaylar.extend({**o, "model_version": s["model_version"], "kaynak": s["kaynak"]} for o in ol)
    tablo = puanla(olaylar)
    return {"mac": mac, "atlanan": atlanan, "tablo": tablo, "uyarilar": uyarilar(tablo, min_n, esik_z)}
