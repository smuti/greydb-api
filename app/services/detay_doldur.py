"""Detay doldurma: `raw_match_details`'i olan ama `match_stats`'ı olmayan bitmiş maçların tüm tablolarını yeniden yazar (match-data/refill-missing-data'nın paralel yerel karşılığı).

Neden: uç tek kilitli ve seri (dakikada ~10 maç); geçmiş doldurmayla (milli, TFF 1. Lig, Konferans, Championship) gelen ~4.700 maç ~8 saat sürerdi. Her işçi yalnız
`id % n = k` maçlarını alır (işçiler çakışmaz), keyset (`id > sonra`) ile ilerler: kaydedilince match_stats oluşmayan (atlanan) ya da hata veren maç döngüye girmez.
Seçim süzgeçleri uçla aynı (+ Periods.All dolu olmalı: FotMob bazı maçlarda boş 'stats' kabuğu verir, kayıt kodu orada AttributeError atar); kayıt `_refill_kaydet` ile aynı (maç başına ayrı işlem)."""
from typing import Any, Callable, Dict, List, Optional

SECIM_SQL = """
    SELECT m.id, m.fotmob_match_id, m.raw_match_details
    FROM public.matches m
    LEFT JOIN public.match_stats ms ON m.id = ms.match_id
    WHERE m.raw_match_details IS NOT NULL
      AND ms.match_id IS NULL
      AND m.finished = true
      AND jsonb_typeof(m.raw_match_details->'content'->'stats') = 'object'
      AND m.raw_match_details->'content'->'stats' <> '{}'::jsonb
      AND jsonb_typeof(m.raw_match_details->'content'->'stats'->'Periods'->'All') = 'object'
      AND m.id % :n = :k
      AND m.id > :sonra
    ORDER BY m.id
    LIMIT :limit
"""


def isle(getir: Callable[[int, int, int, int], List[Dict[str, Any]]], kaydet: Callable[[Any], bool], n: int, k: int, limit: int,
         ilerleme: Optional[Callable[[int], None]] = None, ilerleme_adim: int = 50) -> Dict[str, int]:
    """getir(n, k, sonra, limit) -> satırlar; kaydet(ham) -> match_stats oluştu mu (True/False), hata fırlatabilir. Dilim bitene kadar sürer."""
    if n < 1 or not 0 <= k < n:
        raise ValueError("n >= 1 ve 0 <= k < n olmalı")
    sayim = {"islenen": 0, "dolan": 0, "atlanan": 0, "hata": 0}
    sonra = 0
    while True:
        satirlar = getir(n, k, sonra, limit)
        if not satirlar:
            return sayim
        for s in satirlar:
            sonra = s["id"]
            sayim["islenen"] += 1
            try:
                sayim["dolan" if kaydet(s["raw_match_details"]) else "atlanan"] += 1
            except Exception:  # noqa: BLE001 — o maç atlanır, işçi sürer
                sayim["hata"] += 1
            if ilerleme and sayim["islenen"] % ilerleme_adim == 0:
                ilerleme(sayim["islenen"])


SUTUN_SQL = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'match_player_stats'"


def eksik_sutunlar(sorgu: Callable[..., List[Dict[str, Any]]], gerekli: List[str]) -> List[str]:
    """match_player_stats'ta olmayan gerekli sütunlar. Kayıt kodu her süreçte bir kez `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` çalıştırır; bu, tablodaki tüm açık işlemleri bekleyip
    diğer işçileri kuyrukta tuttuğu için paralel işçilerde (N süreç = N durma) hızı öldürür. Sütunlar tamamsa işçi bu adımı atlar (oyuncu_stat._HAZIR)."""
    var = {r["column_name"] for r in sorgu(SUTUN_SQL)}
    return [c for c in gerekli if c not in var]
