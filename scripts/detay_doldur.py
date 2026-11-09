"""Eksik detay tablolarını (match_stats, oyuncu istatistikleri...) yerelde paralel doldurur. Ayrıntı: app/services/detay_doldur.py.

Her işçi bir dilim alır (id % N = K); N işçiyi ayrı süreçlerde çalıştır. DATABASE_URL ortam değişkeni gerekir (komut satırına/dosyaya yazılmaz). Varsayılan KURU: yalnız sayar.
  DATABASE_URL=... python scripts/detay_doldur.py --n 8 --k 0           # kuru: dilimdeki eksik sayısı
  DATABASE_URL=... python scripts/detay_doldur.py --n 8 --k 0 --yaz     # yazar
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.services import detay_doldur as dd  # noqa: E402
from app.services.db import execute_query, transaction  # noqa: E402
from app.services import oyuncu_stat  # noqa: E402
from app.services.match_saver import save_full_match_data  # noqa: E402


def kaydet(ham) -> bool:
    """routers.match_data._refill_kaydet ile aynı: maçı yeniden yazar, match_stats satırı oluştuysa True."""
    if isinstance(ham, str):
        ham = json.loads(ham)
    with transaction():
        mid = save_full_match_data(ham)
        return bool(execute_query("SELECT 1 AS n FROM public.match_stats WHERE match_id = :id LIMIT 1", {"id": mid}))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, required=True, help="toplam işçi sayısı")
    ap.add_argument("--k", type=int, required=True, help="bu işçinin dilimi (0..n-1)")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--yaz", action="store_true")
    a = ap.parse_args()
    getir = lambda n, k, sonra, limit: execute_query(dd.SECIM_SQL, {"n": n, "k": k, "sonra": sonra, "limit": limit})  # noqa: E731
    if not a.yaz:
        sayi = execute_query("SELECT count(*) AS c FROM (" + dd.SECIM_SQL.replace("LIMIT :limit", "") + ") x", {"n": a.n, "k": a.k, "sonra": 0})[0]["c"]
        print(f"KURU: dilim {a.k}/{a.n}: {sayi} maç doldurulacak (yazılmadı). Yazmak için --yaz.")
        return
    eksik = dd.eksik_sutunlar(execute_query, oyuncu_stat.YENI_SUTUNLAR)
    if eksik:
        sys.exit(f"match_player_stats'ta eksik sütun var ({eksik[:5]}...): önce tek süreçte oyuncu_stat.sutunlari_hazirla() çalıştırılmalı; paralel işçi başlatılmadı.")
    oyuncu_stat._HAZIR = True          # sütunlar tamam: kayıt kodu işçide ALTER TABLE çalıştırmasın (paralel işçileri kilitle durdurur)
    r = dd.isle(getir, kaydet, a.n, a.k, a.limit, ilerleme=lambda t: print(f"  ... dilim {a.k}: {t} işlendi", flush=True))
    print(f"BİTTİ dilim {a.k}/{a.n}: {r}", flush=True)


if __name__ == "__main__":
    main()
