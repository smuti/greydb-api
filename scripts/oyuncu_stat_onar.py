"""Geçmiş bitmiş maçların oyuncu istatistiklerini ham maç verisinden onarır (greydb-api'nin onarım ucunu döngüyle çağırır).

Varsayılan KURU çalışır: yazmaz, ne yapacağını raporlar. Gerçekten yazmak için --yaz.
Anahtar ortam değişkeninden okunur (CRON_ANAHTARI); komut satırına yazılmaz.

  CRON_ANAHTARI=... python scripts/oyuncu_stat_onar.py                 # kuru: 100'er maç, tümü
  CRON_ANAHTARI=... python scripts/oyuncu_stat_onar.py --yaz           # yaz
  CRON_ANAHTARI=... python scripts/oyuncu_stat_onar.py --yaz --sonra 12000   # kaldığı yerden devam (son rapordaki "sonraki")
"""
import argparse
import json
import os
import sys
import urllib.request

KOK = os.environ.get("GREYDB_API_ORIGIN", "https://greydb-api-production.up.railway.app").rstrip("/")


def istek(url: str, anahtar: str) -> dict:
    r = urllib.request.Request(url, method="POST", headers={"X-Cron-Anahtari": anahtar})
    with urllib.request.urlopen(r, timeout=300) as y:
        return json.loads(y.read().decode())


def onar(yaz: bool, sonra: int = 0, limit: int = 100, anahtar: str = "", kok: str = KOK, post=istek, yaz_cikti=print) -> dict:
    """Uç `bitti` diyene kadar çağırır. Toplamlar ve son imleç döner; bir çağrıda hata olursa durur (imleç raporda)."""
    top = dict(islenen_mac=0, guncellenen=0, eklenen=0, belirsiz=0, oyuncusuz_mac=0, hata=0, sonraki=sonra, bitti=False, dry_run=not yaz)
    while True:
        r = post(f"{kok}/api/skorjin/oyuncu-stat/geri-doldur?dry_run={'false' if yaz else 'true'}&sonra={top['sonraki']}&limit={limit}", anahtar)
        for k in ("islenen_mac", "guncellenen", "eklenen", "belirsiz", "oyuncusuz_mac"):
            top[k] += r.get(k, 0)
        top["hata"] += len(r.get("hata") or [])
        top["sonraki"], top["bitti"] = r["sonraki"], bool(r["bitti"])
        yaz_cikti(f"maç {top['islenen_mac']:6} | güncellenen {top['guncellenen']:7} | eklenen {top['eklenen']:5} | belirsiz {top['belirsiz']:3} | hata {top['hata']:3} | imleç {top['sonraki']}")
        if r["bitti"]:
            return top


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--yaz", action="store_true", help="gerçekten yaz (varsayılan: kuru çalışma)")
    ap.add_argument("--sonra", type=int, default=0, help="bu maç kimliğinden sonrasını işle (devam etmek için)")
    ap.add_argument("--limit", type=int, default=100, help="çağrı başına maç (en çok 500)")
    a = ap.parse_args(argv)
    anahtar = os.environ.get("CRON_ANAHTARI", "")
    if not anahtar:
        print("CRON_ANAHTARI ortam değişkeni gerekli", file=sys.stderr)
        return 2
    sonuc = onar(a.yaz, a.sonra, a.limit, anahtar)
    print(("YAZILDI" if a.yaz else "KURU ÇALIŞMA (hiçbir şey yazılmadı)"), json.dumps(sonuc, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
