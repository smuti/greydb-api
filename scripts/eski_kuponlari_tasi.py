"""
Eski kuponları (greydb.coupons + coupon_matches) yeni modele (greydb.kuponlar + kupon_bacaklari) taşır. Eski tablolara DOKUNMAZ.

Yalnız dönüştürülebilenler taşınır (acik_radar -> Açık, bayi -> Bayi; market adı, maç tarihi ve toplam oran tutarlı olanlar). Diğerleri atlanır ve NEDENİYLE yazılır;
eski tabloda durmaya devam eder. Eski kuponlarda sonuç saklanmıyordu: sonuçlar, yayına alınca çalışan otomatik sonuçlandırma işiyle maç skorundan gelir.
Tekrar çalıştırılabilir (kaynak = 'kupon:<id>' olanlar atlanır). Varsayılan KURU ÇALIŞMA.

Kullanım:  DATABASE_URL=... python scripts/eski_kuponlari_tasi.py [--yaz]
"""
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tahminleri_kupona_tasi import sorgu, yaz  # noqa: E402

from app.services.eski_kupon_tasima import Tasinamaz, eski_kupon  # noqa: E402

if __name__ == "__main__":
    YAZ = "--yaz" in sys.argv
    tablo_var = sorgu("SELECT to_regclass('greydb.kuponlar') IS NOT NULL AS v")[0]["v"]
    if YAZ and not tablo_var:
        sys.exit("HATA: greydb.kuponlar yok. Önce scripts/create_kuponlar_tables.py çalıştırın.")
    var = {r["kaynak"] for r in sorgu("SELECT kaynak FROM greydb.kuponlar WHERE kaynak LIKE 'kupon:%'")} if tablo_var else set()
    kuponlar = sorgu("SELECT * FROM greydb.coupons ORDER BY id")
    sayac, atlanan = Counter(), []
    for k in kuponlar:
        if f"kupon:{k['id']}" in var:
            sayac["zaten_var"] += 1
            continue
        bacaklar = sorgu("SELECT * FROM greydb.coupon_matches WHERE coupon_id = :id ORDER BY id", {"id": k["id"]})
        try:
            yeni = eski_kupon(k, bacaklar)
        except Tasinamaz as e:
            atlanan.append((k["id"], k["type"], k["status"], str(e)))
            sayac["atlanan"] += 1
            continue
        sayac[f"tasinan/{yeni['durum']}"] += 1
        if YAZ:
            yaz(yeni, None, None)
    print(f"eski kupon: {len(kuponlar)} | " + ("YAZILDI" if YAZ else "KURU ÇALIŞMA (yazılmadı)"))
    for a, v in sorted(sayac.items()):
        print(f"  {a:22s} {v}")
    print("\nTaşınamayanlar (eski tabloda kalır):")
    for a in atlanan:
        print(f"  #{a[0]} ({a[1]}, {a[2]}): {a[3]}")
