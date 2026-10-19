"""
Eski tahminlerden taşınan kuponlardan SEÇİLENLERİ siler (bacaklarıyla birlikte). Eski tablodaki (greydb.predictions) kayıtlara dokunmaz: arşiv olarak kalır.
Varsayılan KURU çalışmadır: neyin silineceğini gösterir. --yaz ile siler. Yalnızca kaynağı 'tahmin:<id>' olan kuponlar silinebilir.

Kullanım:  DATABASE_URL=... python scripts/kuponlari_sil.py 39,47,77 [--yaz]
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.routers import kuponlar as K  # noqa: E402
from app.services.db import execute_query  # noqa: E402
from app.services.kupon_celiski import silinebilir_mi  # noqa: E402

if __name__ == "__main__":
    idler = [int(x) for x in (sys.argv[1] if len(sys.argv) > 1 else "").split(",") if x.strip().isdigit()]
    if not idler:
        print("Kullanım: python scripts/kuponlari_sil.py 39,47,77 [--yaz]")
        sys.exit(1)
    YAZ = "--yaz" in sys.argv
    print(("SİLİNİYOR" if YAZ else "KURU ÇALIŞMA (silinmez)") + f" | {len(idler)} tahmin")
    silinen = 0
    for tid in idler:
        kaynak = f"tahmin:{tid}"
        satir = execute_query(
            "SELECT k.id, k.durum, k.sonuc, b.ev, b.dep, b.bahis_turu, b.secim FROM greydb.kuponlar k LEFT JOIN greydb.kupon_bacaklari b ON b.kupon_id = k.id WHERE k.kaynak = :k", {"k": kaynak})
        if not satir:
            print(f"  #{tid}: kupon yok (zaten silinmiş olabilir)")
            continue
        if not silinebilir_mi(kaynak):
            print(f"  #{tid}: silinemez (kaynak uygun değil)")
            continue
        s = satir[0]
        metin = f"  #{tid} kupon {s['id']} | {s['ev']} - {s['dep']} | {s['bahis_turu']} / {s['secim']} | durum={s['durum']} sonuç={s['sonuc']}"
        silinen += 1
        if YAZ:
            K.db_sil(s["id"])
        print(metin + (" → silindi" if YAZ else " → silinecek"))
    print(f"\n{'silinen' if YAZ else 'silinecek'}: {silinen}")
