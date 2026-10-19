"""
Eski tahminlerden taşınan kuponlarda admin'in girdiği sonuç ile maç skorundan hesaplanan sonuç çeliştiğinde, SEÇİLEN tahminlerin bacak sonucunu motorunkiyle değiştirir.
Varsayılan KURU çalışmadır (yazmaz): her tahmin için admin sonucu, motor sonucu ve maç skoru yazılır. --yaz ile uygular (sonuç kaynağı 'otomatik', kupon sonucu yeniden hesaplanır).

Kullanım:  DATABASE_URL=... python scripts/celiskileri_duzelt.py 38,39,47 [--yaz]
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.routers import kuponlar as K  # noqa: E402
from app.services.db import execute_query  # noqa: E402
from app.services.kupon_celiski import duzeltme_karari  # noqa: E402
from app.services.kupon_hesap import bacak_sonucu  # noqa: E402
from app.services.kupon_mac_verisi import mac_yukle, maci_bul  # noqa: E402

if __name__ == "__main__":
    idler = [int(x) for x in (sys.argv[1] if len(sys.argv) > 1 else "").split(",") if x.strip().isdigit()]
    if not idler:
        print("Kullanım: python scripts/celiskileri_duzelt.py 38,39,47 [--yaz]")
        sys.exit(1)
    YAZ = "--yaz" in sys.argv
    print(("UYGULANIYOR" if YAZ else "KURU ÇALIŞMA (yazılmaz)") + f" | {len(idler)} tahmin")
    for tid in idler:
        satir = execute_query(
            "SELECT k.id AS kid, b.id AS bid, b.ev, b.dep, b.mac_zamani, b.bahis_turu, b.secim, b.sonuc, b.sonuc_kaynagi "
            "FROM greydb.kuponlar k JOIN greydb.kupon_bacaklari b ON b.kupon_id = k.id WHERE k.kaynak = :k", {"k": f"tahmin:{tid}"})
        if len(satir) != 1:
            print(f"  #{tid}: kupon bulunamadı ya da tek bacaklı değil ({len(satir)} bacak): ATLANDI")
            continue
        b = satir[0]
        mac = maci_bul(execute_query, b["ev"], b["dep"], b["mac_zamani"])
        if mac is None:
            print(f"  #{tid} {b['ev']} - {b['dep']}: maç veritabanında yok: ATLANDI")
            continue
        veri = mac_yukle(execute_query, mac)
        motor = bacak_sonucu(b["bahis_turu"], b["secim"], veri)
        karar = duzeltme_karari(b["sonuc"], motor)
        skor = f"{veri['home']}-{veri['away']}"
        satirmetni = f"  #{tid} {b['ev']} - {b['dep']} ({skor}) | {b['bahis_turu']} / {b['secim']} | admin={b['sonuc']} motor={motor}"
        if karar is None:
            print(satirmetni + " → değişiklik yok")
            continue
        if YAZ:
            K.db_bacak_sonucu(b["kid"], b["bid"], karar, "otomatik")
        print(satirmetni + f" → {karar} {'yazıldı' if YAZ else '(yazılacak)'}")
