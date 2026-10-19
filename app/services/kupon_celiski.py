"""Admin'in elle girdiği bacak sonucu ile maç verisinden hesaplanan sonuç çeliştiğinde düzeltme kararı (saf)."""
from typing import Optional

KESIN = ("won", "lost", "void")


def duzeltme_karari(mevcut: Optional[str], motor: Optional[str]) -> Optional[str]:
    """Motorun sonucunu döndürür ya da None (dokunma). Yalnızca sonuçlanmış bir bacakta, motor kesin ve farklı bir sonuç verdiyse düzeltilir."""
    if mevcut not in KESIN or motor not in KESIN:
        return None
    return motor if motor != mevcut else None


def silinebilir_mi(kaynak: Optional[str]) -> bool:
    """Toplu silme betiği yalnızca eski tahminlerden taşınan kuponlara dokunur; admin'in elle oluşturduğu (kaynağı olmayan) kupon asla silinmez."""
    return bool(kaynak) and kaynak.startswith("tahmin:")
