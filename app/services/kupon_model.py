"""Kupon modeli (saf fonksiyonlar): girdi doğrulama, oran hesabı, sonuç özeti, üyelik seviyesine göre kilit.

Seviyeler birikimli: Elit = Açık + Bayi + Elit görür; Bayi = Açık + Bayi; Açık/giriş yok = yalnız Açık.
Kilitli kuponda maç bilgisi (takımlar, lig, saat) ve kuponun tuttu/yattı sonucu görünür; bahis türü, seçim, oran, neden, ana risk, yorum ve
bacak sonuçları gizlenir (bacak sonucu seçimi ele verirdi).
"""
import copy
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.services.kupon_hesap import en_yuksek_odeme, kombinasyon_sayisi, kupon_sonucu, toplam_oran

SEVIYELER = ("acik", "bayi", "elit")
DURUMLAR = ("taslak", "yayinda", "arsiv")     # arsiv: yayından kaldırıldı (herkesten gizli); sonuçlanmış kuponlar yayinda kalır, Sonuçlar'da görünür
ZAMANLAMALAR = ("pre", "live")
MAX_BACAK, MAX_NEDEN = 20, 10


class KuponHatasi(ValueError):
    def __init__(self, alan: str, mesaj: str):
        super().__init__(mesaj)
        self.alan, self.mesaj = alan, mesaj


def _metin(v: Any, alan: str, zorunlu: bool = True, azami: int = 200) -> Optional[str]:
    if v is None or (isinstance(v, str) and not v.strip()):
        if zorunlu:
            raise KuponHatasi(alan, "boş olamaz")
        return None
    if not isinstance(v, str):
        raise KuponHatasi(alan, "metin olmalı")
    v = v.strip()
    if len(v) > azami:
        raise KuponHatasi(alan, f"en fazla {azami} karakter")
    return v


def _zaman(v: Any, alan: str) -> str:
    try:
        s = str(v)
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return s
    except Exception:
        raise KuponHatasi(alan, "geçerli bir tarih-saat olmalı (ISO)")


def _bacak(b: Dict[str, Any], i: int) -> Dict[str, Any]:
    p = f"bacaklar[{i}]"
    oran = b.get("oran")
    if isinstance(oran, bool) or not isinstance(oran, (int, float)) or not (0 < oran <= 1000):
        raise KuponHatasi(f"{p}.oran", "0 ile 1000 arasında bir sayı olmalı")
    return {
        "iddaa_event_id": b.get("iddaa_event_id"), "fotmob_match_id": b.get("fotmob_match_id"),
        "ev": _metin(b.get("ev"), f"{p}.ev"), "dep": _metin(b.get("dep"), f"{p}.dep"), "lig": _metin(b.get("lig"), f"{p}.lig", zorunlu=False),
        "mac_zamani": _zaman(b.get("mac_zamani"), f"{p}.mac_zamani"),
        "bahis_turu": _metin(b.get("bahis_turu"), f"{p}.bahis_turu"), "secim": _metin(b.get("secim"), f"{p}.secim"), "oran": round(float(oran), 2),
        "sonuc": b.get("sonuc") if b.get("sonuc") in ("pending", "won", "lost", "void") else "pending",
    }


def dogrula_olustur(g: Dict[str, Any]) -> Dict[str, Any]:
    """İstemci girdisini doğrular, temizler ve hesaplanan alanları (oran, kombinasyon) sunucuda ekler. Hata: KuponHatasi(alan, mesaj)."""
    if g.get("seviye") not in SEVIYELER:
        raise KuponHatasi("seviye", f"{', '.join(SEVIYELER)} olmalı")
    durum = g.get("durum", "taslak")
    if durum not in DURUMLAR:
        raise KuponHatasi("durum", f"{', '.join(DURUMLAR)} olmalı")
    zaman = g.get("zamanlama", "pre")
    if zaman not in ZAMANLAMALAR:
        raise KuponHatasi("zamanlama", "pre ya da live olmalı")
    ham = g.get("bacaklar")
    if not isinstance(ham, list) or not (1 <= len(ham) <= MAX_BACAK):
        raise KuponHatasi("bacaklar", f"1 ile {MAX_BACAK} arasında bacak olmalı")
    bacaklar = [_bacak(b, i) for i, b in enumerate(ham)]
    anahtarlar = [("id", b["iddaa_event_id"]) if b["iddaa_event_id"] else ("ad", b["ev"].lower(), b["dep"].lower(), b["mac_zamani"]) for b in bacaklar]
    if len(set(anahtarlar)) != len(anahtarlar):
        raise KuponHatasi("bacaklar", "Aynı maçtan birden fazla bacak eklenemez")
    k = g.get("sistem_k")
    if k is not None and (isinstance(k, bool) or not isinstance(k, int) or not (2 <= k <= len(bacaklar) - 1)):
        raise KuponHatasi("sistem_k", "2 ile bacak sayısı-1 arasında olmalı (kombine için boş bırakın)")
    ham_nedenler = g.get("nedenler", [])
    if not isinstance(ham_nedenler, list) or any(not isinstance(x, str) for x in ham_nedenler):
        raise KuponHatasi("nedenler", "madde listesi (her madde metin) olmalı")
    nedenler = [_metin(x, "nedenler", azami=300) for x in ham_nedenler if x.strip()]
    if len(nedenler) > MAX_NEDEN:
        raise KuponHatasi("nedenler", f"en fazla {MAX_NEDEN} madde")
    return {
        "seviye": g["seviye"], "durum": durum, "zamanlama": zaman, "one_cikan": bool(g.get("one_cikan", False)),
        "sistem_k": k, "nedenler": nedenler, "ana_risk": _metin(g.get("ana_risk"), "ana_risk", zorunlu=False, azami=500),
        "yorum": _metin(g.get("yorum"), "yorum", zorunlu=False, azami=4000), "sinyal": g.get("sinyal"), "risk_seviyesi": g.get("risk_seviyesi"),
        "bacaklar": bacaklar,
        "toplam_oran": toplam_oran([{"odds": b["oran"]} for b in bacaklar]) if k is None else None,
        "en_yuksek_odeme": en_yuksek_odeme([{"odds": b["oran"]} for b in bacaklar], k),
        "kombinasyon_sayisi": 1 if k is None else kombinasyon_sayisi(len(bacaklar), k),
    }


def kupon_ozeti(bacaklar: List[Dict[str, Any]], sistem_k: Optional[int]) -> Dict[str, Any]:
    """Bacak sonuçlarından kupon sonucu ve ödeme (kupon_hesap.kupon_sonucu'nun bacak şemasına uyarlaması)."""
    return kupon_sonucu([{"odds": b["oran"], "result": b["sonuc"]} for b in bacaklar], sistem_k)


def seviye_yeter(kupon_seviyesi: str, uye_seviyesi: Optional[str]) -> bool:
    return SEVIYELER.index(uye_seviyesi if uye_seviyesi in SEVIYELER else "acik") >= SEVIYELER.index(kupon_seviyesi)


def gorunur_mu(kupon: Dict[str, Any], uye_seviyesi: Optional[str]) -> bool:
    return kupon.get("durum") == "yayinda"


_GIZLI_KUPON = ("nedenler", "ana_risk", "yorum", "toplam_oran", "en_yuksek_odeme", "odeme", "sinyal", "risk_seviyesi")
_GIZLI_BACAK = ("bahis_turu", "secim", "oran", "sonuc", "iddaa_event_id", "fotmob_match_id")


def kilitle(kupon: Dict[str, Any], uye_seviyesi: Optional[str]) -> Dict[str, Any]:
    """Üyenin seviyesine göre kuponun görünümü. Girdiyi değiştirmez."""
    k = copy.deepcopy(kupon)
    k["bacak_sayisi"] = len(k.get("bacaklar", []))
    if seviye_yeter(k["seviye"], uye_seviyesi):
        k["kilitli"] = False
        return k
    k["kilitli"] = True
    for a in _GIZLI_KUPON:
        k[a] = None
    for b in k.get("bacaklar", []):
        for a in _GIZLI_BACAK:
            b[a] = None
    return k
