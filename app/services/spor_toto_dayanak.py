"""Senaryo metni için maçın Skorjin paketindeki dayanakları (piyasa, açık maç, bizim okuma-piyasa farkı, seri, puan ihtiyacı, kalabalık...).

Metin üreten LLM yalnızca ham istatistik görüyordu; "neden önemli" sorusunun cevabı (oran beklentisi, puan ihtiyacı) dayanaklardadır.
Burası paketten maçı bulur ve LLM'e giden küçük bir listeye damıtır: kartlar (grafik/tablo) gitmez, yalnız başlık, yön, özet cümle ve notlar.
Eşleşme yalnız (maç no + iki takım adı) birlikte tutarsa yapılır; paket yoksa ya da tutmazsa None: üretim bu yüzden asla düşmez.
"""
import re
import unicodedata
from typing import Any, Callable, Dict, List, Optional

EN_COK = 8
# Metinde geçmesi yasak teknik terimler (Skorjin spor_toto_model._BANNED ile uyumlu): model bunları notlardan kopyalar, bu yüzden daha LLM'e gitmeden ayıklanır
_YASAK = re.compile(r"(?i)(?<!\w)(xg|elo|poisson|model\w*|gol beklentisi\w*|pozisyon kalitesi\w*)(?!\w)")
_ONDALIK = re.compile(r"(?<![\d%.,])(\d+)[.,](\d+)(?![\d%])")
_CUMLE = re.compile(r"(?<=[.!?])\s+")
_YON_SIRASI = {"destek": 0, "risk": 1, "bilgi": 2}


# NFKD'nin ayıramadığı harfler (ascii'ye düşürülürken silinirdi: "Bodø/Glimt" -> "bodglimt")
_HARF = str.maketrans({"ø": "o", "æ": "ae", "ß": "ss", "đ": "d", "ł": "l", "œ": "oe", "þ": "th"})
# Ortak kelime eşleşmesinde sayılmayan genel kelimeler (kısaltma/ek); "Paris St Germain" ile "Paris Saint-Germain" kalan kelimelerden eşleşir
_GENEL = {"club", "city", "united", "real", "saint", "st", "fc", "cf", "sc", "ac", "as", "sk", "fk", "cp", "afc", "rc", "rb", "vfb", "bk"}


def _ascii(ad: Optional[str]) -> str:
    s = (ad or "").replace("İ", "i").replace("I", "ı").lower().replace("ı", "i").translate(_HARF)
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()


def _kok(ad: Optional[str]) -> str:
    return "".join(c for c in _ascii(ad) if c.isalnum())


def _kelimeler(ad: Optional[str]) -> List[str]:
    return [k for k in re.split(r"[^a-z0-9]+", _ascii(ad)) if len(k) >= 4 and k not in _GENEL]


def _benzer_kelime(x: str, y: str) -> bool:
    """Aynı kelime ya da tek harf farkı (Sofya/Sofia, Lizbon/Lisbon)."""
    from difflib import SequenceMatcher
    return x == y or SequenceMatcher(None, x, y).ratio() >= 0.8


def _ayni(a: Optional[str], b: Optional[str]) -> bool:
    """Spor Toto adı ile paketteki ad aynı takım mı. Eşleşme her zaman maç no + İKİ takımla birlikte yapılır, bu yüzden ayırt edici
    bir ortak kelime yeter: "Sporting Lizbon"/"Sporting CP", "CSKA Sofya"/"CSKA Sofia", "Paris St Germain"/"Paris Saint-Germain"."""
    x, y = _kok(a), _kok(b)
    if not (x and y):
        return False
    if x == y or x in y or y in x:
        return True
    return any(_benzer_kelime(k, l) for k in _kelimeler(a) for l in _kelimeler(b))


def _temiz(metin: str) -> str:
    """Yasaklı teknik terim geçen cümleleri atar; ham ondalık sayıyı yuvarlar ("2,64" -> "yaklaşık 3"). Hiçbir cümle kalmazsa boş döner."""
    kalan = [c for c in _CUMLE.split(metin.strip()) if c and not _YASAK.search(c)]
    return _ONDALIK.sub(lambda m: f"yaklaşık {int(float(m.group(1) + '.' + m.group(2)) + 0.5)}", " ".join(kalan))


def damit(dayanaklar: List[Dict[str, Any]], en_cok: int = EN_COK) -> List[Dict[str, Any]]:
    """Kartları atar; özeti olanları yön sırasıyla (destek, risk, bilgi) en çok `en_cok` kadar tutar."""
    cikti = []
    for d in dayanaklar or []:
        ozet = _temiz(d.get("ozet") or "")
        if not ozet:
            continue
        notlar = [t for t in (_temiz(n) for n in (d.get("notlar") or []) if isinstance(n, str)) if t][:2]
        cikti.append({"konu": d.get("baslik") or d.get("id"), "yon": d.get("yon"), "ozet": ozet, "notlar": notlar})
    cikti.sort(key=lambda x: _YON_SIRASI.get(x["yon"], 3))
    return cikti[:en_cok]


def mac_dayanaklari(paket: Optional[Dict[str, Any]], mac_no: int, ev: str, dep: str) -> Optional[List[Dict[str, Any]]]:
    for m in ((paket or {}).get("maclar") or []):
        if m.get("no") == mac_no and _ayni(m.get("ev"), ev) and _ayni(m.get("dep"), dep):
            return damit(m.get("dayanaklar") or []) or None
    return None


def getir(match_id: int, sorgu: Callable[[str, Dict[str, Any]], List[Dict[str, Any]]], paket_oku: Callable[[], Optional[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
    """Maç satırından (mac_no, takımlar) en yeni paketteki dayanaklar; hata ya da eşleşmeme None."""
    try:
        satir = sorgu("SELECT mac_no, ev_takim, deplasman_takim FROM greydb.spor_toto_matches WHERE id = :id", {"id": match_id})
        if not satir:
            return None
        paket = paket_oku()
        paket = paket["paket"] if paket and "paket" in paket else paket
        if isinstance(paket, str):
            import json
            paket = json.loads(paket)
        return mac_dayanaklari(paket, int(satir[0]["mac_no"]), satir[0]["ev_takim"], satir[0]["deplasman_takim"])
    except Exception:  # noqa: BLE001 — dayanak eksikliği metin üretimini düşürmez
        return None
