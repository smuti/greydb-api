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


def _kok(ad: Optional[str]) -> str:
    s = unicodedata.normalize("NFKD", (ad or "").replace("İ", "i").replace("I", "ı").lower().replace("ı", "i")).encode("ascii", "ignore").decode()
    return "".join(c for c in s if c.isalnum())


def _ayni(a: Optional[str], b: Optional[str]) -> bool:
    x, y = _kok(a), _kok(b)
    return bool(x and y and (x == y or x in y or y in x))


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
