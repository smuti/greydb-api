"""Spor Toto ekonomik kupon ve tek atış yayını: tahmin doğrulama ve kolon sayısı (saf mantık, veritabanı yok).

Bir maçta her kişinin (Fuat, Skorjin) dört alanı vardır; hepsi yayından önce admin'de girilir:
  once    : önceki yayında söylediği (1, 1-0, 1-0-2 ...)
  nihai   : ekonomik yorumdan sonraki karar; boşsa değiştirmiyor (= once)
  daralma : kupon daraltma sonrası; boşsa daraltmıyor (= nihai). nihainin alt kümesi olmalı
  tek     : son tek atış; tek işaret (1, 0 ya da 2)
İşaretler her zaman 1, 0, 2 sırasında normalleşir ("0 1" → "1-0")."""
import re
from typing import Any, Dict, Iterable, List, Optional

SIRA = "102"
KISILER = ("fuat", "skorjin")
ALANLAR = ("once", "nihai", "daralma", "tek")


def secenek(s: Any) -> str:
    """'0 1', '10', '1,0' → '1-0'. Boş → ''. 1/0/2 dışı karakter ya da tekrar hatası."""
    if s is None:
        return ""
    metin = str(s)
    if re.search(r"[^102\s,\-/.]", metin):
        raise ValueError("yalnızca 1, 0, 2 girilebilir")
    harfler = re.findall(r"[102]", metin)
    if len(harfler) != len(set(harfler)):
        raise ValueError("aynı işaret iki kez girilemez")
    return "-".join(c for c in SIRA if c in harfler)


def _kume(s: str) -> set:
    return set(s.split("-")) if s else set()


def gecerli(t: Dict[str, Any]) -> str:
    """Kuponda geçerli olan son değer: daralma, yoksa nihai, yoksa önceki."""
    return t.get("daralma") or t.get("nihai") or t.get("once") or ""


def tahmin_dogrula(t: Dict[str, Any]) -> Dict[str, str]:
    d = {a: secenek((t or {}).get(a)) for a in ALANLAR}
    if d["tek"] and "-" in d["tek"]:
        raise ValueError("tek atış tek işaret olmalı")
    if d["daralma"] and not _kume(d["daralma"]) <= _kume(d["nihai"] or d["once"]):
        raise ValueError("daralma, nihai tahminin alt kümesi olmalı")
    return d


def tahminleri_dogrula(liste: Iterable[Dict[str, Any]], izinli_siralar: List[int]) -> List[Dict[str, Any]]:
    sonuc, gorulen = [], set()
    for t in liste:
        sira = t.get("sira")
        if sira not in izinli_siralar:
            raise ValueError(f"geçersiz maç sira: {sira}")
        if sira in gorulen:
            raise ValueError(f"maç tekrar etmiş: {sira}")
        gorulen.add(sira)
        satir: Dict[str, Any] = {"sira": sira}
        for kisi in KISILER:
            try:
                satir[kisi] = tahmin_dogrula(t.get(kisi) or {})
            except ValueError as e:
                raise ValueError(f"{sira}. maç {kisi}: {e}") from e
        sonuc.append(satir)
    return sorted(sonuc, key=lambda x: x["sira"])


def kolon_sayisi(degerler: Iterable[str]) -> Optional[int]:
    """Seçenek sayılarının çarpımı: '1'=1, '1-0'=2, '1-0-2'=3. Boş değer ya da boş liste → None."""
    liste = list(degerler)
    if not liste or any(not v for v in liste):
        return None
    toplam = 1
    for v in liste:
        toplam *= len(v.split("-"))
    return toplam
