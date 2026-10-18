"""iddaa maç kimliğini (iddaa_event_id) kendi maç kaydımıza (fotmob_match_id) bağlar.

iddaa listesi yaklaşık 3 günlük pencere verir ve takım adlarını farklı yazar ("Bayern Münih", "Sporting Lizbon", "Tottenham"),
bu yüzden eşleştirme: ad benzerliği (ev VE deplasman) + başlama saati (±30 dk). Belirsizse yanlış bağlamak yerine atlar.
"""
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Dict, List

TOLERANS_SN = 1800
BENZERLIK_ESIGI = 0.76

_HARF = str.maketrans({"ø": "o", "Ø": "O", "æ": "ae", "Æ": "AE", "ð": "d", "đ": "d", "ł": "l", "Ł": "L", "ı": "i", "ß": "ss"})
_DOLGU = {"fc", "cf", "afc", "fk", "ac", "as", "ss", "us", "the", "calcio", "sk", "if", "bk", "ud", "cd", "rcd", "sv", "vfb", "vfl", "tsg", "hotspur", "moscow", "rc", "eindhoven", "kaliningrad", "montero"}
# iddaa'nın Türkçe/farklı yazımları → bizim yazım
_ESLEME = {"munih": "munchen", "muenchen": "munchen", "lizbon": "cp", "prag": "prague", "milano": "milan", "internazionale": "inter"}
# Kadın ve genç takım işaretleri: erkek A takımıyla karışmamalı
_ISARET = {"u15", "u16", "u17", "u18", "u19", "u20", "u21", "u23", "k", "w", "women", "ii", "b", "reserves", "youth"}


def normalize_takim(ad: str) -> str:
    s = unicodedata.normalize("NFKD", (ad or "").translate(_HARF)).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s.replace("-", " "))
    jetonlar = [_ESLEME.get(t, t) for t in s.split() if t not in _DOLGU and not t.isdigit()]
    return " ".join(jetonlar)


def _jetonlar(ad: str):
    j = normalize_takim(ad).split()
    # Baştaki tek harf ("B. Dortmund") kısaltmadır, ikinci takım işareti değil; yalnızca sondaki "B" işarettir
    isaret = lambda k, t: t in _ISARET and not (t == "b" and k < len(j) - 1)
    return [t for k, t in enumerate(j) if not isaret(k, t)], {t for k, t in enumerate(j) if isaret(k, t)}


def takim_benzer(a: str, b: str) -> bool:
    (ta, ia), (tb, ib) = _jetonlar(a), _jetonlar(b)
    if ia != ib or not ta or not tb:       # kadın/genç işareti farklıysa farklı takım
        return False
    # Kısaltma: tek harfli jeton ("B." Dortmund) diğer addaki aynı harfle başlayan jetonu temsil eder
    def ac(x, y):
        return [next((t for t in y if len(t) > 1 and t[0] == j), j) if len(j) == 1 else j for j in x]
    ta, tb = ac(ta, tb), ac(tb, ta)
    sa, sb = " ".join(ta), " ".join(tb)
    if sa == sb:
        return True
    fa, fb = [t for t in ta if t not in tb], [t for t in tb if t not in ta]
    if fa and fb:
        # İki adda da karşılıksız kelime var: hepsi birbirine benzemeli ("City" ile "United" benzemez, "Sasuolo" ile "Sassuolo" benzer)
        def yakin(x, liste):
            return any(SequenceMatcher(None, x, y).ratio() >= 0.8 for y in liste)
        return all(yakin(t, fb) for t in fa) and all(yakin(t, fa) for t in fb)
    # Biri ötekinin alt kümesi ("Leipzig" / "RB Leipzig"): toplam benzerlik eşiği geçmeli ("Arsenal" / "Arsenal Tula" geçmez)
    return SequenceMatcher(None, sa, sb).ratio() >= BENZERLIK_ESIGI


def eslestir(iddaa_olaylari: List[Dict[str, Any]], maclar: List[Dict[str, Any]], tolerans_sn: int = TOLERANS_SN) -> Dict[str, Any]:
    """
    iddaa_olaylari: i, hn, an, d (saniye, UTC). maclar: fotmob_match_id, home, away, kickoff_ts (saniye, UTC).
    Dönen: eslesen {fotmob_match_id: iddaa_i}, belirsiz [mac], eslesmeyen [mac].
    Bir iddaa olayı birden çok maça aday ise ya da bir maçın birden çok adayı varsa hiçbiri yazılmaz.
    """
    # Aynı maç program ve canlı listede iki ayrı 'i' ile gelir ama 'bri' aynıdır: tek olay say, canlı listedeki (büyük) kimliği al
    tekil: Dict[Any, Dict[str, Any]] = {}
    for e in iddaa_olaylari:
        k = e.get("bri") or ("i", e["i"])
        if k not in tekil or e["i"] > tekil[k]["i"]:
            tekil[k] = e
    iddaa_olaylari = list(tekil.values())
    adaylar: Dict[Any, List[Any]] = {}
    for m in maclar:
        adaylar[m["fotmob_match_id"]] = [
            e["i"] for e in iddaa_olaylari
            if abs(e["d"] - m["kickoff_ts"]) <= tolerans_sn and takim_benzer(m["home"], e["hn"]) and takim_benzer(m["away"], e["an"])
        ]
    sahip: Dict[Any, List[Any]] = {}
    for fid, liste in adaylar.items():
        for i in liste:
            sahip.setdefault(i, []).append(fid)
    eslesen, belirsiz, eslesmeyen = {}, [], []
    for m in maclar:
        liste = adaylar[m["fotmob_match_id"]]
        if not liste:
            eslesmeyen.append(m)
        elif len(liste) > 1 or len(sahip[liste[0]]) > 1:
            belirsiz.append(m)
        else:
            eslesen[m["fotmob_match_id"]] = liste[0]
    return {"eslesen": eslesen, "belirsiz": belirsiz, "eslesmeyen": eslesmeyen}
