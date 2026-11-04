"""Skorjin'in LLM uçlarına (match-analysis, spor-toto-scenarios) sunucudan sunucuya çağrı başlıkları.

Skorjin `SKORJIN_LLM_ANAHTAR_ZORUNLU=1` olunca `X-Sunucu-Anahtari` ister. Anahtar `SKORJIN_SERVER_KEY` ortam değişkeninden gelir
(Skorjin ve web ile aynı değer). Tanımlı değilse başlık gönderilmez (geçiş dönemi: Skorjin izleme kipinde).
"""
import os
from typing import Dict


def skorjin_basliklari() -> Dict[str, str]:
    anahtar = os.environ.get("SKORJIN_SERVER_KEY") or ""
    return {"X-Sunucu-Anahtari": anahtar} if anahtar else {}
