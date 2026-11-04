"""Skorjin LLM uçlarına giden çağrılar sunucu anahtarını başlıkta taşır (ağ yok)."""
import pytest

from app.services import skorjin_match_analysis as sma
from app.services import spor_toto_fetch as stf
from app.services.skorjin_anahtar import skorjin_basliklari


class Yanit:
    def __init__(self, govde): self.status_code, self._g = 200, govde
    def json(self): return self._g


class Istemci:
    cagri = []
    govde = None
    def __init__(self, *a, **k): pass
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def post(self, url, json=None, timeout=None, headers=None):
        Istemci.cagri.append((url, headers))
        return Yanit(Istemci.govde)


@pytest.fixture(autouse=True)
def sahte(monkeypatch):
    Istemci.cagri = []
    monkeypatch.setattr(sma.httpx, "Client", Istemci)
    monkeypatch.setattr(stf.httpx, "Client", Istemci)


def test_basliklar_anahtar_varsa_tasir_yoksa_bos(monkeypatch):
    monkeypatch.setenv("SKORJIN_SERVER_KEY", "gizli")
    assert skorjin_basliklari() == {"X-Sunucu-Anahtari": "gizli"}
    monkeypatch.delenv("SKORJIN_SERVER_KEY")
    assert skorjin_basliklari() == {}


def test_match_analysis_cagrisi_anahtar_gonderir(monkeypatch):
    monkeypatch.setenv("SKORJIN_SERVER_KEY", "gizli")
    Istemci.govde = {"analysis": "### Analiz"}
    assert sma.generate_analysis({"a": 1}) == "### Analiz"
    url, basliklar = Istemci.cagri[0]
    assert url.endswith("/api/v1/match-analysis") and basliklar == {"X-Sunucu-Anahtari": "gizli"}


def test_spor_toto_cagrisi_anahtar_gonderir(monkeypatch):
    monkeypatch.setenv("SKORJIN_SERVER_KEY", "gizli")
    Istemci.govde = {"scenarios": []}
    try:
        stf.generate_scenarios({"a": 1})
    except Exception:
        pass                                   # yanıt doğrulaması bu testin konusu değil; çağrının başlığı
    url, basliklar = Istemci.cagri[0]
    assert url.endswith("/api/v1/spor-toto-scenarios") and basliklar == {"X-Sunucu-Anahtari": "gizli"}
