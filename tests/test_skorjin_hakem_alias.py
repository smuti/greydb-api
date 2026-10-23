"""Hakem alias tablosu (greydb.referee_alias, issue #34): otomatik planlama, elle düzeltmeye saygı, çözümleme, senkron. Veritabanı yok."""
from datetime import datetime

import pytest

from app.services import skorjin_hakem as h
from app.services import skorjin_prematch as sp
from app.services.skorjin_prematch import Kaynaklar, hazirla

ADAYLAR = [("Cláudio Pereira", "Portugal"), ("Cláudio Filipe Ruivo Pereira", "Portugal"), ("Geoffrey Kubler", "France"),
           ("Yucel Buyuk", "Türkiye"), ("Yücel Büyük", "Türkiye"), ("Miguel Ortiz", "Spain"), ("Miguel Ángel Chua Ortiz", "Mexico"),
           ("Bruno Costa", "Portugal"), ("Bruno Pires Costa", "Portugal"), ("Bruno Miguel Ribeiro Costa", "Portugal"),
           ("Bruno José Ribeiro Pires Costa", "Portugal")]
SAYILAR = {"Cláudio Pereira": 1, "Cláudio Filipe Ruivo Pereira": 69, "Yucel Buyuk": 1, "Yücel Büyük": 5}


def satirlar(plan):
    return {(r["variant"], r["country"]): r for r in plan}


# ---------------------------------------------------------------- plan
def test_kisa_ad_uzun_adin_takma_adi_olur_uzun_ad_kendine_isaret_eder():
    p = satirlar(h.alias_plani(ADAYLAR, SAYILAR))
    assert p[("Cláudio Pereira", "Portugal")] == {"variant": "Cláudio Pereira", "country": "Portugal", "canonical": "Cláudio Filipe Ruivo Pereira", "status": "onayli"}
    assert p[("Cláudio Filipe Ruivo Pereira", "Portugal")]["canonical"] == "Cláudio Filipe Ruivo Pereira"


def test_ayni_uzunlukta_yazimlardan_cok_maci_olan_kanonik_olur_esitlikte_alfabetik():
    p = satirlar(h.alias_plani(ADAYLAR, SAYILAR))
    assert p[("Yucel Buyuk", "Türkiye")]["canonical"] == "Yücel Büyük" == p[("Yücel Büyük", "Türkiye")]["canonical"]
    aksan = satirlar(h.alias_plani([("Yucel Buyuk", "Türkiye"), ("Yücel Büyük", "Türkiye")], {}))              # eşit sayıda: aksanlı yazım kanonik
    assert aksan[("Yucel Buyuk", "Türkiye")]["canonical"] == "Yücel Büyük"
    esit = satirlar(h.alias_plani([("A B", ""), ("a b", "")], {}))
    assert esit[("A B", "")]["canonical"] == esit[("a b", "")]["canonical"] == "A B"


def test_tek_basina_ve_belirsizlik_olmayan_ad_icin_satir_yazilmaz_farkli_ulke_birlesmez():
    p = satirlar(h.alias_plani(ADAYLAR, SAYILAR))
    assert ("Geoffrey Kubler", "France") not in p and ("Miguel Ortiz", "Spain") not in p and ("Miguel Ángel Chua Ortiz", "Mexico") not in p


def test_belirsiz_kisa_ad_inceleme_olarak_isaretlenir_ve_kendine_isaret_eder():
    p = satirlar(h.alias_plani(ADAYLAR, SAYILAR))
    assert p[("Bruno Costa", "Portugal")] == {"variant": "Bruno Costa", "country": "Portugal", "canonical": "Bruno Costa", "status": "inceleme"}
    assert p[("Bruno Pires Costa", "Portugal")]["canonical"] == "Bruno José Ribeiro Pires Costa" and p[("Bruno Pires Costa", "Portugal")]["status"] == "onayli"


def test_plan_deterministik_ve_girdiyi_degistirmez():
    kopya = list(ADAYLAR)
    anahtar = lambda r: (r["variant"], r["country"])                            # noqa: E731
    assert sorted(h.alias_plani(ADAYLAR, SAYILAR), key=anahtar) == sorted(h.alias_plani(list(reversed(ADAYLAR)), SAYILAR), key=anahtar)
    assert ADAYLAR == kopya


# ---------------------------------------------------------------- çözümleme
def alias_satiri(variant, canonical, ulke="Portugal", status="onayli", source="auto"):
    return {"variant": variant, "country": ulke, "canonical": canonical, "status": status, "source": source}


def test_cozumle_kanonik_grubun_tum_yazimlarini_dondurur():
    tablo = [alias_satiri("Cláudio Pereira", "Cláudio Filipe Ruivo Pereira"), alias_satiri("Cláudio Filipe Ruivo Pereira", "Cláudio Filipe Ruivo Pereira"),
             alias_satiri("Claudio Pereira", "Cláudio Filipe Ruivo Pereira")]
    adlar = h.alias_cozumle("Cláudio Pereira", "Portugal", tablo, ADAYLAR)
    assert set(adlar) == {"Cláudio Pereira", "Cláudio Filipe Ruivo Pereira", "Claudio Pereira"}


def test_inceleme_satiri_birlestirmez_elle_satir_inceleme_olsa_da_gecerlidir():
    inc = [alias_satiri("Bruno Costa", "Bruno Costa", status="inceleme")]
    assert h.alias_cozumle("Bruno Costa", "Portugal", inc, ADAYLAR) == ["Bruno Costa"]
    manuel = [alias_satiri("Bruno Costa", "Bruno Pires Costa", status="inceleme", source="manual"), alias_satiri("Bruno Pires Costa", "Bruno Pires Costa")]
    assert set(h.alias_cozumle("Bruno Costa", "Portugal", manuel, ADAYLAR)) == {"Bruno Costa", "Bruno Pires Costa"}


def test_tabloda_yoksa_kural_ile_ada_dusulur_ve_ulke_kontrolu_surer():
    assert set(h.alias_cozumle("Cláudio Pereira", "Portugal", [], ADAYLAR)) == {"Cláudio Pereira", "Cláudio Filipe Ruivo Pereira"}
    assert h.alias_cozumle("Hiç Yok", "Italy", [], ADAYLAR) == ["Hiç Yok"]
    ayri = [alias_satiri("Cláudio Pereira", "Cláudio Filipe Ruivo Pereira", ulke="Brazil")]
    assert "Cláudio Filipe Ruivo Pereira" in h.alias_cozumle("Cláudio Pereira", "Portugal", ayri, ADAYLAR)           # Brazil satırı Portekizli için geçerli değil: kural devrede


# ---------------------------------------------------------------- tablo ve yazma
def test_ddl_ve_upsert_elle_satirlari_ezmez():
    ddl = h.ALIAS_DDL
    for parca in ("greydb.referee_alias", "variant TEXT NOT NULL", "canonical TEXT NOT NULL", "source TEXT NOT NULL DEFAULT 'auto'",
                  "status TEXT NOT NULL DEFAULT 'onayli'", "PRIMARY KEY (variant, country)"):
        assert parca in ddl
    assert "ON CONFLICT (variant, country) DO UPDATE" in h.ALIAS_UPSERT_SQL and "WHERE greydb.referee_alias.source = 'auto'" in h.ALIAS_UPSERT_SQL


def test_senkron_kuru_calismada_yazmaz_ama_raporlar(monkeypatch):
    yazilan = []
    monkeypatch.setattr(h, "_sorgu", lambda sql, p: [{"ad": a, "ulke": u, "n": SAYILAR.get(a, 1)} for a, u in ADAYLAR] if sql == h.ADLAR_SQL else [])
    monkeypatch.setattr(h, "_yaz", lambda sql, p: yazilan.append(p))
    r = h.alias_senkronla(dry_run=True)
    assert yazilan == [] and r["yazilan"] == 0 and r["plan"] > 0 and "Bruno Costa" in r["inceleme"]


def test_senkron_yazar_elle_satirlara_dokunmaz_ve_degismeyeni_yazmaz(monkeypatch):
    mevcut = [alias_satiri("Cláudio Pereira", "Cláudio Filipe Ruivo Pereira"),                       # aynı: yazılmaz
              alias_satiri("Yucel Buyuk", "Başka Ad", ulke="Türkiye", source="manual")]                # elle: ezilmez
    yazilan = []

    def sorgu(sql, p):
        if sql == h.ADLAR_SQL:
            return [{"ad": a, "ulke": u, "n": SAYILAR.get(a, 1)} for a, u in ADAYLAR]
        return mevcut
    monkeypatch.setattr(h, "_sorgu", sorgu)
    monkeypatch.setattr(h, "_yaz", lambda sql, p: yazilan.append((p["variant"], p["country"])))
    r = h.alias_senkronla(dry_run=False)
    assert ("Cláudio Pereira", "Portugal") not in yazilan and ("Yucel Buyuk", "Türkiye") not in yazilan
    assert ("Bruno Pires Costa", "Portugal") in yazilan and r["yazilan"] == len(yazilan) > 0


def test_senkron_tablo_yoksa_bos_sayar(monkeypatch):
    def sorgu(sql, p):
        if sql == h.ADLAR_SQL:
            return [{"ad": a, "ulke": u, "n": 1} for a, u in ADAYLAR]
        raise RuntimeError("relation greydb.referee_alias does not exist")
    monkeypatch.setattr(h, "_sorgu", sorgu)
    yazilan = []
    monkeypatch.setattr(h, "_yaz", lambda sql, p: yazilan.append(p))
    assert h.alias_senkronla(dry_run=True)["plan"] > 0


# ---------------------------------------------------------------- hakem kaynağı alias tablosunu kullanır
def test_hakem_kaynagi_alias_tablosundan_cozer(monkeypatch):
    sorgulanan = []

    def sorgu(sql, p):
        if sql == h.ADLAR_SQL:
            return [{"ad": a, "ulke": u, "n": 1} for a, u in ADAYLAR]
        if sql == h.ALIAS_OKU_SQL:
            return [alias_satiri("Cláudio Pereira", "Cláudio Filipe Ruivo Pereira"), alias_satiri("Cláudio Filipe Ruivo Pereira", "Cláudio Filipe Ruivo Pereira"),
                    alias_satiri("Claudio Pereira", "Cláudio Filipe Ruivo Pereira")]
        sorgulanan.append(dict(p))
        return [{"n": 70, "sari": 3.0, "kirmizi": 0.1, "faul": 20.0, "gol": 2.5, "ev_galibiyet": 0.4, "penalti": 0.2}]
    monkeypatch.setattr(h, "_sorgu", sorgu)
    h.hakem_kaynagi()("Cláudio Pereira", "Portugal", 20, datetime(2026, 10, 25))
    assert set(sorgulanan[0]["adlar"]) == {"Cláudio Pereira", "Cláudio Filipe Ruivo Pereira", "Claudio Pereira"}


def test_hakem_kaynagi_alias_tablosu_okunamazsa_kurala_duser(monkeypatch):
    def sorgu(sql, p):
        if sql == h.ADLAR_SQL:
            return [{"ad": a, "ulke": u, "n": 1} for a, u in ADAYLAR]
        if sql == h.ALIAS_OKU_SQL:
            raise RuntimeError("tablo yok")
        return [{"n": 70, "sari": 3.0, "kirmizi": 0.1, "faul": 20.0, "gol": 2.5, "ev_galibiyet": 0.4, "penalti": 0.2}]
    monkeypatch.setattr(h, "_sorgu", sorgu)
    assert h.hakem_kaynagi()("Cláudio Pereira", "Portugal", 20, datetime(2026, 10, 25))[0]["n"] == 70


# ---------------------------------------------------------------- orkestrasyon
SIMDI = datetime(2026, 10, 22, 12, 0)
MAC = {"fotmob_match_id": 1001, "iddaa_event_id": None, "home_team_id": 1, "away_team_id": 2, "home_name": "A", "away_name": "B",
       "league_id": 4, "league_name": "Süper Lig", "match_date": datetime(2026, 10, 25, 17, 0)}


def kur(alias):
    return Kaynaklar(maclar=lambda b, s: [MAC], form=lambda t, d: [], h2h=lambda a, b, d: [], model=lambda ms: {}, iddaa=lambda e: (None, {}, {}),
                     mevcut=lambda ids: set(), yaz=lambda k, s: None, fotmob=lambda f: None, alias=alias)


def test_gunluk_koşu_alias_senkronunu_dry_run_bayragiyla_cagirir_ve_rapora_yazar():
    cagrilar = []
    r = hazirla("gunluk", True, kur(lambda dry: cagrilar.append(dry) or {"plan": 5, "yazilan": 0, "inceleme": ["Bruno Costa"]}), SIMDI, "harman-v1")
    assert cagrilar == [True] and r["hakem_alias"] == {"plan": 5, "yazilan": 0, "inceleme": ["Bruno Costa"]}
    cagrilar.clear()
    hazirla("gunluk", False, kur(lambda dry: cagrilar.append(dry) or {}), SIMDI, "harman-v1")
    assert cagrilar == [False]


def test_alias_hatasi_isi_durdurmaz_ve_oran_kapsami_cagirmaz():
    def patlar(dry):
        raise RuntimeError("db")
    r = hazirla("gunluk", False, kur(patlar), SIMDI, "harman-v1")
    assert r["hazirlanan"] == 1 and any("alias" in x["hata"] for x in r["hatalar"])
    cagrilar = []
    hazirla("oran", False, Kaynaklar(**{**kur(lambda d: cagrilar.append(d)).__dict__, "mevcut": lambda ids: {1001}}), SIMDI, "harman-v1")
    assert cagrilar == []
