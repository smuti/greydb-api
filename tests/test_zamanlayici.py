"""Uygulama içi zamanlayıcı (GitHub Actions cron'unun gecikmesi yerine): ZAMANLAYICI=1 ise açılır. Saatler UTC.
prematch günlük 03:30 · prematch oran 05:00–22:30 her 30 dk · iddaa eşleştir 4 saatte bir (:20)."""
import pytest

from app.services import zamanlayici as Z


def test_is_tanimlari():
    t = {ad: kw for ad, kw, _ in Z.isler()}
    assert t == {"prematch-gunluk": dict(hour="3", minute="30"), "prematch-oran": dict(hour="5-22", minute="0,30"), "iddaa-eslestir": dict(hour="*/4", minute="20")}


def test_kapali_varsayilan(monkeypatch):
    monkeypatch.delenv("ZAMANLAYICI", raising=False)
    assert Z.baslat(calistir=False) is None


def test_acik_uc_is(monkeypatch):
    monkeypatch.setenv("ZAMANLAYICI", "1")
    s = Z.baslat(calistir=False)
    try:
        assert sorted(j.id for j in s.get_jobs()) == ["iddaa-eslestir", "prematch-gunluk", "prematch-oran"]
        j = s.get_job("prematch-oran")
        assert j.max_instances == 1 and j.coalesce and str(j.trigger.timezone) == "UTC"
    finally:
        Z.durdur(s)


def test_zamanli_prematch_yazan_kosu(monkeypatch):
    import app.routers.skorjin_prematch as r
    r._durum_sifirla()
    cagri = []
    monkeypatch.setattr(r, "_kos", lambda kapsam, dry_run: cagri.append((kapsam, dry_run)) or {"aday": 0})
    assert r.zamanli_prematch("oran") == "bitti"
    assert cagri == [("oran", False)] and r._durum["durum"] == "bitti"


def test_zamanli_prematch_suren_varsa_atlar(monkeypatch):
    import app.routers.skorjin_prematch as r
    r._durum_sifirla()
    r._durum.update(durum="calisiyor")
    monkeypatch.setattr(r, "_kos", lambda *a: pytest.fail("çalışmamalı"))
    assert r.zamanli_prematch("gunluk") == "atlandi"
    r._durum_sifirla()


def test_eslestir_calistir_yazar(monkeypatch):
    import app.routers.iddaa as i
    yazilan = []
    monkeypatch.setattr(i, "olaylari_cek", lambda: [])
    monkeypatch.setattr(i, "yakin_maclar", lambda b, s: [])
    monkeypatch.setattr(i, "iddaa_yaz", lambda e: yazilan.append(e))
    r = i.eslestir_calistir(dry_run=False)
    assert r["dry_run"] is False and r["eslesen"] == 0
