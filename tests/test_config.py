"""Veritabanı adresi kodda varsayılan olarak bulunmamalı (şifre sızıntısı); tanımsızsa açılış net bir hatayla başarısız olmalı."""
import pytest
from pydantic import ValidationError

from app.config import Settings


def test_database_url_tanimsizsa_acilis_hata_verir(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValidationError) as e:
        Settings(_env_file=None)
    assert "database_url" in str(e.value).lower()


def test_database_url_ortamdan_okunur(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://a:b@h:1/d")
    assert Settings(_env_file=None).database_url == "postgresql://a:b@h:1/d"


def test_kodda_gomulu_sifre_yok():
    import pathlib
    kaynak = pathlib.Path(__file__).resolve().parents[1].joinpath("app", "config.py").read_text()
    assert "@" not in kaynak.split("database_url")[1].split("\n")[0], "config.py'de bağlantı adresi gömülü olmamalı"
