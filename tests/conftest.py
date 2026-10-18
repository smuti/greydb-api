"""Testlerde gerçek veritabanı yok: DATABASE_URL zorunlu olduğu için sahte bir adres verilir (bağlantı kurulmaz)."""
import os

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
