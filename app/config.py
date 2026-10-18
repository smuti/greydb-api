"""
Uygulama konfigürasyonu
"""
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    # Zorunlu: DATABASE_URL ortam değişkeni (Railway) ya da .env. Kodda varsayılan adres/şifre BULUNMAZ.
    database_url: str
    
    # API
    api_title: str = "GreyDB API"
    api_version: str = "1.0.0"
    api_description: str = "Futbol maç verileri ve istatistikleri API"
    
    # CORS
    cors_origins: list[str] = ["*"]
    
    class Config:
        env_file = ".env"


@lru_cache()
def get_settings() -> Settings:
    return Settings()

