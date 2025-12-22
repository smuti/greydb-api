"""
Uygulama konfigürasyonu
"""
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql://postgres:FKVMHcMBuXpmdMoaulQRmJftkPDRLcAW@trolley.proxy.rlwy.net:31538/railway"
    
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

