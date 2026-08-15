from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = Field(
        default="postgresql+psycopg://recommender:recommender@localhost:5432/recommender",
        validation_alias="DATABASE_URL",
    )
    elasticsearch_url: str = Field(
        default="http://localhost:9200",
        validation_alias="ELASTICSEARCH_URL",
    )
    elasticsearch_index: str = Field(
        default="products",
        validation_alias="ELASTICSEARCH_INDEX",
    )
    openai_api_key: str | None = Field(
        default=None,
        validation_alias="OPENAI_API_KEY",
    )
    openai_embedding_model: str = Field(
        default="text-embedding-3-small",
        validation_alias="OPENAI_EMBEDDING_MODEL",
    )
    embedding_dimensions: int = Field(default=1536)
    source_data_dir: Path = Field(
        default=Path("./data/parquet"),
        validation_alias="SOURCE_DATA_DIR",
    )
    users_parquet_path: Path | None = Field(default=None, validation_alias="USERS_PARQUET_PATH")
    products_parquet_path: Path | None = Field(default=None, validation_alias="PRODUCTS_PARQUET_PATH")
    transactions_parquet_path: Path | None = Field(
        default=None, validation_alias="TRANSACTIONS_PARQUET_PATH"
    )
    interactions_parquet_path: Path | None = Field(
        default=None, validation_alias="INTERACTIONS_PARQUET_PATH"
    )
    session_gap_minutes: int = Field(default=30, validation_alias="SESSION_GAP_MINUTES")
    embedding_batch_size: int = Field(default=50, validation_alias="EMBEDDING_BATCH_SIZE")
    redis_url: str = Field(default="redis://localhost:6379/0", validation_alias="REDIS_URL")
    celery_broker_url: str | None = Field(default=None, validation_alias="CELERY_BROKER_URL")
    celery_result_backend: str | None = Field(default=None, validation_alias="CELERY_RESULT_BACKEND")
    cors_origins: str = Field(
        default="http://localhost:5173,http://127.0.0.1:5173",
        validation_alias="CORS_ORIGINS",
    )
    feature_flag_cache_ttl_seconds: int = Field(default=30)

    @field_validator("database_url")
    @classmethod
    def require_postgres(cls, value: str) -> str:
        if not value.startswith("postgresql"):
            raise ValueError("DATABASE_URL must be a PostgreSQL URL (postgresql+psycopg://...)")
        return value

    def parquet_paths(self) -> dict[str, Path]:
        return {
            "users": self.users_parquet_path or self.source_data_dir / "users.parquet",
            "products": self.products_parquet_path or self.source_data_dir / "products.parquet",
            "transactions": self.transactions_parquet_path
            or self.source_data_dir / "transactions.parquet",
            "interactions": self.interactions_parquet_path
            or self.source_data_dir / "interactions.parquet",
        }

    def cors_origin_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    @property
    def broker_url(self) -> str:
        return self.celery_broker_url or self.redis_url

    @property
    def result_backend(self) -> str:
        return self.celery_result_backend or self.redis_url


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
