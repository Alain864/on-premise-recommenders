from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = Field(
        default="sqlite+pysqlite:///./var/stage1.db",
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
    chroma_persist_directory: Path = Field(
        default=Path("./var/chroma"),
        validation_alias="CHROMA_PERSIST_DIRECTORY",
    )
    chroma_collection: str = Field(
        default="product_embeddings",
        validation_alias="CHROMA_COLLECTION",
    )
    openai_api_key: str | None = Field(
        default=None,
        validation_alias="OPENAI_API_KEY",
    )
    openai_embedding_model: str = Field(
        default="text-embedding-3-small",
        validation_alias="OPENAI_EMBEDDING_MODEL",
    )
    source_data_dir: Path = Field(
        default=Path("./data/parquet"),
        validation_alias="SOURCE_DATA_DIR",
    )
    users_parquet_path: Path | None = Field(
        default=None,
        validation_alias="USERS_PARQUET_PATH",
    )
    products_parquet_path: Path | None = Field(
        default=None,
        validation_alias="PRODUCTS_PARQUET_PATH",
    )
    transactions_parquet_path: Path | None = Field(
        default=None,
        validation_alias="TRANSACTIONS_PARQUET_PATH",
    )
    interactions_parquet_path: Path | None = Field(
        default=None,
        validation_alias="INTERACTIONS_PARQUET_PATH",
    )
    session_gap_minutes: int = Field(
        default=30,
        validation_alias="SESSION_GAP_MINUTES",
    )
    embedding_batch_size: int = Field(
        default=50,
        validation_alias="EMBEDDING_BATCH_SIZE",
    )

    def ensure_local_dirs(self) -> None:
        Path("var").mkdir(exist_ok=True)
        self.chroma_persist_directory.mkdir(parents=True, exist_ok=True)

    def parquet_paths(self) -> dict[str, Path]:
        return {
            "users": self.users_parquet_path or self.source_data_dir / "users.parquet",
            "products": self.products_parquet_path or self.source_data_dir / "products.parquet",
            "transactions": self.transactions_parquet_path or self.source_data_dir / "transactions.parquet",
            "interactions": self.interactions_parquet_path or self.source_data_dir / "interactions.parquet",
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_local_dirs()
    return settings
