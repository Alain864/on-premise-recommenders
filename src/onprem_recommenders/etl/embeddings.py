from __future__ import annotations

from typing import Iterable

import chromadb
import pandas as pd
from openai import OpenAI
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential


def _chunked(rows: list[dict], size: int) -> Iterable[list[dict]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def _embedding_text(record: dict) -> str:
    return " | ".join(
        [
            str(record["title"]).strip(),
            str(record["category_path"]).strip(),
            str(record["description"]).strip(),
        ]
    )


@retry(wait=wait_exponential(min=1, max=20), stop=stop_after_attempt(3))
def _create_embeddings(client: OpenAI, model: str, inputs: list[str]) -> list[list[float]]:
    response = client.embeddings.create(model=model, input=inputs)
    return [row.embedding for row in response.data]


def sync_product_embeddings(
    engine: Engine,
    openai_api_key: str | None,
    model: str,
    persist_directory: str,
    collection_name: str,
    batch_size: int,
) -> int:
    if not openai_api_key:
        raise ValueError("OPENAI_API_KEY is required to generate embeddings.")

    products = pd.read_sql_table("products", engine)
    if products.empty:
        return 0

    openai_client = OpenAI(api_key=openai_api_key)
    chroma_client = chromadb.PersistentClient(path=persist_directory)
    collection = chroma_client.get_or_create_collection(name=collection_name, metadata={"hnsw:space": "cosine"})

    records = products.to_dict(orient="records")
    synced = 0

    for batch in _chunked(records, size=batch_size):
        input_texts = [_embedding_text(record) for record in batch]
        embeddings = _create_embeddings(openai_client, model=model, inputs=input_texts)
        collection.upsert(
            ids=[record["product_id"] for record in batch],
            documents=input_texts,
            embeddings=embeddings,
            metadatas=[
                {
                    "product_id": record["product_id"],
                    "brand": record["brand"],
                    "category_path": record["category_path"],
                }
                for record in batch
            ],
        )
        synced += len(batch)

    return synced

