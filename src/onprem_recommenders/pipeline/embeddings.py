from __future__ import annotations

from typing import Iterable

from openai import OpenAI
from sqlalchemy import select, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from tenacity import retry, stop_after_attempt, wait_exponential

from onprem_recommenders.models import Product
from onprem_recommenders.pipeline.load import content_hash

MAX_EMBEDDING_CHARS = 7000


def _embedding_text(title: str, category_path: str, description: str) -> str:
    text = " | ".join([title.strip(), category_path.strip(), description.strip()])
    return text[:MAX_EMBEDDING_CHARS]


def _chunked(rows: list, size: int) -> Iterable[list]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


@retry(wait=wait_exponential(min=1, max=20), stop=stop_after_attempt(3))
def _create_embeddings(client: OpenAI, model: str, inputs: list[str]) -> list[list[float]]:
    response = client.embeddings.create(model=model, input=inputs)
    return [row.embedding for row in response.data]


def sync_product_embeddings(
    engine: Engine,
    openai_api_key: str | None,
    model: str,
    batch_size: int,
) -> int:
    if not openai_api_key:
        raise ValueError("OPENAI_API_KEY is required to generate embeddings.")

    openai_client = OpenAI(api_key=openai_api_key)
    synced = 0

    with Session(engine) as session:
        pending = session.execute(
            select(Product).where(Product.embedding.is_(None))
        ).scalars().all()
        pending_rows = list(pending)

        for batch in _chunked(pending_rows, size=batch_size):
            texts = [
                _embedding_text(row.title, row.category_path, row.description) for row in batch
            ]
            embeddings = _create_embeddings(openai_client, model=model, inputs=texts)
            for row, embedding in zip(batch, embeddings):
                digest = content_hash(row.title, row.category_path, row.description)
                session.execute(
                    update(Product)
                    .where(Product.product_id == row.product_id)
                    .values(embedding=embedding, embedding_hash=digest)
                )
            session.commit()
            synced += len(batch)

    return synced
