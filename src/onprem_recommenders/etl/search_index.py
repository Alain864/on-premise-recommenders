from __future__ import annotations

import logging

import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


PRODUCT_INDEX_MAPPING = {
    "properties": {
        "product_id": {"type": "keyword"},
        "title": {"type": "text"},
        "brand": {"type": "keyword"},
        "category_path": {
            "type": "text",
            "fields": {
                "keyword": {"type": "keyword", "ignore_above": 256}
            }
        },
        "description": {"type": "text"},
        "price": {"type": "float"},
        "in_stock": {"type": "boolean"},
        "popularity_score": {"type": "float"},
    }
}


def sync_products_to_elasticsearch(engine: Engine, elasticsearch_url: str, index_name: str) -> int:
    products = pd.read_sql_table("products", engine)
    product_stats = pd.read_sql_table("product_stats", engine)
    documents = products.merge(product_stats, on="product_id", how="left")

    if documents.empty:
        return 0

    # Connect to Elasticsearch
    # For local development without SSL/Security
    client = Elasticsearch(
        elasticsearch_url,
        verify_certs=False,
        ssl_show_warn=False,
    )

    # Create index if it doesn't exist (ignore error if already exists)
    try:
        client.indices.create(index=index_name, mappings=PRODUCT_INDEX_MAPPING)
        logger.info(f"Created Elasticsearch index: {index_name}")
    except Exception as e:
        if "resource_already_exists" in str(e) or "already exists" in str(e):
            logger.info(f"Index {index_name} already exists")
        else:
            raise

    actions = []
    for record in documents.to_dict(orient="records"):
        actions.append(
            {
                "_index": index_name,
                "_id": record["product_id"],
                "_source": {
                    "product_id": record["product_id"],
                    "title": record["title"],
                    "brand": record["brand"],
                    "category_path": record["category_path"],
                    "description": record["description"],
                    "price": float(record["price"]),
                    "in_stock": bool(record.get("in_stock", True)),
                    "popularity_score": float(record.get("popularity_score") or 0.0),
                },
            }
        )

    bulk(client, actions, refresh=True)
    return len(actions)
