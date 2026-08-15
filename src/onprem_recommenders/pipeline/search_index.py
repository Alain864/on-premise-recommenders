from __future__ import annotations

import logging

import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

PRODUCT_INDEX_MAPPING = {
    "properties": {
        "product_id": {"type": "keyword"},
        "title": {"type": "text"},
        "brand": {"type": "keyword"},
        "category_path": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
        },
        "description": {"type": "text"},
        "price": {"type": "float"},
        "in_stock": {"type": "boolean"},
        "popularity_score": {"type": "float"},
    }
}


def sync_products_to_elasticsearch(
    engine: Engine,
    elasticsearch_url: str,
    index_name: str,
) -> int:
    products = pd.read_sql(
        text(
            "SELECT p.product_id, p.title, p.brand, p.price, p.category_path, p.description, "
            "COALESCE(s.in_stock, TRUE) AS in_stock, COALESCE(s.popularity_score, 0) AS popularity_score "
            "FROM products p LEFT JOIN product_stats s ON s.product_id = p.product_id"
        ),
        engine,
    )
    if products.empty:
        return 0

    client = Elasticsearch(elasticsearch_url, verify_certs=False, ssl_show_warn=False)
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name, mappings=PRODUCT_INDEX_MAPPING)
        logger.info("Created Elasticsearch index %s", index_name)

    actions = [
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
        for record in products.to_dict(orient="records")
    ]
    bulk(client, actions, refresh=True)
    client.close()
    return len(actions)
