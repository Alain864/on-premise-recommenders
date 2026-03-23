from __future__ import annotations

import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from sqlalchemy.engine import Engine


PRODUCT_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "title": {"type": "text"},
            "brand": {"type": "keyword"},
            "category_path": {"type": "text"},
            "description": {"type": "text"},
            "price": {"type": "float"},
            "in_stock": {"type": "boolean"},
            "popularity_score": {"type": "float"},
        }
    }
}


def sync_products_to_elasticsearch(engine: Engine, elasticsearch_url: str, index_name: str) -> int:
    products = pd.read_sql_table("products", engine)
    product_stats = pd.read_sql_table("product_stats", engine)
    documents = products.merge(product_stats, on="product_id", how="left")

    if documents.empty:
        return 0

    client = Elasticsearch(elasticsearch_url)
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name, mappings=PRODUCT_INDEX_MAPPING["mappings"])

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
