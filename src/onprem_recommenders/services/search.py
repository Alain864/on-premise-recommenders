from __future__ import annotations

from typing import Any

from elasticsearch import Elasticsearch


def fetch_products_by_category(
    client: Elasticsearch,
    category_path: str,
    excluded_ids: set[str],
    size: int = 10,
    index_name: str = "products",
) -> list[dict[str, Any]]:
    query: dict[str, Any] = {
        "query": {
            "bool": {
                "must": [{"match_phrase": {"category_path": category_path}}],
            }
        },
        "sort": [{"popularity_score": {"order": "desc"}}],
        "size": size,
    }
    if excluded_ids:
        query["query"]["bool"]["must_not"] = [{"terms": {"product_id": list(excluded_ids)}}]

    response = client.search(index=index_name, body=query)
    return [_hit_to_product(hit) for hit in response["hits"]["hits"]]


def fetch_products_by_ids(
    client: Elasticsearch,
    product_ids: list[str],
    index_name: str = "products",
) -> list[dict[str, Any]]:
    if not product_ids:
        return []
    response = client.search(
        index=index_name,
        body={"query": {"terms": {"product_id": product_ids}}, "size": len(product_ids)},
    )
    by_id = {}
    for hit in response["hits"]["hits"]:
        product = _hit_to_product(hit)
        by_id[product["product_id"]] = product
    return [by_id[pid] for pid in product_ids if pid in by_id]


def bm25_search(
    client: Elasticsearch,
    query: str,
    size: int,
    index_name: str,
) -> tuple[list[dict[str, Any]], int, float]:
    response = client.search(
        index=index_name,
        body={
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "brand^2", "category_path^1.5", "description"],
                    "type": "best_fields",
                    "fuzziness": "AUTO",
                }
            },
            "size": size,
        },
    )
    hits = response.get("hits", {}).get("hits", [])
    total = response.get("hits", {}).get("total", {}).get("value", 0)
    max_score = max((hit.get("_score") or 0 for hit in hits), default=0.0)
    products = []
    for hit in hits:
        product = _hit_to_product(hit)
        product["bm25_score"] = hit.get("_score") or 0.0
        products.append(product)
    return products, total, max_score


def get_product_source(
    client: Elasticsearch,
    product_id: str,
    index_name: str,
) -> dict[str, Any] | None:
    response = client.search(
        index=index_name,
        body={"query": {"term": {"product_id": product_id}}, "size": 1},
    )
    hits = response.get("hits", {}).get("hits", [])
    if not hits:
        return None
    return _hit_to_product(hits[0], include_description=True)


def _hit_to_product(hit: dict[str, Any], include_description: bool = False) -> dict[str, Any]:
    source = hit["_source"]
    product = {
        "product_id": source["product_id"],
        "title": source["title"],
        "brand": source["brand"],
        "price": source["price"],
        "category_path": source["category_path"],
        "popularity_score": source.get("popularity_score", 0.0),
    }
    if include_description:
        product["description"] = source.get("description")
    return product
