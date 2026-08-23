import os

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.skipif(
    not os.environ.get("RUN_API_TESTS"),
    reason="Set RUN_API_TESTS=1 with Postgres and Elasticsearch running",
)


@pytest.fixture
def client():
    from onprem_recommenders.app import create_app

    with TestClient(create_app()) as test_client:
        yield test_client


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_homepage_anonymous(client):
    first = client.get("/recommendations/homepage")
    second = client.get("/recommendations/homepage")
    assert first.status_code == 200
    assert second.status_code == 200
    body = second.json()
    assert body["is_personalized"] is False
    assert "rows" in body


def test_autocomplete_prefix(client):
    response = client.get("/autocomplete/suggest", params={"prefix": "sm"})
    assert response.status_code == 200
    assert "suggestions" in response.json()
