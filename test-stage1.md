## Test Queries for Stage 1 Data Stores

### Elasticsearch Queries

```bash
docker compose up -d
```

**1. Basic text search - find products matching "laptop":**
```bash
curl -s "http://localhost:9200/products/_search?q=laptop&size=5" | jq '.hits.hits[]._source.title'
```
Returns the first 5 products whose title contains "laptop".

**2. Get document count (total indexed products):**
```bash
curl -s "http://localhost:9200/products/_count" | jq '.count'
```
Should return `40341`.

**3. Search by brand (exact match):**
```bash
curl -s -X GET "http://localhost:9200/products/_search" \
  -H 'Content-Type: application/json' \
  -d '{"query": {"term": {"brand": "Sony"}}, "size": 5}' | jq '.hits.hits[]._source.title'
```
Returns products where `brand` is exactly "Sony" (keyword field).

**4. Full-text search on category_path:**
```bash
curl -s -X GET "http://localhost:9200/products/_search" \
  -H 'Content-Type: application/json' \
  -d '{"query": {"match": {"category_path": "Electronics"}}, "size": 5}' | jq '.hits.hits[]._source'
```
Returns products whose `category_path` contains "Electronics".

**5. Search with price filter:**
```bash
curl -s -X GET "http://localhost:9200/products/_search" \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "bool": {
        "must": [{"match": {"title": "wireless"}}],
        "filter": [{"range": {"price": {"lte": 100}}}]
      }
    },
    "size": 5
  }' | jq '.hits.hits[]._source'
```
Returns products with "wireless" in the title and price ≤ $100.

**6. Get specific product by ID:**
```bash
curl -s "http://localhost:9200/products/_doc/PRODUCT_ID_HERE" | jq '._source'
```
Replace `PRODUCT_ID_HERE` with an actual ID (you can get one from a previous query).

### ChromaDB Queries

**7. Count total embeddings:**
```bash
./.venv/bin/python -c "
import chromadb
client = chromadb.PersistentClient(path='./var/chroma')
collection = client.get_collection('product_embeddings')
print(f'Total embeddings: {collection.count()}')
"
```
Should print `Total embeddings: 40341`.

**8. Peek at stored embeddings:**
```bash
./.venv/bin/python -c "
import chromadb
client = chromadb.PersistentClient(path='./var/chroma')
collection = client.get_collection('product_embeddings')
result = collection.peek(limit=3)
print('IDs:', result['ids'])
print('Metadata:', result['metadatas'])
"
```
Shows the first 3 embedding entries.

**9. Semantic similarity search:**
```bash
./.venv/bin/python -c "
import chromadb
from openai import OpenAI
import os

client = chromadb.PersistentClient(path='./var/chroma')
collection = client.get_collection('product_embeddings')

openai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
response = openai_client.embeddings.create(
    input='wireless bluetooth headphones',
    model='text-embedding-3-small'
)
query_embedding = response.data[0].embedding

results = collection.query(
    query_embeddings=[query_embedding],
    n_results=5
)
for product_id, metadata in zip(results['ids'][0], results['metadatas'][0]):
    print(f'{product_id}: {metadata}')
"
```
Returns the 5 most semantically similar products to the query "wireless bluetooth headphones".

### SQLite Database Queries

**10. Count products:**
```bash
sqlite3 ./var/stage1.db "SELECT COUNT(*) FROM products;"
```

**11. View derived tables:**
```bash
sqlite3 ./var/stage1.db "SELECT * FROM user_category_affinity LIMIT 5;"
sqlite3 ./var/stage1.db "SELECT * FROM product_stats ORDER BY popularity_score DESC LIMIT 5;"
sqlite3 ./var/stage1.db "SELECT * FROM co_purchase_pairs LIMIT 5;"
```

**12. Top viewed products:**
```bash
sqlite3 ./var/stage1.db "SELECT p.title, ps.view_count, ps.popularity_score FROM product_stats ps JOIN products p ON ps.product_id = p.product_id ORDER BY ps.view_count DESC LIMIT 10;"
```

**Explore database**
# View all tables
sqlite3 ./var/stage1.db ".tables"

# View table schema
sqlite3 ./var/stage1.db ".schema products"

# Query data (after running load-parquet)
sqlite3 ./var/stage1.db "SELECT * FROM products LIMIT 5;"

### Stage 2

**Anonymous user (trending products)**
curl -s "http://127.0.0.1:8000/recommendations/homepage?rows=2&products_per_row=5" | jq .

**Specific user (personalized if affinity data exists)**
curl -s "http://127.0.0.1:8000/recommendations/homepage?user_id=USER1&rows=2&products_per_row=5" | jq .

curl -s "http://127.0.0.1:8000/recommendations/homepage?user_id=USR_13914CAFA179&rows=2&products_per_row=5" | jq .

# Different user with affinity for Tablet Cases
curl -s "http://127.0.0.1:8000/recommendations/homepage?user_id=USR_C897116EBA13&rows=2&products_per_row=5" | jq .

# Another user with affinity for Remote Controls
curl -s "http://127.0.0.1:8000/recommendations/homepage?user_id=USR_5FC06B275F9F&rows=2&products_per_row=5" | jq .

### Stage 3 - Feature 4: Product Page Recommenders

**Frequently Bought Together (cross-sell)**
# Get products frequently bought together with a specific product
curl -s "http://127.0.0.1:8000/recommendations/product/B00004Z5V3/frequently-bought-together?limit=5" | jq .

# Test with a product that has co-purchase data
curl -s "http://127.0.0.1:8000/recommendations/product/B00NUXY870/frequently-bought-together?limit=5" | jq .

# Unknown product (shows fallback behavior - returns trending products)
curl -s "http://127.0.0.1:8000/recommendations/product/UNKNOWN_PRODUCT/frequently-bought-together?limit=5" | jq .

**Customers Also Viewed (up-sell)**
# Get products frequently viewed together with a specific product
curl -s "http://127.0.0.1:8000/recommendations/product/B000FW6MIW/customers-also-viewed?limit=5" | jq .

# Test with another product that has co-view data
curl -s "http://127.0.0.1:8000/recommendations/product/B000HN2ZBC/customers-also-viewed?limit=5" | jq .

# Unknown product (shows fallback behavior - returns trending products)
curl -s "http://127.0.0.1:8000/recommendations/product/UNKNOWN_PRODUCT/customers-also-viewed?limit=5" | jq .

**Query co-purchase pairs directly**
sqlite3 ./var/stage1.db "SELECT left_product_id, right_product_id, pair_count FROM co_purchase_pairs ORDER BY pair_count DESC LIMIT 10;"

**Query co-view pairs directly**
sqlite3 ./var/stage1.db "SELECT left_product_id, right_product_id, pair_count FROM co_view_pairs ORDER BY pair_count DESC LIMIT 10;"


# Basic search
curl -s "http://127.0.0.1:8000/recommendations/search?q=adapter&size=5" | jq .

# Personalized search
curl -s "http://127.0.0.1:8000/recommendations/search?q=cable&user_id=USR_13914CAFA179&size=5" | jq .

### Stage 5 - Feature 2: Personalized Query Autocomplete

**Prerequisites - Build autocomplete index (if not done)**
```bash
./.venv/bin/recommender-stage1 build-autocomplete
```

**Anonymous user (global popular queries)**
```bash
curl -s "http://127.0.0.1:8000/autocomplete/suggest?prefix=sm&limit=5" | jq .
```

**Personalized suggestions for known user**
```bash
curl -s "http://127.0.0.1:8000/autocomplete/suggest?prefix=sm&user_id=USR_13914CAFA179&limit=5" | jq .
```

**Test different prefixes**
```bash
# Electronics queries
curl -s "http://127.0.0.1:8000/autocomplete/suggest?prefix=lap&limit=5" | jq .
curl -s "http://127.0.0.1:8000/autocomplete/suggest?prefix=wire&limit=5" | jq .
curl -s "http://127.0.0.1:8000/autocomplete/suggest?prefix=blue&limit=5" | jq .

# With different users
curl -s "http://127.0.0.1:8000/autocomplete/suggest?prefix=phone&user_id=USR_C897116EBA13&limit=5" | jq .
```

**Query autocomplete data directly**
```bash
# View sample query suggestions
sqlite3 ./var/stage1.db "SELECT query_text, frequency, category_path FROM query_suggestions ORDER BY frequency DESC LIMIT 10;"

# Find queries for a specific category
sqlite3 ./var/stage1.db "SELECT query_text, frequency FROM query_suggestions WHERE category_path LIKE '%Electronics%' ORDER BY frequency DESC LIMIT 10;"

# Count total unique queries
sqlite3 ./var/stage1.db "SELECT COUNT(DISTINCT query_text) FROM query_suggestions;"
```

**Expected behavior:**
- **Anonymous requests**: Returns global popular queries sorted by frequency, `is_personalized: false`
- **Known user with affinities**: Returns suggestions boosted by user's category preferences, `is_personalized: true`
- **Unknown user**: Falls back to global popular queries
