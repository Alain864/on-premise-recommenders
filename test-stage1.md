## Test Queries for Stage 1 Data Stores

### Elasticsearch Queries

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