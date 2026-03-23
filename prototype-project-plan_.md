# **Search & Recommendation System — Implementation Plan**

## **Stack**

| Layer | Tool |
| ----- | ----- |
| API | FastAPI |
| Search/Ranking | Elasticsearch |
| Vector Store | ChromaDB |
| Embeddings | OpenAI `text-embedding-3-small` |
| ML/Reranking | scikit-learn, LightGBM |
| Data processing | pandas, NumPy |
| Task queue | Celery \+ Redis |
| Frontend | Leptos \+ WASM |
| DB | PostgreSQL (source of truth) |

---

## **Stage 1 — Data Foundation**

**Goal:** Clean, structured data ready to feed all four features.

1. Load all four tables into PostgreSQL.  
2. Build derived tables:  
   * `user_category_affinity`: aggregate purchase \+ view counts per user per category  
   * `product_stats`: CTR proxy (views→cart ratio), conversion rate, avg review score (if available), stock status  
   * `co_purchase_pairs`: product pairs that appear in the same `order_id`  
   * `co_view_pairs`: product pairs viewed in the same session (session \= user activity within 30-min gaps)  
3. Index all 200k products into Elasticsearch with fields: `title`, `brand`, `category_path`, `description`, `price`, `in_stock`, `popularity_score`.  
4. Generate embeddings for product `title + category + description` via OpenAI API → store in ChromaDB with `product_id` as metadata.

---

## **Stage 2 — Feature 1: Personalized Homepage Feed**

**Approach:** Category-affinity scoring (fast, works with 2 months of data).

* Pull top 3 categories from `user_category_affinity`.  
* For each category, fetch top N products by `popularity_score`.  
* Filter out already-purchased products.  
* Fallback (new users): global bestsellers by category.  
* API: `GET /recommendations/homepage?user_id=`  
* Response includes `row_label` \+ list of products per row.

---

## **Stage 3 — Feature 4: Product Page Recommenders**

**Module A — Frequently Bought Together**

* Run FP-Growth on `co_purchase_pairs` using `mlxtend`.  
* Store association rules (product → \[related products\] \+ confidence score) in PostgreSQL.  
* API: `GET /recommendations/bought-together?product_id=`

**Module B — Customers Also Viewed**

* Use `co_view_pairs` to build item similarity scores (co-occurrence count normalized by frequency).  
* For richer results, run item2vec (`gensim Word2Vec`) on view sessions treated as sequences.  
* Store top-K neighbors per product.  
* API: `GET /recommendations/also-viewed?product_id=`

Both endpoints: exclude current product \+ out-of-stock items.

---

## **Stage 4 — Feature 3: Search Results Ranking**

**Baseline:**

* Elasticsearch BM25 for text matching → returns candidate set.

Re-rank candidates using a weighted score:  
 score \= w1\*bm25 \+ w2\*popularity \+ w3\*conversion\_rate \+ w4\*review\_score \+ w5\*in\_stock\_boost

*   
* Tune weights manually first, then replace with LightGBM LambdaMART once you collect click logs.

**Semantic layer:**

* If BM25 results are weak (low text overlap), fall back to ChromaDB vector search on query embedding → merge candidate sets.

* API: `POST /search?q=&user_id=`

---

## **Stage 5 — Feature 2: Personalized Query Autocomplete**

* Build a prefix trie from the `Searches` table (query\_text, frequency).  
* On keystroke, trie returns top global completions by frequency.  
* Personalization re-rank: boost completions that overlap with user's top categories.  
* API: `GET /autocomplete?prefix=&user_id=` — must respond \<100ms; serve trie from memory.  
* Fallback: global frequency ranking only.

---

## **Stage 6 — Shared Infrastructure**

**Logging:**

* Every API response logs `impression` events: `{user_id, feature, product_ids_shown, timestamp}`.  
* A `/event` endpoint accepts click events from the frontend.  
* Both write to a `events` PostgreSQL table → feeds future retraining.

**A/B flags:**

* Simple `feature_flags` table: `{feature_name, variant, user_segment, enabled}`.  
* Each endpoint reads its flag on startup (cached). No external tool needed at this stage.

**Fallbacks:**

* A `trending` endpoint precomputes hourly bestsellers and most-viewed — all four features fall back to this for anonymous/cold-start users.

**Celery jobs:**

* Nightly: recompute `user_category_affinity`, `product_stats`, `co_purchase_pairs`, `co_view_pairs`.  
* Nightly: rebuild trie, association rules, item2vec neighbors.

---

## **Stage 7 — Minimal Frontend**

**Stack:** Leptos (Rust → WASM):

* On mount: calls FastAPI `/recommendations/homepage?user_id=`  
* Search bar **(`search_bar.rs`)** — lives in the top nav, present on all pages  
* Search Results ([search.rs](http://search.rs)) Reads q from URL query param via Leptos router, create\_resource keyed on q — refetches automatically when query changes, Renders ranked list: position number, title, brand, price, category pill.  
* Product Detail ([product.rs](http://product.rs)) Two parallel create\_resource calls: bought-together \+ also-viewed, Both render inside independent Suspense blocks so they load independently, "Add to Cart" button: create\_action posts to FastAPI /event, button switches to "Added ✓" reactively via signal.

No images. Products shown as cards with: title, brand, price, category.

---

