**Search & Recommendation System**

**Context**

We need to implement four ML-powered features for our e-commerce platform, covering the full customer journey from homepage to product page. Below are the four components, their expected behavior, and the data signals each one depends on.

**Feature 1 — Personalized Homepage Feed**

**What it does:** Displays product recommendations to a logged-in user before they search for anything.

**Expected output:** Horizontally scrollable rows with labels like *"Based on your recent purchase"* or *"Inspired by your browsing history"*, populated with relevant products.

**Input signals:**

* User's past purchases  
* User's recently viewed products  
* User's inferred category interests (e.g. fitness, fashion)

**Suggested approach:** User-based or item-based collaborative filtering, or a two-tower model if you want to go deeper. A simpler fallback is category-affinity scoring based on purchase/view history.

**Feature 2 — Personalized Query Autocomplete**

**What it does:** As a user types in the search bar, it suggests query completions that are tailored to their profile — not just generic completions based on global popularity.

**Expected output:** A dropdown list of 5–8 suggested queries, appearing within \~100ms of each keystroke.

**Input signals:**

* Current typed prefix  
* User's demographic segment (if available)  
* User's purchase and browsing history  
* Global query popularity / trending searches

**Suggested approach:** Prefix trie or Elasticsearch completion suggester for speed, re-ranked by a personalization layer using user signals. Results should differ meaningfully across user segments.

**Feature 3 — Search Results Ranking**

**What it does:** When a user submits a search query, ranks the matching products intelligently rather than returning them in arbitrary or purely lexical order.

**Expected output:** A results page where the top products are the most relevant, highest quality, and most likely to convert — not just text matches.

**Ranking signals to incorporate:**

* Relevance to query (text match, semantic similarity)  
* Product popularity (click-through rate, conversion rate)  
* Inventory status (in-stock items ranked above out-of-stock)  
* Review score and review count  
* User personalization (optional, can be Phase 2\)

**Suggested approach:** A learning-to-rank model (LambdaMART or similar), or a simpler weighted scoring function if you want to start fast. Elasticsearch/OpenSearch can handle baseline ranking; the ML layer sits on top to re-rank.

**Feature 4 — Product Page Recommenders (Cross-sell & Up-sell)**

**What it does:** On a product detail page, displays two distinct recommendation modules.

**Module A — "Frequently Bought Together":**

* Logic: Items commonly purchased in the same transaction as the viewed product  
* Goal: Increase average order value  
* Suggested approach: Market Basket Analysis (Apriori / FP-Growth) or co-purchase embeddings

**Module B — "Customers Also Viewed":**

* Logic: Items that users browsed during the same session without necessarily buying  
* Goal: Keep the user on-site if the current product doesn't convert  
* Suggested approach: Session-based collaborative filtering or item2vec on view sequences

Both modules should exclude the currently viewed product and out-of-stock items from results.

**Shared Requirements (apply to all four features)**

* **Latency:** Recommendations and autocomplete must respond in under 200ms at p95  
* **Fallback:** Each feature needs a non-personalized fallback (e.g. trending, bestsellers) for new users or cold-start situations  
* **A/B testing hooks:** Each feature should be behind a flag so we can test variants  
* **Logging:** All impressions and clicks must be logged — this data feeds model retraining.

Now this is a startup and they require a proof of concept and a working prototype. We have a database with these key data:    
1\. User Data (Who they are)  
\* user\_id    
\* signup\_date    
\* country. We only  have US right now  
2\. Product Data (What you sell)  
product\_id  
title (name)  
brand  
price  
category\_path  
description  
3\. Transaction Data (What they bought)  
\* order\_id  
\* user\_id  
\* product\_id  
\* timestamp  
4\. Interaction Data (What they looked at)  
\* Page Views: user\_id viewed product\_id at timestamp.  
\* Add-to-Carts: user\_id added product\_id at timestamp.  
\* Searches: user\_id searched for query\_text at timestamp.  

