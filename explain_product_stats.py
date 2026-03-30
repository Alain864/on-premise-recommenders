"""
Product Stats Table Explanation
===============================

This script explains the `product_stats` table used in the search ranking algorithm.
The table contains pre-computed engagement metrics for each product.

Table Structure:
----------------
| Column            | Type    | Description                                    |
|-------------------|---------|------------------------------------------------|
| product_id        | TEXT    | Primary key, links to products table           |
| view_count        | INTEGER | Number of page views                           |
| add_to_cart_count | INTEGER | Times added to cart                             |
| purchase_count    | INTEGER | Times purchased                                 |
| ctr_proxy         | REAL    | Click-through rate proxy                        |
| conversion_rate   | REAL    | Purchase conversion rate                       |
| popularity_score  | REAL    | Log-scaled engagement score                     |
| review_score      | REAL    | Average review rating (default 3.5)             |
| review_count      | INTEGER | Number of reviews (default 0)                   |
| in_stock          | INTEGER | Stock status (default 1)                        |

Key Formulas:
-------------

1. CTR Proxy (Click-Through Rate Proxy):
   ctr_proxy = add_to_cart_count / max(view_count, add_to_cart_count)
   
   This represents the proportion of views that resulted in adding to cart.
   The max() prevents division by zero.

2. Conversion Rate:
   conversion_rate = purchase_count / max(view_count, purchase_count)
   
   This represents the proportion of views that resulted in a purchase.

3. Popularity Score (Log-scaled):
   popularity_score = log1p(purchase_count * 5 + add_to_cart_count * 2 + view_count)
   
   The weights prioritize purchases (5x) over add-to-cart (2x) over views (1x).
   The log1p() function (log(1+x)) prevents extreme values from dominating.

Why These Metrics Matter:
-------------------------
In the weighted re-ranking phase, these metrics influence the final ranking:

- Popularity (25%) - Products with more engagement rank higher
- Conversion Rate (15%) - Efficient converters get a boost
- Review Score (10%) - Higher rated products preferred
- In-Stock (10%) - Available products prioritized

Example Calculations:
---------------------
"""

import sqlite3
import pandas as pd
import numpy as np

# Configuration
DB_PATH = 'var/stage1.db'


def calc_popularity_score(views: int, add_to_cart: int, purchases: int) -> float:
    """
    Calculate popularity score with weighted engagement.
    
    Weights: purchase=5, add_to_cart=2, view=1
    Uses log1p to compress the range.
    """
    weighted_sum = purchases * 5 + add_to_cart * 2 + views * 1
    return np.log1p(weighted_sum)


def calc_ctr_proxy(views: int, add_to_cart: int) -> float:
    """
    Calculate CTR proxy.
    
    Returns add_to_cart / max(views, add_to_cart)
    """
    if views == 0 and add_to_cart == 0:
        return 0.0
    return add_to_cart / max(views, add_to_cart)


def calc_conversion_rate(views: int, purchases: int) -> float:
    """
    Calculate conversion rate.
    
    Returns purchases / max(views, purchases)
    """
    if views == 0 and purchases == 0:
        return 0.0
    return purchases / max(views, purchases)


def show_popularity_examples():
    """Demonstrate popularity score calculation with examples."""
    print("\n" + "=" * 70)
    print("POPULARITY SCORE EXAMPLES")
    print("=" * 70)
    
    examples = [
        {"name": "Low engagement", "views": 10, "cart": 2, "purchases": 0},
        {"name": "Medium engagement", "views": 100, "cart": 20, "purchases": 5},
        {"name": "High engagement", "views": 1000, "cart": 200, "purchases": 50},
        {"name": "Best seller", "views": 5000, "cart": 1000, "purchases": 300},
        {"name": "Viral product", "views": 10000, "cart": 2000, "purchases": 500},
    ]
    
    print(f"\n{'Product':<20} {'Views':>8} {'Cart':>8} {'Purchases':>10} {'Score':>10}")
    print("-" * 70)
    for ex in examples:
        score = calc_popularity_score(ex["views"], ex["cart"], ex["purchases"])
        print(f"{ex['name']:<20} {ex['views']:>8} {ex['cart']:>8} {ex['purchases']:>10} {score:>10.2f}")
    
    print("\nNote: The log scale compresses high values, preventing dominant products")
    print("      from having disproportionately high scores.")


def show_database_samples():
    """Show sample product_stats records from the database."""
    print("\n" + "=" * 70)
    print("SAMPLE RECORDS FROM DATABASE")
    print("=" * 70)
    
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Get sample records
        df = pd.read_sql_query("""
            SELECT 
                ps.product_id,
                p.title,
                ps.view_count,
                ps.add_to_cart_count,
                ps.purchase_count,
                ROUND(ps.ctr_proxy, 4) as ctr_proxy,
                ROUND(ps.conversion_rate, 4) as conversion_rate,
                ROUND(ps.popularity_score, 2) as popularity_score,
                ps.review_score,
                ps.review_count,
                ps.in_stock
            FROM product_stats ps
            LEFT JOIN products p ON ps.product_id = p.product_id
            ORDER BY ps.popularity_score DESC
            LIMIT 10
        """, conn)
        
        print("\nTop 10 products by popularity score:")
        print(df.to_string(index=False))
        
        # Statistics summary
        stats = pd.read_sql_query("""
            SELECT 
                COUNT(*) as total_products,
                AVG(view_count) as avg_views,
                AVG(add_to_cart_count) as avg_add_to_cart,
                AVG(purchase_count) as avg_purchases,
                AVG(popularity_score) as avg_popularity,
                MAX(popularity_score) as max_popularity,
                MIN(popularity_score) as min_popularity
            FROM product_stats
        """, conn)
        
        print("\n" + "-" * 70)
        print("Database Statistics:")
        for col in stats.columns:
            print(f"  {col}: {stats[col].iloc[0]:.2f}")
        
        conn.close()
        
    except Exception as e:
        print(f"\nError connecting to database: {e}")
        print("Make sure the database exists at:", DB_PATH)


def show_weighted_scoring():
    """Explain how the weighted scoring works."""
    print("\n" + "=" * 70)
    print("WEIGHTED SCORING FORMULA")
    print("=" * 70)
    
    print("""
The final search ranking uses weighted scoring:

final_score = 0.40 * normalized_bm25 
            + 0.25 * normalized_popularity 
            + 0.15 * normalized_conversion 
            + 0.10 * normalized_review 
            + 0.10 * in_stock_boost

Where:
- normalized_* = (value - min) / (max - min) → scales to 0-1 range
- in_stock_boost = 0.5 if in stock, 0.0 otherwise (prototype default: in_stock=1)

Why these weights?
- BM25 (40%): Primary relevance signal from text matching
- Popularity (25%): Engagement indicates product quality/interest
- Conversion (15%): Efficiency of converting views to purchases
- Review (10%): Customer satisfaction indicator
- In-Stock (10%): Availability preference
""")


def main():
    """Run all explanations."""
    print("\n" + "=" * 70)
    print("PRODUCT STATS TABLE EXPLANATION")
    print("=" * 70)
    
    # Show example calculations
    show_popularity_examples()
    
    # Show weighted scoring
    show_weighted_scoring()
    
    # Show database samples
    show_database_samples()
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print("""
The product_stats table enables data-driven ranking by:
1. Tracking engagement metrics (views, cart adds, purchases)
2. Computing derived scores (CTR, conversion, popularity)
3. Providing signals for weighted re-ranking

This allows the search algorithm to balance text relevance (BM25)
with business signals (popularity, conversion, reviews, availability).
""")


if __name__ == "__main__":
    main()