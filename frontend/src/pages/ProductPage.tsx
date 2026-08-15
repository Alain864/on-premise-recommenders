import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { fetchAlsoViewed, fetchBoughtTogether, fetchProduct, logEvent } from "../api";
import { ProductCard } from "../components/ProductCard";
import type { ProductDetail, ProductPageResponse } from "../types";
import { useUserId } from "../user";

export function ProductPage() {
  const { id = "" } = useParams();
  const navigate = useNavigate();
  const { userId } = useUserId();
  const [product, setProduct] = useState<ProductDetail | null>(null);
  const [bought, setBought] = useState<ProductPageResponse | null>(null);
  const [viewed, setViewed] = useState<ProductPageResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [added, setAdded] = useState(false);
  const [adding, setAdding] = useState(false);

  useEffect(() => {
    setAdded(false);
    setProduct(null);
    setBought(null);
    setViewed(null);
    fetchProduct(id).then(setProduct).catch((err: Error) => setError(err.message));
    fetchBoughtTogether(id).then(setBought).catch(() => undefined);
    fetchAlsoViewed(id).then(setViewed).catch(() => undefined);
  }, [id]);

  async function addToCart() {
    if (added || adding) return;
    setAdding(true);
    try {
      await logEvent({
        user_id: userId || null,
        feature: "product_page",
        event_type: "add_to_cart",
        product_ids: [id],
      });
      setAdded(true);
    } finally {
      setAdding(false);
    }
  }

  if (error) {
    return <div className="error">Error loading product: {error}</div>;
  }

  return (
    <div className="product-page">
      <button className="back-button" onClick={() => navigate(-1)}>
        Back
      </button>
      <div className="product-detail">
        {!product ? (
          <div className="loading">Loading product details...</div>
        ) : (
          <div className="product-main">
            <h1 className="product-title">{product.title}</h1>
            <p className="product-brand">{product.brand}</p>
            <p className="product-price">${Number(product.price).toFixed(2)}</p>
            <span className="category-pill">{product.category_path}</span>
            {product.description && <p className="product-description">{product.description}</p>}
            <button
              className={`add-to-cart-btn${added ? " added" : ""}${adding ? " loading" : ""}`}
              onClick={addToCart}
              disabled={added || adding}
            >
              {added ? "Added" : adding ? "Adding..." : "Add to Cart"}
            </button>
          </div>
        )}
        <div className="recommendation-sections">
          {bought && bought.recommendations.length > 0 && (
            <section className="recommendation-section">
              <h2>Frequently Bought Together</h2>
              <div className="products-grid">
                {bought.recommendations.map((item) => (
                  <ProductCard key={item.product_id} product={item} />
                ))}
              </div>
            </section>
          )}
          {viewed && viewed.recommendations.length > 0 && (
            <section className="recommendation-section">
              <h2>Customers Also Viewed</h2>
              <div className="products-grid">
                {viewed.recommendations.map((item) => (
                  <ProductCard key={item.product_id} product={item} />
                ))}
              </div>
            </section>
          )}
        </div>
      </div>
    </div>
  );
}
