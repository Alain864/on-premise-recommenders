import { useEffect, useState } from "react";
import { fetchHomepage } from "../api";
import { ProductCard } from "../components/ProductCard";
import type { HomepageResponse } from "../types";
import { useUserId } from "../user";

export function HomePage() {
  const { userId } = useUserId();
  const [data, setData] = useState<HomepageResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setData(null);
    fetchHomepage(userId || null)
      .then(setData)
      .catch((err: Error) => setError(err.message));
  }, [userId]);

  if (error) {
    return (
      <div className="error">
        <p>Failed to load recommendations</p>
        <p className="error-detail">{error}</p>
      </div>
    );
  }
  if (!data) {
    return <div className="loading">Loading recommendations...</div>;
  }

  return (
    <div className="homepage">
      <h1 className="homepage-title">
        {data.is_personalized ? `Welcome back, ${data.user_id}` : "Trending Products"}
      </h1>
      {data.rows.map((row) => (
        <section className="recommendation-row" key={row.row_label}>
          <h2 className="row-label">{row.row_label}</h2>
          <div className="products-grid">
            {row.products.map((product) => (
              <ProductCard key={product.product_id} product={product} />
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}
