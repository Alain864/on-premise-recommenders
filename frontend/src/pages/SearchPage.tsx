import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { fetchSearch } from "../api";
import { ProductCard } from "../components/ProductCard";
import type { SearchResponse } from "../types";
import { useUserId } from "../user";

export function SearchPage() {
  const [params] = useSearchParams();
  const query = params.get("q") ?? "";
  const { userId } = useUserId();
  const [data, setData] = useState<SearchResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!query) {
      setError("Empty search query");
      return;
    }
    setData(null);
    setError(null);
    fetchSearch(query, userId || null)
      .then(setData)
      .catch((err: Error) => setError(err.message));
  }, [query, userId]);

  if (error) {
    return (
      <div className="error">
        <p>Search failed</p>
        <p className="error-detail">{error}</p>
      </div>
    );
  }
  if (!data) {
    return <div className="loading">Searching...</div>;
  }
  if (data.results.length === 0) {
    return (
      <div className="no-results">
        <h2>No results found</h2>
        <p>No products found for "{data.query}"</p>
      </div>
    );
  }

  return (
    <div className="search-results">
      <div className="search-header">
        <h1>Search: "{data.query}"</h1>
        <span className="results-count">{data.total_hits} results</span>
        {data.used_semantic_fallback && (
          <span className="semantic-badge">Semantic search used</span>
        )}
      </div>
      <div className="search-results-list">
        {data.results.map((product, index) => (
          <ProductCard
            key={product.product_id}
            product={product}
            position={index + 1}
          />
        ))}
      </div>
    </div>
  );
}
