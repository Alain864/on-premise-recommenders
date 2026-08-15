import { Link } from "react-router-dom";
import type { ProductItem, SearchProductItem } from "../types";

interface Props {
  product: ProductItem | SearchProductItem;
  position?: number;
}

export function ProductCard({ product, position }: Props) {
  const scored = "final_score" in product ? product : null;
  return (
    <Link to={`/product/${product.product_id}`} className="product-card">
      {position != null && <div className="product-position">#{position}</div>}
      <div className="product-info">
        <h3 className="product-title">{product.title}</h3>
        <p className="product-brand">{product.brand}</p>
        <p className="product-price">${Number(product.price).toFixed(2)}</p>
        <span className="product-category-pill">{product.category_path}</span>
        {scored?.final_score != null && (
          <span className="product-score">Score: {scored.final_score.toFixed(2)}</span>
        )}
      </div>
    </Link>
  );
}
