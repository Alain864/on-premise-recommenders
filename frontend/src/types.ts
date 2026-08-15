export interface ProductItem {
  product_id: string;
  title: string;
  brand: string;
  price: number;
  category_path: string;
  popularity_score: number;
}

export interface ProductDetail extends ProductItem {
  description?: string | null;
}

export interface RecommendationRow {
  row_label: string;
  products: ProductItem[];
}

export interface HomepageResponse {
  user_id: string;
  rows: RecommendationRow[];
  is_personalized: boolean;
}

export interface ProductPageResponse {
  product_id: string;
  recommendations: ProductItem[];
  recommendation_type: string;
  fallback: boolean;
}

export interface SearchProductItem extends ProductItem {
  bm25_score?: number | null;
  final_score?: number | null;
}

export interface SearchResponse {
  query: string;
  user_id: string | null;
  results: SearchProductItem[];
  total_hits: number;
  is_personalized: boolean;
  used_semantic_fallback: boolean;
}

export interface AutocompleteSuggestion {
  query_text: string;
  frequency: number;
  relevance_score: number;
  category_match: string | null;
}

export interface AutocompleteResponse {
  prefix: string;
  suggestions: AutocompleteSuggestion[];
  is_personalized: boolean;
  user_id: string | null;
}

export interface EventRequest {
  user_id?: string | null;
  feature: string;
  event_type: string;
  product_ids?: string[] | null;
  query_text?: string | null;
}
