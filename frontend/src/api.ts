import type {
  AutocompleteResponse,
  EventRequest,
  HomepageResponse,
  ProductDetail,
  ProductPageResponse,
  SearchResponse,
} from "./types";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://127.0.0.1:8000";

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json() as Promise<T>;
}

function withUser(params: URLSearchParams, userId: string | null) {
  if (userId) {
    params.set("user_id", userId);
  }
  return params;
}

export async function fetchHomepage(
  userId: string | null,
  rows = 3,
  productsPerRow = 10
): Promise<HomepageResponse> {
  const params = withUser(
    new URLSearchParams({ rows: String(rows), products_per_row: String(productsPerRow) }),
    userId
  );
  return getJson(`/recommendations/homepage?${params}`);
}

export async function fetchSearch(
  query: string,
  userId: string | null,
  size = 20
): Promise<SearchResponse> {
  const params = withUser(new URLSearchParams({ q: query, size: String(size) }), userId);
  return getJson(`/recommendations/search?${params}`);
}

export async function fetchProduct(productId: string): Promise<ProductDetail> {
  return getJson(`/recommendations/product/${encodeURIComponent(productId)}`);
}

export async function fetchBoughtTogether(
  productId: string,
  limit = 5
): Promise<ProductPageResponse> {
  return getJson(
    `/recommendations/product/${encodeURIComponent(productId)}/frequently-bought-together?limit=${limit}`
  );
}

export async function fetchAlsoViewed(
  productId: string,
  limit = 5
): Promise<ProductPageResponse> {
  return getJson(
    `/recommendations/product/${encodeURIComponent(productId)}/customers-also-viewed?limit=${limit}`
  );
}

export async function fetchAutocomplete(
  prefix: string,
  userId: string | null,
  limit = 5
): Promise<AutocompleteResponse> {
  const params = withUser(
    new URLSearchParams({ prefix, limit: String(limit) }),
    userId
  );
  return getJson(`/autocomplete/suggest?${params}`);
}

export async function logEvent(event: EventRequest): Promise<void> {
  await fetch(`${API_BASE}/events`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(event),
  });
}
