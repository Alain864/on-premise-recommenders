//! API types matching FastAPI response models

use serde::{Deserialize, Serialize};

/// Product item from homepage recommendations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductItem {
    pub product_id: String,
    pub title: String,
    pub brand: String,
    pub price: f64,
    pub category_path: String,
    pub popularity_score: f64,
}

/// Recommendation row in homepage response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendationRow {
    pub row_label: String,
    pub products: Vec<ProductItem>,
}

/// Homepage recommendations response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HomepageResponse {
    pub user_id: String,
    pub rows: Vec<RecommendationRow>,
    pub is_personalized: bool,
}

/// Search product item with ranking details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchProductItem {
    pub product_id: String,
    pub title: String,
    pub brand: String,
    pub price: f64,
    pub category_path: String,
    pub popularity_score: f64,
    #[serde(default)]
    pub bm25_score: Option<f64>,
    #[serde(default)]
    pub final_score: Option<f64>,
}

/// Search results response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub query: String,
    #[serde(default)]
    pub user_id: Option<String>,
    pub results: Vec<SearchProductItem>,
    pub total_hits: i32,
    pub is_personalized: bool,
    pub used_semantic_fallback: bool,
}

/// Product page recommendations response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductPageResponse {
    pub product_id: String,
    pub recommendations: Vec<ProductItem>,
    pub recommendation_type: String,
    pub fallback: bool,
}

/// Autocomplete suggestion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutocompleteSuggestion {
    pub query_text: String,
    pub frequency: i32,
    pub relevance_score: f64,
    #[serde(default)]
    pub category_match: Option<String>,
}

/// Autocomplete response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutocompleteResponse {
    pub prefix: String,
    pub suggestions: Vec<AutocompleteSuggestion>,
    pub is_personalized: bool,
    #[serde(default)]
    pub user_id: Option<String>,
}

/// Event logging request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    pub feature: String,
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub product_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Event logging response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventResponse {
    pub success: bool,
    #[serde(default)]
    pub event_id: Option<i32>,
    pub message: String,
}

/// Product detail from /recommendations/product/{id}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductDetail {
    pub product_id: String,
    pub title: String,
    pub brand: String,
    pub price: f64,
    pub category_path: String,
    #[serde(default)]
    pub description: Option<String>,
    pub popularity_score: f64,
}
