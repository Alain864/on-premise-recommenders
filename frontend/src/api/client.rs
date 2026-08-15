//! API client for FastAPI backend communication

use reqwasm::http::Request;
use serde::de::DeserializeOwned;

use super::types::*;

/// Base URL for the FastAPI backend
/// In production, this should come from environment config
const API_BASE_URL: &str = "http://127.0.0.1:8000";

/// Fetch homepage recommendations
pub async fn fetch_homepage_recommendations(
    user_id: Option<&str>,
    rows: i32,
    products_per_row: i32,
) -> Result<HomepageResponse, String> {
    let mut url = format!(
        "{}/recommendations/homepage?rows={}&products_per_row={}",
        API_BASE_URL, rows, products_per_row
    );
    if let Some(uid) = user_id {
        url.push_str(&format!("&user_id={}", uid));
    }
    
    fetch_json(&url).await
}

/// Fetch search results
pub async fn fetch_search_results(
    query: &str,
    user_id: Option<&str>,
    size: i32,
) -> Result<SearchResponse, String> {
    let mut url = format!(
        "{}/recommendations/search?q={}&size={}",
        API_BASE_URL,
        urlencoding::encode(query),
        size
    );
    if let Some(uid) = user_id {
        url.push_str(&format!("&user_id={}", uid));
    }
    
    fetch_json(&url).await
}

/// Fetch product detail by ID
pub async fn fetch_product_detail(product_id: &str) -> Result<ProductDetail, String> {
    let url = format!(
        "{}/recommendations/product/{}",
        API_BASE_URL, product_id
    );
    
    fetch_json(&url).await
}

/// Fetch frequently bought together recommendations
pub async fn fetch_frequently_bought_together(
    product_id: &str,
    limit: i32,
) -> Result<ProductPageResponse, String> {
    let url = format!(
        "{}/recommendations/product/{}/frequently-bought-together?limit={}",
        API_BASE_URL, product_id, limit
    );
    
    fetch_json(&url).await
}

/// Fetch customers also viewed recommendations
pub async fn fetch_customers_also_viewed(
    product_id: &str,
    limit: i32,
) -> Result<ProductPageResponse, String> {
    let url = format!(
        "{}/recommendations/product/{}/customers-also-viewed?limit={}",
        API_BASE_URL, product_id, limit
    );
    
    fetch_json(&url).await
}

/// Fetch autocomplete suggestions
pub async fn fetch_autocomplete_suggestions(
    prefix: &str,
    user_id: Option<&str>,
    limit: i32,
) -> Result<AutocompleteResponse, String> {
    let mut url = format!(
        "{}/autocomplete/suggest?prefix={}&limit={}",
        API_BASE_URL,
        urlencoding::encode(prefix),
        limit
    );
    if let Some(uid) = user_id {
        url.push_str(&format!("&user_id={}", uid));
    }
    
    fetch_json(&url).await
}

/// Log an event (click, add_to_cart, etc.)
pub async fn log_event(event: EventRequest) -> Result<EventResponse, String> {
    let url = format!("{}/events", API_BASE_URL);
    
    let response = Request::post(&url)
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&event).unwrap_or_default())
        .send()
        .await
        .map_err(|e| e.to_string())?;
    
    if response.ok() {
        response
            .json()
            .await
            .map_err(|e| e.to_string())
    } else {
        Err(format!("HTTP error: {}", response.status()))
    }
}

/// Generic JSON fetcher
async fn fetch_json<T: DeserializeOwned>(url: &str) -> Result<T, String> {
    let response = Request::get(url)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    
    if response.ok() {
        response
            .json()
            .await
            .map_err(|e| e.to_string())
    } else {
        Err(format!("HTTP error: {}", response.status()))
    }
}

/// URL encoding module (simple implementation)
mod urlencoding {
    pub fn encode(s: &str) -> String {
        s.chars()
            .map(|c| match c {
                ' ' => "+".to_string(),
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
                _ => format!("%{:02X}", c as u8),
            })
            .collect()
    }
}