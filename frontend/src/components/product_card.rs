//! Product card component for displaying product info

use leptos::*;
use leptos_router::*;
use crate::api::{ProductItem, SearchProductItem};

/// Product card component - displays title, brand, price, category
#[component]
pub fn ProductCard(
    product: ProductItem,
    position: Option<i32>,
) -> impl IntoView {
    let product_id = product.product_id.clone();
    view! {
        <A href=format!("/product/{}", product_id) class="product-card">
            {move || {
                position.map(|p| view! {
                    <div class="product-position">{format!("#{}", p)}</div>
                })
            }}
            <div class="product-info">
                <h3 class="product-title">{&product.title}</h3>
                <p class="product-brand">{&product.brand}</p>
                <p class="product-price">{format!("${:.2}", product.price)}</p>
                <span class="product-category-pill">{&product.category_path}</span>
            </div>
        </A>
    }
}

/// Product card for search results with ranking details
#[component]
pub fn SearchProductCard(
    product: SearchProductItem,
    position: i32,
) -> impl IntoView {
    let product_id = product.product_id.clone();
    view! {
        <A href=format!("/product/{}", product_id) class="product-card search-result">
            <div class="product-position">{format!("#{}", position)}</div>
            <div class="product-info">
                <h3 class="product-title">{&product.title}</h3>
                <p class="product-brand">{&product.brand}</p>
                <p class="product-price">{format!("${:.2}", product.price)}</p>
                <span class="product-category-pill">{&product.category_path}</span>
                {move || {
                    product.final_score.map(|score| view! {
                        <span class="product-score">{format!("Score: {:.2}", score)}</span>
                    })
                }}
            </div>
        </A>
    }
}