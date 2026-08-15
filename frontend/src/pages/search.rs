//! Search results page - reads q from URL query param

use leptos::*;
use leptos_router::*;
use crate::api::*;
use crate::components::SearchProductCard;

/// Search results page component
#[component]
pub fn SearchPage() -> impl IntoView {
    let query = use_query_map();
    
    // Get 'q' from URL query param
    let search_query = create_memo(move |_| {
        query.with(|q| q.get("q").cloned().unwrap_or_default())
    });
    
    // create_resource keyed on q - refetches automatically when query changes
    let search_results = create_resource(
        move || search_query.get(),
        move |q| async move {
            if q.is_empty() {
                Err("Empty search query".to_string())
            } else {
                fetch_search_results(&q, None, 20).await
            }
        },
    );
    
    view! {
        <div class="search-page">
            <Suspense fallback=move || view! {
                <div class="loading">"Searching..."</div>
            }>
                {move || {
                    let _q = search_query.get();
                    search_results.get().map(|result| {
                        match result {
                            Ok(data) if data.results.is_empty() => view! {
                                <div class="no-results">
                                    <h2>"No results found"</h2>
                                    <p>{format!("No products found for \"{}\"", data.query)}</p>
                                </div>
                            }.into_view(),
                            Ok(data) => view! {
                                <div class="search-results">
                                    <div class="search-header">
                                        <h1>{format!("Search: \"{}\"", data.query)}</h1>
                                        <span class="results-count">
                                            {format!("{} results", data.total_hits)}
                                        </span>
                                        {move || {
                                            if data.used_semantic_fallback {
                                                view! {
                                                    <span class="semantic-badge">
                                                        "Semantic search used"
                                                    </span>
                                                }.into_view()
                                            } else {
                                                view! { <div/> }.into_view()
                                            }
                                        }}
                                    </div>
                                    <div class="search-results-list">
                                        {data.results.iter().enumerate().map(|(i, product)| {
                                            let p = product.clone();
                                            let position = (i + 1) as i32;
                                            view! { <SearchProductCard product=p position=position/> }
                                        }).collect_view()}
                                    </div>
                                </div>
                            }.into_view(),
                            Err(e) => view! {
                                <div class="error">
                                    <p>"Search failed"</p>
                                    <p class="error-detail">{e}</p>
                                </div>
                            }.into_view(),
                        }
                    })
                }}
            </Suspense>
        </div>
    }
}