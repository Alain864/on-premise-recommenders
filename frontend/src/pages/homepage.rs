//! Homepage with personalized recommendations

use leptos::*;
use crate::api::*;
use crate::components::ProductCard;

/// Homepage component - displays personalized recommendations
#[component]
pub fn HomePage() -> impl IntoView {
    // On mount, fetch homepage recommendations
    let recommendations = create_resource(
        || (),
        move |_| async move {
            // TODO: Get user_id from session/cookie
            fetch_homepage_recommendations(None, 3, 10).await
        },
    );
    
    view! {
        <div class="homepage">
            <Suspense fallback=move || view! {
                <div class="loading">
                    "Loading recommendations..."
                </div>
            }>
                {move || {
                    recommendations.get().map(|result| {
                        match result {
                            Ok(data) => view! {
                                <div class="recommendations">
                                    <h1 class="homepage-title">
                                        {if data.is_personalized {
                                            format!("Welcome back, {}", data.user_id)
                                        } else {
                                            "Trending Products".to_string()
                                        }}
                                    </h1>
                                    {data.rows.iter().map(|row| view! {
                                        <section class="recommendation-row">
                                            <h2 class="row-label">{&row.row_label}</h2>
                                            <div class="products-grid">
                                                {row.products.iter().map(|product| {
                                                    let p = product.clone();
                                                    view! { <ProductCard product=p position=None/> }
                                                }).collect_view()}
                                            </div>
                                        </section>
                                    }).collect_view()}
                                </div>
                            }.into_view(),
                            Err(e) => view! {
                                <div class="error">
                                    <p>"Failed to load recommendations"</p>
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