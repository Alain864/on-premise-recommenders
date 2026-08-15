//! Product detail page with cross-sell and up-sell recommendations

use leptos::*;
use leptos_router::*;
use crate::api::*;
use crate::components::ProductCard;

/// Product detail page component
#[component]
pub fn ProductPage() -> impl IntoView {
    let params = use_params_map();
    let navigate = use_navigate();
    
    // Get product_id from URL path
    let product_id = create_memo(move |_| {
        params.with(|p| p.get("id").cloned().unwrap_or_default())
    });
    
    // Three parallel create_resource calls - product detail + recommendations
    let product_detail = create_resource(
        move || product_id.get(),
        move |id| async move {
            if id.is_empty() {
                Err("No product ID".to_string())
            } else {
                fetch_product_detail(&id).await
            }
        },
    );
    
    let bought_together = create_resource(
        move || product_id.get(),
        move |id| async move {
            if id.is_empty() {
                Err("No product ID".to_string())
            } else {
                fetch_frequently_bought_together(&id, 5).await
            }
        },
    );
    
    let also_viewed = create_resource(
        move || product_id.get(),
        move |id| async move {
            if id.is_empty() {
                Err("No product ID".to_string())
            } else {
                fetch_customers_also_viewed(&id, 5).await
            }
        },
    );
    
    // Add to cart action state
    let added_to_cart = create_rw_signal(false);
    let adding = create_rw_signal(false);
    
    // create_action posts to FastAPI /event
    let add_to_cart_action = create_action(move |_| {
        let pid = product_id.get();
        async move {
            adding.set(true);
            let event = EventRequest {
                user_id: None,
                feature: "product_page".to_string(),
                event_type: "add_to_cart".to_string(),
                product_ids: Some(vec![pid]),
                query_text: None,
                metadata: None,
            };
            let result = log_event(event).await;
            adding.set(false);
            if result.is_ok() {
                added_to_cart.set(true);
            }
            result
        }
    });
    
    // Handle add to cart click
    let on_add_to_cart = move |_| {
        if !added_to_cart.get() {
            add_to_cart_action.dispatch(());
        }
    };
    
    view! {
        <div class="product-page">
            <button 
                class="back-button" 
                on:click=move |_| { navigate("/", Default::default()); }
            >
                "← Back"
            </button>
            
            <div class="product-detail">
                // Product details section with suspense
                <Suspense fallback=move || view! {
                    <div class="loading">"Loading product details..."</div>
                }>
                    {move || {
                        product_detail.get().map(|result| {
                            match result {
                                Ok(product) => view! {
                                    <div class="product-main">
                                        <h1 class="product-title">{&product.title}</h1>
                                        <p class="product-brand">{&product.brand}</p>
                                        <p class="product-price">{format!("${:.2}", product.price)}</p>
                                        <span class="category-pill">{&product.category_path}</span>
                                        {product.description.as_ref().map(|desc| view! {
                                            <p class="product-description">{desc}</p>
                                        })}
                                        
                                        <button
                                            class=move || {
                                                if added_to_cart.get() {
                                                    "add-to-cart-btn added"
                                                } else if adding.get() {
                                                    "add-to-cart-btn loading"
                                                } else {
                                                    "add-to-cart-btn"
                                                }
                                            }
                                            on:click=on_add_to_cart
                                            disabled=move || adding.get() || added_to_cart.get()
                                        >
                                            {move || {
                                                if added_to_cart.get() {
                                                    "Added ✓".to_string()
                                                } else if adding.get() {
                                                    "Adding...".to_string()
                                                } else {
                                                    "Add to Cart".to_string()
                                                }
                                            }}
                                        </button>
                                    </div>
                                }.into_view(),
                                Err(e) => view! {
                                    <div class="error">{"Error loading product: "}{e}</div>
                                }.into_view(),
                            }
                        })
                    }}
                </Suspense>
                
                // Both render inside independent Suspense blocks
                // so they load independently
                <div class="recommendation-sections">
                    // Frequently Bought Together (Cross-sell)
                    <div class="recommendation-section">
                        <Suspense fallback=move || view! {
                            <div class="loading">"Loading frequently bought together..."</div>
                        }>
                            {move || {
                                bought_together.get().map(|result| {
                                    match result {
                                        Ok(data) if !data.recommendations.is_empty() => view! {
                                            <section class="cross-sell">
                                                <h2>"Frequently Bought Together"</h2>
                                                <div class="products-grid">
                                                    {data.recommendations.iter().map(|product| {
                                                        let p = product.clone();
                                                        view! { <ProductCard product=p position=None/> }
                                                    }).collect_view()}
                                                </div>
                                            </section>
                                        }.into_view(),
                                        Ok(_) => view! { <div/> }.into_view(),
                                        Err(e) => view! {
                                            <div class="error-small">{format!("Could not load: {}", e)}</div>
                                        }.into_view(),
                                    }
                                })
                            }}
                        </Suspense>
                    </div>
                    
                    // Customers Also Viewed (Up-sell)
                    <div class="recommendation-section">
                        <Suspense fallback=move || view! {
                            <div class="loading">"Loading customers also viewed..."</div>
                        }>
                            {move || {
                                also_viewed.get().map(|result| {
                                    match result {
                                        Ok(data) if !data.recommendations.is_empty() => view! {
                                            <section class="up-sell">
                                                <h2>"Customers Also Viewed"</h2>
                                                <div class="products-grid">
                                                    {data.recommendations.iter().map(|product| {
                                                        let p = product.clone();
                                                        view! { <ProductCard product=p position=None/> }
                                                    }).collect_view()}
                                                </div>
                                            </section>
                                        }.into_view(),
                                        Ok(_) => view! { <div/> }.into_view(),
                                        Err(e) => view! {
                                            <div class="error-small">{format!("Could not load: {}", e)}</div>
                                        }.into_view(),
                                    }
                                })
                            }}
                        </Suspense>
                    </div>
                </div>
            </div>
        </div>
    }
}