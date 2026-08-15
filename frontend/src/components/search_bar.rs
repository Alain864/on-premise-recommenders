//! Search bar component - lives in top nav, present on all pages

use leptos::*;
use leptos_router::*;
use crate::api::*;

/// Search bar component with autocomplete support
#[component]
pub fn SearchBar() -> impl IntoView {
    let query = create_rw_signal(String::new());
    let show_suggestions = create_rw_signal(false);
    let navigate = StoredValue::new(use_navigate());
    
    // Fetch autocomplete suggestions
    let suggestions_resource = create_resource(
        move || query.get(),
        move |q| async move {
            if q.len() >= 2 {
                fetch_autocomplete_suggestions(&q, None, 5).await.ok()
            } else {
                None
            }
        },
    );
    
    // Handle form submission
    let on_submit = move |ev: ev::SubmitEvent| {
        ev.prevent_default();
        let q = query.get();
        if !q.is_empty() {
            navigate.with_value(|nav| {
                nav(&format!("/search?q={}", urlencoding::encode(&q)), Default::default());
            });
            show_suggestions.set(false);
        }
    };
    
    // Show condition check
    let should_show = move || {
        show_suggestions.get() && query.get().len() >= 2
    };
    
    view! {
        <form class="search-form" on:submit=on_submit>
            <div class="search-container">
                <input
                    type="text"
                    class="search-input"
                    placeholder="Search products..."
                    bind:value=query
                    on:focus=move |_| show_suggestions.set(true)
                    on:blur=move |_| {
                        let show = show_suggestions;
                        leptos::spawn_local(async move {
                            gloo_timers::future::TimeoutFuture::new(200).await;
                            show.set(false);
                        });
                    }
                />
                <button type="submit" class="search-button">
                    "Search"
                </button>
            </div>
            
            <Show when=should_show>
                <div class="suggestions-dropdown">
                    <Suspense fallback=move || view! { <div>"Loading..."</div> }>
                        <SuggestionList 
                            suggestions_resource=suggestions_resource
                            query=query
                            show_suggestions=show_suggestions
                        />
                    </Suspense>
                </div>
            </Show>
        </form>
    }
}

/// Separate component for suggestion list to handle lifetimes
#[component]
fn SuggestionList(
    suggestions_resource: Resource<String, Option<AutocompleteResponse>>,
    query: RwSignal<String>,
    show_suggestions: RwSignal<bool>,
) -> impl IntoView {
    let navigate = StoredValue::new(use_navigate());
    
    move || {
        suggestions_resource.get().map(|suggestions| {
            match suggestions {
                Some(s) if !s.suggestions.is_empty() => {
                    let items: Vec<_> = s.suggestions.iter().map(|sug| {
                        (sug.query_text.clone(), sug.category_match.clone())
                    }).collect();
                    
                    view! {
                        <ul class="suggestions-list">
                            {items.into_iter().map(|(text, category)| {
                                let q = query;
                                let nav = navigate;
                                let show = show_suggestions;
                                let text_for_click = text.clone();
                                view! {
                                    <li 
                                        class="suggestion-item"
                                        on:click=move |_| {
                                            q.set(text_for_click.clone());
                                            nav.with_value(|n| {
                                                n(&format!("/search?q={}", urlencoding::encode(&text_for_click)), Default::default());
                                            });
                                            show.set(false);
                                        }
                                    >
                                        <span class="suggestion-text">{&text}</span>
                                        {category.map(|cat| view! {
                                            <span class="suggestion-category">{cat}</span>
                                        })}
                                    </li>
                                }
                            }).collect_view()}
                        </ul>
                    }.into_view()
                },
                _ => view! { <div class="no-suggestions">"No suggestions"</div> }.into_view(),
            }
        })
    }
}