//! On-Premise Recommenders Frontend
//! A Leptos-based minimal frontend for the recommendation system

pub mod api;
pub mod components;
pub mod pages;

use leptos::*;
use leptos_router::*;
use leptos_meta::*;
use components::SearchBar;
use pages::{HomePage, SearchPage, ProductPage};

/// Top navigation bar with search - present on all pages
#[component]
pub fn TopNav() -> impl IntoView {
    view! {
        <nav class="top-nav">
            <div class="nav-content">
                <A href="/" class="logo">
                    "🛒 Recommender"
                </A>
                <SearchBar/>
            </div>
        </nav>
    }
}

/// Main application component with routing
#[component]
pub fn App() -> impl IntoView {
    provide_meta_context();
    
    view! {
        <Stylesheet id="leptos" href="/style.css"/>
        <Title text="On-Premise Recommender"/>
        
        <Router>
            <div class="app">
                <TopNav/>
                <main class="main-content">
                    <Routes>
                        <Route path="/" view=HomePage/>
                        <Route path="/search" view=SearchPage/>
                        <Route path="/product/:id" view=ProductPage/>
                    </Routes>
                </main>
                <footer class="footer">
                    <p>"On-Premise Recommender System - Stage 7"</p>
                </footer>
            </div>
        </Router>
    }
}