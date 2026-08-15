//! Entry point for the Leptos frontend application

use leptos::*;
use onprem_recommenders_frontend::App;

fn main() {
    console_error_panic_hook::set_once();
    
    mount_to_body(|| {
        view! { <App/> }
    });
}