//! Minimal GPUI app driven by an swr query: a "system status" key polled
//! every 2 seconds; the view observes the query entity and re-renders only
//! when it changes.
//!
//! Run with: `cargo run -p swr-gpui --example status`

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use gpui::{
    App, Application, Bounds, Context, Window, WindowBounds, WindowOptions, div, prelude::*, px,
    rgb, size,
};
use swr_core::{QueryOptions, SwrClient};
use swr_gpui::Query;

struct StatusView {
    query: Query<String, String>,
}

impl StatusView {
    fn new(client: &SwrClient, cx: &mut Context<Self>) -> Self {
        // A fake upstream: each poll returns a new revision. `subscribe_eq`
        // would keep the Arc stable when content does not change (D-30).
        let polls = Arc::new(AtomicU32::new(0));
        let handle = client.subscribe(
            ("status",),
            move |_key: (&'static str,)| {
                let polls = Arc::clone(&polls);
                async move {
                    let n = polls.fetch_add(1, Ordering::SeqCst) + 1;
                    Ok::<String, String>(format!("all systems nominal (rev {n})"))
                }
            },
            QueryOptions {
                refresh_interval: Some(Duration::from_secs(2)),
                ..QueryOptions::default()
            },
        );
        let query = Query::new(client, handle, cx);
        cx.observe(query.state(), |_, _, cx| cx.notify()).detach();
        Self { query }
    }
}

impl Render for StatusView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let state = self.query.read(cx);
        let text = match (&state.data, &state.error) {
            (Some(status), _) => format!("status: {status}"),
            (None, Some(error)) => format!("error: {error}"),
            (None, None) => "loading...".to_string(),
        };
        div()
            .flex()
            .flex_col()
            .gap_2()
            .bg(rgb(0x1e1e2e))
            .size(px(420.0))
            .justify_center()
            .items_center()
            .text_color(rgb(0xcdd6f4))
            .child(text)
            .child(if state.is_validating {
                "refreshing..."
            } else {
                ""
            })
    }
}

fn main() {
    Application::new().run(|cx: &mut App| {
        let client = swr_gpui::client(cx);
        let bounds = Bounds::centered(None, size(px(420.0), px(420.0)), cx);
        cx.open_window(
            WindowOptions {
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                ..Default::default()
            },
            |_, cx| cx.new(|cx| StatusView::new(&client, cx)),
        )
        .unwrap();
    });
}
