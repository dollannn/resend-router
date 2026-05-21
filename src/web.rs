use crate::{
    config::Config,
    db::{Db, EnqueueOutcome},
    error::AppError,
    resend::{parse_event_for_routing, verify_webhook_signature},
    router::RouteMatcher,
};
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use std::sync::Arc;
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: Db,
    pub routes: Arc<RouteMatcher>,
}

impl AppState {
    pub fn new(config: Arc<Config>, db: Db) -> Self {
        let routes = Arc::new(RouteMatcher::new(config.destinations.clone()));
        Self { config, db, routes }
    }
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/webhooks/resend", post(resend_webhook))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            std::time::Duration::from_secs(30),
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn healthz() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn readyz(State(state): State<AppState>) -> Result<StatusCode, AppError> {
    state.db.ping().await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn resend_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, AppError> {
    verify_webhook_signature(
        &state.config.resend_webhook_secret,
        state.config.resend_signature_tolerance_secs,
        &headers,
        &body,
    )?;

    let event = parse_event_for_routing(&body)?;
    let destinations = state.routes.match_destinations(&event);

    if destinations.is_empty() {
        tracing::info!(
            resend_event_id = event.event_id.as_deref(),
            resend_event_type = event.event_type.as_deref(),
            from_domain = event.from_domain.as_deref(),
            to_domains = ?event.to_domains,
            "dropping unmatched resend webhook"
        );
        return Ok(StatusCode::NO_CONTENT);
    }

    let matched_destination_names = destinations
        .iter()
        .map(|destination| destination.name.as_str())
        .collect::<Vec<_>>();
    let enqueue_outcome = state
        .db
        .enqueue_event(
            &headers,
            &body,
            &event,
            &destinations,
            state.config.retry_window(),
        )
        .await?;

    match enqueue_outcome {
        EnqueueOutcome::Inserted(event_row_id) => {
            tracing::info!(
                event_row_id = %event_row_id,
                resend_event_id = event.event_id.as_deref(),
                resend_event_type = event.event_type.as_deref(),
                from_domain = event.from_domain.as_deref(),
                matched_destinations = ?matched_destination_names,
                "accepted resend webhook for delivery"
            );
        }
        EnqueueOutcome::Duplicate => {
            tracing::info!(
                resend_event_id = event.event_id.as_deref(),
                resend_event_type = event.event_type.as_deref(),
                from_domain = event.from_domain.as_deref(),
                "duplicate resend webhook ignored"
            );
        }
    }

    Ok(StatusCode::ACCEPTED)
}
