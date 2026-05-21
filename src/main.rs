use resend_router::{config::Config, db::Db, delivery, error::AppResult, web};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio::{net::TcpListener, sync::watch};
use tracing_subscriber::EnvFilter;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[tokio::main]
async fn main() -> AppResult<()> {
    init_tracing();

    let config = Arc::new(Config::from_env()?);
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .connect(&config.database_url)
        .await?;
    MIGRATOR.run(&pool).await?;

    let db = Db::new(pool);
    let client = reqwest::Client::builder()
        .timeout(config.delivery_timeout())
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut worker_handles = Vec::with_capacity(config.delivery_worker_count);

    for worker_index in 0..config.delivery_worker_count {
        worker_handles.push(tokio::spawn(delivery::run_worker(
            db.clone(),
            Arc::clone(&config),
            client.clone(),
            worker_index,
            shutdown_rx.clone(),
        )));
    }

    let listener = TcpListener::bind(config.server_bind).await?;
    tracing::info!(bind = %config.server_bind, "resend-router listening");

    axum::serve(
        listener,
        web::app(web::AppState::new(Arc::clone(&config), db)),
    )
    .with_graceful_shutdown(shutdown_signal(shutdown_tx.clone()))
    .await?;

    let _ = shutdown_tx.send(true);
    for handle in worker_handles {
        match tokio::time::timeout(config.worker_drain_timeout(), handle).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::error!(%error, "delivery worker join failed"),
            Err(_) => tracing::warn!(
                timeout_secs = config.worker_drain_timeout_secs,
                "timed out waiting for delivery worker shutdown"
            ),
        }
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,resend_router=debug,tower_http=info"));

    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn shutdown_signal(shutdown_tx: watch::Sender<bool>) {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "failed to install SIGTERM handler");
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received");
    let _ = shutdown_tx.send(true);
}
