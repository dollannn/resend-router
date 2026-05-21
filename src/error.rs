use axum::{http::StatusCode, response::IntoResponse};
use thiserror::Error;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("webhook signature verification failed: {0}")]
    Signature(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("database migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("http client error: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl AppError {
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    pub fn signature(message: impl Into<String>) -> Self {
        Self::Signature(message.into())
    }

    fn status_code(&self) -> StatusCode {
        match self {
            AppError::Config(_)
            | AppError::Database(_)
            | AppError::Migration(_)
            | AppError::HttpClient(_)
            | AppError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::BadRequest(_) | AppError::Signature(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn public_message(&self) -> &'static str {
        match self {
            AppError::Config(_)
            | AppError::Database(_)
            | AppError::Migration(_)
            | AppError::HttpClient(_)
            | AppError::Io(_) => "internal server error",
            AppError::BadRequest(_) => "bad request",
            AppError::Signature(_) => "invalid webhook signature",
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status_code();

        if status.is_server_error() {
            tracing::error!(error = %self, "request failed");
        } else {
            tracing::debug!(error = %self, "request rejected");
        }

        (status, self.public_message()).into_response()
    }
}
