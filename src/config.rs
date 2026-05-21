use crate::error::{AppError, AppResult};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, env, net::SocketAddr, time::Duration as StdDuration};
use time::Duration;
use url::Url;

const THREE_DAYS_SECS: u64 = 3 * 24 * 60 * 60;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub database_max_connections: u32,
    pub server_bind: SocketAddr,
    pub resend_webhook_secret: String,
    pub resend_signature_tolerance_secs: i64,
    pub router_signing_secret: String,
    pub destinations: Vec<DestinationConfig>,
    pub delivery_worker_count: usize,
    pub delivery_claim_limit: i64,
    pub delivery_timeout_secs: u64,
    pub retry_window_secs: u64,
    pub warn_after_attempts: i32,
    pub stale_delivery_lock_secs: i64,
    pub worker_drain_timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DestinationConfig {
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub from_domains: Vec<String>,
    #[serde(default)]
    pub to_domains: Vec<String>,
    #[serde(default)]
    pub event_types: Vec<String>,
    #[serde(default)]
    pub catch_all: bool,
}

impl Config {
    pub fn from_env() -> AppResult<Self> {
        let database_url = required_env("DATABASE_URL")?;
        let database_max_connections = parse_env("DATABASE_MAX_CONNECTIONS", 5_u32)?;
        let server_bind = parse_bind_addr()?;
        let resend_webhook_secret = required_env("RESEND_WEBHOOK_SECRET")?;
        let resend_signature_tolerance_secs =
            parse_env("RESEND_SIGNATURE_TOLERANCE_SECS", 300_i64)?;
        let router_signing_secret = required_env("ROUTER_SIGNING_SECRET")?;
        let destinations = parse_destinations()?;
        let delivery_worker_count = parse_env("DELIVERY_WORKERS", 2_usize)?;
        let delivery_claim_limit = parse_env("DELIVERY_CLAIM_LIMIT", 5_i64)?;
        let delivery_timeout_secs = parse_env("DELIVERY_TIMEOUT_SECS", 20_u64)?;
        let retry_window_secs = parse_env("RETRY_WINDOW_SECS", THREE_DAYS_SECS)?;
        let warn_after_attempts = parse_env("WARN_AFTER_ATTEMPTS", 10_i32)?;
        let stale_delivery_lock_secs = parse_env("STALE_DELIVERY_LOCK_SECS", 5 * 60_i64)?;
        let worker_drain_timeout_secs = parse_env("WORKER_DRAIN_TIMEOUT_SECS", 30_u64)?;

        let config = Self {
            database_url,
            database_max_connections,
            server_bind,
            resend_webhook_secret,
            resend_signature_tolerance_secs,
            router_signing_secret,
            destinations,
            delivery_worker_count,
            delivery_claim_limit,
            delivery_timeout_secs,
            retry_window_secs,
            warn_after_attempts,
            stale_delivery_lock_secs,
            worker_drain_timeout_secs,
        };

        config.validate()?;
        Ok(config)
    }

    pub fn retry_window(&self) -> Duration {
        Duration::seconds(self.retry_window_secs as i64)
    }

    pub fn delivery_timeout(&self) -> StdDuration {
        StdDuration::from_secs(self.delivery_timeout_secs)
    }

    pub fn worker_drain_timeout(&self) -> StdDuration {
        StdDuration::from_secs(self.worker_drain_timeout_secs)
    }

    fn validate(&self) -> AppResult<()> {
        if self.destinations.is_empty() {
            return Err(AppError::config(
                "DESTINATIONS_JSON must contain at least one destination",
            ));
        }

        if self.router_signing_secret.len() < 32 {
            return Err(AppError::config(
                "ROUTER_SIGNING_SECRET should be at least 32 characters",
            ));
        }

        if self.delivery_worker_count == 0 {
            return Err(AppError::config(
                "DELIVERY_WORKERS must be greater than zero",
            ));
        }

        if self.database_max_connections == 0 {
            return Err(AppError::config(
                "DATABASE_MAX_CONNECTIONS must be greater than zero",
            ));
        }

        if self.delivery_claim_limit <= 0 {
            return Err(AppError::config(
                "DELIVERY_CLAIM_LIMIT must be greater than zero",
            ));
        }

        if self.delivery_claim_limit > 100 {
            return Err(AppError::config(
                "DELIVERY_CLAIM_LIMIT must be 100 or lower",
            ));
        }

        if self.delivery_timeout_secs == 0 {
            return Err(AppError::config(
                "DELIVERY_TIMEOUT_SECS must be greater than zero",
            ));
        }

        if self.retry_window_secs == 0 {
            return Err(AppError::config(
                "RETRY_WINDOW_SECS must be greater than zero",
            ));
        }

        if self.resend_signature_tolerance_secs <= 0 {
            return Err(AppError::config(
                "RESEND_SIGNATURE_TOLERANCE_SECS must be positive",
            ));
        }

        if self.warn_after_attempts <= 0 {
            return Err(AppError::config("WARN_AFTER_ATTEMPTS must be positive"));
        }

        if self.stale_delivery_lock_secs <= 0 {
            return Err(AppError::config(
                "STALE_DELIVERY_LOCK_SECS must be positive",
            ));
        }

        if self.stale_delivery_lock_secs <= self.delivery_timeout_secs as i64 + 5 {
            return Err(AppError::config(
                "STALE_DELIVERY_LOCK_SECS must be at least DELIVERY_TIMEOUT_SECS + 5",
            ));
        }

        if self.worker_drain_timeout_secs == 0 {
            return Err(AppError::config(
                "WORKER_DRAIN_TIMEOUT_SECS must be positive",
            ));
        }

        let mut names = HashSet::new();
        for destination in &self.destinations {
            destination.validate()?;
            if !names.insert(destination.name.clone()) {
                return Err(AppError::config(format!(
                    "duplicate destination name `{}`",
                    destination.name
                )));
            }
        }

        Ok(())
    }
}

impl DestinationConfig {
    fn validate(&self) -> AppResult<()> {
        if self.name.trim().is_empty() {
            return Err(AppError::config("destination name cannot be empty"));
        }

        if !self.name.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        }) {
            return Err(AppError::config(format!(
                "destination `{}` name may only contain ASCII letters, numbers, dots, dashes, and underscores",
                self.name
            )));
        }

        let url = Url::parse(&self.url).map_err(|error| {
            AppError::config(format!(
                "destination `{}` has invalid url: {error}",
                self.name
            ))
        })?;
        match url.scheme() {
            "http" | "https" => {}
            scheme => {
                return Err(AppError::config(format!(
                    "destination `{}` uses unsupported url scheme `{scheme}`",
                    self.name
                )));
            }
        }

        let has_criteria = self.catch_all
            || !self.from_domains.is_empty()
            || !self.to_domains.is_empty()
            || !self.event_types.is_empty();
        if !has_criteria {
            return Err(AppError::config(format!(
                "destination `{}` needs at least one match criterion or catch_all=true",
                self.name
            )));
        }

        validate_domains(&self.name, "from_domains", &self.from_domains)?;
        validate_domains(&self.name, "to_domains", &self.to_domains)?;

        if self
            .event_types
            .iter()
            .any(|event_type| event_type.trim().is_empty())
        {
            return Err(AppError::config(format!(
                "destination `{}` has an empty event type",
                self.name
            )));
        }

        Ok(())
    }
}

fn validate_domains(destination_name: &str, field: &str, domains: &[String]) -> AppResult<()> {
    for domain in domains {
        if domain
            .trim()
            .trim_start_matches('@')
            .trim_end_matches('.')
            .is_empty()
        {
            return Err(AppError::config(format!(
                "destination `{destination_name}` has an empty {field} entry"
            )));
        }
    }

    Ok(())
}

fn required_env(key: &str) -> AppResult<String> {
    env::var(key).map_err(|_| AppError::config(format!("missing required env var {key}")))
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.trim().is_empty())
}

fn parse_env<T>(key: &str, default: T) -> AppResult<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match optional_env(key) {
        Some(value) => value
            .parse::<T>()
            .map_err(|error| AppError::config(format!("invalid {key}: {error}"))),
        None => Ok(default),
    }
}

fn parse_bind_addr() -> AppResult<SocketAddr> {
    if let Some(bind) = optional_env("SERVER_BIND") {
        return bind
            .parse::<SocketAddr>()
            .map_err(|error| AppError::config(format!("invalid SERVER_BIND: {error}")));
    }

    let port = optional_env("PORT").unwrap_or_else(|| "3000".to_string());
    format!("0.0.0.0:{port}")
        .parse::<SocketAddr>()
        .map_err(|error| AppError::config(format!("invalid PORT: {error}")))
}

fn parse_destinations() -> AppResult<Vec<DestinationConfig>> {
    let raw = optional_env("DESTINATIONS_JSON")
        .or_else(|| optional_env("ROUTES_JSON"))
        .ok_or_else(|| AppError::config("missing DESTINATIONS_JSON"))?;

    serde_json::from_str::<Vec<DestinationConfig>>(&raw)
        .map_err(|error| AppError::config(format!("invalid DESTINATIONS_JSON: {error}")))
}

#[cfg(test)]
mod tests {
    use super::DestinationConfig;

    #[test]
    fn destination_requires_criteria() {
        let destination = DestinationConfig {
            name: "empty".to_string(),
            url: "https://example.com/webhook".to_string(),
            from_domains: vec![],
            to_domains: vec![],
            event_types: vec![],
            catch_all: false,
        };

        assert!(destination.validate().is_err());
    }
}
