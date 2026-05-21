use crate::{
    config::Config,
    db::{ClaimedJob, Db, header_value_from_json},
    error::AppResult,
    signing::sign_delivery,
};
use reqwest::{Client, StatusCode, header};
use std::{sync::Arc, time::Duration as StdDuration};
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{
    sync::watch,
    task::JoinSet,
    time::{Instant, sleep},
};
use uuid::Uuid;

const IDLE_SLEEP: StdDuration = StdDuration::from_secs(1);
const MAX_RETRY_DELAY_SECS: u64 = 60 * 60;

pub async fn run_worker(
    db: Db,
    config: Arc<Config>,
    client: Client,
    worker_index: usize,
    mut shutdown: watch::Receiver<bool>,
) {
    let worker_id = format!("delivery-worker-{worker_index}-{}", Uuid::new_v4());
    tracing::info!(worker_id, "delivery worker started");

    loop {
        if *shutdown.borrow() {
            break;
        }

        let claim = db.claim_due_jobs(
            &worker_id,
            config.delivery_claim_limit,
            config.stale_delivery_lock_secs,
        );

        let claim_result = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
            result = claim => result,
        };

        match claim_result {
            Ok(jobs) if jobs.is_empty() => {
                if sleep_or_shutdown(&mut shutdown).await {
                    break;
                }
            }
            Ok(jobs) => {
                process_jobs(
                    db.clone(),
                    Arc::clone(&config),
                    client.clone(),
                    jobs,
                    &worker_id,
                )
                .await;
            }
            Err(error) => {
                tracing::error!(worker_id, error = %error, "delivery worker failed to claim jobs");
                if sleep_or_shutdown(&mut shutdown).await {
                    break;
                }
            }
        }
    }

    tracing::info!(worker_id, "delivery worker stopped");
}

async fn sleep_or_shutdown(shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = sleep(IDLE_SLEEP) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
    }
}

async fn process_jobs(
    db: Db,
    config: Arc<Config>,
    client: Client,
    jobs: Vec<ClaimedJob>,
    worker_id: &str,
) {
    let claimed_count = jobs.len();
    tracing::debug!(worker_id, claimed_count, "processing claimed delivery jobs");

    let mut join_set = JoinSet::new();
    for job in jobs {
        let db = db.clone();
        let config = Arc::clone(&config);
        let client = client.clone();

        join_set.spawn(async move { deliver_and_record(&db, &config, &client, job).await });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(worker_id, error = %error, "delivery worker failed to record job outcome");
            }
            Err(error) => {
                tracing::error!(worker_id, error = %error, "delivery task panicked");
            }
        }
    }
}

async fn deliver_and_record(
    db: &Db,
    config: &Config,
    client: &Client,
    job: ClaimedJob,
) -> AppResult<()> {
    if OffsetDateTime::now_utc() >= job.deadline_at {
        let error = "retry window expired before delivery attempt";
        match db.record_final_failure(&job, None, Some(error), 0).await? {
            Some(attempt) => {
                tracing::error!(
                    delivery_id = %job.id,
                    event_id = %job.event_id,
                    destination = %job.destination_name,
                    attempt,
                    deadline_at = %job.deadline_at,
                    "webhook delivery failed permanently after retry window"
                );
            }
            None => log_lost_lock(&job, "deadline-expired"),
        }
        return Ok(());
    }

    let started = Instant::now();
    let attempt = job.attempt_count + 1;
    let response = deliver(client, config, &job, attempt).await;
    let duration_ms = elapsed_millis(started);

    match response {
        Ok(status) if status.is_success() => {
            if db
                .record_success(&job, status.as_u16(), duration_ms)
                .await?
            {
                tracing::info!(
                    delivery_id = %job.id,
                    event_id = %job.event_id,
                    destination = %job.destination_name,
                    attempt,
                    status = status.as_u16(),
                    duration_ms,
                    "webhook delivery succeeded"
                );
            } else {
                log_lost_lock(&job, "success");
            }
        }
        Ok(status) => {
            record_failure(
                db,
                config,
                &job,
                Some(status.as_u16()),
                Some(format!("destination returned HTTP {}", status.as_u16())),
                duration_ms,
            )
            .await?;
        }
        Err(error) => {
            record_failure(db, config, &job, None, Some(error.to_string()), duration_ms).await?;
        }
    }

    Ok(())
}

async fn deliver(
    client: &Client,
    config: &Config,
    job: &ClaimedJob,
    attempt: i32,
) -> Result<StatusCode, reqwest::Error> {
    let timestamp = OffsetDateTime::now_utc().unix_timestamp().to_string();
    let delivery_id = job.id.to_string();
    let signature = sign_delivery(
        &config.router_signing_secret,
        &timestamp,
        &delivery_id,
        &job.event_id.to_string(),
        &job.destination_name,
        attempt,
        &job.raw_body,
    );

    let content_type = header_value_from_json(&job.headers, "content-type")
        .unwrap_or_else(|| "application/json".to_string());

    let request = client
        .post(&job.destination_url)
        .header(header::CONTENT_TYPE, content_type)
        .header("x-resend-router-delivery-id", delivery_id)
        .header("x-resend-router-event-id", job.event_id.to_string())
        .header("x-resend-router-attempt", attempt.to_string())
        .header("x-resend-router-destination", job.destination_name.as_str())
        .header("x-resend-router-timestamp", timestamp)
        .header("x-resend-router-signature", signature)
        .body(job.raw_body.clone());

    let response = request.send().await?;
    Ok(response.status())
}

async fn record_failure(
    db: &Db,
    config: &Config,
    job: &ClaimedJob,
    status_code: Option<u16>,
    error: Option<String>,
    duration_ms: i64,
) -> AppResult<()> {
    let now = OffsetDateTime::now_utc();
    let error_message = error.as_deref();

    if now >= job.deadline_at {
        let attempt = db
            .record_final_failure(job, status_code, error_message, duration_ms)
            .await?;
        let Some(attempt) = attempt else {
            log_lost_lock(job, "final-failure");
            return Ok(());
        };
        tracing::error!(
            delivery_id = %job.id,
            event_id = %job.event_id,
            destination = %job.destination_name,
            attempt,
            status = status_code,
            error = error_message,
            duration_ms,
            deadline_at = %job.deadline_at,
            "webhook delivery failed permanently after retry window"
        );
        return Ok(());
    }

    let delay = retry_delay(job.attempt_count + 1);
    let mut next_attempt_at = now + TimeDuration::try_from(delay).unwrap_or(TimeDuration::hours(1));
    if next_attempt_at > job.deadline_at {
        next_attempt_at = job.deadline_at;
    }

    let attempt = db
        .record_retry(
            job,
            status_code,
            error_message,
            duration_ms,
            next_attempt_at,
        )
        .await?;
    let Some(attempt) = attempt else {
        log_lost_lock(job, "retry");
        return Ok(());
    };

    if attempt >= config.warn_after_attempts {
        tracing::warn!(
            delivery_id = %job.id,
            event_id = %job.event_id,
            destination = %job.destination_name,
            attempt,
            status = status_code,
            error = error_message,
            duration_ms,
            next_attempt_at = %next_attempt_at,
            deadline_at = %job.deadline_at,
            "webhook delivery still failing; scheduled retry"
        );
    } else {
        tracing::info!(
            delivery_id = %job.id,
            event_id = %job.event_id,
            destination = %job.destination_name,
            attempt,
            status = status_code,
            next_attempt_at = %next_attempt_at,
            "webhook delivery failed; scheduled retry"
        );
    }

    Ok(())
}

fn log_lost_lock(job: &ClaimedJob, outcome: &str) {
    tracing::warn!(
        delivery_id = %job.id,
        event_id = %job.event_id,
        destination = %job.destination_name,
        outcome,
        "skipping delivery outcome because job lock was lost"
    );
}

pub fn base_retry_delay_secs(attempt_after_failure: i32) -> u64 {
    match attempt_after_failure {
        i32::MIN..=0 => 15,
        1 => 15,
        2 => 60,
        3 => 5 * 60,
        4 => 15 * 60,
        5 => 30 * 60,
        _ => MAX_RETRY_DELAY_SECS,
    }
}

pub fn retry_delay(attempt_after_failure: i32) -> StdDuration {
    let base = base_retry_delay_secs(attempt_after_failure);
    let jitter_window = base / 5;

    if jitter_window == 0 {
        return StdDuration::from_secs(base.max(1));
    }

    let random = fastrand::u64(0..=(jitter_window * 2));
    let offset = random as i64 - jitter_window as i64;
    let jittered = (base as i64 + offset).max(1) as u64;

    StdDuration::from_secs(jittered.min(MAX_RETRY_DELAY_SECS))
}

fn elapsed_millis(started: Instant) -> i64 {
    let millis = started.elapsed().as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::base_retry_delay_secs;

    #[test]
    fn retry_schedule_caps_at_one_hour() {
        assert_eq!(base_retry_delay_secs(1), 15);
        assert_eq!(base_retry_delay_secs(2), 60);
        assert_eq!(base_retry_delay_secs(3), 300);
        assert_eq!(base_retry_delay_secs(99), 3600);
    }
}
