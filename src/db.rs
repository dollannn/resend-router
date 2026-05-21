use crate::{
    config::DestinationConfig,
    error::{AppError, AppResult},
    resend::ResendEventForRouting,
};
use axum::http::HeaderMap;
use bytes::Bytes;
use serde_json::{Map, Value};
use sqlx::{FromRow, PgPool, Postgres, Transaction, types::Json};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Db {
    pool: PgPool,
}

#[derive(Debug, Clone, FromRow)]
pub struct ClaimedJob {
    pub id: Uuid,
    pub event_id: Uuid,
    pub destination_name: String,
    pub destination_url: String,
    pub attempt_count: i32,
    pub deadline_at: OffsetDateTime,
    pub locked_by: String,
    pub raw_body: Vec<u8>,
    pub headers: Json<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Inserted(Uuid),
    Duplicate,
}

impl Db {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn ping(&self) -> AppResult<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    pub async fn enqueue_event(
        &self,
        headers: &HeaderMap,
        body: &Bytes,
        event: &ResendEventForRouting,
        destinations: &[DestinationConfig],
        retry_window: Duration,
    ) -> AppResult<EnqueueOutcome> {
        let event_row_id = Uuid::new_v4();
        let now = OffsetDateTime::now_utc();
        let deadline_at = now + retry_window;
        let headers_json = headers_to_json(headers);
        let source_svix_id = header_to_string(headers, "svix-id")
            .ok_or_else(|| AppError::bad_request("missing svix-id"))?;
        let source_svix_timestamp = header_to_string(headers, "svix-timestamp")
            .ok_or_else(|| AppError::bad_request("missing svix-timestamp"))?;
        let mut transaction = self.pool.begin().await?;

        let inserted_event_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO webhook_events (
                id,
                source_svix_id,
                source_svix_timestamp,
                received_at,
                resend_event_id,
                resend_event_type,
                raw_body,
                headers,
                matched_destination_count
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (source_svix_id) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(event_row_id)
        .bind(source_svix_id)
        .bind(source_svix_timestamp)
        .bind(now)
        .bind(event.event_id.as_deref())
        .bind(event.event_type.as_deref())
        .bind(body.to_vec())
        .bind(Json(headers_json))
        .bind(destinations.len() as i32)
        .fetch_optional(&mut *transaction)
        .await?;

        let Some(event_row_id) = inserted_event_id else {
            transaction.rollback().await?;
            return Ok(EnqueueOutcome::Duplicate);
        };

        for destination in destinations {
            insert_delivery_job(
                &mut transaction,
                event_row_id,
                destination,
                now,
                deadline_at,
            )
            .await?;
        }

        transaction.commit().await?;
        Ok(EnqueueOutcome::Inserted(event_row_id))
    }

    pub async fn claim_due_jobs(
        &self,
        worker_id: &str,
        limit: i64,
        stale_lock_secs: i64,
    ) -> AppResult<Vec<ClaimedJob>> {
        let jobs = sqlx::query_as::<_, ClaimedJob>(
            r#"
            WITH picked AS (
                SELECT id
                FROM delivery_jobs
                WHERE (
                    status IN ('queued', 'retrying')
                    AND next_attempt_at <= now()
                ) OR (
                    status = 'delivering'
                    AND locked_at IS NOT NULL
                    AND locked_at < now() - ($2::double precision * interval '1 second')
                )
                ORDER BY next_attempt_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE delivery_jobs AS job
            SET
                status = 'delivering',
                locked_at = now(),
                locked_by = $3,
                updated_at = now()
            FROM picked
            WHERE job.id = picked.id
            RETURNING
                job.id,
                job.event_id,
                job.destination_name,
                job.destination_url,
                job.attempt_count,
                job.deadline_at,
                job.locked_by,
                (SELECT event.raw_body FROM webhook_events event WHERE event.id = job.event_id) AS raw_body,
                (SELECT event.headers FROM webhook_events event WHERE event.id = job.event_id) AS headers
            "#,
        )
        .bind(limit)
        .bind(stale_lock_secs as f64)
        .bind(worker_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(jobs)
    }

    pub async fn record_success(
        &self,
        job: &ClaimedJob,
        status_code: u16,
        duration_ms: i64,
    ) -> AppResult<bool> {
        let now = OffsetDateTime::now_utc();
        let attempt_number = job.attempt_count + 1;
        let mut transaction = self.pool.begin().await?;

        insert_delivery_attempt(
            &mut transaction,
            job.id,
            attempt_number,
            now,
            Some(status_code as i32),
            None,
            duration_ms,
            false,
            None,
        )
        .await?;

        let update_result = sqlx::query(
            r#"
            UPDATE delivery_jobs
            SET
                status = 'succeeded',
                attempt_count = $2,
                last_attempt_at = $3,
                last_status_code = $4,
                last_error = NULL,
                completed_at = $3,
                locked_at = NULL,
                locked_by = NULL,
                updated_at = $3
            WHERE id = $1
              AND locked_by = $5
              AND status = 'delivering'
            "#,
        )
        .bind(job.id)
        .bind(attempt_number)
        .bind(now)
        .bind(status_code as i32)
        .bind(&job.locked_by)
        .execute(&mut *transaction)
        .await?;

        if update_result.rows_affected() == 0 {
            transaction.rollback().await?;
            return Ok(false);
        }

        transaction.commit().await?;
        Ok(true)
    }

    pub async fn record_retry(
        &self,
        job: &ClaimedJob,
        status_code: Option<u16>,
        error: Option<&str>,
        duration_ms: i64,
        next_attempt_at: OffsetDateTime,
    ) -> AppResult<Option<i32>> {
        let now = OffsetDateTime::now_utc();
        let attempt_number = job.attempt_count + 1;
        let mut transaction = self.pool.begin().await?;

        insert_delivery_attempt(
            &mut transaction,
            job.id,
            attempt_number,
            now,
            status_code.map(i32::from),
            error,
            duration_ms,
            true,
            Some(next_attempt_at),
        )
        .await?;

        let update_result = sqlx::query(
            r#"
            UPDATE delivery_jobs
            SET
                status = 'retrying',
                attempt_count = $2,
                next_attempt_at = $3,
                last_attempt_at = $4,
                last_status_code = $5,
                last_error = $6,
                locked_at = NULL,
                locked_by = NULL,
                updated_at = $4
            WHERE id = $1
              AND locked_by = $7
              AND status = 'delivering'
            "#,
        )
        .bind(job.id)
        .bind(attempt_number)
        .bind(next_attempt_at)
        .bind(now)
        .bind(status_code.map(i32::from))
        .bind(error)
        .bind(&job.locked_by)
        .execute(&mut *transaction)
        .await?;

        if update_result.rows_affected() == 0 {
            transaction.rollback().await?;
            return Ok(None);
        }

        transaction.commit().await?;
        Ok(Some(attempt_number))
    }

    pub async fn record_final_failure(
        &self,
        job: &ClaimedJob,
        status_code: Option<u16>,
        error: Option<&str>,
        duration_ms: i64,
    ) -> AppResult<Option<i32>> {
        let now = OffsetDateTime::now_utc();
        let attempt_number = job.attempt_count + 1;
        let mut transaction = self.pool.begin().await?;

        insert_delivery_attempt(
            &mut transaction,
            job.id,
            attempt_number,
            now,
            status_code.map(i32::from),
            error,
            duration_ms,
            false,
            None,
        )
        .await?;

        let update_result = sqlx::query(
            r#"
            UPDATE delivery_jobs
            SET
                status = 'failed',
                attempt_count = $2,
                last_attempt_at = $3,
                last_status_code = $4,
                last_error = $5,
                completed_at = $3,
                locked_at = NULL,
                locked_by = NULL,
                updated_at = $3
            WHERE id = $1
              AND locked_by = $6
              AND status = 'delivering'
            "#,
        )
        .bind(job.id)
        .bind(attempt_number)
        .bind(now)
        .bind(status_code.map(i32::from))
        .bind(error)
        .bind(&job.locked_by)
        .execute(&mut *transaction)
        .await?;

        if update_result.rows_affected() == 0 {
            transaction.rollback().await?;
            return Ok(None);
        }

        transaction.commit().await?;
        Ok(Some(attempt_number))
    }
}

async fn insert_delivery_job(
    transaction: &mut Transaction<'_, Postgres>,
    event_id: Uuid,
    destination: &DestinationConfig,
    now: OffsetDateTime,
    deadline_at: OffsetDateTime,
) -> AppResult<()> {
    sqlx::query(
        r#"
        INSERT INTO delivery_jobs (
            id,
            event_id,
            destination_name,
            destination_url,
            status,
            attempt_count,
            next_attempt_at,
            deadline_at,
            created_at,
            updated_at
        ) VALUES ($1, $2, $3, $4, 'queued', 0, $5, $6, $5, $5)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(event_id)
    .bind(&destination.name)
    .bind(&destination.url)
    .bind(now)
    .bind(deadline_at)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_delivery_attempt(
    transaction: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    attempt_number: i32,
    attempted_at: OffsetDateTime,
    status_code: Option<i32>,
    error: Option<&str>,
    duration_ms: i64,
    will_retry: bool,
    next_attempt_at: Option<OffsetDateTime>,
) -> AppResult<()> {
    sqlx::query(
        r#"
        INSERT INTO delivery_attempts (
            id,
            job_id,
            attempt_number,
            attempted_at,
            status_code,
            error,
            duration_ms,
            will_retry,
            next_attempt_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(job_id)
    .bind(attempt_number)
    .bind(attempted_at)
    .bind(status_code)
    .bind(error)
    .bind(duration_ms as i32)
    .bind(will_retry)
    .bind(next_attempt_at)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

pub fn headers_to_json(headers: &HeaderMap) -> Value {
    let mut map = Map::new();

    for (name, value) in headers.iter() {
        let header_name = name.as_str().to_ascii_lowercase();
        let header_value = value
            .to_str()
            .map(ToString::to_string)
            .unwrap_or_else(|_| "<non-utf8>".to_string());
        insert_header_value(&mut map, header_name, header_value);
    }

    Value::Object(map)
}

pub fn header_value_from_json(headers: &Json<Value>, name: &str) -> Option<String> {
    let key = name.to_ascii_lowercase();
    let value = headers.0.get(&key)?;

    match value {
        Value::String(value) => Some(value.clone()),
        Value::Array(values) => values
            .first()
            .and_then(Value::as_str)
            .map(ToString::to_string),
        _ => None,
    }
}

fn header_to_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn insert_header_value(map: &mut Map<String, Value>, key: String, value: String) {
    match map.get_mut(&key) {
        Some(Value::Array(values)) => values.push(Value::String(value)),
        Some(existing) => {
            let previous = existing.take();
            *existing = Value::Array(vec![previous, Value::String(value)]);
        }
        None => {
            map.insert(key, Value::String(value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{header_value_from_json, headers_to_json};
    use axum::http::{HeaderMap, HeaderValue};
    use sqlx::types::Json;

    #[test]
    fn serializes_headers_case_insensitively() {
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));

        let json = Json(headers_to_json(&headers));

        assert_eq!(
            header_value_from_json(&json, "content-type").as_deref(),
            Some("application/json")
        );
    }
}
