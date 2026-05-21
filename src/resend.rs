use crate::error::{AppError, AppResult};
use axum::http::HeaderMap;
use base64::{Engine as _, engine::general_purpose};
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use time::OffsetDateTime;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResendEventForRouting {
    pub event_id: Option<String>,
    pub event_type: Option<String>,
    pub from_domain: Option<String>,
    pub to_domains: Vec<String>,
}

pub fn verify_webhook_signature(
    secret: &str,
    tolerance_secs: i64,
    headers: &HeaderMap,
    body: &[u8],
) -> AppResult<()> {
    let svix_id = header(headers, "svix-id")?;
    let svix_timestamp = header(headers, "svix-timestamp")?;
    let svix_signature = header(headers, "svix-signature")?;

    verify_timestamp(svix_timestamp, tolerance_secs)?;

    let secret = secret.strip_prefix("whsec_").unwrap_or(secret);
    let key = decode_base64(secret)
        .map_err(|_| AppError::signature("webhook secret is not valid base64"))?;

    let mut signed_content =
        Vec::with_capacity(svix_id.len() + svix_timestamp.len() + body.len() + 2);
    signed_content.extend_from_slice(svix_id.as_bytes());
    signed_content.push(b'.');
    signed_content.extend_from_slice(svix_timestamp.as_bytes());
    signed_content.push(b'.');
    signed_content.extend_from_slice(body);

    let mut mac = HmacSha256::new_from_slice(&key)
        .map_err(|_| AppError::signature("unable to initialize signature verifier"))?;
    mac.update(&signed_content);
    let expected = mac.finalize().into_bytes();

    for candidate in svix_signature.split_whitespace() {
        let Some(signature) = candidate.strip_prefix("v1,") else {
            continue;
        };

        if let Ok(decoded) = decode_base64(signature)
            && decoded.as_slice().ct_eq(expected.as_slice()).into()
        {
            return Ok(());
        }
    }

    Err(AppError::signature("no matching v1 signature"))
}

pub fn parse_event_for_routing(body: &[u8]) -> AppResult<ResendEventForRouting> {
    let value = serde_json::from_slice::<Value>(body)
        .map_err(|error| AppError::bad_request(format!("invalid json payload: {error}")))?;

    let data = value.get("data").unwrap_or(&Value::Null);
    let event_id = value
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| data.get("email_id").and_then(Value::as_str))
        .or_else(|| data.get("id").and_then(Value::as_str))
        .map(ToString::to_string);
    let event_type = value
        .get("type")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let from_domain = data
        .get("from")
        .and_then(Value::as_str)
        .and_then(extract_email_domain);
    let to_domains = extract_domains_from_value(data.get("to"));

    Ok(ResendEventForRouting {
        event_id,
        event_type,
        from_domain,
        to_domains,
    })
}

pub fn extract_email_domain(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }

    let candidate = if let Some(start) = trimmed.find('<') {
        let after_start = &trimmed[start + 1..];
        if let Some(end) = after_start.find('>') {
            &after_start[..end]
        } else {
            after_start
        }
    } else {
        trimmed
    };

    let candidate = candidate.trim().trim_matches('"').trim_matches('\'');
    let at = candidate.rfind('@')?;
    let domain = candidate[at + 1..]
        .trim()
        .trim_matches('>')
        .trim_matches(';')
        .split(|character: char| character == ',' || character.is_whitespace())
        .next()?
        .trim()
        .trim_end_matches('.')
        .to_ascii_lowercase();

    if domain.is_empty() || domain.contains('@') {
        None
    } else {
        Some(domain)
    }
}

fn extract_domains_from_value(value: Option<&Value>) -> Vec<String> {
    let Some(value) = value else {
        return vec![];
    };

    match value {
        Value::String(address) => extract_email_domain(address).into_iter().collect(),
        Value::Array(addresses) => addresses
            .iter()
            .filter_map(Value::as_str)
            .filter_map(extract_email_domain)
            .collect(),
        _ => vec![],
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> AppResult<&'a str> {
    headers
        .get(name)
        .ok_or_else(|| AppError::signature(format!("missing {name}")))?
        .to_str()
        .map_err(|_| AppError::signature(format!("{name} is not valid utf-8")))
}

fn verify_timestamp(timestamp: &str, tolerance_secs: i64) -> AppResult<()> {
    if tolerance_secs <= 0 {
        return Err(AppError::signature("signature tolerance must be positive"));
    }

    let timestamp = timestamp
        .parse::<i64>()
        .map_err(|_| AppError::signature("svix-timestamp is not an integer"))?;
    let now = OffsetDateTime::now_utc().unix_timestamp();
    let age = now.abs_diff(timestamp);

    if age > tolerance_secs as u64 {
        return Err(AppError::signature("svix-timestamp is outside tolerance"));
    }

    Ok(())
}

fn decode_base64(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
    general_purpose::STANDARD
        .decode(input)
        .or_else(|_| general_purpose::STANDARD_NO_PAD.decode(input))
        .or_else(|_| general_purpose::URL_SAFE.decode(input))
        .or_else(|_| general_purpose::URL_SAFE_NO_PAD.decode(input))
}

#[cfg(test)]
mod tests {
    use super::{extract_email_domain, parse_event_for_routing, verify_webhook_signature};
    use axum::http::{HeaderMap, HeaderValue};
    use base64::{Engine as _, engine::general_purpose};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use time::OffsetDateTime;

    type HmacSha256 = Hmac<Sha256>;

    #[test]
    fn extracts_domain_from_resend_from_header() {
        assert_eq!(
            extract_email_domain("Example <hello@mail.example.com>"),
            Some("mail.example.com".to_string())
        );
        assert_eq!(
            extract_email_domain("sender@example.com"),
            Some("example.com".to_string())
        );
    }

    #[test]
    fn parses_routing_fields() {
        let payload = br#"{
            "type": "email.delivered",
            "data": {
                "email_id": "email_123",
                "from": "Example <hello@example.com>",
                "to": ["Person <person@customer.com>"]
            }
        }"#;

        let event = parse_event_for_routing(payload).unwrap();

        assert_eq!(event.event_id.as_deref(), Some("email_123"));
        assert_eq!(event.event_type.as_deref(), Some("email.delivered"));
        assert_eq!(event.from_domain.as_deref(), Some("example.com"));
        assert_eq!(event.to_domains, vec!["customer.com"]);
    }

    #[test]
    fn verifies_svix_signature() {
        let key = b"a very secret webhook key";
        let secret = format!("whsec_{}", general_purpose::STANDARD.encode(key));
        let body = br#"{"test":true}"#;
        let svix_id = "msg_test";
        let timestamp = OffsetDateTime::now_utc().unix_timestamp().to_string();

        let mut signed_content = Vec::new();
        signed_content.extend_from_slice(svix_id.as_bytes());
        signed_content.push(b'.');
        signed_content.extend_from_slice(timestamp.as_bytes());
        signed_content.push(b'.');
        signed_content.extend_from_slice(body);

        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(&signed_content);
        let signature = general_purpose::STANDARD.encode(mac.finalize().into_bytes());

        let mut headers = HeaderMap::new();
        headers.insert("svix-id", HeaderValue::from_static(svix_id));
        headers.insert("svix-timestamp", HeaderValue::from_str(&timestamp).unwrap());
        headers.insert(
            "svix-signature",
            HeaderValue::from_str(&format!("v1,{signature}")).unwrap(),
        );

        verify_webhook_signature(&secret, 300, &headers, body).unwrap();
    }
}
