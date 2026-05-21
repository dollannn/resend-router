use base64::{Engine as _, engine::general_purpose};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_delivery(
    secret: &str,
    timestamp: &str,
    delivery_id: &str,
    event_id: &str,
    destination: &str,
    attempt: i32,
    body: &[u8],
) -> String {
    let attempt = attempt.to_string();
    let mut signed_content = Vec::with_capacity(
        timestamp.len()
            + delivery_id.len()
            + event_id.len()
            + destination.len()
            + attempt.len()
            + body.len()
            + 5,
    );
    signed_content.extend_from_slice(timestamp.as_bytes());
    signed_content.push(b'.');
    signed_content.extend_from_slice(delivery_id.as_bytes());
    signed_content.push(b'.');
    signed_content.extend_from_slice(event_id.as_bytes());
    signed_content.push(b'.');
    signed_content.extend_from_slice(destination.as_bytes());
    signed_content.push(b'.');
    signed_content.extend_from_slice(attempt.as_bytes());
    signed_content.push(b'.');
    signed_content.extend_from_slice(body);

    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(&signed_content);
    let digest = mac.finalize().into_bytes();

    format!("v1,{}", general_purpose::STANDARD.encode(digest))
}

#[cfg(test)]
mod tests {
    use super::sign_delivery;

    #[test]
    fn signatures_are_deterministic() {
        let first = sign_delivery(
            "secret",
            "1700000000",
            "delivery",
            "event",
            "destination",
            1,
            b"body",
        );
        let second = sign_delivery(
            "secret",
            "1700000000",
            "delivery",
            "event",
            "destination",
            1,
            b"body",
        );

        assert_eq!(first, second);
        assert!(first.starts_with("v1,"));
    }
}
