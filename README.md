# resend-router

Durable Resend webhook router for cases where Resend cannot scope webhook delivery by domain.

The service receives every Resend webhook, verifies the Resend/Svix signature, routes matching events to configured destinations, and retries failed downstream deliveries for up to 3 days. Unmatched webhooks are intentionally dropped.

## Behavior

- `POST /webhooks/resend` receives raw Resend webhooks.
- The raw body is never mutated before forwarding.
- Inbound Resend signatures are verified using the `svix-id`, `svix-timestamp`, and `svix-signature` headers.
- Routing defaults to the sender domain extracted from `data.from`.
- Matching deliveries are persisted in Postgres before the handler returns `202 Accepted`.
- Resend/Svix duplicate deliveries are deduplicated by `svix-id`.
- Unmatched webhooks return `204 No Content`.
- Delivery succeeds on any downstream `2xx` response.
- Any non-`2xx` response or network error is retried until the 3-day deadline.
- Permanent failures are logged as errors only after the retry window is exhausted.

## Endpoints

```text
POST /webhooks/resend
GET  /healthz
GET  /readyz
```

Use `https://<your-domain>/webhooks/resend` as the Resend webhook URL for your deployment.

## Configuration

Configuration is env-var based for Railway.

```bash
DATABASE_URL=postgres://...
PORT=3000
RESEND_WEBHOOK_SECRET=whsec_...
ROUTER_SIGNING_SECRET='generate-a-long-random-string'
DESTINATIONS_JSON='[
  {
    "name": "main-app",
    "url": "https://app.example.com/webhooks/resend",
    "from_domains": ["example.com"]
  }
]'
```

Destination fields:

```json
{
  "name": "main-app",
  "url": "https://app.example.com/webhooks/resend",
  "from_domains": ["example.com"],
  "to_domains": [],
  "event_types": [],
  "catch_all": false
}
```

Matching semantics are additive: if `from_domains`, `to_domains`, and `event_types` are all present, all configured constraints must match. Domain matching is exact after lowercasing and trimming a leading `@`; wildcards are not expanded. Use `catch_all: true` for an intentional catch-all destination.

## Outbound delivery verification

Downstream destinations should verify router-owned signatures instead of Resend signatures, because retries may happen hours or days after the original Resend delivery.

Forwarded requests include:

```text
x-resend-router-delivery-id
x-resend-router-event-id
x-resend-router-attempt
x-resend-router-destination
x-resend-router-timestamp
x-resend-router-signature
```

The signature is:

```text
base64(hmac_sha256(ROUTER_SIGNING_SECRET, "{timestamp}.{delivery_id}.{event_id}.{destination}.{attempt}.{raw_body}"))
```

The header value is prefixed with `v1,`.

## Railway deployment

1. Create a Railway project.
2. Add a Postgres service.
3. Deploy this repository as a service.
4. Set env vars from `.env.example`.
   - Set `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=60` so in-flight delivery workers can drain on deploys/restarts.
5. Configure your custom domain, e.g. `resend-router.example.com`.
6. Point Resend webhooks at `https://<your-domain>/webhooks/resend`.

Migrations are embedded and run on startup.

## Local development

```bash
cargo test
cargo run
```

The app requires `DATABASE_URL`, `RESEND_WEBHOOK_SECRET`, `ROUTER_SIGNING_SECRET`, and `DESTINATIONS_JSON` to start.
