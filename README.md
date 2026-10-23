# rewards-payout-service

FastAPI microservice that pays out Bitcoin (priced in USD) for the paired
Moodle plugin [`local_btcrewards`](https://github.com/LibreriadeSatoshi/moodle-btcrewards).

The wallet is a Breez SDK Spark wallet. Only Lightning addresses
(`user@domain`) are accepted as destinations.

## Architecture

```
plugin  ──POST /pay──▶  service     auth: X-Internal-Token
plugin  ◀POST /webhook  service     auth: HMAC-SHA256, retried until 2xx (404s give up after WEBHOOK_GIVE_UP_DAYS)
```

The service verifies the destination is a Lightning address via the SDK,
stages the outbound payment, and pushes terminal state back via webhook.
The plugin never polls. Any non-Lightning-address destination (onchain,
bolt11, bolt12, Spark address) is rejected at `/pay` with HTTP 400.

## Endpoints

| Method | Path     | Auth                | Purpose                                |
|--------|----------|---------------------|----------------------------------------|
| GET    | /rate    | `X-Internal-Token`  | Cached BTC/USD rate (cents per BTC)    |
| POST   | /pay     | `X-Internal-Token`  | Submit a Lightning-address payment     |
| GET    | /status  | `X-Internal-Token`  | Live SDK state for a tx_id (debug)     |
| GET    | /balance | `X-Internal-Token`  | Wallet balance + pending sats (debug)  |
| GET    | /webhooks/undelivered | `X-Internal-Token` | Webhooks still retrying or given up |

Outbound:

| Method | Target                         | Auth                                |
|--------|--------------------------------|-------------------------------------|
| POST   | `MOODLE_WEBHOOK_URL`           | `X-Webhook-Signature: sha256=<hex>` |

## Modules

- `app.py` — FastAPI surface. Auth via `Depends`. Amount, daily-cap and idempotency checks on `/pay`.
- `spark_client.py` — SDK singleton and Lightning send path.
- `rate.py` — BTC/USD rate cache.
- `webhook.py` — outbox retrier and event→status helpers.
- `db.py` — SQLite webhook outbox and payout ledger (payment state itself lives in the SDK).
- `config.py` — pydantic-settings, loaded from `.env`.

## Setup

```bash
cp .env.example .env      # then fill in real values
make setup                # creates .venv, installs requirements
make run                  # starts uvicorn with reload
PYTHONPATH=. .venv/bin/python -m unittest discover -s tests   # tests
```

## Configuration

| Variable              | Notes                                                                          |
|-----------------------|--------------------------------------------------------------------------------|
| `BREEZ_API_KEY`       | From breez.technology                                                          |
| `BREEZ_MNEMONIC`      | 12-word seed for the Spark wallet. Treat as a private key.                     |
| `BREEZ_NETWORK`       | `mainnet` or `regtest`.                                                        |
| `BREEZ_WORKING_DIR`   | Where the SDK stores wallet state (default `./data/breez`).                    |
| `INTERNAL_TOKEN`      | Shared secret. Plugin sends as `X-Internal-Token` on every request.            |
| `WEBHOOK_SECRET`      | HMAC key for outbound webhook signatures. Distinct from `INTERNAL_TOKEN`.      |
| `MOODLE_WEBHOOK_URL`  | Where to POST terminal events. Plugin endpoint: `…/local/btcrewards/webhook.php`. |
| `HOST`, `PORT`        | uvicorn bind. Defaults `0.0.0.0:3000`.                                         |
| `DAILY_SEND_CAP_SATS` | Optional rolling 24h spending ceiling, also the largest single payout. Fails closed. |
| `WEBHOOK_GIVE_UP_DAYS` | Days Moodle may answer "tx_id not found" for a webhook, without interruption, before the service gives up on it (default 7, 0 = forever). |
| `MOCK_BTC_USD_RATE`   | Optional fallback rate; only used if Spark `list_fiat_rates` fails.            |

`.env` and `data/` are gitignored. `.env.example` is the tracked template.

## Pay contract

`POST /pay` takes `{payout_id, amount_sats, destination}`. `payout_id` is the
plugin's id for the payout, unique per Moodle site.

- **Pays at most once.** Every send passes the Breez SDK an idempotency key
  derived from `payout_id` (a UUIDv5). The SDK checks its storage for that key
  and uses it as the Spark transfer id (and as the payment id), so repeating it
  returns the original payment instead of paying again, also across restarts.
- **Failed sends are retried, never abandoned.** If the send raises, `/pay`
  answers HTTP 503 and keeps the payout `pending`. The plugin treats 503 as
  transient and retries the same `payout_id` without spending an attempt, so a
  payout whose payment may have gone out is never failed and re-claimed under
  a new id. On the retry the service first looks the key up in the wallet: if
  the payment exists it answers with it and sends nothing.
- **Ledger.** `payout_id`, amount and destination are recorded in
  `data/outbox.db` before the first send. A repeat with a different amount or
  destination gets HTTP 409. A repeat of a paid payout answers with that
  payment's live status and sends nothing. A payment the SDK reports failed is
  final (`retryable: false`).
- **Limits.** `amount_sats` must be positive and, with a cap set, at most
  `DAILY_SEND_CAP_SATS`. The cap is checked before every send, retries
  included; failed sends don't count. If the wallet history can't be read,
  `/pay` answers HTTP 503 instead of paying.
- **One at a time.** Payouts are serialized, so concurrent requests can't both
  pass the cap check. A request that waited more than 30 s for its turn gets
  HTTP 503 rather than being sent after the plugin gave up on it. Run a single
  uvicorn worker.

## Webhook contract

Every outbound delivery body is JSON and includes a fresh `timestamp` (epoch
seconds). The plugin verifies:

- `X-Webhook-Signature: sha256=<hex>` matches `HMAC_SHA256(WEBHOOK_SECRET, body)`.
- `|now - timestamp|` is within 5 minutes (replay window).
- `tx_id` is known and `status` is one of `settled` or `failed`.

The service retries non-2xx answers with exponential backoff (capped at one
hour) and the plugin must be idempotent by `tx_id`. Network errors, 5xx and
other rejections are retried forever, so an outage on either side never drops
an event. The one answer it gives up on is the plugin's own HTTP 404
`{"error": "tx_id not found", "tx_id": …}` for that event; any other 404 (a
wrong `MOODLE_WEBHOOK_URL`, a missing plugin, a web-server page) is treated as
an outage. Early "tx_id not found" answers are normal, since the event can
arrive before Moodle has stored the `tx_id`, but once Moodle has given that
answer without interruption for `WEBHOOK_GIVE_UP_DAYS` (default 7; any other
answer restarts the count) the service logs an error ("given up … needs
manual review") and keeps the row with `abandoned_at` set. `GET /webhooks/undelivered` lists rows still retrying and
rows given up. If Moodle calls `/pay` again for that payout, the event is
retried from scratch; to retry one by hand, clear its `abandoned_at` and
`rejected_since` in `data/outbox.db`.

## Constraints

- Lightning-address-only. Bolt11 invoices, bolt12 offers, onchain (`bc1…`),
  and raw Spark addresses are all rejected at `/pay` with HTTP 400.
- Domain allowlisting is the plugin's responsibility — this service accepts
  any LN address.
- Breez SDK Spark supports **mainnet** and **regtest** only — there is no testnet.
- In regtest, `list_fiat_rates` may not return USD; set `MOCK_BTC_USD_RATE` as a fallback.
