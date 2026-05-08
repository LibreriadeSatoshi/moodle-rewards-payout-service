# rewards-payout-service

FastAPI microservice that pays out Bitcoin (priced in USD) for the paired
Moodle plugin [`local_btcrewards`](https://github.com/LibreriadeSatoshi/moodle-btcrewards).

The wallet holds L-BTC; every Bitcoin payment crosses chains via a Breez SDK
Liquid → Boltz swap (Lightning or onchain).

## Architecture

```
plugin  ──POST /pay──▶  service     auth: X-Internal-Token
plugin  ◀POST /webhook  service     auth: HMAC-SHA256, retried forever
```

The service classifies the destination itself (bolt11, bolt12, LN address,
onchain), stages the outbound payment, and pushes terminal state back via
webhook. The plugin never polls.

## Endpoints

| Method | Path     | Auth                | Purpose                                |
|--------|----------|---------------------|----------------------------------------|
| GET    | /rate    | `X-Internal-Token`  | Cached BTC/USD rate (cents per BTC)    |
| GET    | /limits  | `X-Internal-Token`  | Per-rail send min/max in sats          |
| POST   | /pay     | `X-Internal-Token`  | Submit a payment; returns initial state|

Outbound:

| Method | Target                         | Auth                                |
|--------|--------------------------------|-------------------------------------|
| POST   | `MOODLE_WEBHOOK_URL`           | `X-Webhook-Signature: sha256=<hex>` |

## Modules

- `app.py` — FastAPI surface. Auth + SDK injected via `Depends`. Daily-cap check on `/pay`.
- `breez_client.py` — SDK singleton, send paths, destination classifier, send-limits cache.
- `rate.py` — BTC/USD rate cache.
- `webhook.py` — outbox retrier and event→status helpers.
- `db.py` — SQLite outbox (delivery state only; payment state lives in the SDK).
- `config.py` — pydantic-settings, loaded from `.env`.

## Setup

```bash
cp .env.example .env      # then fill in real values
make setup                # creates .venv, installs requirements
make run                  # starts uvicorn with reload
```

## Configuration

| Variable              | Notes                                                                          |
|-----------------------|--------------------------------------------------------------------------------|
| `BREEZ_API_KEY`       | From breez.technology                                                           |
| `BREEZ_MNEMONIC`      | 12-word seed for the Liquid wallet. Treat as a private key.                    |
| `BREEZ_NETWORK`       | `mainnet` or `regtest`. Testnet is **not** supported by Breez SDK Liquid.       |
| `BREEZ_WORKING_DIR`   | Where the SDK stores wallet state (default `./data/breez`).                    |
| `INTERNAL_TOKEN`      | Shared secret. Plugin sends as `X-Internal-Token` on every request.            |
| `WEBHOOK_SECRET`      | HMAC key for outbound webhook signatures. Distinct from `INTERNAL_TOKEN`.      |
| `MOODLE_WEBHOOK_URL`  | Where to POST terminal events. Plugin endpoint: `…/local/btcrewards/webhook.php`. |
| `HOST`, `PORT`        | uvicorn bind. Defaults `0.0.0.0:3000`.                                         |
| `DAILY_SEND_CAP_SATS` | Optional global daily-spending ceiling (defense in depth).                     |
| `MOCK_BTC_USD_RATE`   | Optional fallback rate; only used if Breez `fetch_fiat_rates` fails.           |

`.env` and `data/` are gitignored. `.env.example` is the tracked template.

## Webhook contract

Every outbound delivery body is JSON and includes a fresh `timestamp` (epoch
seconds). The plugin verifies:

- `X-Webhook-Signature: sha256=<hex>` matches `HMAC_SHA256(WEBHOOK_SECRET, body)`.
- `|now - timestamp|` is within 5 minutes (replay window).
- `tx_id` is known and `status` is one of `settled` or `failed`.

The service retries forever on non-2xx; the plugin must be idempotent by `tx_id`.

## Constraints

- Boltz L-BTC→BTC swap minimum is **25,000 sats** (~$20). Below that, onchain
  payouts are rejected with `retryable=false`. Lightning has a 21-sat minimum.
- Breez SDK Liquid supports **mainnet** and **regtest** only — there is no testnet.
- In regtest, `fetch_fiat_rates` still returns real Breez data; set
  `MOCK_BTC_USD_RATE` only as a fallback.
