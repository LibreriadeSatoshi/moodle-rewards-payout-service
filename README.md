# rewards-payout-service

FastAPI microservice that pays out Bitcoin (priced in USD) for the paired
Moodle plugin [`local_btcrewards`](https://github.com/LibreriadeSatoshi/moodle-btcrewards).

The wallet is a Breez SDK Spark wallet. Only Lightning addresses
(`user@domain`) are accepted as destinations.

## Architecture

```
plugin  ──POST /pay──▶  service     auth: X-Internal-Token
plugin  ◀POST /webhook  service     auth: HMAC-SHA256, retried forever
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
| `MOCK_BTC_USD_RATE`   | Optional fallback rate; only used if Spark `list_fiat_rates` fails.            |

`.env` and `data/` are gitignored. `.env.example` is the tracked template.

## Pay contract

`POST /pay` takes `{payout_id, amount_sats, destination}`. `payout_id` is the
plugin's id for the payout, unique per Moodle site.

- **Never pays twice.** Each `payout_id` is recorded before the send. A repeat
  returns the first attempt's outcome and sends nothing; a repeat with a
  different amount or destination gets HTTP 409. Only a send that failed before
  reaching the SDK (parse or prepare error) is forgotten, so the plugin's retry
  can send it.
- **Unknown outcome.** If the SDK send raises, the service looks for a matching
  outbound payment in the wallet. If none is found, the payout is recorded as
  `unknown` and answered with `retryable: false`, so it is never resent
  automatically. To resolve one, check the wallet; only if nothing went out,
  delete its row (`DELETE FROM payouts WHERE payout_id = ?` in
  `data/outbox.db`) before the student claims again.
- **Limits.** `amount_sats` must be positive and, with a cap set, at most
  `DAILY_SEND_CAP_SATS`. Failed sends don't count toward the cap. If the
  wallet history can't be read, `/pay` answers HTTP 503 instead of paying.
- **One at a time.** Payouts are serialized, so concurrent requests can't both
  pass the cap check. Run a single uvicorn worker.

## Webhook contract

Every outbound delivery body is JSON and includes a fresh `timestamp` (epoch
seconds). The plugin verifies:

- `X-Webhook-Signature: sha256=<hex>` matches `HMAC_SHA256(WEBHOOK_SECRET, body)`.
- `|now - timestamp|` is within 5 minutes (replay window).
- `tx_id` is known and `status` is one of `settled` or `failed`.

The service retries forever on non-2xx; the plugin must be idempotent by `tx_id`.

## Constraints

- Lightning-address-only. Bolt11 invoices, bolt12 offers, onchain (`bc1…`),
  and raw Spark addresses are all rejected at `/pay` with HTTP 400.
- Domain allowlisting is the plugin's responsibility — this service accepts
  any LN address.
- Breez SDK Spark supports **mainnet** and **regtest** only — there is no testnet.
- In regtest, `list_fiat_rates` may not return USD; set `MOCK_BTC_USD_RATE` as a fallback.
