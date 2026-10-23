"""Application settings loaded from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Breez SDK Spark.
    breez_api_key: str
    breez_mnemonic: str
    breez_network: str = "mainnet"
    breez_working_dir: str = "./data/breez"

    # Auth token the Moodle plugin sends via X-Internal-Token on inbound calls.
    internal_token: str

    # Outbound webhook — this service pushes terminal payment events here.
    moodle_webhook_url: str
    webhook_secret: str

    # Regtest-only BTC/USD rate override in USD (not cents), e.g. "60000".
    # The Spark SDK's list_fiat_rates has no data on regtest — set this to
    # let /rate return a deterministic number. Leave empty on mainnet.
    mock_btc_usd_rate: str = ""

    # Maximum sats this service is allowed to send in any rolling 24h window.
    # 0 disables the cap. Setting a positive value is a defense-in-depth
    # measure: even with a leaked INTERNAL_TOKEN or a compromised service,
    # the blast radius is one day's quota instead of the entire wallet.
    daily_send_cap_sats: int = 0

    # Days Moodle may keep answering 404 "tx_id not found" (no payout with this
    # tx_id), without interruption, before the retrier gives up on the event and
    # logs an error. 0 retries forever. Early
    # 404s are normal (the event can arrive before Moodle has stored the tx_id),
    # so keep this well above the backoff cap of one hour. Other failures are
    # always retried.
    webhook_give_up_days: float = 7

    # A payout the SDK reports failed is checked with the receiver's LUD-21
    # verify URL every VERIFY_INTERVAL_SECONDS. It is reported failed to Moodle
    # only once the receiver still says "not paid" after VERIFY_FAIL_AFTER_HOURS:
    # a payment still in flight settles or fails well within that.
    verify_interval_seconds: float = 300
    verify_fail_after_hours: float = 24

    # Uvicorn.
    host: str = "0.0.0.0"
    port: int = 3000

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()  # type: ignore[call-arg]
