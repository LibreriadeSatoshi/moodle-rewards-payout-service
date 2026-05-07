"""Application settings loaded from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Breez SDK.
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
    # Breez's fetch_fiat_rates has no data on regtest — set this to let /rate
    # return a deterministic number. Leave empty on mainnet.
    mock_btc_usd_rate: str = ""

    # Maximum sats this service is allowed to send in any rolling 24h window.
    # 0 disables the cap. Setting a positive value is a defense-in-depth
    # measure: even with a leaked INTERNAL_TOKEN or a compromised service,
    # the blast radius is one day's quota instead of the entire wallet.
    daily_send_cap_sats: int = 0

    # Uvicorn.
    host: str = "0.0.0.0"
    port: int = 3000

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()  # type: ignore[call-arg]
