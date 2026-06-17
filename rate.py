"""BTC/USD rate lookup with TTL cache and regtest fallback.

Self-contained: owns its own cache state and exception class. The plugin's
claim() flow calls /rate to lock the rate at submit time; the UI also calls
it on every render. The cache exists so a busy UI doesn't hammer the SDK.
"""

from __future__ import annotations

import logging
import time

import spark_client
from config import settings

logger = logging.getLogger(__name__)


class RateUnavailable(Exception):
    """Raised when we can't resolve a BTC/USD rate."""


_TTL_SEC = 60
_cache: tuple[int, float] | None = None  # (cents_per_btc, fetched_at_epoch)


async def get_cents_per_btc() -> int:
    """Return the current BTC/USD rate in cents per BTC, cached ~60s.

    On regtest (or when the SDK has no fiat data), falls back to
    ``settings.mock_btc_usd_rate`` if set. Raises RateUnavailable otherwise.
    """
    global _cache
    now = time.time()
    if _cache and now - _cache[1] < _TTL_SEC:
        return _cache[0]

    sdk = await spark_client.connect()
    cents: int | None = None
    try:
        response = await sdk.list_fiat_rates()
        for r in response.rates:
            if r.coin.upper() == "USD":
                cents = int(round(r.value * 100))
                break
    except Exception:
        logger.exception("list_fiat_rates failed")

    if cents is None and settings.mock_btc_usd_rate:
        try:
            cents = int(round(float(settings.mock_btc_usd_rate) * 100))
            logger.info("using MOCK_BTC_USD_RATE=%s (regtest)", settings.mock_btc_usd_rate)
        except ValueError:
            logger.warning("mock_btc_usd_rate is not a number: %r", settings.mock_btc_usd_rate)

    if cents is None or cents <= 0:
        raise RateUnavailable("No USD rate available from Spark; set MOCK_BTC_USD_RATE on regtest.")

    _cache = (cents, now)
    return cents
