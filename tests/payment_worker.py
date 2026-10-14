"""Subprocess boundary for crash tests. Never connects to a real wallet."""
import asyncio
import json
import os
import sqlite3
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import breez_sdk_spark as spark
import httpx

# The parent sets fake configuration and an isolated cwd before this import.
import app


class ParsedAddress:
    def is_lightning_address(self):
        return os.environ['PROOF_MODE'] != 'unsupported'

    def __getitem__(self, index):
        return SimpleNamespace(pay_request=object())


class FakeSDK:
    """Persist simulated network acceptance independently of the service ledger."""
    async def parse(self, *, input):
        if os.environ['PROOF_MODE'] == 'resolution_outage':
            raise ConnectionError('Simulated LNURL host outage')
        return ParsedAddress()

    async def list_payments(self, *, request):
        return SimpleNamespace(payments=[])

    async def prepare_lnurl_pay(self, *, request):
        return object()

    async def lnurl_pay(self, *, request):
        assert isinstance(request, spark.LnurlPayRequest)
        payment_id = request.idempotency_key or str(uuid4())
        if os.environ['PROOF_MODE'] == 'crash_before_acceptance':
            os._exit(75)
        await asyncio.sleep(0.1)
        with sqlite3.connect('network.db') as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS sends (request_id TEXT)')
            conn.execute('INSERT INTO sends VALUES (?)', (payment_id,))
        if os.environ['PROOF_MODE'] == 'crash':
            # Real process death after external acceptance, before service persistence.
            os._exit(75)
        return SimpleNamespace(payment=self.payment(payment_id))

    async def get_payment(self, *, request):
        assert isinstance(request, spark.GetPaymentRequest)
        if os.environ['PROOF_MODE'] == 'unavailable':
            raise ConnectionError('Simulated network outage')
        if os.environ['PROOF_MODE'] == 'missing':
            raise LookupError('Payment not yet visible')
        with sqlite3.connect('network.db') as conn:
            row = conn.execute('SELECT request_id FROM sends WHERE request_id = ?', (request.payment_id,)).fetchone()
        if row is None:
            raise LookupError('Payment not yet visible')
        return SimpleNamespace(payment=self.payment(row[0]))

    @staticmethod
    def payment(key):
        return SimpleNamespace(id=key, status=spark.PaymentStatus.COMPLETED, amount=1000)


async def main():
    with patch.object(app.spark_client, 'connect', AsyncMock(return_value=FakeSDK())):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app), base_url='http://test') as client:
            response = await client.post(os.environ.get('PROOF_PATH', '/pay'), json=json.loads(sys.argv[1]),
                                         headers={'X-Internal-Token': os.environ.get('PROOF_TOKEN', 'test')})
            print(json.dumps({'code': response.status_code, 'body': response.json()}))


asyncio.run(main())
