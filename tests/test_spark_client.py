"""Exercise service requests against the installed SDK's actual data types."""

import importlib
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import breez_sdk_spark as spark


class DepositInvoiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_funding_invoice_preserves_amount_description_and_fee(self):
        # Import settings and initialize the outbox away from real wallet data.
        original_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, {
                "BREEZ_API_KEY": "test",
                "BREEZ_MNEMONIC": "test",
                "INTERNAL_TOKEN": "test",
                "MOODLE_WEBHOOK_URL": "https://example.invalid/webhook",
                "WEBHOOK_SECRET": "test",
            }):
                try:
                    os.chdir(directory)
                    client = importlib.import_module("spark_client")
                finally:
                    os.chdir(original_cwd)

            async def receive_payment(*, request):
                method = request.payment_method
                self.assertTrue(method.is_bolt11_invoice())
                self.assertEqual(method.description, "Fund rewards wallet")
                self.assertEqual(method.amount_sats, 1000)
                self.assertEqual(method.expiry_secs, 3600)
                self.assertIsNone(method.payment_hash)
                self.assertIsNone(method.receiver_identity_public_key)
                return spark.ReceivePaymentResponse(
                    payment_request="lnbc-test-invoice", fee=2,
                    cross_chain_info=None,
                )

            sdk = SimpleNamespace(receive_payment=receive_payment)
            with patch.object(client, "connect", AsyncMock(return_value=sdk)):
                result = await client.create_deposit_invoice(1000, "Fund rewards wallet")

        self.assertEqual(result, {
            "payment_request": "lnbc-test-invoice", "fee_sat": 2,
        })


if __name__ == "__main__":
    unittest.main()
