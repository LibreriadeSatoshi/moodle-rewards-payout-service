"""Exercise the HTTP API across real process restarts, with a simulated network."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

ROOT = Path(__file__).resolve().parents[1]
REQUEST = {'request_id': 'bb385061-d87a-4414-a2bb-2afc7de7a931', 'destination': 'student@example.invalid', 'amount_sats': 1000}


class PaymentRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def call(self, mode='normal', cap=0, keyed=True, path='/pay', token='test', **changes):
        env = dict(os.environ, PYTHONPATH=str(ROOT), PROOF_MODE=mode,
                   PROOF_PATH=path, PROOF_TOKEN=token,
                   BREEZ_API_KEY='test', BREEZ_MNEMONIC='test', INTERNAL_TOKEN='test',
                   MOODLE_WEBHOOK_URL='https://example.invalid/webhook', WEBHOOK_SECRET='test',
                   DAILY_SEND_CAP_SATS=str(cap), BREEZ_NETWORK='regtest')
        payload = dict(REQUEST)
        if not keyed:
            payload.pop('request_id')
        payload.update(changes)
        result = subprocess.run([sys.executable, str(ROOT / 'tests/payment_worker.py'), json.dumps(payload)],
                                cwd=self.directory.name, env=env, capture_output=True, text=True, timeout=20)
        if mode.startswith('crash'):
            self.assertEqual(75, result.returncode, result.stdout + result.stderr)
            return None
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        return json.loads(result.stdout)

    def count_sends(self):
        with sqlite3.connect(Path(self.directory.name) / 'network.db') as conn:
            return conn.execute('SELECT COUNT(*) FROM sends').fetchone()[0]

    def test_crash_after_acceptance_recovers_without_resending(self):
        self.call('crash')
        for _ in range(2):
            result = self.call()
            self.assertEqual(200, result['code'])
            self.assertEqual('settled', result['body']['status'])
            self.assertEqual(REQUEST['request_id'], result['body']['tx_id'])
        self.assertEqual(1, self.count_sends())

    def test_unavailable_lookup_preserves_unknown_without_resending(self):
        self.call('crash')
        for mode in ['unavailable', 'missing']:
            result = self.call(mode)
            self.assertEqual(200, result['code'])
            self.assertEqual('unknown', result['body']['status'])
            self.assertFalse(result['body']['retryable'])
        self.assertEqual(1, self.count_sends())
        self.assertEqual('settled', self.call()['body']['status'])

    def test_same_key_with_different_payload_is_rejected(self):
        self.assertEqual(200, self.call()['code'])
        for changes in [{'amount_sats': 2000}, {'destination': 'other@example.invalid'}]:
            self.assertEqual(409, self.call(**changes)['code'])
        self.assertEqual(1, self.count_sends())

    def test_invalid_request_never_reaches_sdk(self):
        for changes in [{'amount_sats': 0}, {'destination': ''}, {'request_id': 'not-a-uuid'},
                        {'extra_field': 'ignored?'}]:
            self.assertEqual(422, self.call(**changes)['code'])
        self.assertFalse((Path(self.directory.name) / 'network.db').exists())

    def test_legacy_requests_without_id_are_independent_payments(self):
        responses = [self.call(keyed=False, extra_field='previously ignored') for _ in range(2)]
        for response in responses:
            self.assertEqual(200, response['code'])
            self.assertEqual('settled', response['body']['status'])
        self.assertNotEqual(responses[0]['body']['tx_id'], responses[1]['body']['tx_id'])
        self.assertEqual(2, self.count_sends())

    def test_null_request_id_keeps_legacy_behavior(self):
        responses = [self.call(request_id=None) for _ in range(2)]
        self.assertTrue(all(response['code'] == 200 for response in responses))
        self.assertNotEqual(responses[0]['body']['tx_id'], responses[1]['body']['tx_id'])
        self.assertEqual(2, self.count_sends())

    def test_legacy_rejection_is_not_saved_as_a_keyed_request(self):
        self.assertEqual(429, self.call(keyed=False, cap=500)['code'])
        self.assertFalse((Path(self.directory.name) / 'network.db').exists())
        self.assertEqual(200, self.call(keyed=False)['code'])
        self.assertEqual(1, self.count_sends())

    def test_legacy_resolution_failure_remains_bad_request(self):
        self.assertEqual(400, self.call(mode='resolution_outage', keyed=False)['code'])
        self.assertFalse((Path(self.directory.name) / 'network.db').exists())

    def test_payout_requires_authentication_with_or_without_id(self):
        for keyed in [True, False]:
            self.assertEqual(401, self.call(keyed=keyed, token='wrong')['code'])
        self.assertFalse((Path(self.directory.name) / 'network.db').exists())

    def test_separate_versioned_payout_route_is_not_available(self):
        self.assertEqual(404, self.call(path='/v2/pay')['code'])
        self.assertFalse((Path(self.directory.name) / 'network.db').exists())

    def test_spending_cap_rejection_is_definitive_and_never_sends(self):
        for cap in [500, 0]:
            result = self.call(cap=cap)
            self.assertEqual(429, result['code'])
            self.assertEqual('daily send cap reached', result['body']['detail'])
        self.assertFalse((Path(self.directory.name) / 'network.db').exists())

    def test_unsupported_destination_rejection_is_definitive_and_never_sends(self):
        for mode in ['unsupported', 'normal']:
            result = self.call(mode)
            self.assertEqual(400, result['code'])
            self.assertEqual('Only Lightning address destinations are supported', result['body']['detail'])
        self.assertFalse((Path(self.directory.name) / 'network.db').exists())

    def test_destination_resolution_failure_is_definitive_502_and_never_sends(self):
        for mode in ['resolution_outage', 'normal']:
            result = self.call(mode)
            self.assertEqual(502, result['code'])
            self.assertEqual('destination resolution failed: Unparseable destination: Simulated LNURL host outage',
                             result['body']['detail'])
        self.assertFalse((Path(self.directory.name) / 'network.db').exists())

    def test_crash_before_acceptance_stays_unknown_without_sending(self):
        self.call('crash_before_acceptance')
        self.assertEqual('unknown', self.call('missing')['body']['status'])
        self.assertFalse((Path(self.directory.name) / 'network.db').exists())

    def test_concurrent_duplicate_requests_submit_only_once(self):
        with ThreadPoolExecutor(max_workers=4) as executor:
            responses = list(executor.map(lambda _: self.call(), range(4)))
        for response in responses:
            self.assertEqual(200, response['code'])
            self.assertIn(response['body']['status'], ['unknown', 'settled'])
        self.assertEqual(1, self.count_sends())
        self.assertEqual('settled', self.call()['body']['status'])
