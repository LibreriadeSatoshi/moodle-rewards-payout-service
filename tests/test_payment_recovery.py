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

    def call(self, mode='normal', **changes):
        env = dict(os.environ, PYTHONPATH=str(ROOT), PROOF_MODE=mode,
                   BREEZ_API_KEY='test', BREEZ_MNEMONIC='test', INTERNAL_TOKEN='test',
                   MOODLE_WEBHOOK_URL='https://example.invalid/webhook', WEBHOOK_SECRET='test',
                   DAILY_SEND_CAP_SATS='0', BREEZ_NETWORK='regtest')
        result = subprocess.run([sys.executable, str(ROOT / 'tests/payment_worker.py'), json.dumps(REQUEST | changes)],
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
        for changes in [{'amount_sats': 0}, {'request_id': 'not-a-uuid'}, {'extra_field': 'ignored?'}]:
            self.assertEqual(422, self.call(**changes)['code'])
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
