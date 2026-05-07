"""Local test receiver for the Moodle webhook — dev use only.

Listens on :3099, verifies the X-Webhook-Signature header using WEBHOOK_SECRET
from .env, and prints the body. Returns 200 on valid signature, 401 otherwise.

Run in a separate terminal:
    .venv/bin/python scripts/webhook_receiver.py
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        return  # silence default access log — we print our own summary

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length") or "0")
        body = self.rfile.read(length)
        sig = self.headers.get("X-Webhook-Signature", "")
        expected = "sha256=" + hmac.new(
            settings.webhook_secret.encode(), body, hashlib.sha256
        ).hexdigest()
        valid = hmac.compare_digest(sig, expected)

        pretty = body.decode("utf-8", "replace")
        try:
            pretty = json.dumps(json.loads(pretty), indent=2)
        except Exception:
            pass

        marker = "OK" if valid else "BAD-SIG"
        print(f"[{marker}] {self.path} sig={sig[:14]}…", flush=True)
        print(pretty, flush=True)
        print("---", flush=True)

        self.send_response(200 if valid else 401)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"received": true}\n' if valid else b'{"error": "bad signature"}\n')


def main() -> None:
    port = int(os.environ.get("WEBHOOK_PORT", "3099"))
    server = HTTPServer(("127.0.0.1", port), Handler)
    print(f"webhook receiver listening on http://127.0.0.1:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
