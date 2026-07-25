from __future__ import annotations

import io
import json
import threading
import unittest
import urllib.error
import urllib.request
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from tools.whisper_cpp_bridge import (
    WhisperBridgeHandler,
    WhisperBridgeServer,
    WhisperCppClient,
)


def _wav_bytes() -> bytes:
    output = io.BytesIO()
    with wave.open(output, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(16000)
        wav_file.writeframes(b"\x00\x00" * 16000)
    return output.getvalue()


class FakeWhisperHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        body = b"ready"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):  # noqa: N802
        content_length = int(self.headers.get("Content-Length") or "0")
        body = self.rfile.read(content_length)
        if b'name="file"; filename="audio.wav"' not in body:
            self.send_error(400)
            return
        payload = json.dumps({"text": "Bridge transcript."}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        return


class WhisperCppBridgeTests(unittest.TestCase):
    def setUp(self):
        self.backend = ThreadingHTTPServer(("127.0.0.1", 0), FakeWhisperHandler)
        self.backend_thread = threading.Thread(
            target=self.backend.serve_forever,
            daemon=True,
        )
        self.backend_thread.start()
        backend_port = self.backend.server_address[1]
        client = WhisperCppClient(
            backend_url=f"http://127.0.0.1:{backend_port}/inference",
            backend_timeout_seconds=2,
            model="ggml-large-v3",
            device="vulkan:1",
        )
        self.bridge = WhisperBridgeServer(
            ("127.0.0.1", 0),
            WhisperBridgeHandler,
            client=client,
            quiet=True,
            max_body_bytes=2 * 1024 * 1024,
            max_queued_requests=2,
            queue_timeout_seconds=1,
            api_token="test-token",
        )
        self.bridge_thread = threading.Thread(
            target=self.bridge.serve_forever,
            daemon=True,
        )
        self.bridge_thread.start()
        self.base_url = f"http://127.0.0.1:{self.bridge.server_address[1]}"

    def tearDown(self):
        self.bridge.shutdown()
        self.bridge.server_close()
        self.backend.shutdown()
        self.backend.server_close()

    def _request(self, path: str, *, data: bytes | None = None, token=True):
        headers = {}
        if token:
            headers["Authorization"] = "Bearer test-token"
        if data is not None:
            headers["Content-Type"] = "audio/wav"
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            headers=headers,
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            return response.status, json.loads(response.read())

    def test_health_reports_backend_and_model(self):
        status, payload = self._request("/healthz", token=False)

        self.assertEqual(status, 200)
        self.assertTrue(payload["backend_ready"])
        self.assertEqual(payload["model"], "ggml-large-v3")
        self.assertEqual(payload["device"], "vulkan:1")

    def test_raw_wav_contract_returns_text_and_metrics(self):
        status, payload = self._request(
            "/transcription?language=en",
            data=_wav_bytes(),
        )

        self.assertEqual(status, 200)
        self.assertEqual(payload["text"], "Bridge transcript.")
        self.assertEqual(payload["audio_seconds"], 1.0)
        self.assertEqual(payload["model"], "ggml-large-v3")
        self.assertEqual(payload["language"], "en")
        self.assertGreaterEqual(payload["inference_seconds"], 0)

    def test_stats_requires_token(self):
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self._request("/stats", token=False)

        self.assertEqual(raised.exception.code, 401)
        raised.exception.close()

    def test_invalid_wav_is_rejected(self):
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self._request("/transcription", data=b"not a wav")

        self.assertEqual(raised.exception.code, 400)
        raised.exception.close()

    def test_backend_unit_waits_for_second_amd_vulkan_device(self):
        unit = (
            Path(__file__).resolve().parent
            / "ops/linux/ntc-whisper-cpp-backend.service"
        ).read_text(encoding="utf-8")

        self.assertIn("ExecStartPre=", unit)
        self.assertIn("vulkaninfo --summary", unit)
        self.assertIn('"^GPU1:"', unit)
        self.assertIn('"deviceName.*AMD Radeon"', unit)
        self.assertIn("TimeoutStartSec=120", unit)


if __name__ == "__main__":
    unittest.main()
