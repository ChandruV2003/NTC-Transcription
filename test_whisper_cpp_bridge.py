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
from unittest import mock

from tools.whisper_cpp_bridge import (
    WhisperBridgeHandler,
    WhisperBridgeServer,
    WhisperCppClient,
    _strip_prompt_echo,
)


def _wav_bytes(seconds: float = 1.0) -> bytes:
    output = io.BytesIO()
    with wave.open(output, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(16000)
        wav_file.writeframes(b"\x00\x00" * round(16000 * seconds))
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
        batch_client = WhisperCppClient(
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
            batch_client=batch_client,
            batch_threshold_seconds=5,
            max_batch_queued_requests=1,
            batch_queue_timeout_seconds=0,
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
        self.assertTrue(payload["live_backend_ready"])
        self.assertTrue(payload["batch_backend_ready"])
        self.assertEqual(payload["lanes"]["live"]["capacity"], 2)
        self.assertEqual(payload["lanes"]["batch"]["capacity"], 1)

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
        self.assertEqual(payload["lane"], "live")
        self.assertGreaterEqual(payload["inference_seconds"], 0)

    def test_prompt_echo_is_removed_before_it_reaches_the_caller(self):
        prompt = (
            "Transcribe this NTC Newark church recording clearly. Preserve "
            "speaker names, scripture references, sermon titles, testimony "
            "introductions, and whether this sounds like a personal testimony "
            "or a preached message."
        )
        echoed = (
            "testimony introductions, and whether this sounds like a\n"
            "personal testimony or a preached message.\n"
            "testimony introductions, and whether this sounds like a\n"
            "personal testimony or a preached message."
        )

        self.assertEqual(_strip_prompt_echo(echoed, prompt), "")

    def test_real_transcript_with_a_few_prompt_words_is_preserved(self):
        prompt = (
            "Preserve speaker names, scripture references, sermon titles, "
            "testimony introductions, and whether this sounds like a personal "
            "testimony or a preached message."
        )
        transcript = (
            "Praise the Lord. My name is Kevin, and I want to thank God for "
            "helping me through school this year."
        )

        self.assertEqual(_strip_prompt_echo(transcript, prompt), transcript)

    def test_long_audio_routes_to_bounded_batch_lane(self):
        status, payload = self._request(
            "/transcription?language=en",
            data=_wav_bytes(6.0),
        )

        self.assertEqual(status, 200)
        self.assertEqual(payload["audio_seconds"], 6.0)
        self.assertEqual(payload["lane"], "batch")
        self.assertEqual(
            payload["endpoint_version"],
            "2026-07-26-whisper-cpp-dual-lane",
        )

    def test_explicit_batch_lane_keeps_refinement_off_live_lane(self):
        status, payload = self._request(
            "/transcription?language=en&lane=batch",
            data=_wav_bytes(2.0),
        )

        self.assertEqual(status, 200)
        self.assertEqual(payload["audio_seconds"], 2.0)
        self.assertEqual(payload["lane"], "batch")

    def test_batch_lane_does_not_forward_instruction_prompt(self):
        with mock.patch.object(
            self.bridge.batch_client,
            "transcribe",
            return_value={"text": "Real speech.", "inference_seconds": 0.1},
        ) as transcribe:
            status, payload = self._request(
                "/transcription?language=en&lane=batch&prompt=classify+this+audio",
                data=_wav_bytes(2.0),
            )

        self.assertEqual(status, 200)
        self.assertEqual(payload["text"], "Real speech.")
        transcribe.assert_called_once_with(
            mock.ANY,
            language="en",
            prompt="",
        )

    def test_empty_batch_transcript_is_a_terminal_analysis_result(self):
        with (
            mock.patch.object(
                self.bridge.batch_client,
                "transcribe",
                return_value={"text": "", "inference_seconds": 0.1},
            ),
            self.assertRaises(urllib.error.HTTPError) as raised,
        ):
            self._request(
                "/transcription?language=en&lane=batch",
                data=_wav_bytes(2.0),
            )

        self.assertEqual(raised.exception.code, 422)
        payload = json.loads(raised.exception.read())
        self.assertEqual(payload["error"], "no speech recognized in batch audio")
        self.assertFalse(payload["retryable"])
        raised.exception.close()

    def test_batch_is_deferred_when_live_transcription_is_active(self):
        with (
            mock.patch.object(
                self.bridge,
                "batch_inhibit_state",
                return_value=(True, "live_transcription_active"),
            ),
            self.assertRaises(urllib.error.HTTPError) as raised,
        ):
            self._request(
                "/transcription?language=en&lane=batch",
                data=_wav_bytes(2.0),
            )

        self.assertEqual(raised.exception.code, 429)
        payload = json.loads(raised.exception.read())
        self.assertEqual(payload["lane"], "batch")
        self.assertEqual(payload["reason"], "live_transcription_active")
        self.assertTrue(payload["retryable"])
        raised.exception.close()

    def test_live_lane_does_not_poll_batch_gate(self):
        with mock.patch.object(
            self.bridge,
            "batch_inhibit_state",
            side_effect=AssertionError("live lane must bypass batch gate"),
        ):
            status, payload = self._request(
                "/transcription?language=en&lane=live",
                data=_wav_bytes(2.0),
            )

        self.assertEqual(status, 200)
        self.assertEqual(payload["lane"], "live")

    def test_batch_gate_fails_closed_when_activity_status_is_unavailable(self):
        self.bridge.batch_inhibit_url = "http://operations.test/status"
        self.bridge.batch_inhibit_cache_seconds = 0

        with mock.patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.URLError("offline"),
        ):
            inhibited, reason = self.bridge.batch_inhibit_state()

        self.assertTrue(inhibited)
        self.assertEqual(reason, "activity_unavailable")

    def test_batch_gate_uses_configured_activity_timeout(self):
        self.bridge.batch_inhibit_url = "http://operations.test/status"
        self.bridge.batch_inhibit_cache_seconds = 0
        self.bridge.batch_inhibit_timeout_seconds = 3.5
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(
            {"mix": {"analysis": {"rms_dbfs": -90, "peak_dbfs": -90}}}
        ).encode()

        with mock.patch(
            "urllib.request.urlopen",
            return_value=response,
        ) as urlopen:
            inhibited, reason = self.bridge.batch_inhibit_state()

        self.assertFalse(inhibited)
        self.assertEqual(reason, "idle")
        urlopen.assert_called_once_with(
            "http://operations.test/status",
            timeout=3.5,
        )

    def test_batch_gate_recognizes_live_transcription_only(self):
        self.assertEqual(
            self.bridge._activity_inhibits_batch(
                {
                    "transcription": {
                        "active": {
                            "configured": True,
                            "ok": True,
                            "stale": False,
                        }
                    }
                }
            ),
            (True, "live_transcription_active"),
        )
        self.assertEqual(
            self.bridge._activity_inhibits_batch(
                {
                    "webcall": {"live": True},
                    "mix": {
                        "analysis": {"rms_dbfs": -20, "peak_dbfs": -10},
                        "da6400": {"transport": "RECORD"},
                    },
                    "operations": {
                        "ikon_amplifiers": [
                            {"protocol_ok": True, "state": "operational"}
                        ]
                    },
                    "transcription": {
                        "active": {
                            "configured": True,
                            "ok": True,
                            "stale": True,
                        }
                    },
                }
            ),
            (False, "idle"),
        )

    def test_batch_gate_checks_all_configured_transcription_rooms(self):
        self.assertEqual(
            self.bridge._activity_inhibits_batch(
                {
                    "transcription": {
                        "active": {"configured": True, "ok": True, "stale": True},
                        "room_b": {"configured": True, "ok": True, "stale": False},
                    }
                }
            ),
            (True, "live_transcription_active"),
        )

    def test_batch_gate_holds_through_meeting_pauses(self):
        self.bridge.batch_activity_hold_seconds = 15

        self.assertEqual(
            self.bridge._apply_batch_activity_hold(
                True,
                "live_transcription_active",
                100.0,
            ),
            (True, "live_transcription_active"),
        )
        self.assertEqual(
            self.bridge._apply_batch_activity_hold(False, "idle", 200.0),
            (False, "idle"),
        )
        self.assertEqual(
            self.bridge._apply_batch_activity_hold(False, "idle", 1900.0),
            (False, "idle"),
        )

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

    def test_managed_batch_client_stops_conflict_and_starts_backend(self):
        backend_port = self.backend.server_address[1]
        client = WhisperCppClient(
            backend_url=f"http://127.0.0.1:{backend_port}/inference",
            backend_timeout_seconds=2,
            model="ggml-large-v3",
            device="vulkan:1",
            managed_service="ntc-whisper-cpp-backend.service",
            conflicting_service="ntc-agent-llm.service",
            service_idle_seconds=0,
        )

        with (
            mock.patch.object(client, "ready", side_effect=[False, True]),
            mock.patch.object(client, "_systemctl") as systemctl,
        ):
            payload = client.transcribe(
                _wav_bytes(),
                language="en",
                prompt="",
            )

        self.assertEqual(payload["text"], "Bridge transcript.")
        self.assertEqual(
            systemctl.call_args_list,
            [
                mock.call("stop", "ntc-agent-llm.service"),
                mock.call("start", "ntc-whisper-cpp-backend.service"),
            ],
        )

    def test_managed_batch_stop_clears_forced_stop_failure_state(self):
        backend_port = self.backend.server_address[1]
        client = WhisperCppClient(
            backend_url=f"http://127.0.0.1:{backend_port}/inference",
            backend_timeout_seconds=2,
            model="ggml-large-v3",
            device="vulkan:1",
            managed_service="ntc-whisper-cpp-backend.service",
        )

        with mock.patch.object(client, "_systemctl") as systemctl:
            client.stop_managed_service()

        self.assertEqual(
            systemctl.call_args_list,
            [
                mock.call("stop", "ntc-whisper-cpp-backend.service"),
                mock.call("reset-failed", "ntc-whisper-cpp-backend.service"),
            ],
        )

    def test_batch_backend_unit_uses_compute_gpu_and_conflicts_with_agent(self):
        unit = (
            Path(__file__).resolve().parent
            / "ops/linux/ntc-whisper-cpp-backend.service"
        ).read_text(encoding="utf-8")

        self.assertIn("ExecStartPre=", unit)
        self.assertIn("vulkaninfo --summary", unit)
        self.assertIn('"^GPU1:"', unit)
        self.assertIn('"deviceName.*AMD Radeon"', unit)
        self.assertIn("-dev 1", unit)
        self.assertIn("Conflicts=ntc-agent-llm.service", unit)
        self.assertIn("TimeoutStartSec=120", unit)
        self.assertIn("TimeoutStopSec=2", unit)
        self.assertNotIn("--convert", unit)
        self.assertNotIn("-p 2", unit)

    def test_bridge_unit_reserves_live_and_batch_lanes(self):
        unit = (
            Path(__file__).resolve().parent
            / "ops/linux/ntc-whisper-bridge.service"
        ).read_text(encoding="utf-8")

        self.assertIn("--backend-url http://127.0.0.1:8769/inference", unit)
        self.assertIn("--batch-backend-url http://127.0.0.1:8767/inference", unit)
        self.assertIn("--batch-threshold-seconds 30", unit)
        self.assertIn(
            "--batch-inhibit-url http://127.0.0.1:1986/api/operations/status",
            unit,
        )
        self.assertIn("--batch-inhibit-cache-seconds 1", unit)
        self.assertIn("--batch-activity-hold-seconds 0", unit)
        self.assertIn("--max-queued-requests 2", unit)
        self.assertIn("--max-batch-queued-requests 1", unit)
        self.assertIn(
            "--batch-service ntc-whisper-cpp-backend.service",
            unit,
        )
        self.assertIn(
            "--batch-conflicting-service ntc-agent-llm.service",
            unit,
        )
        self.assertIn("--model ggml-large-v3-turbo", unit)
        self.assertIn("--batch-model ggml-large-v3-turbo", unit)
        self.assertIn("--device vulkan:1", unit)
        self.assertIn("--batch-device vulkan:1", unit)
        self.assertNotIn(
            "Requires=ntc-whisper-live-backend.service "
            "ntc-whisper-cpp-backend.service",
            unit,
        )

        source = (
            Path(__file__).resolve().parent / "ntc_transcription_source.py"
        ).read_text(encoding="utf-8")
        self.assertIn('lane="batch"', source)

        live_unit = (
            Path(__file__).resolve().parent
            / "ops/linux/ntc-whisper-live-backend.service"
        ).read_text(encoding="utf-8")
        self.assertIn('"^GPU1:"', live_unit)
        self.assertIn("ggml-large-v3-turbo.bin", live_unit)
        self.assertIn("--port 8769", live_unit)
        self.assertNotIn("--convert", live_unit)

        batch_unit = (
            Path(__file__).resolve().parent
            / "ops/linux/ntc-whisper-cpp-backend.service"
        ).read_text(encoding="utf-8")
        self.assertIn("ggml-large-v3-turbo.bin", batch_unit)
        self.assertNotIn("ggml-large-v3.bin", batch_unit)

    def test_batch_inhibit_monitor_stops_running_batch_backend(self):
        with (
            mock.patch.object(
                self.bridge,
                "batch_inhibit_state",
                return_value=(True, "live_transcription_active"),
            ),
            mock.patch.object(self.bridge, "stop_batch_backend") as stop_backend,
        ):
            inhibited, reason = self.bridge.enforce_batch_inhibit_once()

        self.assertTrue(inhibited)
        self.assertEqual(reason, "live_transcription_active")
        stop_backend.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
