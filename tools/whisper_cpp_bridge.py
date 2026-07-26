#!/usr/bin/env python3
"""Raw-WAV compatibility bridge for a persistent whisper.cpp server."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import threading
import time
import urllib.error
import urllib.request
import uuid
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


DEFAULT_MAX_BODY_MB = 96
DEFAULT_MAX_QUEUED_REQUESTS = 8
DEFAULT_QUEUE_TIMEOUT_SECONDS = 120.0
DEFAULT_BACKEND_TIMEOUT_SECONDS = 180.0
DEFAULT_BATCH_THRESHOLD_SECONDS = 30.0
DEFAULT_MAX_BATCH_QUEUED_REQUESTS = 1
DEFAULT_BATCH_QUEUE_TIMEOUT_SECONDS = 1.0
DEFAULT_BATCH_BACKEND_TIMEOUT_SECONDS = 300.0


def _json_bytes(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _wav_duration_seconds(wav_bytes: bytes) -> float:
    with wave.open(io.BytesIO(wav_bytes), "rb") as wav_file:
        frame_rate = wav_file.getframerate()
        if frame_rate <= 0:
            return 0.0
        return wav_file.getnframes() / float(frame_rate)


def _multipart_body(
    wav_bytes: bytes,
    *,
    language: str,
    prompt: str,
) -> tuple[bytes, str]:
    boundary = f"ntc-whisper-{uuid.uuid4().hex}"
    chunks: list[bytes] = []

    def add_field(name: str, value: str) -> None:
        chunks.extend(
            [
                f"--{boundary}\r\n".encode(),
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode(),
                value.encode("utf-8"),
                b"\r\n",
            ]
        )

    add_field("response_format", "json")
    add_field("temperature", "0.0")
    if language:
        add_field("language", language)
    if prompt:
        add_field("prompt", prompt)

    chunks.extend(
        [
            f"--{boundary}\r\n".encode(),
            b'Content-Disposition: form-data; name="file"; filename="audio.wav"\r\n',
            b"Content-Type: audio/wav\r\n\r\n",
            wav_bytes,
            b"\r\n",
            f"--{boundary}--\r\n".encode(),
        ]
    )
    return b"".join(chunks), f"multipart/form-data; boundary={boundary}"


class WhisperCppClient:
    def __init__(
        self,
        *,
        backend_url: str,
        backend_timeout_seconds: float,
        model: str,
        device: str,
    ):
        self.backend_url = backend_url
        self.backend_timeout_seconds = backend_timeout_seconds
        self.model = model
        self.device = device
        self.lock = threading.Lock()

    def ready(self) -> bool:
        root_url = self.backend_url.rsplit("/", 1)[0] + "/"
        try:
            with urllib.request.urlopen(root_url, timeout=2.0) as response:
                return 200 <= response.status < 300
        except (OSError, urllib.error.URLError):
            return False

    def transcribe(self, wav_bytes: bytes, *, language: str, prompt: str) -> dict:
        body, content_type = _multipart_body(
            wav_bytes,
            language=language,
            prompt=prompt,
        )
        request = urllib.request.Request(
            self.backend_url,
            data=body,
            headers={
                "Content-Type": content_type,
                "Content-Length": str(len(body)),
            },
            method="POST",
        )
        started_at = time.monotonic()
        with self.lock:
            with urllib.request.urlopen(
                request,
                timeout=self.backend_timeout_seconds,
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
        return {
            "text": str(payload.get("text") or "").strip(),
            "inference_seconds": round(time.monotonic() - started_at, 3),
        }


class WhisperBridgeServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address,
        handler_cls,
        *,
        client: WhisperCppClient,
        quiet: bool,
        max_body_bytes: int,
        max_queued_requests: int,
        queue_timeout_seconds: float,
        api_token: str,
        batch_client: WhisperCppClient | None = None,
        batch_threshold_seconds: float = DEFAULT_BATCH_THRESHOLD_SECONDS,
        max_batch_queued_requests: int = DEFAULT_MAX_BATCH_QUEUED_REQUESTS,
        batch_queue_timeout_seconds: float = DEFAULT_BATCH_QUEUE_TIMEOUT_SECONDS,
    ):
        super().__init__(server_address, handler_cls)
        self.client = client
        self.batch_client = batch_client
        self.clients = {
            "live": client,
            "batch": batch_client or client,
        }
        self.batch_enabled = batch_client is not None
        self.batch_threshold_seconds = max(0.0, float(batch_threshold_seconds))
        self.quiet = quiet
        self.max_body_bytes = max_body_bytes
        self.max_queued_requests = max(1, int(max_queued_requests))
        self.queue_timeout_seconds = max(0.0, float(queue_timeout_seconds))
        self.max_batch_queued_requests = max(1, int(max_batch_queued_requests))
        self.batch_queue_timeout_seconds = max(
            0.0,
            float(batch_queue_timeout_seconds),
        )
        self.api_token = api_token
        self.lane_limits = {
            "live": self.max_queued_requests,
            "batch": self.max_batch_queued_requests,
        }
        self.lane_timeouts = {
            "live": self.queue_timeout_seconds,
            "batch": self.batch_queue_timeout_seconds,
        }
        self.request_slots = {
            lane: threading.BoundedSemaphore(limit)
            for lane, limit in self.lane_limits.items()
        }
        self.stats_lock = threading.Lock()
        self.active_requests = 0
        self.accepted_requests = 0
        self.completed_requests = 0
        self.failed_requests = 0
        self.rejected_requests = 0
        self.lane_stats = {
            lane: {
                "active_requests": 0,
                "accepted_requests": 0,
                "completed_requests": 0,
                "failed_requests": 0,
                "rejected_requests": 0,
            }
            for lane in self.lane_limits
        }
        self.started_at = time.time()

    def stats(self) -> dict:
        with self.stats_lock:
            return {
                "active_requests": self.active_requests,
                "queued_capacity": self.max_queued_requests,
                "accepted_requests": self.accepted_requests,
                "completed_requests": self.completed_requests,
                "failed_requests": self.failed_requests,
                "rejected_requests": self.rejected_requests,
                "batch_threshold_seconds": self.batch_threshold_seconds,
                "lanes": {
                    lane: {
                        **values,
                        "capacity": self.lane_limits[lane],
                        "queue_timeout_seconds": self.lane_timeouts[lane],
                    }
                    for lane, values in self.lane_stats.items()
                },
            }

    def select_lane(
        self,
        audio_seconds: float,
        requested_lane: str = "",
    ) -> str:
        if self.batch_enabled and requested_lane == "batch":
            return "batch"
        if requested_lane == "live" and audio_seconds <= self.batch_threshold_seconds:
            return "live"
        if self.batch_enabled and audio_seconds > self.batch_threshold_seconds:
            return "batch"
        return "live"

    def client_for(self, lane: str) -> WhisperCppClient:
        return self.clients[lane]

    def lane_available(self, lane: str) -> bool:
        with self.stats_lock:
            return (
                self.lane_stats[lane]["active_requests"]
                < self.lane_limits[lane]
            )

    @contextlib.contextmanager
    def request_slot(self, lane: str):
        started_at = time.monotonic()
        acquired = self.request_slots[lane].acquire(
            timeout=self.lane_timeouts[lane]
        )
        queue_wait_seconds = time.monotonic() - started_at
        if not acquired:
            with self.stats_lock:
                self.rejected_requests += 1
                self.lane_stats[lane]["rejected_requests"] += 1
            yield False, queue_wait_seconds
            return
        with self.stats_lock:
            self.active_requests += 1
            self.accepted_requests += 1
            self.lane_stats[lane]["active_requests"] += 1
            self.lane_stats[lane]["accepted_requests"] += 1
        try:
            yield True, queue_wait_seconds
        finally:
            with self.stats_lock:
                self.active_requests = max(0, self.active_requests - 1)
                self.lane_stats[lane]["active_requests"] = max(
                    0,
                    self.lane_stats[lane]["active_requests"] - 1,
                )
            self.request_slots[lane].release()

    def record_failure(self, lane: str) -> None:
        with self.stats_lock:
            self.failed_requests += 1
            self.lane_stats[lane]["failed_requests"] += 1

    def record_completion(self, lane: str) -> None:
        with self.stats_lock:
            self.completed_requests += 1
            self.lane_stats[lane]["completed_requests"] += 1


class WhisperBridgeHandler(BaseHTTPRequestHandler):
    server_version = "NTCWhisperCppBridge/1.0"

    def do_GET(self):  # noqa: N802
        path = urlsplit(self.path).path
        if path not in {"/healthz", "/readyz", "/stats"}:
            self.send_error(404)
            return
        if path == "/stats" and not self._authorized():
            self._send_json({"error": "unauthorized"}, status=401)
            return
        live_ready = self.server.client.ready()
        batch_ready = (
            self.server.batch_client.ready()
            if self.server.batch_client is not None
            else live_ready
        )
        ready = live_ready
        if path == "/readyz":
            ready = ready and self.server.lane_available("live")
        status = 200 if ready else 503
        self._send_json(
            {
                "ok": ready,
                "backend_ready": live_ready,
                "live_backend_ready": live_ready,
                "batch_backend_ready": batch_ready,
                "model": self.server.client.model,
                "device": self.server.client.device,
                "uptime_seconds": round(time.time() - self.server.started_at, 3),
                "max_body_bytes": self.server.max_body_bytes,
                **self.server.stats(),
            },
            status=status,
        )

    def do_POST(self):  # noqa: N802
        request_started_at = time.monotonic()
        request_id = self.headers.get("X-Request-ID") or uuid.uuid4().hex
        if urlsplit(self.path).path not in {
            "/transcription",
            "/transcribe",
            "/v1/audio/transcriptions",
        }:
            self.send_error(404)
            return
        if not self._authorized():
            self._send_json(
                {"error": "unauthorized", "request_id": request_id},
                status=401,
            )
            return
        try:
            content_length = int(self.headers.get("Content-Length") or "0")
        except ValueError:
            self._send_json(
                {"error": "invalid Content-Length", "request_id": request_id},
                status=400,
            )
            return
        if content_length <= 0:
            self._send_json(
                {"error": "missing WAV body", "request_id": request_id},
                status=400,
            )
            return
        if content_length > self.server.max_body_bytes:
            with self.server.stats_lock:
                self.server.rejected_requests += 1
            self._send_json(
                {
                    "error": "request body too large",
                    "request_id": request_id,
                    "max_body_bytes": self.server.max_body_bytes,
                },
                status=413,
            )
            return

        query = parse_qs(urlsplit(self.path).query)
        language = query.get(
            "language",
            [os.getenv("NTC_WHISPER_LANGUAGE", "en")],
        )[0]
        prompt = query.get(
            "prompt",
            [os.getenv("NTC_WHISPER_PROMPT", "")],
        )[0]
        wav_bytes = self.rfile.read(content_length)
        try:
            audio_seconds = _wav_duration_seconds(wav_bytes)
        except (EOFError, wave.Error) as exc:
            self._send_json(
                {"error": f"invalid WAV body: {exc}", "request_id": request_id},
                status=400,
            )
            return

        requested_lane = str(query.get("lane", [""])[0]).strip().lower()
        lane = self.server.select_lane(audio_seconds, requested_lane)
        client = self.server.client_for(lane)
        with self.server.request_slot(lane) as (accepted, queue_wait_seconds):
            if not accepted:
                self._send_json(
                    {
                        "error": f"{lane} transcription queue is full",
                        "lane": lane,
                        "request_id": request_id,
                        "queue_wait_seconds": round(queue_wait_seconds, 3),
                    },
                    status=429,
                )
                return
            try:
                result = client.transcribe(
                    wav_bytes,
                    language=language,
                    prompt=prompt,
                )
            except Exception as exc:
                self.server.record_failure(lane)
                self._send_json(
                    {
                        "error": str(exc)[:240],
                        "lane": lane,
                        "request_id": request_id,
                    },
                    status=503,
                )
                return

        self.server.record_completion(lane)
        result.update(
            {
                "audio_seconds": round(audio_seconds, 3),
                "seconds": round(time.monotonic() - request_started_at, 3),
                "queue_wait_seconds": round(queue_wait_seconds, 3),
                "lane": lane,
                "request_id": request_id,
                "endpoint_version": "2026-07-26-whisper-cpp-dual-lane",
                "model": client.model,
                "device": client.device,
                "language": language,
            }
        )
        self._send_json(result)

    def _authorized(self) -> bool:
        expected = self.server.api_token
        if not expected:
            return True
        authorization = self.headers.get("Authorization", "")
        if (
            authorization.startswith("Bearer ")
            and authorization.removeprefix("Bearer ").strip() == expected
        ):
            return True
        return self.headers.get("X-NTC-Whisper-Token", "") == expected

    def log_message(self, fmt, *args):
        if self.server.quiet:
            return
        super().log_message(fmt, *args)

    def _send_json(self, payload: dict, status: int = 200):
        body = _json_bytes(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            return


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Expose the NTC raw-WAV API over a whisper.cpp server.",
    )
    parser.add_argument(
        "--host",
        default=os.getenv("NTC_WHISPER_HOST", "127.0.0.1"),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("NTC_WHISPER_PORT", "8766")),
    )
    parser.add_argument(
        "--backend-url",
        default=os.getenv(
            "NTC_WHISPER_CPP_BACKEND_URL",
            "http://127.0.0.1:8767/inference",
        ),
    )
    parser.add_argument(
        "--backend-timeout-seconds",
        type=float,
        default=float(
            os.getenv(
                "NTC_WHISPER_BACKEND_TIMEOUT_SECONDS",
                str(DEFAULT_BACKEND_TIMEOUT_SECONDS),
            )
        ),
    )
    parser.add_argument(
        "--batch-backend-url",
        default=os.getenv("NTC_WHISPER_CPP_BATCH_BACKEND_URL", ""),
    )
    parser.add_argument(
        "--batch-backend-timeout-seconds",
        type=float,
        default=float(
            os.getenv(
                "NTC_WHISPER_BATCH_BACKEND_TIMEOUT_SECONDS",
                str(DEFAULT_BATCH_BACKEND_TIMEOUT_SECONDS),
            )
        ),
    )
    parser.add_argument(
        "--batch-threshold-seconds",
        type=float,
        default=float(
            os.getenv(
                "NTC_WHISPER_BATCH_THRESHOLD_SECONDS",
                str(DEFAULT_BATCH_THRESHOLD_SECONDS),
            )
        ),
    )
    parser.add_argument(
        "--model",
        default=os.getenv("NTC_WHISPER_MODEL", "ggml-large-v3"),
    )
    parser.add_argument(
        "--device",
        default=os.getenv("NTC_WHISPER_DEVICE", "vulkan:1"),
    )
    parser.add_argument(
        "--max-body-mb",
        type=int,
        default=int(
            os.getenv("NTC_WHISPER_MAX_BODY_MB", str(DEFAULT_MAX_BODY_MB))
        ),
    )
    parser.add_argument(
        "--max-queued-requests",
        type=int,
        default=int(
            os.getenv(
                "NTC_WHISPER_MAX_QUEUED_REQUESTS",
                str(DEFAULT_MAX_QUEUED_REQUESTS),
            )
        ),
    )
    parser.add_argument(
        "--queue-timeout-seconds",
        type=float,
        default=float(
            os.getenv(
                "NTC_WHISPER_QUEUE_TIMEOUT_SECONDS",
                str(DEFAULT_QUEUE_TIMEOUT_SECONDS),
            )
        ),
    )
    parser.add_argument(
        "--max-batch-queued-requests",
        type=int,
        default=int(
            os.getenv(
                "NTC_WHISPER_MAX_BATCH_QUEUED_REQUESTS",
                str(DEFAULT_MAX_BATCH_QUEUED_REQUESTS),
            )
        ),
    )
    parser.add_argument(
        "--batch-queue-timeout-seconds",
        type=float,
        default=float(
            os.getenv(
                "NTC_WHISPER_BATCH_QUEUE_TIMEOUT_SECONDS",
                str(DEFAULT_BATCH_QUEUE_TIMEOUT_SECONDS),
            )
        ),
    )
    parser.add_argument(
        "--api-token",
        default=os.getenv("NTC_WHISPER_API_TOKEN", ""),
    )
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    client = WhisperCppClient(
        backend_url=args.backend_url,
        backend_timeout_seconds=max(1.0, args.backend_timeout_seconds),
        model=args.model,
        device=args.device,
    )
    batch_client = None
    if args.batch_backend_url:
        batch_client = WhisperCppClient(
            backend_url=args.batch_backend_url,
            backend_timeout_seconds=max(
                1.0,
                args.batch_backend_timeout_seconds,
            ),
            model=args.model,
            device=args.device,
        )
    server = WhisperBridgeServer(
        (args.host, args.port),
        WhisperBridgeHandler,
        client=client,
        quiet=args.quiet,
        max_body_bytes=max(1, args.max_body_mb) * 1024 * 1024,
        max_queued_requests=max(1, args.max_queued_requests),
        queue_timeout_seconds=max(0.0, args.queue_timeout_seconds),
        api_token=args.api_token,
        batch_client=batch_client,
        batch_threshold_seconds=max(0.0, args.batch_threshold_seconds),
        max_batch_queued_requests=max(1, args.max_batch_queued_requests),
        batch_queue_timeout_seconds=max(
            0.0,
            args.batch_queue_timeout_seconds,
        ),
    )
    print(
        f"NTC Whisper.cpp bridge listening on "
        f"http://{args.host}:{args.port}/transcription "
        f"backend={args.backend_url} batch_backend={args.batch_backend_url or 'disabled'} "
        f"model={args.model} device={args.device}",
        flush=True,
    )
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
