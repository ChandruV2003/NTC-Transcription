#!/usr/bin/env python3
"""Persistent local Whisper HTTP bridge for NTC transcription.

This is intended to run on the M4 Mac mini. WebCall posts WAV chunks to
``/transcription`` and receives JSON: ``{"text": "..."}``. ``/transcribe``
is kept as a compatibility alias.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import math
import os
import threading
import time
import uuid
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import numpy as np
import torch
from scipy.signal import resample_poly
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor


TARGET_RATE_HZ = 16000
DEFAULT_MAX_BODY_MB = 96
DEFAULT_MAX_QUEUED_REQUESTS = 8
DEFAULT_QUEUE_TIMEOUT_SECONDS = 120.0


def _json_bytes(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _decode_wav(wav_bytes: bytes) -> tuple[np.ndarray, float]:
    with wave.open(io.BytesIO(wav_bytes), "rb") as wav_file:
        channels = wav_file.getnchannels()
        sample_width = wav_file.getsampwidth()
        sample_rate_hz = wav_file.getframerate()
        frames = wav_file.readframes(wav_file.getnframes())

    if sample_width == 1:
        audio = (np.frombuffer(frames, dtype=np.uint8).astype(np.float32) - 128.0) / 128.0
    elif sample_width == 2:
        audio = np.frombuffer(frames, dtype="<i2").astype(np.float32) / 32768.0
    elif sample_width == 4:
        audio = np.frombuffer(frames, dtype="<i4").astype(np.float32) / 2147483648.0
    else:
        raise ValueError(f"unsupported WAV sample width: {sample_width}")

    if channels > 1:
        audio = audio.reshape(-1, channels).mean(axis=1)

    source_seconds = len(audio) / float(sample_rate_hz or TARGET_RATE_HZ)
    if sample_rate_hz != TARGET_RATE_HZ:
        gcd = math.gcd(sample_rate_hz, TARGET_RATE_HZ)
        audio = resample_poly(audio, TARGET_RATE_HZ // gcd, sample_rate_hz // gcd).astype(np.float32)

    return np.ascontiguousarray(audio, dtype=np.float32), source_seconds


class WhisperLargeTranscriber:
    def __init__(self, *, model_id: str, device: str, max_new_tokens: int):
        self.model_id = model_id
        self.device = device
        self.max_new_tokens = max_new_tokens
        # Keep MPS on float32. float16 is faster but produced unstable Whisper
        # decoding on this Mac mini with large-v3.
        self.dtype = torch.float32
        started_at = time.monotonic()
        self.processor = AutoProcessor.from_pretrained(model_id)
        self.model = AutoModelForSpeechSeq2Seq.from_pretrained(
            model_id,
            dtype=self.dtype,
            low_cpu_mem_usage=False,
        )
        self.model.to(device)
        self.model.eval()
        self.load_seconds = time.monotonic() - started_at
        self.lock = threading.Lock()

    def transcribe(self, wav_bytes: bytes, *, language: str, prompt: str, max_new_tokens: int | None = None) -> dict:
        audio, audio_seconds = _decode_wav(wav_bytes)
        if len(audio) == 0:
            return {"text": "", "audio_seconds": 0.0, "inference_seconds": 0.0}

        started_at = time.monotonic()
        inputs = self.processor(
            audio,
            sampling_rate=TARGET_RATE_HZ,
            return_tensors="pt",
            return_attention_mask=True,
        )
        input_features = inputs.input_features.to(self.device, dtype=self.dtype)
        attention_mask = getattr(inputs, "attention_mask", None)
        generate_kwargs = {
            "task": "transcribe",
            "max_new_tokens": max_new_tokens or self.max_new_tokens,
        }
        if language:
            generate_kwargs["language"] = language
        if attention_mask is not None:
            generate_kwargs["attention_mask"] = attention_mask.to(self.device)
        if prompt:
            try:
                prompt_ids = self.processor.get_prompt_ids(prompt, return_tensors="pt")
                generate_kwargs["prompt_ids"] = prompt_ids.to(self.device)
            except Exception:
                pass

        with self.lock:
            with torch.no_grad():
                predicted_ids = self.model.generate(input_features, **generate_kwargs)

        text = self.processor.batch_decode(predicted_ids, skip_special_tokens=True)[0].strip()
        return {
            "text": text,
            "audio_seconds": round(audio_seconds, 3),
            "inference_seconds": round(time.monotonic() - started_at, 3),
        }


class WhisperServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address,
        handler_cls,
        *,
        transcriber: WhisperLargeTranscriber,
        quiet: bool,
        max_body_bytes: int,
        max_queued_requests: int,
        queue_timeout_seconds: float,
        api_token: str,
    ):
        super().__init__(server_address, handler_cls)
        self.transcriber = transcriber
        self.quiet = quiet
        self.max_body_bytes = max_body_bytes
        self.max_queued_requests = max(1, int(max_queued_requests or 1))
        self.queue_timeout_seconds = max(0.0, float(queue_timeout_seconds or 0.0))
        self.api_token = api_token
        self.request_slots = threading.BoundedSemaphore(self.max_queued_requests)
        self.stats_lock = threading.Lock()
        self.active_requests = 0
        self.accepted_requests = 0
        self.completed_requests = 0
        self.failed_requests = 0
        self.rejected_requests = 0
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
            }

    @contextlib.contextmanager
    def request_slot(self, *, timeout_seconds: float):
        started_at = time.monotonic()
        acquired = self.request_slots.acquire(timeout=timeout_seconds)
        queue_wait_seconds = time.monotonic() - started_at
        if not acquired:
            with self.stats_lock:
                self.rejected_requests += 1
            yield False, queue_wait_seconds
            return
        with self.stats_lock:
            self.active_requests += 1
            self.accepted_requests += 1
        try:
            yield True, queue_wait_seconds
        finally:
            with self.stats_lock:
                self.active_requests = max(0, self.active_requests - 1)
            self.request_slots.release()


class WhisperHandler(BaseHTTPRequestHandler):
    server_version = "NTCWhisperLarge/1.0"

    def do_GET(self):  # noqa: N802
        path = urlsplit(self.path).path
        if path not in {"/healthz", "/readyz", "/stats"}:
            self.send_error(404)
            return
        if path == "/stats" and not self._authorized():
            self._send_json({"error": "unauthorized"}, status=401)
            return
        self._send_json(
            {
                "ok": True,
                "model": self.server.transcriber.model_id,
                "device": self.server.transcriber.device,
                "load_seconds": round(self.server.transcriber.load_seconds, 3),
                "uptime_seconds": round(time.time() - self.server.started_at, 3),
                "max_body_bytes": self.server.max_body_bytes,
                **self.server.stats(),
            }
        )

    def do_POST(self):  # noqa: N802
        request_started_at = time.monotonic()
        request_id = self.headers.get("X-Request-ID") or uuid.uuid4().hex
        if urlsplit(self.path).path not in {"/transcription", "/transcribe", "/v1/audio/transcriptions"}:
            self.send_error(404)
            return
        if not self._authorized():
            self._send_json({"error": "unauthorized", "request_id": request_id}, status=401)
            return
        try:
            content_length = int(self.headers.get("Content-Length") or "0")
        except ValueError:
            self._send_json({"error": "invalid Content-Length", "request_id": request_id}, status=400)
            return
        if content_length <= 0:
            self._send_json({"error": "missing WAV body", "request_id": request_id}, status=400)
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
        language = query.get("language", [os.getenv("NTC_WHISPER_LANGUAGE", "en")])[0]
        prompt = query.get("prompt", [os.getenv("NTC_WHISPER_PROMPT", "")])[0]
        try:
            max_new_tokens = int(query.get("max_new_tokens", [str(self.server.transcriber.max_new_tokens)])[0])
        except ValueError:
            self._send_json({"error": "invalid max_new_tokens", "request_id": request_id}, status=400)
            return
        wav_bytes = self.rfile.read(content_length)

        with self.server.request_slot(timeout_seconds=self.server.queue_timeout_seconds) as (accepted, queue_wait_seconds):
            if not accepted:
                self._send_json(
                    {
                        "error": "transcription queue is full",
                        "request_id": request_id,
                        "queue_wait_seconds": round(queue_wait_seconds, 3),
                    },
                    status=429,
                )
                return
            try:
                result = self.server.transcriber.transcribe(
                    wav_bytes,
                    language=language,
                    prompt=prompt,
                    max_new_tokens=max_new_tokens,
                )
            except Exception as exc:
                with self.server.stats_lock:
                    self.server.failed_requests += 1
                self._send_json({"error": str(exc)[:240], "request_id": request_id}, status=500)
                return

        with self.server.stats_lock:
            self.server.completed_requests += 1
        elapsed_seconds = time.monotonic() - request_started_at
        result.setdefault("seconds", round(elapsed_seconds, 3))
        result.setdefault("queue_wait_seconds", round(queue_wait_seconds, 3))
        result["request_id"] = request_id
        result["endpoint_version"] = "2026-06-13"

        result.update(
            {
                "model": self.server.transcriber.model_id,
                "device": self.server.transcriber.device,
                "language": language,
            }
        )
        self._send_json(result)

    def _authorized(self) -> bool:
        expected = self.server.api_token
        if not expected:
            return True
        authorization = self.headers.get("Authorization", "")
        if authorization.startswith("Bearer ") and authorization.removeprefix("Bearer ").strip() == expected:
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
        self.wfile.write(body)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a persistent Whisper large HTTP bridge for NTC.")
    parser.add_argument("--host", default=os.getenv("NTC_WHISPER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("NTC_WHISPER_PORT", "8766")))
    parser.add_argument("--model", default=os.getenv("NTC_WHISPER_MODEL", "openai/whisper-large-v3"))
    parser.add_argument("--device", default=os.getenv("NTC_WHISPER_DEVICE", "cpu"), choices=("cpu", "mps"))
    parser.add_argument("--max-new-tokens", type=int, default=int(os.getenv("NTC_WHISPER_MAX_NEW_TOKENS", "160")))
    parser.add_argument("--max-body-mb", type=int, default=int(os.getenv("NTC_WHISPER_MAX_BODY_MB", str(DEFAULT_MAX_BODY_MB))))
    parser.add_argument(
        "--max-queued-requests",
        type=int,
        default=int(os.getenv("NTC_WHISPER_MAX_QUEUED_REQUESTS", str(DEFAULT_MAX_QUEUED_REQUESTS))),
    )
    parser.add_argument(
        "--queue-timeout-seconds",
        type=float,
        default=float(os.getenv("NTC_WHISPER_QUEUE_TIMEOUT_SECONDS", str(DEFAULT_QUEUE_TIMEOUT_SECONDS))),
    )
    parser.add_argument("--api-token", default=os.getenv("NTC_WHISPER_API_TOKEN", ""))
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    transcriber = WhisperLargeTranscriber(
        model_id=args.model,
        device=args.device,
        max_new_tokens=args.max_new_tokens,
    )
    server = WhisperServer(
        (args.host, args.port),
        WhisperHandler,
        transcriber=transcriber,
        quiet=args.quiet,
        max_body_bytes=max(1, args.max_body_mb) * 1024 * 1024,
        max_queued_requests=max(1, args.max_queued_requests),
        queue_timeout_seconds=max(0.0, args.queue_timeout_seconds),
        api_token=args.api_token,
    )
    print(
        f"NTC Whisper listening on http://{args.host}:{args.port}/transcription "
        f"model={args.model} device={args.device} queued={args.max_queued_requests}",
        flush=True,
    )
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
