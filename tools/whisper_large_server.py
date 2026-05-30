#!/usr/bin/env python3
"""Persistent local Whisper HTTP bridge for NTC transcription.

This is intended to run on the M4 Mac mini. WebCall posts WAV chunks to
``/transcription`` and receives JSON: ``{"text": "..."}``. ``/transcribe``
is kept as a compatibility alias.
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import threading
import time
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import numpy as np
import torch
from scipy.signal import resample_poly
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor


TARGET_RATE_HZ = 16000


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
        self.dtype = torch.float16 if device == "mps" else torch.float32
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
            return {"text": "", "audio_seconds": 0.0, "seconds": 0.0}

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
            "seconds": round(time.monotonic() - started_at, 3),
        }


class WhisperServer(ThreadingHTTPServer):
    def __init__(self, server_address, handler_cls, *, transcriber: WhisperLargeTranscriber, quiet: bool):
        super().__init__(server_address, handler_cls)
        self.transcriber = transcriber
        self.quiet = quiet
        self.started_at = time.time()


class WhisperHandler(BaseHTTPRequestHandler):
    server_version = "NTCWhisperLarge/1.0"

    def do_GET(self):  # noqa: N802
        if urlsplit(self.path).path != "/healthz":
            self.send_error(404)
            return
        self._send_json(
            {
                "ok": True,
                "model": self.server.transcriber.model_id,
                "device": self.server.transcriber.device,
                "load_seconds": round(self.server.transcriber.load_seconds, 3),
                "uptime_seconds": round(time.time() - self.server.started_at, 3),
            }
        )

    def do_POST(self):  # noqa: N802
        if urlsplit(self.path).path not in {"/transcription", "/transcribe"}:
            self.send_error(404)
            return
        content_length = int(self.headers.get("Content-Length") or "0")
        if content_length <= 0:
            self.send_error(400, "missing WAV body")
            return

        query = parse_qs(urlsplit(self.path).query)
        language = query.get("language", [os.getenv("NTC_WHISPER_LANGUAGE", "en")])[0]
        prompt = query.get("prompt", [os.getenv("NTC_WHISPER_PROMPT", "")])[0]
        max_new_tokens = int(query.get("max_new_tokens", [str(self.server.transcriber.max_new_tokens)])[0])
        wav_bytes = self.rfile.read(content_length)

        try:
            result = self.server.transcriber.transcribe(
                wav_bytes,
                language=language,
                prompt=prompt,
                max_new_tokens=max_new_tokens,
            )
        except Exception as exc:
            self.send_error(500, str(exc)[:240])
            return

        result.update(
            {
                "model": self.server.transcriber.model_id,
                "device": self.server.transcriber.device,
                "language": language,
            }
        )
        self._send_json(result)

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
    )
    print(
        f"NTC Whisper listening on http://{args.host}:{args.port}/transcription "
        f"model={args.model} device={args.device}",
        flush=True,
    )
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
