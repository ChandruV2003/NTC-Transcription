#!/usr/bin/env python3
"""Bridge NTC Transcription text into a ToneVision transmitter room."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import random
import socket
import ssl
import struct
import sys
import time
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen


GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


@dataclass
class Segment:
    id: int
    text: str


class ToneVisionWebSocket:
    def __init__(self, url: str, *, timeout: float = 10.0):
        self.url = url
        self.timeout = timeout
        self.sock: socket.socket | ssl.SSLSocket | None = None

    def connect(self) -> None:
        parsed = urlparse(self.url)
        if parsed.scheme not in {"ws", "wss"}:
            raise ValueError("ToneVision websocket URL must start with ws:// or wss://")
        host = parsed.hostname or ""
        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        raw_sock = socket.create_connection((host, port), timeout=self.timeout)
        raw_sock.settimeout(self.timeout)
        if parsed.scheme == "wss":
            self.sock = ssl.create_default_context().wrap_socket(raw_sock, server_hostname=host)
        else:
            self.sock = raw_sock
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        ).encode("ascii")
        self.sock.sendall(request)
        response = self._read_http_response()
        if " 101 " not in response.split("\r\n", 1)[0]:
            raise RuntimeError(f"ToneVision websocket handshake failed: {response.splitlines()[0] if response else 'empty response'}")
        expected_accept = base64.b64encode(hashlib.sha1((key + GUID).encode("ascii")).digest()).decode("ascii")
        if expected_accept not in response:
            raise RuntimeError("ToneVision websocket handshake returned an unexpected accept key")

    def _read_http_response(self) -> str:
        assert self.sock is not None
        chunks = bytearray()
        while b"\r\n\r\n" not in chunks:
            chunk = self.sock.recv(4096)
            if not chunk:
                break
            chunks.extend(chunk)
            if len(chunks) > 65536:
                raise RuntimeError("ToneVision websocket handshake response was too large")
        return chunks.decode("iso-8859-1", errors="replace")

    def send_json(self, payload: dict) -> None:
        self.send_text(json.dumps(payload, separators=(",", ":")))

    def send_text(self, text: str) -> None:
        assert self.sock is not None
        payload = text.encode("utf-8")
        header = bytearray([0x81])
        length = len(payload)
        if length < 126:
            header.append(0x80 | length)
        elif length <= 0xFFFF:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", length))
        mask = random.randbytes(4) if hasattr(random, "randbytes") else os.urandom(4)
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self.sock.sendall(bytes(header) + mask + masked)

    def close(self) -> None:
        if not self.sock:
            return
        try:
            self.sock.sendall(b"\x88\x80" + os.urandom(4))
        except OSError:
            pass
        try:
            self.sock.close()
        finally:
            self.sock = None


def http_json(url: str, *, timeout: float = 10.0):
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_segments(base_url: str, room_slug: str, after_id: int, *, timeout: float = 10.0) -> list[Segment]:
    base = base_url.rstrip("/")
    room = quote(room_slug, safe="")
    query = urlencode({"after_id": str(max(0, after_id))})
    payload = http_json(f"{base}/api/public/transcription/{room}/segments?{query}", timeout=timeout)
    segments = []
    for item in payload.get("segments") or []:
        try:
            segment_id = int(item.get("id") or 0)
        except (TypeError, ValueError):
            segment_id = 0
        text = " ".join(str(item.get("text") or "").split())
        if segment_id > 0 and text:
            segments.append(Segment(segment_id, text))
    return segments


def room_has_active_typist(base_url: str, room_id: str, password: str, *, timeout: float = 10.0) -> bool:
    query = urlencode({"password": password})
    rooms = http_json(f"{base_url.rstrip('/')}/api/rooms?{query}", timeout=timeout)
    for room in rooms:
        if str(room.get("room_id") or "").casefold() == room_id.casefold():
            return bool(room.get("tx_connected"))
    raise RuntimeError(f'ToneVision room "{room_id}" was not found')


def tonevision_ws_url(base_url: str, room_id: str, pin: str) -> str:
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("ToneVision base URL must start with http:// or https://")
    scheme = "wss" if parsed.scheme == "https" else "ws"
    netloc = parsed.netloc
    query = urlencode({"pin": pin})
    return f"{scheme}://{netloc}/ws/tx/{quote(room_id, safe='')}?{query}"


def trim_buffer(parts: Iterable[str], max_chars: int) -> str:
    text = "\n".join(part for part in parts if part).strip()
    if len(text) <= max_chars:
        return text
    return text[-max_chars:].lstrip()


def run_bridge(args: argparse.Namespace) -> int:
    if args.tonevision_admin_password and not args.force_takeover:
        if room_has_active_typist(args.tonevision_base_url, args.tonevision_room, args.tonevision_admin_password, timeout=args.timeout):
            print(
                f'ToneVision room "{args.tonevision_room}" already has an active typist; '
                "refusing to take it over without --force-takeover.",
                file=sys.stderr,
            )
            return 3

    last_id = 0
    buffer_parts: list[str] = []
    if args.start_at_tail:
        initial = fetch_segments(args.ntc_base_url, args.ntc_room, 0, timeout=args.timeout)
        last_id = max((segment.id for segment in initial), default=0)
        print(f"Starting at current NTC tail after_id={last_id}", flush=True)
    else:
        initial = fetch_segments(args.ntc_base_url, args.ntc_room, 0, timeout=args.timeout)
        buffer_parts.extend(segment.text for segment in initial)
        last_id = max((segment.id for segment in initial), default=0)

    ws = ToneVisionWebSocket(tonevision_ws_url(args.tonevision_base_url, args.tonevision_room, args.tonevision_pin), timeout=args.timeout)
    ws.connect()
    print(f'Connected to ToneVision room "{args.tonevision_room}"', flush=True)
    ws.send_json({"type": "pause", "paused": False})
    if buffer_parts:
        ws.send_json({"type": "text", "text": trim_buffer(buffer_parts, args.max_chars)})

    try:
        while True:
            segments = fetch_segments(args.ntc_base_url, args.ntc_room, last_id, timeout=args.timeout)
            if segments:
                for segment in segments:
                    buffer_parts.append(segment.text)
                    last_id = max(last_id, segment.id)
                text = trim_buffer(buffer_parts, args.max_chars)
                ws.send_json({"type": "text", "text": text})
                print(f"Published through segment {last_id}; chars={len(text)}", flush=True)
            time.sleep(max(0.2, args.poll_seconds))
    finally:
        ws.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream NTC Transcription text into a ToneVision transmitter room.")
    parser.add_argument("--ntc-base-url", default=os.getenv("NTC_TRANSCRIPTION_PUBLIC_BASE_URL", "https://ntcnas.myftp.org/transcription"))
    parser.add_argument("--ntc-room", default=os.getenv("NTC_TONEVISION_NTC_ROOM", "convention-laptop"))
    parser.add_argument("--tonevision-base-url", default=os.getenv("NTC_TONEVISION_BASE_URL", "http://100.96.175.75:8080"))
    parser.add_argument("--tonevision-room", default=os.getenv("NTC_TONEVISION_ROOM", "english"))
    parser.add_argument("--tonevision-pin", default=os.getenv("NTC_TONEVISION_PIN", ""))
    parser.add_argument("--tonevision-admin-password", default=os.getenv("NTC_TONEVISION_ADMIN_PASSWORD", ""))
    parser.add_argument("--force-takeover", action="store_true")
    parser.add_argument("--start-at-tail", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--poll-seconds", type=float, default=float(os.getenv("NTC_TONEVISION_POLL_SECONDS", "1.0")))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("NTC_TONEVISION_TIMEOUT_SECONDS", "10.0")))
    parser.add_argument("--max-chars", type=int, default=int(os.getenv("NTC_TONEVISION_MAX_CHARS", "60000")))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.tonevision_pin:
        print("ToneVision PIN is required via --tonevision-pin or NTC_TONEVISION_PIN.", file=sys.stderr)
        return 2
    return run_bridge(args)


if __name__ == "__main__":
    raise SystemExit(main())
