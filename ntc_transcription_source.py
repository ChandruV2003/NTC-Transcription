"""Native source ingest and transcription worker for NTC Transcription."""

from __future__ import annotations

import hmac
import json
import math
import os
import queue
import re
import shlex
import struct
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import requests
from flask import jsonify, render_template_string, request

try:
    import audioop  # type: ignore
except ImportError:  # pragma: no cover - Python 3.13+ may not ship audioop
    audioop = None


INGEST_CHUNK_SIZE = 3072
SOURCE_INGEST_READ_SIZE = 24576
RECENT_CHUNK_BACKLOG = 96
TRANSCRIPTION_STREAM_PROFILE = {
    "channels": 1,
    "sample_rate_hz": 16000,
    "bits_per_sample": 16,
}
STREAM_PROFILES = {
    "wav_pcm24": {
        "mimetype": "audio/wav",
        "channels": 1,
        "sample_rate_hz": 48000,
        "bits_per_sample": 24,
    }
}

BROWSER_CAPTURE_STREAM_PROFILE = "browser_pcm16"
BROWSER_CAPTURE_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="theme-color" content="#07111c">
  <title>{{ title }}</title>
  <link rel="icon" href="data:,">
  <style>
    :root {
      color-scheme: dark;
      --bg: #07111c;
      --panel: rgba(8, 18, 30, 0.86);
      --line: rgba(119, 180, 224, 0.34);
      --text: #f3f8ff;
      --muted: #a8b9cb;
      --good: #79edb5;
      --bad: #ff9a9a;
      --warn: #ffc06f;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100svh;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        linear-gradient(180deg, rgba(3, 8, 14, 0.22), rgba(3, 8, 14, 0.82)),
        url("{{ brand_background_url }}") center / cover fixed,
        var(--bg);
      color: var(--text);
    }
    main {
      width: min(680px, calc(100vw - 32px));
      min-height: 100svh;
      margin: 0 auto;
      display: grid;
      align-content: center;
      gap: 18px;
      padding: max(24px, env(safe-area-inset-top)) 0 max(24px, env(safe-area-inset-bottom));
    }
    header { text-align: center; }
    .eyebrow {
      display: inline-flex;
      align-items: center;
      height: 28px;
      padding: 0 14px;
      border: 1px solid var(--line);
      border-radius: 999px;
      color: #9fd4ff;
      font-size: 13px;
      font-weight: 900;
      letter-spacing: .18em;
      text-transform: uppercase;
      background: rgba(10, 24, 39, 0.76);
    }
    h1 {
      margin: 12px 0 0;
      font-size: clamp(38px, 11vw, 70px);
      line-height: .95;
      letter-spacing: 0;
    }
    .panel {
      border: 1px solid var(--line);
      border-radius: 28px;
      background: var(--panel);
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05), 0 20px 80px rgba(0, 0, 0, 0.26);
      padding: clamp(22px, 5vw, 34px);
      backdrop-filter: blur(14px);
    }
    .status-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      margin-bottom: 22px;
    }
    .source-name {
      min-width: 0;
    }
    .source-name span {
      display: block;
      color: var(--muted);
      font-size: 14px;
      font-weight: 800;
      letter-spacing: .14em;
      text-transform: uppercase;
    }
    .source-name strong {
      display: block;
      margin-top: 4px;
      font-size: clamp(26px, 7vw, 42px);
      line-height: 1.04;
    }
    .pill {
      flex: 0 0 auto;
      display: inline-flex;
      align-items: center;
      min-height: 44px;
      padding: 0 18px;
      border-radius: 999px;
      font-size: 17px;
      font-weight: 900;
      background: rgba(36, 48, 64, 0.78);
      color: var(--muted);
    }
    .pill.good { background: rgba(31, 95, 68, 0.62); color: var(--good); }
    .pill.bad { background: rgba(91, 49, 58, 0.62); color: var(--bad); }
    .pill.warn { background: rgba(89, 68, 42, 0.66); color: var(--warn); }
    .meter {
      height: 18px;
      overflow: hidden;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(4, 10, 18, 0.78);
    }
    .meter > div {
      width: 0%;
      height: 100%;
      border-radius: inherit;
      background: linear-gradient(90deg, #6dd7ff, #79edb5);
      transition: width 90ms linear;
    }
    .metrics {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      margin: 18px 0 24px;
    }
    .metric {
      border: 1px solid rgba(119, 180, 224, 0.26);
      border-radius: 18px;
      padding: 14px;
      background: rgba(5, 13, 22, 0.54);
    }
    .metric span {
      display: block;
      color: var(--muted);
      font-size: 13px;
      font-weight: 800;
      letter-spacing: .12em;
      text-transform: uppercase;
    }
    .metric strong {
      display: block;
      margin-top: 6px;
      font-size: 24px;
      line-height: 1.1;
    }
    .actions {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
    }
    .quick-actions {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 12px;
      margin-top: 12px;
    }
    button {
      min-height: 64px;
      border: 1px solid rgba(119, 180, 224, 0.42);
      border-radius: 18px;
      background: rgba(17, 37, 58, 0.92);
      color: var(--text);
      font: inherit;
      font-size: 22px;
      font-weight: 900;
      cursor: pointer;
    }
    button.primary {
      border-color: rgba(121, 237, 181, 0.58);
      background: rgba(23, 95, 69, 0.78);
      color: var(--good);
    }
    button.danger {
      border-color: rgba(255, 154, 154, 0.50);
      background: rgba(88, 41, 51, 0.72);
      color: var(--bad);
    }
    button.marker {
      font-size: 20px;
      background: rgba(17, 37, 58, 0.78);
    }
    button.muted {
      border-color: rgba(255, 192, 111, 0.58);
      background: rgba(89, 68, 42, 0.78);
      color: var(--warn);
    }
    button:disabled {
      cursor: not-allowed;
      opacity: .48;
    }
    .message {
      min-height: 24px;
      margin-top: 16px;
      color: var(--muted);
      font-size: 16px;
      line-height: 1.35;
    }
    .debug {
      display: grid;
      gap: 6px;
      margin-top: 14px;
      border: 1px solid rgba(119, 180, 224, 0.22);
      border-radius: 16px;
      padding: 12px 14px;
      background: rgba(4, 10, 18, 0.44);
      color: var(--muted);
      font-size: 13px;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }
    .debug strong {
      color: var(--text);
      font-size: 12px;
      letter-spacing: .12em;
      text-transform: uppercase;
    }
    @media (max-width: 560px) {
      main { width: min(100vw - 22px, 680px); }
      .status-row, .actions, .quick-actions { grid-template-columns: 1fr; }
      .status-row { align-items: stretch; flex-direction: column; }
      .pill { justify-content: center; }
      .metrics { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <span class="eyebrow">Source Agent</span>
      <h1>Room C Capture</h1>
    </header>
    <section class="panel">
      <div class="status-row">
        <div class="source-name">
          <span>Input</span>
          <strong>{{ host_label }}</strong>
        </div>
        <div class="pill" id="state-pill">Idle</div>
      </div>
      <div class="meter" aria-label="Input level"><div id="level-bar"></div></div>
      <div class="metrics">
        <div class="metric">
          <span>Sample Rate</span>
          <strong id="sample-rate">--</strong>
        </div>
        <div class="metric">
          <span>Queued</span>
          <strong id="queued">0</strong>
        </div>
      </div>
      <div class="actions">
        <button class="primary" id="start-button" type="button" onclick="startCapture()">Start Capture</button>
        <button class="danger" id="stop-button" type="button" onclick="stopCapture(true)" disabled>Stop Capture</button>
      </div>
      <div class="quick-actions">
        <button class="marker" id="mute-button" type="button" onclick="toggleMute()" disabled>Mute Mic</button>
        <button class="marker" type="button" data-marker-text="[Song]">[Song]</button>
        <button class="marker" type="button" data-marker-text="[Prophecy]">[Prophecy]</button>
        <button class="marker" id="tonevision-button" type="button" onclick="takeToneVision()">Take ToneVision</button>
      </div>
      <div class="message" id="message"></div>
      <div class="debug" id="debug-box" aria-live="polite">
        <strong>Debug</strong>
        <span id="debug-line">Loading capture controls...</span>
      </div>
    </section>
  </main>
  <script>
    const hostSlug = {{ host_slug_json | safe }};
    const token = {{ token_json | safe }};
    const startUrl = `/transcription/api/source/browser/start/${encodeURIComponent(hostSlug)}?token=${encodeURIComponent(token)}`;
    const chunkUrl = `/transcription/api/source/browser/chunk/${encodeURIComponent(hostSlug)}?token=${encodeURIComponent(token)}`;
    const stopUrl = `/transcription/api/source/browser/stop/${encodeURIComponent(hostSlug)}?token=${encodeURIComponent(token)}`;
    const markerUrl = `/transcription/api/source/browser/marker/${encodeURIComponent(hostSlug)}?token=${encodeURIComponent(token)}`;
    const tonevisionTakeoverUrl = `/transcription/api/source/browser/tonevision/takeover/${encodeURIComponent(hostSlug)}?token=${encodeURIComponent(token)}`;
    const startButton = document.getElementById("start-button");
    const stopButton = document.getElementById("stop-button");
    const muteButton = document.getElementById("mute-button");
    const tonevisionButton = document.getElementById("tonevision-button");
    const statePill = document.getElementById("state-pill");
    const levelBar = document.getElementById("level-bar");
    const sampleRateValue = document.getElementById("sample-rate");
    const queuedValue = document.getElementById("queued");
    const message = document.getElementById("message");
    const debugLine = document.getElementById("debug-line");

    let mediaStream = null;
    let audioContext = null;
    let processor = null;
    let sourceNode = null;
    let wakeLock = null;
    let captureActive = false;
    let sampleRate = 48000;
    let pendingChunks = [];
    let sending = false;
    let sampleBuffer = [];
    let bufferedSamples = 0;
    let startInProgress = false;
    let captureMuted = false;

    function debug(text) {
      if (debugLine) debugLine.textContent = text || "";
      console.log(`[ntc-capture] ${text || ""}`);
    }

    function setState(label, tone = "") {
      statePill.textContent = label;
      statePill.className = `pill ${tone}`.trim();
    }

    function setMessage(text) {
      message.textContent = text || "";
    }

    function updateMuteUi() {
      muteButton.disabled = !captureActive;
      muteButton.textContent = captureMuted ? "Unmute Mic" : "Mute Mic";
      muteButton.classList.toggle("muted", captureMuted);
      if (captureActive && captureMuted) {
        setState("Muted", "warn");
        setMessage("Mic is muted. iPhone still owns Room C and is sending silence.");
      } else if (captureActive) {
        setState("Live", "good");
      }
    }

    function ensureCaptureSupported() {
      if (!window.isSecureContext) {
        throw new Error("Microphone capture needs HTTPS. Open the ntcnas.myftp.org link directly in Safari.");
      }
      if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
        throw new Error("This browser is not exposing microphone access. Open this page in Safari, not an in-app browser.");
      }
      if (!window.AudioContext && !window.webkitAudioContext) {
        throw new Error("This browser is not exposing Web Audio capture.");
      }
    }

    function withTimeout(promise, milliseconds, messageText) {
      let timeoutId = null;
      const timeout = new Promise((_, reject) => {
        timeoutId = window.setTimeout(() => reject(new Error(messageText)), milliseconds);
      });
      return Promise.race([promise, timeout]).finally(() => window.clearTimeout(timeoutId));
    }

    async function postJson(url, payload) {
      const response = await fetch(url, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload || {})
      });
      if (!response.ok) {
        let errorText = `${response.status}`;
        try {
          const body = await response.json();
          errorText = body.error || errorText;
        } catch (_) {}
        throw new Error(errorText);
      }
      return response.json();
    }

    function pcm16BufferFromFloat32(channels) {
      const merged = new Float32Array(bufferedSamples);
      let offset = 0;
      for (const item of channels) {
        merged.set(item, offset);
        offset += item.length;
      }
      sampleBuffer = [];
      bufferedSamples = 0;

      const output = new ArrayBuffer(merged.length * 2);
      const view = new DataView(output);
      let sumSquares = 0;
      let peak = 0;
      for (let index = 0; index < merged.length; index += 1) {
        const sample = Math.max(-1, Math.min(1, merged[index] || 0));
        const intSample = sample < 0 ? sample * 0x8000 : sample * 0x7fff;
        view.setInt16(index * 2, intSample, true);
        const abs = Math.abs(sample);
        peak = Math.max(peak, abs);
        sumSquares += sample * sample;
      }
      const rms = merged.length ? Math.sqrt(sumSquares / merged.length) : 0;
      levelBar.style.width = `${Math.min(100, Math.round(Math.max(peak, rms * 2.2) * 100))}%`;
      return output;
    }

    function queueSamples(input) {
      if (!captureActive || !input || !input.length) return;
      const copy = new Float32Array(input.length);
      if (!captureMuted) copy.set(input);
      sampleBuffer.push(copy);
      bufferedSamples += copy.length;
      const targetSamples = Math.max(2048, Math.floor(sampleRate * 0.25));
      if (bufferedSamples >= targetSamples) {
        pendingChunks.push(pcm16BufferFromFloat32(sampleBuffer));
        if (pendingChunks.length > 12) pendingChunks.splice(0, pendingChunks.length - 12);
        queuedValue.textContent = String(pendingChunks.length);
        drainChunks();
      }
    }

    async function toggleMute() {
      if (!captureActive) return;
      captureMuted = !captureMuted;
      if (captureMuted) {
        sampleBuffer = [];
        bufferedSamples = 0;
        levelBar.style.width = "0%";
        debug("Mic muted; uploading silence to hold Room C.");
      } else {
        debug("Mic unmuted.");
      }
      updateMuteUi();
    }

    async function insertMarker(text) {
      if (!token) {
        setState("Token Needed", "warn");
        setMessage("Open the paired capture link from transcription settings.");
        return;
      }
      const markerText = String(text || "").trim();
      if (!markerText) return;
      try {
        const payload = await postJson(markerUrl, {text: markerText});
        setMessage(`${markerText} inserted.`);
        debug(`Inserted marker ${markerText}; segment=${payload.segment_id || ""}`);
      } catch (error) {
        setState("Error", "bad");
        setMessage(error.message || "Marker insert failed.");
        debug(`Marker failed: ${error.message || error}`);
      }
    }

    async function takeToneVision() {
      if (!token) {
        setState("Token Needed", "warn");
        setMessage("Open the paired capture link from transcription settings.");
        return;
      }
      tonevisionButton.disabled = true;
      setMessage("Taking ToneVision transmitter...");
      try {
        const payload = await postJson(tonevisionTakeoverUrl, {});
        setMessage(`TrueNAS is now transmitter for ${payload.tonevision_room || "ToneVision"}.`);
        debug(`ToneVision takeover started; room=${payload.tonevision_room || ""}`);
      } catch (error) {
        setState("Error", "bad");
        setMessage(error.message || "ToneVision takeover failed.");
        debug(`ToneVision takeover failed: ${error.message || error}`);
      } finally {
        tonevisionButton.disabled = false;
      }
    }

    async function drainChunks() {
      if (sending) return;
      sending = true;
      try {
        while (captureActive && pendingChunks.length) {
          const body = pendingChunks.shift();
          queuedValue.textContent = String(pendingChunks.length);
          const response = await fetch(`${chunkUrl}&sample_rate_hz=${encodeURIComponent(sampleRate)}`, {
            method: "POST",
            headers: {"Content-Type": "application/octet-stream"},
            body
          });
          if (!response.ok) {
            let text = `${response.status}`;
            try {
              const payload = await response.json();
              text = payload.error || text;
            } catch (_) {}
            throw new Error(text);
          }
        }
      } catch (error) {
        setState("Error", "bad");
        setMessage(error.message || "Audio upload failed.");
        await stopCapture(false);
      } finally {
        sending = false;
      }
    }

    async function requestWakeLock() {
      try {
        if ("wakeLock" in navigator) {
          wakeLock = await navigator.wakeLock.request("screen");
        }
      } catch (_) {
        wakeLock = null;
      }
    }

    async function startCapture() {
      if (captureActive || startInProgress) return;
      startInProgress = true;
      debug(`Start tapped. secure=${window.isSecureContext ? "yes" : "no"} media=${navigator.mediaDevices && navigator.mediaDevices.getUserMedia ? "yes" : "no"}`);
      if (!token) {
        setState("Token Needed", "warn");
        setMessage("Open the paired capture link from transcription settings.");
        debug("Missing source token in URL.");
        startInProgress = false;
        return;
      }
      setState("Starting", "warn");
      setMessage("Requesting microphone permission...");
      startButton.disabled = true;
      try {
        ensureCaptureSupported();
        mediaStream = await withTimeout(navigator.mediaDevices.getUserMedia({
          audio: {
            channelCount: 1,
            echoCancellation: false,
            noiseSuppression: false,
            autoGainControl: true
          }
        }), 12000, "Microphone permission did not complete. Check Safari mic permission and keep the page in the foreground.");
        debug("Microphone stream opened.");
        audioContext = new (window.AudioContext || window.webkitAudioContext)();
        await withTimeout(audioContext.resume(), 5000, "Audio engine did not start. Try tapping Start Capture again.");
        sampleRate = Math.round(audioContext.sampleRate || 48000);
        sampleRateValue.textContent = `${sampleRate} Hz`;
        debug(`Audio context ready at ${sampleRate} Hz. Starting server source...`);
        await withTimeout(postJson(startUrl, {
          sample_rate_hz: sampleRate,
          current_device: "iPhone microphone",
          user_agent: navigator.userAgent
        }), 10000, "Timed out reaching NTC Transcription from the phone.");
        debug("Server accepted iPhone as Room C source.");
        captureActive = true;
        sourceNode = audioContext.createMediaStreamSource(mediaStream);
        processor = audioContext.createScriptProcessor(4096, 1, 1);
        processor.onaudioprocess = (event) => {
          queueSamples(event.inputBuffer.getChannelData(0));
        };
        sourceNode.connect(processor);
        processor.connect(audioContext.destination);
        await requestWakeLock();
        setState("Live", "good");
        setMessage("iPhone microphone is feeding Room C. Keep this page open and unlocked.");
        debug("Live. Audio chunks should begin uploading as the level moves.");
        stopButton.disabled = false;
        captureMuted = false;
        updateMuteUi();
      } catch (error) {
        setState("Error", "bad");
        setMessage(error.message || "Microphone start failed.");
        debug(`Start failed: ${error.message || error}`);
        await stopCapture(false);
      } finally {
        startButton.disabled = captureActive;
        startInProgress = false;
      }
    }

    async function stopCapture(notifyServer = true) {
      const wasActive = captureActive;
      captureActive = false;
      captureMuted = false;
      startButton.disabled = false;
      stopButton.disabled = true;
      updateMuteUi();
      if (processor) {
        processor.disconnect();
        processor.onaudioprocess = null;
      }
      if (sourceNode) sourceNode.disconnect();
      if (audioContext) await audioContext.close().catch(() => {});
      if (mediaStream) mediaStream.getTracks().forEach((track) => track.stop());
      if (wakeLock) await wakeLock.release().catch(() => {});
      processor = null;
      sourceNode = null;
      audioContext = null;
      mediaStream = null;
      wakeLock = null;
      pendingChunks = [];
      sampleBuffer = [];
      bufferedSamples = 0;
      queuedValue.textContent = "0";
      levelBar.style.width = "0%";
      if (notifyServer && wasActive) {
        await postJson(stopUrl, {sample_rate_hz: sampleRate}).catch(() => {});
      }
      setState("Idle");
      debug(wasActive ? "Capture stopped." : "Capture reset.");
    }

    window.startCapture = startCapture;
    window.stopCapture = stopCapture;
    window.toggleMute = toggleMute;
    window.insertMarker = insertMarker;
    window.takeToneVision = takeToneVision;
    startButton.addEventListener("click", startCapture);
    stopButton.addEventListener("click", () => stopCapture(true));
    document.querySelectorAll("[data-marker-text]").forEach((button) => {
      button.addEventListener("click", () => insertMarker(button.getAttribute("data-marker-text") || ""));
    });
    updateMuteUi();
    debug(`Ready. secure=${window.isSecureContext ? "yes" : "no"} media=${navigator.mediaDevices && navigator.mediaDevices.getUserMedia ? "yes" : "no"} audio=${window.AudioContext || window.webkitAudioContext ? "yes" : "no"}`);
    window.addEventListener("pagehide", () => {
      if (captureActive) navigator.sendBeacon(stopUrl, new Blob(["{}"], {type: "application/json"}));
    });
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible" && captureActive) requestWakeLock();
    });
  </script>
</body>
</html>
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _config_enabled(app, name: str, *, default: bool = False) -> bool:
    raw = str(app.config.get(name, "1" if default else "0")).strip().lower()
    return raw not in {"", "0", "false", "no", "off"}


def _extract_transcription_text(payload) -> str:
    if payload is None:
        return ""
    if isinstance(payload, dict):
        text = payload.get("text") or payload.get("transcript")
        if text:
            return str(text).strip()
        segments = payload.get("segments")
        if isinstance(segments, list):
            return " ".join(str(segment.get("text", "")).strip() for segment in segments if isinstance(segment, dict)).strip()
        return ""
    if isinstance(payload, list):
        return " ".join(_extract_transcription_text(item) for item in payload).strip()
    raw_text = str(payload).strip()
    if not raw_text:
        return ""
    try:
        return _extract_transcription_text(json.loads(raw_text))
    except (TypeError, ValueError):
        pass
    cleaned_lines = []
    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue
        line = re.sub(r"^\[[0-9:. ]+-->\s*[0-9:. ]+\]\s*", "", line).strip()
        if line:
            cleaned_lines.append(line)
    return " ".join(cleaned_lines).strip()


_TRANSCRIPT_WORD_PATTERN = re.compile(r"[A-Za-z0-9]+(?:'[A-Za-z0-9]+)?")


def _transcript_word_tokens(text: str) -> list[tuple[str, int, int]]:
    return [(match.group(0).casefold(), match.start(), match.end()) for match in _TRANSCRIPT_WORD_PATTERN.finditer(text or "")]


def _trim_overlapped_transcript_prefix(previous_text: str, current_text: str, *, min_words: int = 3, max_words: int = 16) -> str:
    current = (current_text or "").strip()
    if not previous_text or not current:
        return current
    previous_tokens = _transcript_word_tokens(previous_text)
    current_tokens = _transcript_word_tokens(current)
    max_match = min(len(previous_tokens), len(current_tokens), max(1, int(max_words)))
    best_match = 0
    for token_count in range(1, max_match + 1):
        previous_tail = [token for token, _, _ in previous_tokens[-token_count:]]
        current_head = [token for token, _, _ in current_tokens[:token_count]]
        if previous_tail == current_head:
            best_match = token_count
    if best_match < max(1, int(min_words)):
        return current
    trim_end = current_tokens[best_match - 1][2]
    return current[trim_end:].lstrip(" \t\r\n,.;:!?-")


def _transcription_matches_suppressed_pattern(text: str, pattern: str) -> bool:
    if not text or not pattern:
        return False
    try:
        return bool(re.search(pattern, text, flags=re.IGNORECASE))
    except re.error:
        return False


def _wav_file_bytes(*, channels: int, sample_rate_hz: int, bits_per_sample: int, payload: bytes) -> bytes:
    block_align = channels * (bits_per_sample // 8)
    byte_rate = sample_rate_hz * block_align
    data_size = len(payload)
    riff_size = 36 + data_size
    return struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF",
        riff_size,
        b"WAVE",
        b"fmt ",
        16,
        1,
        channels,
        sample_rate_hz,
        byte_rate,
        block_align,
        bits_per_sample,
        b"data",
        data_size,
    ) + payload


def _pcm24le_to_int(sample: bytes) -> int:
    value = sample[0] | (sample[1] << 8) | (sample[2] << 16)
    if value & 0x800000:
        value -= 1 << 24
    return value


def _pcm16le_rms_db(pcm_bytes: bytes) -> float | None:
    usable = len(pcm_bytes) - (len(pcm_bytes) % 2)
    if usable <= 0:
        return None
    payload = pcm_bytes[:usable]
    if audioop is not None:
        rms_value = audioop.rms(payload, 2)
        if rms_value <= 0:
            return None
        return 20.0 * math.log10(max(float(rms_value) / 32767.0, 1e-8))
    sum_squares = 0.0
    sample_count = 0
    for offset in range(0, usable, 2):
        sample_value = struct.unpack_from("<h", payload, offset)[0]
        sum_squares += float(sample_value) * float(sample_value)
        sample_count += 1
    if sample_count <= 0:
        return None
    return 20.0 * math.log10(max(math.sqrt(sum_squares / sample_count) / 32767.0, 1e-8))


def _chunk_signal_stats(chunk: bytes, *, bits_per_sample: int) -> tuple[float | None, float | None]:
    if bits_per_sample not in (16, 24):
        return None, None
    bytes_per_sample = bits_per_sample // 8
    usable = len(chunk) - (len(chunk) % bytes_per_sample)
    if usable <= 0:
        return None, None
    payload = chunk[:usable]
    full_scale = 32767 if bits_per_sample == 16 else 8388607
    if audioop is not None:
        rms_value = audioop.rms(payload, bytes_per_sample)
        peak_value = audioop.max(payload, bytes_per_sample)
        if rms_value <= 0 and peak_value <= 0:
            return None, None
        rms_db = 20.0 * math.log10(max(float(rms_value) / full_scale, 1e-8)) if rms_value > 0 else None
        peak_db = 20.0 * math.log10(max(float(peak_value) / full_scale, 1e-8)) if peak_value > 0 else None
        return rms_db, peak_db
    peak_value = 0
    sum_squares = 0.0
    sample_count = 0
    for offset in range(0, usable, bytes_per_sample):
        sample_value = _pcm24le_to_int(payload[offset:offset + 3]) if bits_per_sample == 24 else struct.unpack_from("<h", payload, offset)[0]
        peak_value = max(peak_value, abs(sample_value))
        sum_squares += float(sample_value) * float(sample_value)
        sample_count += 1
    if sample_count <= 0:
        return None, None
    rms_db = 20.0 * math.log10(max(math.sqrt(sum_squares / sample_count) / full_scale, 1e-8))
    peak_db = 20.0 * math.log10(max(peak_value / full_scale, 1e-8))
    return rms_db, peak_db


def _align_pcm16_byte_count(byte_count: int) -> int:
    return max(0, int(byte_count) - (int(byte_count) % 2))


def _transcription_overlap_byte_count(*, cut_position: int, bytes_per_second: int, overlap_seconds: float, force: bool = False) -> int:
    if force or overlap_seconds <= 0:
        return 0
    cut_position = _align_pcm16_byte_count(cut_position)
    overlap_bytes = _align_pcm16_byte_count(max(0.0, float(bytes_per_second) * float(overlap_seconds)))
    return min(overlap_bytes, max(0, cut_position - 2))


def _find_transcription_cut_position(
    pcm_bytes: bytes | bytearray,
    *,
    bytes_per_second: int,
    target_seconds: float,
    min_seconds: float,
    max_seconds: float,
    vad_frame_ms: float,
    silence_db: float,
    min_silence_ms: float,
    search_seconds: float,
    force: bool = False,
) -> int | None:
    usable = _align_pcm16_byte_count(len(pcm_bytes))
    if usable <= 0:
        return None
    bytes_per_second = max(1, int(bytes_per_second or 1))
    min_bytes = _align_pcm16_byte_count(bytes_per_second * max(1.0, float(min_seconds)))
    target_bytes = _align_pcm16_byte_count(bytes_per_second * max(float(min_seconds), float(target_seconds)))
    max_bytes = _align_pcm16_byte_count(bytes_per_second * max(float(target_seconds), float(max_seconds)))
    search_bytes = _align_pcm16_byte_count(bytes_per_second * max(0.1, float(search_seconds)))
    frame_bytes = max(2, _align_pcm16_byte_count(bytes_per_second * max(10.0, float(vad_frame_ms)) / 1000.0))
    silence_frames_needed = max(1, math.ceil(max(1.0, float(min_silence_ms)) / max(10.0, float(vad_frame_ms))))
    if force:
        if usable < min_bytes:
            return usable
        max_bytes = min(max_bytes, usable)
    elif usable < target_bytes:
        return None
    search_start = max(min_bytes, target_bytes - search_bytes)
    search_end = min(usable, target_bytes + search_bytes)
    silent_run_frames = 0
    candidates: list[int] = []
    for offset in range(search_start, search_end, frame_bytes):
        frame_end = min(search_end, offset + frame_bytes)
        rms_db = _pcm16le_rms_db(bytes(pcm_bytes[offset:frame_end]))
        if rms_db is None or rms_db <= silence_db:
            silent_run_frames += 1
            if silent_run_frames >= silence_frames_needed:
                candidates.append(_align_pcm16_byte_count(frame_end))
        else:
            silent_run_frames = 0
    if candidates:
        return min(candidates, key=lambda value: abs(value - target_bytes))
    if usable >= max_bytes:
        return min(usable, max_bytes)
    if force:
        return min(usable, max_bytes)
    return None


class PcmStreamTranscoder:
    def __init__(self, *, source_channels: int, source_rate_hz: int, bits_per_sample: int, output_channels: int, output_rate_hz: int, output_bits_per_sample: int):
        self.source_channels = max(1, int(source_channels or 1))
        self.source_rate_hz = max(8000, int(source_rate_hz or 48000))
        self.bits_per_sample = max(16, int(bits_per_sample or 24))
        self.output_channels = max(1, int(output_channels or self.source_channels))
        self.output_rate_hz = max(8000, int(output_rate_hz or self.source_rate_hz))
        self.output_bits_per_sample = max(16, int(output_bits_per_sample or 16))
        self._source_width = max(1, self.bits_per_sample // 8)
        self._output_width = max(1, self.output_bits_per_sample // 8)
        self._frame_width = self.source_channels * self._source_width
        self._carry = b""
        self._rate_state = None
        self._sample_accumulator = 0

    def transcode(self, chunk: bytes) -> bytes:
        if not chunk:
            return b""
        data = self._carry + chunk
        usable = len(data) - (len(data) % self._frame_width)
        self._carry = data[usable:]
        if usable <= 0:
            return b""
        payload = data[:usable]
        if (
            audioop is not None
            and self.source_channels in (1, 2)
            and self.output_channels in (1, 2)
            and self._source_width in (1, 2, 3, 4)
            and self._output_width in (1, 2, 3, 4)
        ):
            working = payload
            channel_count = self.source_channels
            if self.source_channels == 2 and self.output_channels == 1:
                working = audioop.tomono(working, self._source_width, 0.5, 0.5)
                channel_count = 1
            elif self.source_channels == 1 and self.output_channels == 2:
                working = audioop.tostereo(working, self._source_width, 1.0, 1.0)
                channel_count = 2
            if self.source_rate_hz != self.output_rate_hz:
                working, self._rate_state = audioop.ratecv(
                    working,
                    self._source_width,
                    channel_count,
                    self.source_rate_hz,
                    self.output_rate_hz,
                    self._rate_state,
                )
            if self._source_width != self._output_width:
                working = audioop.lin2lin(working, self._source_width, self._output_width)
            return working
        output = bytearray()
        for offset in range(0, usable, self._frame_width):
            frame = payload[offset:offset + self._frame_width]
            self._sample_accumulator += self.output_rate_hz
            if self._sample_accumulator < self.source_rate_hz:
                continue
            self._sample_accumulator -= self.source_rate_hz
            frame_samples: list[int] = []
            for channel_index in range(self.source_channels):
                start = channel_index * self._source_width
                raw_sample = frame[start:start + self._source_width]
                if self.bits_per_sample == 24:
                    sample_value = _pcm24le_to_int(raw_sample)
                elif self.bits_per_sample == 16:
                    sample_value = struct.unpack("<h", raw_sample)[0] << 8
                else:
                    continue
                frame_samples.append(sample_value)
            if not frame_samples:
                continue
            if self.source_channels == 2 and self.output_channels == 1:
                frame_samples = [int(sum(frame_samples) / len(frame_samples))]
            elif self.source_channels == 1 and self.output_channels == 2:
                frame_samples = [frame_samples[0], frame_samples[0]]
            for sample_value in frame_samples[: self.output_channels]:
                scaled = max(-32768, min(32767, sample_value >> 8))
                output.extend(struct.pack("<h", scaled))
        return bytes(output)


@dataclass
class RoomState:
    active_host_slug: str | None = None
    active: bool = False
    listeners: dict[int, queue.Queue] = field(default_factory=dict)
    stream_channels: int = 1
    sample_rate_hz: int = 48000
    bits_per_sample: int = 24


class TranscriptionStreamHub:
    def __init__(self):
        self._lock = threading.Lock()
        self._rooms: dict[str, RoomState] = {}

    def _get_room(self, room_slug: str) -> RoomState:
        if room_slug not in self._rooms:
            self._rooms[room_slug] = RoomState()
        return self._rooms[room_slug]

    def start_room(self, room_slug: str, host_slug: str, *, stream_channels: int, sample_rate_hz: int, bits_per_sample: int) -> None:
        with self._lock:
            room = self._get_room(room_slug)
            room.active_host_slug = host_slug
            room.active = True
            room.stream_channels = max(1, int(stream_channels or 1))
            room.sample_rate_hz = max(8000, int(sample_rate_hz or 48000))
            room.bits_per_sample = max(16, int(bits_per_sample or 24))

    def finish_room(self, room_slug: str, host_slug: str) -> None:
        with self._lock:
            room = self._get_room(room_slug)
            if room.active_host_slug != host_slug:
                return
            room.active = False
            room.active_host_slug = None
            listeners = list(room.listeners.values())
        for listener in listeners:
            try:
                listener.put_nowait(None)
            except queue.Full:
                pass

    def publish_from(self, room_slug: str, host_slug: str, chunk: bytes) -> bool:
        with self._lock:
            room = self._get_room(room_slug)
            if not room.active or room.active_host_slug != host_slug:
                return False
            listeners = list(room.listeners.values())
        for listener in listeners:
            try:
                listener.put_nowait(chunk)
            except queue.Full:
                while True:
                    try:
                        listener.get_nowait()
                    except queue.Empty:
                        break
                try:
                    listener.put_nowait(chunk)
                except queue.Full:
                    continue
        return True

    def open_listener(self, room_slug: str, *, maxsize: int):
        listener = queue.Queue(maxsize=max(1, int(maxsize or 1)))
        listener_id = id(listener)
        with self._lock:
            self._get_room(room_slug).listeners[listener_id] = listener
        return listener_id, listener

    def close_listener(self, room_slug: str, listener_id: int) -> None:
        with self._lock:
            self._get_room(room_slug).listeners.pop(listener_id, None)


@dataclass
class TranscriptionWorkerState:
    room_slug: str
    host_slug: str
    stop_event: threading.Event = field(default_factory=threading.Event)
    worker_thread: threading.Thread | None = None
    listener_id: int | None = None
    last_error: str = ""


def install_source_api(app, transcription_store) -> None:
    stream_hub = TranscriptionStreamHub()
    transcription_workers: dict[str, TranscriptionWorkerState] = {}
    transcription_lock = threading.Lock()
    browser_capture_states: dict[str, dict] = {}
    browser_capture_lock = threading.Lock()
    tonevision_bridge_lock = threading.Lock()
    tonevision_bridge_state: dict[str, object] = {
        "thread": None,
        "stop_event": None,
        "started_at": "",
        "last_error": "",
        "tonevision_room": "",
        "ntc_room": "",
    }

    def source_base_url() -> str:
        configured = (app.config.get("NTC_TRANSCRIPTION_SOURCE_PUBLIC_BASE_URL") or "").strip().rstrip("/")
        if configured:
            return configured
        forwarded_prefix = (request.headers.get("X-Forwarded-Prefix") or request.environ.get("SCRIPT_NAME") or "").strip().rstrip("/")
        return request.url_root.rstrip("/") + forwarded_prefix

    def provider() -> str:
        configured = (app.config.get("NTC_TRANSCRIPTION_PROVIDER") or "openai").strip().lower()
        return configured if configured in {"openai", "telnyx", "local_cmd", "local_http"} else "openai"

    def model(current_provider: str) -> str:
        configured = (app.config.get("NTC_TRANSCRIPTION_MODEL") or "").strip()
        if configured:
            return configured
        if current_provider == "telnyx":
            return "openai/whisper-large-v3-turbo"
        if current_provider in {"local_cmd", "local_http"}:
            return "local"
        return "gpt-4o-mini-transcribe"

    def local_url() -> str:
        return (app.config.get("NTC_TRANSCRIPTION_LOCAL_URL") or "").strip()

    def provider_ready(current_provider: str) -> bool:
        if current_provider == "local_http":
            return bool(local_url())
        if current_provider == "local_cmd":
            return _config_enabled(app, "NTC_TRANSCRIPTION_ALLOW_LOCAL_COMMAND") and bool(app.config.get("NTC_TRANSCRIPTION_LOCAL_COMMAND"))
        if current_provider == "telnyx":
            return bool(os.getenv("TELNYX_API_KEY", ""))
        return bool(os.getenv("OPENAI_API_KEY", ""))

    def source_desired(host: dict) -> bool:
        if _config_enabled(app, "NTC_TRANSCRIPTION_HARD_DISABLED"):
            return False
        return bool(host.get("transcription_desired_active") and provider_ready(provider()))

    def start_tonevision_takeover(host: dict) -> dict:
        try:
            from tools.tonevision_bridge import ToneVisionWebSocket, tonevision_ws_url, trim_buffer
        except ImportError as exc:  # pragma: no cover - deployment packaging guard
            raise RuntimeError("ToneVision bridge tool is not available in this deployment") from exc

        tonevision_base_url = (
            app.config.get("NTC_TONEVISION_BASE_URL")
            or os.getenv("NTC_TONEVISION_BASE_URL")
            or "http://100.96.175.75:8080"
        ).strip()
        tonevision_room = (
            app.config.get("NTC_TONEVISION_ROOM")
            or os.getenv("NTC_TONEVISION_ROOM")
            or "english"
        ).strip()
        tonevision_pin = (
            app.config.get("NTC_TONEVISION_PIN")
            or os.getenv("NTC_TONEVISION_PIN")
            or "1234"
        ).strip()
        if not tonevision_pin:
            raise RuntimeError("ToneVision PIN is not configured")
        poll_seconds = max(0.2, float(app.config.get("NTC_TONEVISION_POLL_SECONDS", os.getenv("NTC_TONEVISION_POLL_SECONDS", "0.7"))))
        max_chars = max(1000, int(app.config.get("NTC_TONEVISION_MAX_CHARS", os.getenv("NTC_TONEVISION_MAX_CHARS", "60000"))))
        timeout_seconds = max(2.0, float(app.config.get("NTC_TONEVISION_TIMEOUT_SECONDS", os.getenv("NTC_TONEVISION_TIMEOUT_SECONDS", "10.0"))))
        room_slug = host["room_slug"]

        with tonevision_bridge_lock:
            previous_stop = tonevision_bridge_state.get("stop_event")
            if isinstance(previous_stop, threading.Event):
                previous_stop.set()
            stop_event = threading.Event()
            tonevision_bridge_state.update(
                {
                    "thread": None,
                    "stop_event": stop_event,
                    "started_at": _utc_now(),
                    "last_error": "",
                    "tonevision_room": tonevision_room,
                    "ntc_room": room_slug,
                }
            )

        def bridge_worker() -> None:
            ws = None
            last_id = 0
            buffer_parts: list[str] = []
            try:
                initial = transcription_store.list_segments_after(room_slug, after_id=0, limit=500)
                last_id = max((int(segment.get("id") or 0) for segment in initial), default=0)
                ws = ToneVisionWebSocket(
                    tonevision_ws_url(tonevision_base_url, tonevision_room, tonevision_pin),
                    timeout=timeout_seconds,
                )
                ws.connect()
                ws.send_json({"type": "pause", "paused": False})
                app.logger.info("ToneVision takeover connected room=%s ntc_room=%s after_id=%s", tonevision_room, room_slug, last_id)
                while not stop_event.wait(poll_seconds):
                    segments = transcription_store.list_segments_after(room_slug, after_id=last_id, limit=80)
                    if not segments:
                        continue
                    for segment in segments:
                        segment_id = int(segment.get("id") or 0)
                        text = " ".join(str(segment.get("text") or "").split())
                        if segment_id > 0:
                            last_id = max(last_id, segment_id)
                        if text:
                            buffer_parts.append(text)
                    if buffer_parts:
                        ws.send_json({"type": "text", "text": trim_buffer(buffer_parts, max_chars)})
            except Exception as exc:
                with tonevision_bridge_lock:
                    tonevision_bridge_state["last_error"] = str(exc)
                app.logger.warning("ToneVision takeover bridge failed room=%s error=%s", tonevision_room, exc)
            finally:
                if ws:
                    ws.close()

        thread = threading.Thread(target=bridge_worker, daemon=True, name=f"ntc-tonevision-{tonevision_room}")
        with tonevision_bridge_lock:
            tonevision_bridge_state["thread"] = thread
        thread.start()
        return {
            "tonevision_base_url": tonevision_base_url,
            "tonevision_room": tonevision_room,
            "ntc_room": room_slug,
            "started_at": tonevision_bridge_state["started_at"],
        }

    def stream_descriptor(snapshot: dict | None = None):
        runtime = (snapshot or {}).get("runtime") or {}
        return {
            "channels": max(1, int(runtime.get("stream_channels") or (2 if (snapshot or {}).get("capture_mode") == "stereo" else 1))),
            "sample_rate_hz": max(8000, int(runtime.get("sample_rate_hz") or (snapshot or {}).get("capture_sample_rate_hz") or 48000)),
            "bits_per_sample": max(16, int(runtime.get("sample_bits") or 24)),
        }

    def listener_maxsize(snapshot: dict | None = None) -> int:
        descriptor = stream_descriptor(snapshot)
        source_bytes_per_second = descriptor["channels"] * descriptor["sample_rate_hz"] * max(1, descriptor["bits_per_sample"] // 8)
        queue_seconds = max(5.0, float(app.config.get("NTC_TRANSCRIPTION_QUEUE_SECONDS", 120.0)))
        needed_chunks = math.ceil((source_bytes_per_second * queue_seconds) / float(INGEST_CHUNK_SIZE))
        return max(128, min(65536, int(needed_chunks)))

    def transcribe_audio_chunk_local_http(wav_bytes: bytes, *, current_model: str, prompt: str, language: str) -> str:
        url = local_url()
        if not url:
            raise RuntimeError("local transcription URL is not configured")
        timeout_seconds = max(5.0, float(app.config.get("NTC_TRANSCRIPTION_TIMEOUT_SECONDS", 25.0)))
        params = {}
        if current_model:
            params["model"] = current_model
        if language:
            params["language"] = language
        if prompt:
            params["prompt"] = prompt
        response = requests.post(
            url,
            params=params,
            data=wav_bytes,
            headers={"Content-Type": "audio/wav"},
            timeout=timeout_seconds,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"local transcription service failed: HTTP {response.status_code} {response.text[:240]}")
        try:
            return _extract_transcription_text(response.json())
        except ValueError:
            return _extract_transcription_text(response.text)

    def transcribe_audio_chunk_local_cmd(wav_bytes: bytes, *, current_model: str, prompt: str, language: str) -> str:
        if not _config_enabled(app, "NTC_TRANSCRIPTION_ALLOW_LOCAL_COMMAND"):
            raise RuntimeError("local transcription command is disabled")
        command_template = (app.config.get("NTC_TRANSCRIPTION_LOCAL_COMMAND") or "").strip()
        if not command_template:
            raise RuntimeError("local transcription command is not configured")
        timeout_seconds = max(5.0, float(app.config.get("NTC_TRANSCRIPTION_TIMEOUT_SECONDS", 25.0)))
        with tempfile.TemporaryDirectory(prefix="ntc-transcribe-") as temp_dir:
            audio_path = os.path.join(temp_dir, "chunk.wav")
            with open(audio_path, "wb") as handle:
                handle.write(wav_bytes)
            substitutions = {
                "audio": shlex.quote(audio_path),
                "model": shlex.quote(current_model),
                "language": shlex.quote(language),
                "prompt": shlex.quote(prompt),
            }
            command = command_template.format(**substitutions)
            if "{audio}" not in command_template:
                command = f"{command} {shlex.quote(audio_path)}"
            completed = subprocess.run(
                shlex.split(command),
                capture_output=True,
                check=False,
                text=True,
                timeout=timeout_seconds,
            )
        if completed.returncode != 0:
            error_text = (completed.stderr or completed.stdout or "").strip()
            raise RuntimeError(f"local transcription command failed: {error_text[:240]}")
        return _extract_transcription_text(completed.stdout)

    def transcribe_audio_chunk(wav_bytes: bytes, *, current_provider: str, current_model: str) -> str:
        prompt = (app.config.get("NTC_TRANSCRIPTION_PROMPT") or "").strip()
        language = (app.config.get("NTC_TRANSCRIPTION_LANGUAGE") or "").strip()
        if current_provider == "local_http":
            return transcribe_audio_chunk_local_http(wav_bytes, current_model=current_model, prompt=prompt, language=language)
        if current_provider == "local_cmd":
            return transcribe_audio_chunk_local_cmd(wav_bytes, current_model=current_model, prompt=prompt, language=language)
        raise RuntimeError(f"{current_provider} transcription is not configured in ntc-transcription source API yet")

    def run_transcription_worker(state: TranscriptionWorkerState, snapshot: dict):
        current_provider = provider()
        current_model = model(current_provider)
        descriptor = stream_descriptor(snapshot)
        transcriber = PcmStreamTranscoder(
            source_channels=descriptor["channels"],
            source_rate_hz=descriptor["sample_rate_hz"],
            bits_per_sample=descriptor["bits_per_sample"],
            output_channels=TRANSCRIPTION_STREAM_PROFILE["channels"],
            output_rate_hz=TRANSCRIPTION_STREAM_PROFILE["sample_rate_hz"],
            output_bits_per_sample=TRANSCRIPTION_STREAM_PROFILE["bits_per_sample"],
        )
        target_seconds = max(3.0, float(app.config.get("NTC_TRANSCRIPTION_CHUNK_SECONDS", 3.2)))
        min_chunk_seconds = max(1.0, float(app.config.get("NTC_TRANSCRIPTION_MIN_CHUNK_SECONDS", 1.8)))
        max_chunk_seconds = max(target_seconds, float(app.config.get("NTC_TRANSCRIPTION_MAX_CHUNK_SECONDS", 6.0)))
        overlap_seconds = max(0.0, float(app.config.get("NTC_TRANSCRIPTION_CHUNK_OVERLAP_SECONDS", 0.75)))
        overlap_seconds = min(overlap_seconds, max(0.0, min_chunk_seconds - 0.5), target_seconds / 2.0)
        vad_enabled = _config_enabled(app, "NTC_TRANSCRIPTION_VAD_ENABLED", default=True)
        bytes_per_second = TRANSCRIPTION_STREAM_PROFILE["sample_rate_hz"] * 2
        flush_floor_seconds = min(2.0, max(1.0, min_chunk_seconds))
        pcm_buffer = bytearray()
        chunk_started_at = datetime.now(timezone.utc)
        previous_transcript_text = ""
        listener_id, listener = stream_hub.open_listener(state.room_slug, maxsize=listener_maxsize(snapshot))
        state.listener_id = listener_id

        def flush_buffer(*, force: bool = False):
            nonlocal chunk_started_at, previous_transcript_text
            if len(pcm_buffer) < bytes_per_second * flush_floor_seconds and not force:
                return
            if not pcm_buffer:
                return
            if vad_enabled:
                cut_position = _find_transcription_cut_position(
                    pcm_buffer,
                    bytes_per_second=bytes_per_second,
                    target_seconds=target_seconds,
                    min_seconds=min_chunk_seconds,
                    max_seconds=max_chunk_seconds,
                    vad_frame_ms=max(10.0, float(app.config.get("NTC_TRANSCRIPTION_VAD_FRAME_MS", 100.0))),
                    silence_db=float(app.config.get("NTC_TRANSCRIPTION_VAD_SILENCE_DB", -46.0)),
                    min_silence_ms=max(1.0, float(app.config.get("NTC_TRANSCRIPTION_VAD_MIN_SILENCE_MS", 220.0))),
                    search_seconds=max(0.1, float(app.config.get("NTC_TRANSCRIPTION_BOUNDARY_SEARCH_SECONDS", 1.8))),
                    force=force,
                )
            else:
                cut_position = _align_pcm16_byte_count(bytes_per_second * target_seconds)
                if force:
                    cut_position = _align_pcm16_byte_count(len(pcm_buffer))
                elif len(pcm_buffer) < cut_position:
                    return
            if cut_position is None or cut_position <= 0:
                return
            payload = bytes(pcm_buffer[:cut_position])
            overlap_bytes = _transcription_overlap_byte_count(
                cut_position=cut_position,
                bytes_per_second=bytes_per_second,
                overlap_seconds=overlap_seconds,
                force=force,
            )
            del pcm_buffer[:cut_position - overlap_bytes]
            segment_started_at = chunk_started_at
            duration_seconds = len(payload) / float(bytes_per_second)
            segment_ended_at = segment_started_at + timedelta(seconds=duration_seconds)
            chunk_started_at = segment_ended_at - timedelta(seconds=overlap_bytes / float(bytes_per_second))
            rms_db, _ = _chunk_signal_stats(payload, bits_per_sample=16)
            if rms_db is None or rms_db < float(app.config.get("NTC_TRANSCRIPTION_MIN_RMS_DB", -52.0)):
                return
            text = transcribe_audio_chunk(
                _wav_file_bytes(
                    channels=TRANSCRIPTION_STREAM_PROFILE["channels"],
                    sample_rate_hz=TRANSCRIPTION_STREAM_PROFILE["sample_rate_hz"],
                    bits_per_sample=TRANSCRIPTION_STREAM_PROFILE["bits_per_sample"],
                    payload=payload,
                ),
                current_provider=current_provider,
                current_model=current_model,
            )
            suppress_pattern = (app.config.get("NTC_TRANSCRIPTION_SUPPRESS_REGEX") or "").strip()
            if not text or _transcription_matches_suppressed_pattern(text, suppress_pattern):
                return
            text = _trim_overlapped_transcript_prefix(previous_transcript_text, text)
            if not text or _transcription_matches_suppressed_pattern(text, suppress_pattern):
                return
            received_at = _utc_now()
            segment_id = transcription_store.record_transcript_segment(
                state.room_slug,
                host_slug=state.host_slug,
                provider=current_provider,
                model=current_model,
                started_at=segment_started_at.isoformat(),
                ended_at=segment_ended_at.isoformat(),
                received_at=received_at,
                text=text,
            )
            if segment_id:
                previous_transcript_text = text

        try:
            while not state.stop_event.is_set():
                try:
                    chunk = listener.get(timeout=1.0)
                except queue.Empty:
                    continue
                if chunk is None:
                    break
                converted = transcriber.transcode(chunk)
                if converted:
                    pcm_buffer.extend(converted)
                while True:
                    before_bytes = len(pcm_buffer)
                    flush_buffer()
                    if len(pcm_buffer) == before_bytes:
                        break
            while pcm_buffer:
                before_bytes = len(pcm_buffer)
                flush_buffer(force=True)
                if len(pcm_buffer) == before_bytes:
                    break
        except Exception as exc:
            state.last_error = str(exc)
            app.logger.warning("transcription source worker failed room_slug=%s error=%s", state.room_slug, exc)
        finally:
            stream_hub.close_listener(state.room_slug, listener_id)

    def start_room_transcription(room_slug: str, host_slug: str, snapshot: dict):
        if not source_desired(snapshot):
            return
        stale_state = None
        with transcription_lock:
            existing = transcription_workers.get(room_slug)
            if existing:
                alive = existing.worker_thread and existing.worker_thread.is_alive() and not existing.stop_event.is_set()
                if alive and existing.host_slug == host_slug:
                    return
                stale_state = transcription_workers.pop(room_slug, None)
        if stale_state:
            stale_state.stop_event.set()
        with transcription_lock:
            state = TranscriptionWorkerState(room_slug=room_slug, host_slug=host_slug)
            state.worker_thread = threading.Thread(target=run_transcription_worker, args=(state, dict(snapshot or {})), daemon=True)
            transcription_workers[room_slug] = state
            state.worker_thread.start()

    def stop_room_transcription(room_slug: str, host_slug: str | None = None):
        with transcription_lock:
            state = transcription_workers.get(room_slug)
            if state and host_slug and state.host_slug != host_slug:
                return
            state = transcription_workers.pop(room_slug, None)
        if not state:
            return
        state.stop_event.set()
        if state.listener_id is not None:
            stream_hub.close_listener(room_slug, state.listener_id)
        if state.worker_thread and state.worker_thread.is_alive():
            state.worker_thread.join(timeout=2.0)

    def auth_host_from_payload(payload: dict):
        host_slug = (payload.get("host_slug") or "").strip()
        token = (payload.get("token") or "").strip()
        host = transcription_store.get_host(host_slug, include_secret=True)
        if not host or not hmac.compare_digest(token, host.get("heartbeat_token", "")):
            return None
        return host

    def auth_host_from_request(host_slug: str):
        token = (
            request.args.get("token")
            or request.headers.get("X-NTC-Source-Token")
            or ""
        ).strip()
        host = transcription_store.get_host((host_slug or "").strip(), include_secret=True)
        if not host or not hmac.compare_digest(token, host.get("heartbeat_token", "")):
            return None
        return host

    def browser_stream_settings(sample_rate_hz: int | str | None) -> dict:
        try:
            sample_rate = int(sample_rate_hz or 48000)
        except (TypeError, ValueError):
            sample_rate = 48000
        return {
            "stream_channels": 1,
            "sample_rate_hz": max(8000, min(96000, sample_rate)),
            "bits_per_sample": 16,
        }

    def browser_capture_snapshot(host: dict, stream_settings: dict) -> dict:
        snapshot = dict(host)
        runtime = dict((host.get("runtime") or {}))
        runtime.update(
            {
                "stream_channels": stream_settings["stream_channels"],
                "sample_rate_hz": stream_settings["sample_rate_hz"],
                "sample_bits": stream_settings["bits_per_sample"],
            }
        )
        snapshot["runtime"] = runtime
        return snapshot

    def active_browser_capture_for_room(room_slug: str, *, excluding_host_slug: str | None = None) -> dict | None:
        now = time.time()
        stale_seconds = max(3.0, float(app.config.get("NTC_TRANSCRIPTION_BROWSER_CAPTURE_STALE_SECONDS", 8.0)))
        with browser_capture_lock:
            for host_slug, state in browser_capture_states.items():
                if excluding_host_slug and host_slug == excluding_host_slug:
                    continue
                if state.get("room_slug") == room_slug and now - float(state.get("last_seen", 0)) <= stale_seconds:
                    return dict(state)
        return None

    def stop_browser_capture(host_slug: str, *, last_error: str = "") -> bool:
        with browser_capture_lock:
            state = browser_capture_states.pop(host_slug, None)
        host = transcription_store.get_host(host_slug, include_secret=True)
        room_slug = (state or {}).get("room_slug") or (host or {}).get("room_slug")
        if not host or not room_slug:
            return False
        stream_settings = browser_stream_settings((state or {}).get("sample_rate_hz"))
        stream_hub.finish_room(room_slug, host_slug)
        stop_room_transcription(room_slug, host_slug)
        transcription_store.record_source_heartbeat(
            host_slug,
            current_device=(state or {}).get("current_device") or (host.get("runtime") or {}).get("current_device", ""),
            devices=[(state or {}).get("current_device") or "iPhone microphone"],
            is_ingesting=False,
            last_error=last_error,
            desired_active=source_desired(transcription_store.get_host(host_slug) or host),
            stream_profile=BROWSER_CAPTURE_STREAM_PROFILE,
            stream_channels=stream_settings["stream_channels"],
            sample_rate_hz=stream_settings["sample_rate_hz"],
            sample_bits=stream_settings["bits_per_sample"],
        )
        return True

    def start_browser_capture(host: dict, *, sample_rate_hz: int | str | None, current_device: str) -> tuple[dict | None, tuple[dict, int] | None]:
        stream_settings = browser_stream_settings(sample_rate_hz)
        if not source_desired(host):
            transcription_store.record_source_heartbeat(
                host["slug"],
                current_device=current_device,
                devices=[current_device],
                is_ingesting=False,
                last_error="Transcription is not requested or transcription provider is unavailable.",
                desired_active=False,
                stream_profile=BROWSER_CAPTURE_STREAM_PROFILE,
                stream_channels=1,
                sample_rate_hz=stream_settings["sample_rate_hz"],
                sample_bits=16,
            )
            return None, ({"error": "transcription not requested"}, 409)
        room_slug = host["room_slug"]
        with browser_capture_lock:
            other_browser_states = list(browser_capture_states.values())
        for state in other_browser_states:
            if state.get("room_slug") == room_slug and state.get("host_slug") != host["slug"]:
                stop_browser_capture(state["host_slug"], last_error="Replaced by another Room C source.")
        stream_hub.start_room(room_slug, host["slug"], **stream_settings)
        transcription_store.record_source_heartbeat(
            host["slug"],
            current_device=current_device,
            devices=[current_device],
            is_ingesting=True,
            last_error="",
            desired_active=True,
            stream_profile=BROWSER_CAPTURE_STREAM_PROFILE,
            stream_channels=stream_settings["stream_channels"],
            sample_rate_hz=stream_settings["sample_rate_hz"],
            sample_bits=stream_settings["bits_per_sample"],
        )
        snapshot = browser_capture_snapshot(transcription_store.get_host(host["slug"]) or host, stream_settings)
        start_room_transcription(room_slug, host["slug"], snapshot)
        state = {
            "host_slug": host["slug"],
            "room_slug": room_slug,
            "current_device": current_device,
            "sample_rate_hz": stream_settings["sample_rate_hz"],
            "last_seen": time.time(),
        }
        with browser_capture_lock:
            browser_capture_states[host["slug"]] = state
        return state, None

    def browser_capture_cleanup_loop() -> None:
        stale_seconds = max(3.0, float(app.config.get("NTC_TRANSCRIPTION_BROWSER_CAPTURE_STALE_SECONDS", 8.0)))
        while True:
            now = time.time()
            stale_hosts = []
            with browser_capture_lock:
                for host_slug, state in browser_capture_states.items():
                    if now - float(state.get("last_seen", 0)) > stale_seconds:
                        stale_hosts.append(host_slug)
            for host_slug in stale_hosts:
                stop_browser_capture(
                    host_slug,
                    last_error=f"Browser capture disconnected for more than {int(stale_seconds)} seconds.",
                )
            time.sleep(max(1.0, min(5.0, stale_seconds / 2.0)))

    if not app.config.get("TESTING"):
        threading.Thread(target=browser_capture_cleanup_loop, daemon=True, name="ntc-browser-capture-cleanup").start()

    @app.post("/transcription/api/source/heartbeat")
    @app.post("/api/source/heartbeat")
    def transcription_source_heartbeat():
        payload = request.get_json(silent=True) or {}
        host = auth_host_from_payload(payload)
        if not host:
            return jsonify({"error": "unauthorized"}), 403
        desired_active = source_desired(host)
        transcription_store.record_source_heartbeat(
            host["slug"],
            current_device=(payload.get("current_device") or "").strip(),
            devices=payload.get("devices") or [],
            is_ingesting=bool(payload.get("is_ingesting")),
            last_error=payload.get("last_error") or "",
            desired_active=desired_active,
            stream_profile=(payload.get("stream_profile") or "").strip(),
            stream_channels=int(payload.get("stream_channels") or 1),
            sample_rate_hz=int(payload.get("sample_rate_hz") or 48000),
            sample_bits=int(payload.get("sample_bits") or 0),
        )
        refreshed = transcription_store.get_host(host["slug"], include_secret=True) or host
        desired_active = source_desired(refreshed)
        base_url = source_base_url()
        token = quote(refreshed.get("heartbeat_token", ""), safe="")
        host_slug = quote(refreshed["slug"], safe="")
        return jsonify(
            {
                "ok": True,
                "project": "ntc-transcription",
                "desired_active": desired_active,
                "public_desired_active": False,
                "transcription_desired_active": desired_active,
                "room_slug": refreshed["room_slug"],
                "room_label": refreshed["room_label"],
                "device_order": refreshed.get("device_order", []),
                "preferred_audio_pattern": refreshed.get("preferred_audio_pattern", ""),
                "fallback_audio_pattern": refreshed.get("fallback_audio_pattern", ""),
                "capture_mode": refreshed.get("capture_mode", "auto"),
                "capture_sample_rate_hz": int(refreshed.get("capture_sample_rate_hz") or 48000),
                "stream_profile": "wav_pcm24",
                "source_command": None,
                "ingest_url": f"{base_url}/api/source/ingest/{host_slug}?token={token}",
            }
        )

    @app.post("/transcription/api/source/event")
    @app.post("/api/source/event")
    def transcription_source_event():
        payload = request.get_json(silent=True) or {}
        host = auth_host_from_payload(payload)
        if not host:
            return jsonify({"error": "unauthorized"}), 403
        transcription_store.record_source_event(
            host["slug"],
            event_type=payload.get("event_type") or "source-event",
            level=payload.get("level") or "info",
            message=payload.get("message") or "",
            details=payload.get("details") if isinstance(payload.get("details"), dict) else {},
        )
        return jsonify({"ok": True})

    @app.get("/transcription/capture")
    @app.get("/transcription/capture/<host_slug>")
    def transcription_browser_capture(host_slug: str | None = None):
        resolved_host_slug = (host_slug or request.args.get("source") or "iphone15pro").strip()
        host = transcription_store.get_host(resolved_host_slug, include_secret=False)
        if not host:
            return jsonify({"error": "unknown source"}), 404
        token = (request.args.get("token") or "").strip()
        return render_template_string(
            BROWSER_CAPTURE_TEMPLATE,
            title=f"{host['label']} Capture",
            host_slug_json=json.dumps(host["slug"]),
            token_json=json.dumps(token),
            host_label=host["label"] or host["slug"],
            brand_background_url="/transcription/brand/ntc-embossed-background.jpg",
        )

    @app.post("/transcription/api/source/browser/start/<host_slug>")
    @app.post("/api/source/browser/start/<host_slug>")
    def transcription_browser_capture_start(host_slug: str):
        host = auth_host_from_request(host_slug)
        if not host:
            return jsonify({"error": "unauthorized"}), 403
        payload = request.get_json(silent=True) or {}
        current_device = (payload.get("current_device") or host.get("label") or "iPhone microphone").strip()
        state, error = start_browser_capture(
            host,
            sample_rate_hz=payload.get("sample_rate_hz"),
            current_device=current_device,
        )
        if error:
            body, status = error
            return jsonify(body), status
        return jsonify(
            {
                "ok": True,
                "project": "ntc-transcription",
                "source": "browser",
                "host_slug": host["slug"],
                "room_slug": host["room_slug"],
                "stream_profile": BROWSER_CAPTURE_STREAM_PROFILE,
                "stream_channels": 1,
                "sample_rate_hz": int((state or {}).get("sample_rate_hz") or 48000),
                "sample_bits": 16,
            }
        )

    @app.post("/transcription/api/source/browser/chunk/<host_slug>")
    @app.post("/api/source/browser/chunk/<host_slug>")
    def transcription_browser_capture_chunk(host_slug: str):
        host = auth_host_from_request(host_slug)
        if not host:
            return jsonify({"error": "unauthorized"}), 403
        current_device = (
            request.headers.get("X-NTC-Current-Device")
            or (host.get("runtime") or {}).get("current_device")
            or host.get("label")
            or "iPhone microphone"
        ).strip()
        state, error = start_browser_capture(
            host,
            sample_rate_hz=request.args.get("sample_rate_hz"),
            current_device=current_device,
        )
        if error:
            body, status = error
            return jsonify(body), status
        max_chunk_bytes = max(4096, int(app.config.get("NTC_TRANSCRIPTION_BROWSER_CAPTURE_MAX_CHUNK_BYTES", 524288)))
        chunk = request.get_data(cache=False) or b""
        if len(chunk) > max_chunk_bytes:
            return jsonify({"error": "audio chunk too large"}), 413
        usable = _align_pcm16_byte_count(len(chunk))
        if usable <= 0:
            return jsonify({"ok": True, "bytes": 0})
        if usable != len(chunk):
            chunk = chunk[:usable]
        with browser_capture_lock:
            if host["slug"] in browser_capture_states:
                browser_capture_states[host["slug"]]["last_seen"] = time.time()
        if not stream_hub.publish_from(host["room_slug"], host["slug"], chunk):
            return jsonify({"error": "source is not active"}), 409
        return jsonify({"ok": True, "bytes": len(chunk), "queued": True})

    @app.post("/transcription/api/source/browser/stop/<host_slug>")
    @app.post("/api/source/browser/stop/<host_slug>")
    def transcription_browser_capture_stop(host_slug: str):
        host = auth_host_from_request(host_slug)
        if not host:
            return jsonify({"error": "unauthorized"}), 403
        stop_browser_capture(host["slug"])
        return jsonify({"ok": True})

    @app.post("/transcription/api/source/browser/marker/<host_slug>")
    @app.post("/api/source/browser/marker/<host_slug>")
    def transcription_browser_capture_marker(host_slug: str):
        host = auth_host_from_request(host_slug)
        if not host:
            return jsonify({"error": "unauthorized"}), 403
        payload = request.get_json(silent=True) or {}
        raw_text = str(payload.get("text") or "").strip()
        allowed_markers = {"[Song]", "[Prophecy]"}
        marker_text = {
            "song": "[Song]",
            "[song]": "[Song]",
            "prophecy": "[Prophecy]",
            "[prophecy]": "[Prophecy]",
        }.get(raw_text.casefold(), raw_text)
        if marker_text not in allowed_markers:
            return jsonify({"error": "unsupported marker"}), 400
        now = _utc_now()
        segment_id = transcription_store.record_transcript_segment(
            host["room_slug"],
            host_slug=host["slug"],
            provider="browser_capture",
            model="marker",
            started_at=now,
            ended_at=now,
            received_at=now,
            text=marker_text,
            source="manual_marker",
        )
        transcription_store.record_source_event(
            host["slug"],
            event_type="browser-marker-inserted",
            level="info",
            message=f"Inserted transcript marker {marker_text}.",
            details={"segment_id": segment_id, "marker": marker_text},
        )
        return jsonify({"ok": True, "segment_id": segment_id, "text": marker_text})

    @app.post("/transcription/api/source/browser/tonevision/takeover/<host_slug>")
    @app.post("/api/source/browser/tonevision/takeover/<host_slug>")
    def transcription_browser_capture_tonevision_takeover(host_slug: str):
        host = auth_host_from_request(host_slug)
        if not host:
            return jsonify({"error": "unauthorized"}), 403
        try:
            takeover = start_tonevision_takeover(host)
        except RuntimeError as exc:
            return jsonify({"error": str(exc)}), 500
        return jsonify({"ok": True, **takeover})

    @app.post("/transcription/api/source/ingest/<host_slug>")
    @app.post("/api/source/ingest/<host_slug>")
    def transcription_source_ingest(host_slug: str):
        token = (request.args.get("token") or "").strip()
        host = transcription_store.get_host(host_slug, include_secret=True)
        if not host or not hmac.compare_digest(token, host.get("heartbeat_token", "")):
            return jsonify({"error": "unauthorized"}), 403
        active_browser = active_browser_capture_for_room(host["room_slug"], excluding_host_slug=host_slug)
        if active_browser:
            return jsonify({"error": "another source is active", "active_host_slug": active_browser["host_slug"]}), 409
        if not source_desired(host):
            transcription_store.record_source_heartbeat(
                host_slug,
                current_device=(host.get("runtime") or {}).get("current_device", ""),
                devices=[],
                is_ingesting=False,
                last_error="Transcription is not requested or transcription provider is unavailable.",
                desired_active=False,
                stream_profile="wav_pcm24",
                stream_channels=1,
                sample_rate_hz=48000,
                sample_bits=24,
            )
            return jsonify({"error": "transcription not requested"}), 409
        runtime = host.get("runtime") or {}
        stream_settings = {
            "stream_channels": max(1, int(runtime.get("stream_channels") or (2 if host.get("capture_mode") == "stereo" else 1))),
            "sample_rate_hz": max(8000, int(runtime.get("sample_rate_hz") or host.get("capture_sample_rate_hz") or 48000)),
            "bits_per_sample": max(16, int(runtime.get("sample_bits") or 24)),
        }
        room_slug = host["room_slug"]
        stream_hub.start_room(room_slug, host_slug, **stream_settings)
        transcription_store.record_source_heartbeat(
            host_slug,
            current_device=runtime.get("current_device", ""),
            devices=[],
            is_ingesting=True,
            last_error="",
            desired_active=True,
            stream_profile="wav_pcm24",
            stream_channels=stream_settings["stream_channels"],
            sample_rate_hz=stream_settings["sample_rate_hz"],
            sample_bits=stream_settings["bits_per_sample"],
        )
        start_room_transcription(room_slug, host_slug, transcription_store.get_host(host_slug) or host)
        try:
            while True:
                chunk = request.stream.read(SOURCE_INGEST_READ_SIZE)
                if not chunk:
                    break
                if not stream_hub.publish_from(room_slug, host_slug, chunk):
                    break
        finally:
            stream_hub.finish_room(room_slug, host_slug)
            stop_room_transcription(room_slug, host_slug)
            transcription_store.record_source_heartbeat(
                host_slug,
                current_device=runtime.get("current_device", ""),
                devices=[],
                is_ingesting=False,
                last_error="",
                desired_active=source_desired(transcription_store.get_host(host_slug) or host),
                stream_profile="wav_pcm24",
                stream_channels=stream_settings["stream_channels"],
                sample_rate_hz=stream_settings["sample_rate_hz"],
                sample_bits=stream_settings["bits_per_sample"],
            )
        return jsonify({"ok": True})

    app.extensions["ntc_transcription_source"] = {
        "stream_hub": stream_hub,
        "workers": transcription_workers,
    }
