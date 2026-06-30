# NTC Transcription

Public read-only transcription display for NTC Newark.

This service is intentionally separate from WebCall and the internal Translator control panel. It reads transcript rows from the shared NTC SQLite database and renders a simple public transcription page. It does not expose meeting state, source status, translation settings, audio output controls, or internal room controls.

## Runtime

- Container port: `1975`
- Default published port: `6768`
- Entry point: `ntc_transcription_app:app`
- Shared runtime DB: `/app/data/ntccast.db`

## Endpoints

- `/healthz`
- `/transcription`
- `/transcription/<room-slug>`
- `/api/public/transcription/<room-slug>/segments`
- `/api/internal/transcription/<room-slug>/segments`

`/transcription` defaults to Room A. Room aliases include `room-a`, `room-b`, and
`convention`, which maps to the `convention-laptop` room. The legacy `/transcribe`
page URLs redirect to `/transcription`, and the legacy public API path remains
available as a compatibility alias. The APIs return only the recent live
transcription window and are not transcript archives. The internal API is for the
`NTC-Translator` container to read current transcript text over the Docker network.

## Local Validation

```bash
python3 -m py_compile ntc_transcription_app.py
python3 -m unittest test_transcription_app.py
```

## Local Whisper Worker

The M4 Mac mini can run a separate high-accuracy local Whisper endpoint for
WebCall audio chunks:

```bash
python3 tools/whisper_large_server.py \
  --host 0.0.0.0 \
  --port 8766 \
  --model openai/whisper-large-v3 \
  --device cpu \
  --quiet
```

The endpoint implements the same bridge contract as the lightweight local
worker: `POST /transcription` with an `audio/wav` body returns JSON containing a
`text` field. `POST /transcribe` is accepted as a compatibility alias.
`POST /v1/audio/transcriptions` is also accepted for future clients that expect
a versioned speech-to-text endpoint, using the same raw WAV request body.

The bridge is designed for multiple source machines to call it at the same time.
HTTP requests are accepted concurrently, then bounded by an in-process queue
before model inference. The Whisper model itself runs one generation at a time
inside the process so the M4 does not get overloaded by parallel large-v3
requests.

Operational endpoints:

- `GET /healthz` and `GET /readyz`: service/model readiness and queue stats.
- `GET /stats`: same stats, protected when `NTC_WHISPER_API_TOKEN` is set.
- `POST /transcription`, `/transcribe`, or `/v1/audio/transcriptions`: raw WAV
  body, optional `language`, `prompt`, and `max_new_tokens` query parameters.

Useful hardening knobs:

- `NTC_WHISPER_MAX_BODY_MB` / `--max-body-mb`: reject oversized WAV chunks.
- `NTC_WHISPER_MAX_QUEUED_REQUESTS` / `--max-queued-requests`: cap concurrent
  queued transcription requests.
- `NTC_WHISPER_QUEUE_TIMEOUT_SECONDS` / `--queue-timeout-seconds`: return 429
  instead of waiting forever when the queue is saturated.
- `NTC_WHISPER_API_TOKEN` / `--api-token`: optional bearer or
  `X-NTC-Whisper-Token` auth. Leave unset for the current private Tailscale-only
  deployment; set it before exposing the endpoint more broadly.
