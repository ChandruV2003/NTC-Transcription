# NTC Transcription

Public transcription display and private source controls for NTC Newark.

This service is intentionally separate from WebCall and the internal Translator control panel. `/transcription` is the public display surface. `/transcription/settings` is the private control surface for choosing the active source, monitoring source-agent health, viewing transcript sessions, and managing translation output where configured.

Only one visible transcription source should be active at a time. The current source set is Room A, Room B, and the convention laptop. Room A and Room B can still be started by WebCall meeting automation, but transcription audio is not tapped from the WebCall process. Source agents send audio directly to NTC Transcription, which delegates speech recognition to the configured Whisper backend.

## Runtime

- Container port: `1975`
- Default published port: `6768`
- Entry point: `ntc_transcription_app:app`
- Shared runtime DB: `/app/data/ntccast.db`

## Endpoints

- `/healthz`
- `/transcription`
- `/transcription/<room-slug>`
- `/transcription/capture/<source-host>`
- `/api/public/transcription/<room-slug>/segments`
- `/api/internal/transcription/<room-slug>/segments`

`/transcription` renders the configured default room from `NTC_TRANSCRIPTION_DEFAULT_ROOM`.
Room aliases include `room-a`, `room-b`, and `convention`, which maps to the
`convention-laptop` room. The legacy `/transcribe` page URLs redirect to
`/transcription`, and the legacy public API path remains available as a
compatibility alias. The APIs return only the recent live transcription window and
are not transcript archives. The internal API is for the `NTC-Translator`
container to read current transcript text over the Docker network.

## Browser Source Capture

Room C can use a mobile browser source such as the iPhone source host:

```text
/transcription/capture/iphone15pro?token=<source-token>
```

The page requests microphone access, keeps the capture foregrounded, converts
the browser microphone stream to mono PCM16, and posts short chunks into the same
source worker used by the native agents. NTC Transcription still delegates speech
recognition to the configured Whisper backend; the phone is only an audio tap
point. While a browser source is active for Room C, other Room C source agents
are rejected so microphones are not mixed together.

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
