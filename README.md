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

`/transcription` defaults to Room A. The legacy `/transcribe` page URLs redirect to `/transcription`, and the legacy public API path remains available as a compatibility alias. The APIs return only the recent live transcription window and are not transcript archives. The internal API is for the `NTC-Translator` container to read current transcript text over the Docker network.

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
