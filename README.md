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
- `/transcribe`
- `/transcribe/<room-slug>`
- `/api/public/transcribe/<room-slug>/segments`
- `/api/internal/transcription/<room-slug>/segments`

`/transcribe` defaults to Room A. The APIs return only the recent live transcription window and are not transcript archives. The internal API is for the `NTC-Translator` container to read current transcript text over the Docker network.

## Local Validation

```bash
python3 -m py_compile ntc_transcription_app.py
python3 -m unittest test_transcription_app.py
```
