# NTC Live Captions

Public read-only live caption display for NTC Newark.

This service is intentionally separate from WebCall and the internal Translator/Transcriptor control panel. It reads transcript rows from the shared NTC SQLite database and renders a simple public caption page. It does not expose meeting state, source status, translation settings, audio output controls, or internal room controls.

## Runtime

- Container port: `1975`
- Default published port: `6768`
- Entry point: `ntc_live_captions_app:app`
- Shared runtime DB: `/app/data/ntccast.db`

## Endpoints

- `/healthz`
- `/transcribe`
- `/transcribe/<room-slug>`
- `/api/public/transcribe/<room-slug>/segments`

`/transcribe` defaults to Room A. The public API returns only the recent live caption window and is not a transcript archive.

## Local Validation

```bash
python3 -m py_compile ntc_live_captions_app.py
python3 -m unittest test_live_captions_app.py
```
