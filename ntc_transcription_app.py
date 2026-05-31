"""Public transcription display and private transcription controls for NTC."""

from __future__ import annotations

import hmac
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, Response, jsonify, redirect, render_template_string, request, url_for


ROOM_SLUG_ALIASES = {
    "study-room": "room-a",
    "meeting-hall": "room-b",
}
DEFAULT_VISIBLE_ROOM_SLUGS = ("room-a", "room-b")
TRANSLATION_LANGUAGE_OPTIONS = [
    {"code": "zh-CN", "label": "Mandarin Chinese"},
    {"code": "es", "label": "Spanish"},
    {"code": "fr", "label": "French"},
    {"code": "hi", "label": "Hindi"},
]
TRANSLATION_LANGUAGE_LABELS = {item["code"]: item["label"] for item in TRANSLATION_LANGUAGE_OPTIONS}
TRANSLATION_OUTPUT_HOST_SLUGS = {"hp-envy-16-ad0xx"}


def _canonical_room_slug(room_slug: str | None) -> str:
    normalized = (room_slug or "").strip()
    return ROOM_SLUG_ALIASES.get(normalized, normalized)


def _csv_tuple(value: str, default: tuple[str, ...]) -> tuple[str, ...]:
    items = tuple(item.strip() for item in value.split(",") if item.strip())
    return items or default


class TranscriptionStore:
    def __init__(self, db_path: str | None):
        self.db_path = Path(db_path or "/app/data/ntccast.db")

    def _connect(self, *, readonly: bool = True):
        if readonly:
            uri = f"file:{self.db_path}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, timeout=5)
        else:
            connection = sqlite3.connect(self.db_path, timeout=5)
        connection.row_factory = sqlite3.Row
        return connection

    def get_room(self, room_slug: str):
        with self._connect(readonly=True) as connection:
            row = connection.execute(
                """
                SELECT slug, label
                FROM rooms
                WHERE slug = ?
                """,
                (room_slug,),
            ).fetchone()
        if not row:
            return None
        return {"slug": row["slug"], "label": row["label"] or row["slug"]}

    def health(self) -> bool:
        with self._connect(readonly=True) as connection:
            connection.execute("SELECT 1 FROM rooms LIMIT 1").fetchone()
            connection.execute("SELECT 1 FROM transcript_segments LIMIT 1").fetchone()
        return True

    def list_recent_segments(self, room_slug: str, *, limit: int = 30):
        with self._connect(readonly=True) as connection:
            rows = connection.execute(
                """
                SELECT id, room_slug, received_at, text, is_final
                FROM transcript_segments
                WHERE room_slug = ?
                ORDER BY received_at DESC, id DESC
                LIMIT ?
                """,
                (room_slug, max(1, min(500, int(limit or 30)))),
            ).fetchall()
        return [_segment_payload(row) for row in rows]

    def list_segments_after(self, room_slug: str, *, after_id: int = 0, limit: int = 80):
        with self._connect(readonly=True) as connection:
            rows = connection.execute(
                """
                SELECT id, room_slug, received_at, text, is_final
                FROM transcript_segments
                WHERE room_slug = ?
                  AND id > ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (room_slug, max(0, int(after_id or 0)), max(1, min(500, int(limit or 80)))),
            ).fetchall()
        return [_segment_payload(row) for row in rows]

    def list_settings_rooms(self, visible_room_slugs: tuple[str, ...]):
        placeholders = ",".join("?" for _ in visible_room_slugs)
        if not placeholders:
            return []
        with self._connect(readonly=True) as connection:
            rows = connection.execute(
                f"""
                SELECT rooms.slug,
                       rooms.label,
                       rooms.enabled AS room_enabled,
                       rooms.transcription_enabled,
                       hosts.slug AS host_slug,
                       hosts.label AS host_label,
                       hosts.enabled AS host_enabled,
                       hosts.manual_mode,
                       hosts.translation_output_enabled,
                       hosts.translation_target_language,
                       source_runtime.desired_active,
                       source_runtime.is_ingesting,
                       source_runtime.current_device,
                       source_runtime.last_seen_at,
                       source_runtime.last_error
                FROM rooms
                LEFT JOIN hosts ON hosts.room_slug = rooms.slug
                LEFT JOIN source_runtime ON source_runtime.host_slug = hosts.slug
                WHERE rooms.slug IN ({placeholders})
                ORDER BY rooms.label
                """,
                visible_room_slugs,
            ).fetchall()
        rooms = []
        for row in rows:
            stats = self.transcript_stats(row["slug"], window_minutes=180)
            rooms.append(
                {
                    "slug": row["slug"],
                    "label": row["label"] or row["slug"],
                    "room_enabled": bool(row["room_enabled"]),
                    "transcription_enabled": bool(row["transcription_enabled"]),
                    "host_slug": row["host_slug"] or "",
                    "host_label": row["host_label"] or "No source host",
                    "host_enabled": bool(row["host_enabled"]) if row["host_slug"] else False,
                    "manual_mode": row["manual_mode"] or "",
                    "source_requested": bool(row["desired_active"]),
                    "source_ingesting": bool(row["is_ingesting"]),
                    "current_device": row["current_device"] or "",
                    "last_seen_at": row["last_seen_at"] or "",
                    "last_error": row["last_error"] or "",
                    "translation_output_supported": row["host_slug"] in TRANSLATION_OUTPUT_HOST_SLUGS,
                    "translation_output_enabled": bool(row["translation_output_enabled"]) if row["host_slug"] else False,
                    "translation_target_language": row["translation_target_language"] or "zh-CN",
                    "translation_target_language_label": TRANSLATION_LANGUAGE_LABELS.get(
                        row["translation_target_language"] or "zh-CN",
                        row["translation_target_language"] or "zh-CN",
                    ),
                    "stats": stats,
                }
            )
        return rooms

    def set_room_transcription_enabled(self, room_slug: str, enabled: bool) -> bool:
        timestamp = datetime.now(timezone.utc).isoformat()
        with self._connect(readonly=False) as connection:
            cursor = connection.execute(
                """
                UPDATE rooms
                SET transcription_enabled = ?, updated_at = ?
                WHERE slug = ?
                """,
                (1 if enabled else 0, timestamp, room_slug),
            )
            if not cursor.rowcount:
                return False
            self._record_event(
                connection,
                component="transcription",
                event_type="transcription-config-updated",
                message=f"Transcription {'enabled' if enabled else 'disabled'} for {room_slug}.",
                room_slug=room_slug,
                details={"transcription_enabled": bool(enabled), "surface": "/transcription/settings"},
            )
            return True

    def set_host_translation_output_enabled(self, host_slug: str, enabled: bool) -> bool:
        timestamp = datetime.now(timezone.utc).isoformat()
        with self._connect(readonly=False) as connection:
            cursor = connection.execute(
                """
                UPDATE hosts
                SET translation_output_enabled = ?, updated_at = ?
                WHERE slug = ?
                """,
                (1 if enabled else 0, timestamp, host_slug),
            )
            return bool(cursor.rowcount)

    def set_host_translation_target_language(self, host_slug: str, target_language: str) -> bool:
        timestamp = datetime.now(timezone.utc).isoformat()
        with self._connect(readonly=False) as connection:
            cursor = connection.execute(
                """
                UPDATE hosts
                SET translation_target_language = ?, updated_at = ?
                WHERE slug = ?
                """,
                (target_language, timestamp, host_slug),
            )
            return bool(cursor.rowcount)

    def transcript_stats(self, room_slug: str, *, window_minutes: int = 180):
        cutoff = datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() - max(1, int(window_minutes or 180)) * 60,
            tz=timezone.utc,
        ).isoformat()
        with self._connect(readonly=True) as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS segment_count,
                       COALESCE(SUM(LENGTH(text)), 0) AS character_count,
                       MIN(received_at) AS first_received_at,
                       MAX(received_at) AS last_received_at
                FROM transcript_segments
                WHERE room_slug = ?
                  AND received_at >= ?
                """,
                (room_slug, cutoff),
            ).fetchone()
        return {
            "window_minutes": max(1, int(window_minutes or 180)),
            "segment_count": int((row or {})["segment_count"] or 0),
            "character_count": int((row or {})["character_count"] or 0),
            "first_received_at": (row or {})["first_received_at"],
            "last_received_at": (row or {})["last_received_at"],
        }

    def _record_event(self, connection: sqlite3.Connection, *, component: str, event_type: str, message: str, room_slug: str, details: dict):
        try:
            connection.execute(
                """
                INSERT INTO room_events (
                    room_slug, host_slug, component, event_type, level, message, details_json, occurred_at
                )
                VALUES (?, '', ?, ?, 'info', ?, ?, ?)
                """,
                (room_slug, component, event_type, message, _json_details(details), datetime.now(timezone.utc).isoformat()),
            )
        except sqlite3.Error:
            return


def _json_details(details: dict) -> str:
    import json

    return json.dumps(details, sort_keys=True, separators=(",", ":"))


def _segment_payload(row: sqlite3.Row) -> dict:
    return {
        "id": int(row["id"]),
        "room_slug": row["room_slug"],
        "received_at": row["received_at"],
        "text": row["text"],
        "is_final": bool(row["is_final"]),
    }


def create_app(test_config: dict | None = None, *, store: TranscriptionStore | None = None) -> Flask:
    app = Flask(__name__)
    app.config.update(
        NTC_DB_PATH=os.getenv("NTC_DB_PATH", "/app/data/ntccast.db"),
        NTC_TRANSCRIPTION_TITLE=os.getenv("NTC_TRANSCRIPTION_TITLE", "NTC Transcription"),
        NTC_TRANSCRIPTION_DEFAULT_ROOM=os.getenv("NTC_TRANSCRIPTION_DEFAULT_ROOM", "room-a"),
        NTC_TRANSCRIPTION_VISIBLE_ROOMS=os.getenv("NTC_TRANSCRIPTION_VISIBLE_ROOMS", "room-a,room-b"),
        NTC_TRANSCRIPTION_POLL_MS=int(os.getenv("NTC_TRANSCRIPTION_POLL_MS", "1000")),
        NTC_TRANSCRIPTION_INITIAL_LINES=int(os.getenv("NTC_TRANSCRIPTION_INITIAL_LINES", "30")),
        NTC_TRANSCRIPTION_API_LINES=int(os.getenv("NTC_TRANSCRIPTION_API_LINES", "80")),
        NTC_TRANSCRIPTION_RENDER_LINES=int(os.getenv("NTC_TRANSCRIPTION_RENDER_LINES", "18")),
        NTC_TRANSCRIPTION_SETTINGS_AUTH_ENABLED=os.getenv("NTC_TRANSCRIPTION_SETTINGS_AUTH_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"},
        NTC_TRANSCRIPTION_SETTINGS_PASSWORD=os.getenv("NTC_TRANSCRIPTION_SETTINGS_PASSWORD", "") or os.getenv("NTC_ADMIN_PASSWORD", ""),
    )
    if test_config:
        app.config.update(test_config)

    transcription_store = store or TranscriptionStore(app.config["NTC_DB_PATH"])
    app.transcription_store = transcription_store

    def _visible_rooms() -> tuple[str, ...]:
        return _csv_tuple(app.config.get("NTC_TRANSCRIPTION_VISIBLE_ROOMS", ""), DEFAULT_VISIBLE_ROOM_SLUGS)

    def _room_or_404(room_slug: str):
        canonical = _canonical_room_slug(room_slug)
        if canonical not in _visible_rooms():
            return None
        return transcription_store.get_room(canonical)

    def _settings_authorized() -> bool:
        if not app.config.get("NTC_TRANSCRIPTION_SETTINGS_AUTH_ENABLED", True):
            return True
        expected = app.config.get("NTC_TRANSCRIPTION_SETTINGS_PASSWORD", "")
        if not expected:
            return False
        auth = request.authorization
        return bool(auth and hmac.compare_digest(auth.password or "", expected))

    def _settings_auth_required():
        return Response(
            "Authentication required",
            401,
            {"WWW-Authenticate": 'Basic realm="NTC Transcription Settings"'},
        )

    def _require_settings_auth():
        if not _settings_authorized():
            return _settings_auth_required()
        return None

    @app.after_request
    def no_store(response):
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/healthz")
    def healthz():
        try:
            transcription_store.health()
            return jsonify({"ok": True, "timestamp": datetime.now(timezone.utc).isoformat()})
        except Exception as exc:  # pragma: no cover - defensive runtime guard
            app.logger.exception("transcription health check failed")
            return jsonify({"ok": False, "error": str(exc)}), 500

    @app.get("/")
    def index():
        return redirect(url_for("public_transcription"))

    def _render_public_transcription(room_slug: str):
        room = _room_or_404(room_slug)
        if not room:
            return jsonify({"error": "unknown room"}), 404
        recent_segments = list(
            reversed(
                transcription_store.list_recent_segments(
                    room["slug"],
                    limit=app.config["NTC_TRANSCRIPTION_INITIAL_LINES"],
                )
            )
        )
        return render_template_string(
            PUBLIC_TRANSCRIBE_TEMPLATE,
            title=app.config["NTC_TRANSCRIPTION_TITLE"],
            room=room,
            segments=recent_segments,
            poll_ms=app.config["NTC_TRANSCRIPTION_POLL_MS"],
            render_lines=app.config["NTC_TRANSCRIPTION_RENDER_LINES"],
        )

    @app.get("/transcription")
    def public_transcription():
        return _render_public_transcription(app.config["NTC_TRANSCRIPTION_DEFAULT_ROOM"])

    @app.get("/transcription/<room_slug>")
    def public_transcription_room(room_slug: str):
        return _render_public_transcription(room_slug)

    @app.get("/transcription/settings")
    def transcription_settings():
        auth_response = _require_settings_auth()
        if auth_response:
            return auth_response
        rooms = transcription_store.list_settings_rooms(_visible_rooms())
        selected_slug = _canonical_room_slug(request.args.get("room")) or app.config["NTC_TRANSCRIPTION_DEFAULT_ROOM"]
        selected = next((room for room in rooms if room["slug"] == selected_slug), None) or (rooms[0] if rooms else None)
        return render_template_string(
            SETTINGS_TEMPLATE,
            title=f"{app.config['NTC_TRANSCRIPTION_TITLE']} Settings",
            rooms=rooms,
            selected=selected,
            language_options=TRANSLATION_LANGUAGE_OPTIONS,
            public_base=url_for("public_transcription"),
            message=request.args.get("message"),
            error=request.args.get("error"),
        )

    @app.post("/transcription/settings/rooms/<room_slug>/transcription")
    def set_room_transcription(room_slug: str):
        auth_response = _require_settings_auth()
        if auth_response:
            return auth_response
        room = _room_or_404(room_slug)
        if not room:
            return jsonify({"error": "unknown room"}), 404
        value = str(request.form.get("transcription_enabled", "")).strip().lower()
        enabled = value in {"1", "true", "yes", "on"}
        transcription_store.set_room_transcription_enabled(room["slug"], enabled)
        return redirect(
            url_for(
                "transcription_settings",
                room=room["slug"],
                message=f"Transcription {'enabled' if enabled else 'disabled'} for {room['label']}.",
            )
        )

    @app.post("/transcription/settings/rooms/<room_slug>/translation-output")
    def set_translation_output(room_slug: str):
        auth_response = _require_settings_auth()
        if auth_response:
            return auth_response
        rooms = transcription_store.list_settings_rooms(_visible_rooms())
        room = next((item for item in rooms if item["slug"] == _canonical_room_slug(room_slug)), None)
        if not room:
            return jsonify({"error": "unknown room"}), 404
        if not room["translation_output_supported"]:
            return redirect(url_for("transcription_settings", room=room["slug"], error="Translation room output is not configured for that room."))
        value = str(request.form.get("translation_output_enabled", "")).strip().lower()
        enabled = value in {"1", "true", "yes", "on"}
        transcription_store.set_host_translation_output_enabled(room["host_slug"], enabled)
        return redirect(url_for("transcription_settings", room=room["slug"], message=f"Translation output {'enabled' if enabled else 'disabled'} for {room['label']}."))

    @app.post("/transcription/settings/rooms/<room_slug>/translation-settings")
    def set_translation_settings(room_slug: str):
        auth_response = _require_settings_auth()
        if auth_response:
            return auth_response
        rooms = transcription_store.list_settings_rooms(_visible_rooms())
        room = next((item for item in rooms if item["slug"] == _canonical_room_slug(room_slug)), None)
        if not room:
            return jsonify({"error": "unknown room"}), 404
        target_language = (request.form.get("target_language") or "").strip()
        if target_language not in TRANSLATION_LANGUAGE_LABELS:
            return redirect(url_for("transcription_settings", room=room["slug"], error="Choose a supported translation language."))
        if not room["translation_output_supported"]:
            return redirect(url_for("transcription_settings", room=room["slug"], error="Translation room output is not configured for that room."))
        transcription_store.set_host_translation_target_language(room["host_slug"], target_language)
        return redirect(url_for("transcription_settings", room=room["slug"], message=f"Translation target set to {TRANSLATION_LANGUAGE_LABELS[target_language]}."))

    @app.get("/transcribe")
    def legacy_public_transcribe():
        return redirect(url_for("public_transcription"), code=308)

    @app.get("/transcribe/<room_slug>")
    def legacy_public_transcribe_room(room_slug: str):
        return redirect(url_for("public_transcription_room", room_slug=room_slug), code=308)

    @app.get("/api/public/transcription/<room_slug>/segments")
    @app.get("/api/public/transcribe/<room_slug>/segments")
    def public_transcription_segments(room_slug: str):
        return _segments_response(room_slug)

    @app.get("/api/internal/transcription/<room_slug>/segments")
    def internal_transcription_segments(room_slug: str):
        return _segments_response(room_slug)

    def _segments_response(room_slug: str):
        room = _room_or_404(room_slug)
        if not room:
            return jsonify({"error": "unknown room"}), 404
        try:
            after_id = int(request.args.get("after_id", "0") or "0")
        except ValueError:
            after_id = 0
        limit = app.config["NTC_TRANSCRIPTION_API_LINES"]
        recent_segments = transcription_store.list_recent_segments(room["slug"], limit=limit)
        recent_floor_id = min((int(segment["id"]) for segment in recent_segments), default=0)
        if after_id <= 0 or (recent_floor_id and after_id < recent_floor_id):
            segments = list(reversed(recent_segments))
        else:
            segments = transcription_store.list_segments_after(room["slug"], after_id=after_id, limit=limit)
        return jsonify({"room_slug": room["slug"], "segments": segments})

    return app


SETTINGS_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{ title }}</title>
    <style>
      * { box-sizing: border-box; }
      body {
        margin: 0;
        background: #101214;
        color: #f6f7f8;
        font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }
      main {
        width: min(1120px, calc(100vw - 32px));
        margin: 0 auto;
        padding: 28px 0 40px;
      }
      header {
        display: flex;
        justify-content: space-between;
        gap: 16px;
        align-items: end;
        margin-bottom: 20px;
      }
      h1 { margin: 0; font-size: clamp(28px, 4vw, 46px); line-height: 1; letter-spacing: 0; }
      .sub { color: #a8b0b8; margin-top: 8px; }
      .tabs { display: flex; gap: 8px; flex-wrap: wrap; margin: 20px 0; }
      .tab,
      .button {
        border: 1px solid #343b44;
        border-radius: 8px;
        background: #181c20;
        color: #f6f7f8;
        padding: 10px 14px;
        text-decoration: none;
        font-weight: 800;
        cursor: pointer;
      }
      .tab.active,
      .button.primary { background: #f6f7f8; color: #101214; border-color: #f6f7f8; }
      .panel {
        border: 1px solid #2c333b;
        border-radius: 8px;
        background: #15191d;
        padding: 20px;
      }
      .grid {
        display: grid;
        grid-template-columns: minmax(0, 1.1fr) minmax(280px, .9fr);
        gap: 16px;
      }
      .stack { display: grid; gap: 14px; }
      .control {
        border: 1px solid #2c333b;
        border-radius: 8px;
        background: #111417;
        padding: 16px;
      }
      .control h2 { margin: 0 0 8px; font-size: 18px; letter-spacing: 0; }
      .control p { margin: 0; color: #a8b0b8; line-height: 1.45; }
      .row {
        display: flex;
        gap: 12px;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
      }
      .pill {
        display: inline-flex;
        align-items: center;
        min-height: 30px;
        padding: 6px 10px;
        border-radius: 999px;
        background: #252b31;
        color: #d8dde2;
        font-size: 13px;
        font-weight: 850;
      }
      .good { background: #113f32; color: #8ff0c0; }
      .warn { background: #473614; color: #ffd784; }
      .bad { background: #451d24; color: #ff9aa9; }
      .muted { color: #a8b0b8; }
      select {
        min-height: 42px;
        border-radius: 8px;
        border: 1px solid #343b44;
        background: #0f1215;
        color: #f6f7f8;
        padding: 0 10px;
      }
      .notice {
        border-radius: 8px;
        padding: 12px 14px;
        margin-bottom: 14px;
        background: #17251f;
        color: #9cf0bd;
      }
      .notice.error { background: #32181f; color: #ff9aa9; }
      .stats {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 10px;
      }
      .metric {
        border: 1px solid #2c333b;
        border-radius: 8px;
        padding: 14px;
        background: #101316;
      }
      .metric strong { display: block; font-size: 28px; }
      .metric span { color: #a8b0b8; font-size: 13px; }
      @media (max-width: 760px) {
        main { width: min(100vw - 24px, 1120px); padding-top: 18px; }
        header,
        .grid { grid-template-columns: 1fr; display: grid; align-items: start; }
      }
    </style>
  </head>
  <body>
    <main>
      <header>
        <div>
          <h1>Transcription Settings</h1>
          <div class="sub">Controls for source transcription and translated room output. WebCall stays separate.</div>
        </div>
        <a class="button" href="{{ public_base }}">Open Display</a>
      </header>

      {% if message %}<div class="notice">{{ message }}</div>{% endif %}
      {% if error %}<div class="notice error">{{ error }}</div>{% endif %}

      <nav class="tabs">
        {% for room in rooms %}
          <a class="tab {% if selected and room.slug == selected.slug %}active{% endif %}" href="{{ url_for('transcription_settings', room=room.slug) }}">
            {{ room.label }}
          </a>
        {% endfor %}
      </nav>

      {% if selected %}
      <section class="panel">
        <div class="grid">
          <div class="stack">
            <div class="control">
              <div class="row">
                <div>
                  <h2>Source Transcription</h2>
                  <p>Turns on the room source for transcription without starting a public WebCall meeting.</p>
                </div>
                <span class="pill {% if selected.transcription_enabled %}good{% else %}warn{% endif %}">
                  {{ "On" if selected.transcription_enabled else "Off" }}
                </span>
              </div>
              <form method="post" action="{{ url_for('set_room_transcription', room_slug=selected.slug) }}" style="margin-top: 14px;">
                <input type="hidden" name="transcription_enabled" value="{{ "0" if selected.transcription_enabled else "1" }}">
                <button class="button primary" type="submit">
                  {{ "Turn Transcription Off" if selected.transcription_enabled else "Turn Transcription On" }}
                </button>
              </form>
            </div>

            <div class="control">
              <div class="row">
                <div>
                  <h2>Translation Output</h2>
                  <p>Controls translated audio sent back to the supported room output path.</p>
                </div>
                {% if selected.translation_output_supported %}
                  <span class="pill {% if selected.translation_output_enabled %}good{% else %}warn{% endif %}">
                    {{ "On" if selected.translation_output_enabled else "Off" }}
                  </span>
                {% else %}
                  <span class="pill">Unavailable</span>
                {% endif %}
              </div>
              {% if selected.translation_output_supported %}
                <form method="post" action="{{ url_for('set_translation_output', room_slug=selected.slug) }}" style="margin-top: 14px;">
                  <input type="hidden" name="translation_output_enabled" value="{{ "0" if selected.translation_output_enabled else "1" }}">
                  <button class="button" type="submit">
                    {{ "Turn Translation Output Off" if selected.translation_output_enabled else "Turn Translation Output On" }}
                  </button>
                </form>
                <form method="post" action="{{ url_for('set_translation_settings', room_slug=selected.slug) }}" class="row" style="margin-top: 12px; justify-content: flex-start;">
                  <select name="target_language" aria-label="Translation target language">
                    {% for option in language_options %}
                      <option value="{{ option.code }}" {% if selected.translation_target_language == option.code %}selected{% endif %}>{{ option.label }}</option>
                    {% endfor %}
                  </select>
                  <button class="button" type="submit">Set Language</button>
                </form>
              {% endif %}
            </div>
          </div>

          <aside class="stack">
            <div class="control">
              <h2>Source Status</h2>
              <p>{{ selected.host_label }}{% if selected.current_device %} · {{ selected.current_device }}{% endif %}</p>
              <div class="row" style="justify-content: flex-start; margin-top: 12px;">
                <span class="pill {% if selected.source_requested %}good{% else %}warn{% endif %}">Source {{ "Requested" if selected.source_requested else "Idle" }}</span>
                <span class="pill {% if selected.source_ingesting %}good{% else %}warn{% endif %}">Ingest {{ "Running" if selected.source_ingesting else "Stopped" }}</span>
              </div>
              {% if selected.last_seen_at %}<p style="margin-top: 10px;">Last seen {{ selected.last_seen_at }}</p>{% endif %}
              {% if selected.last_error %}<p class="bad" style="margin-top: 10px;">{{ selected.last_error }}</p>{% endif %}
            </div>

            <div class="control">
              <h2>Recent Transcription Stats</h2>
              <div class="stats">
                <div class="metric">
                  <strong>{{ selected.stats.segment_count }}</strong>
                  <span>segments in {{ selected.stats.window_minutes }} min</span>
                </div>
                <div class="metric">
                  <strong>{{ selected.stats.character_count }}</strong>
                  <span>characters</span>
                </div>
              </div>
              {% if selected.stats.last_received_at %}
                <p style="margin-top: 12px;">Latest line {{ selected.stats.last_received_at }}</p>
              {% else %}
                <p style="margin-top: 12px;">No recent transcript lines.</p>
              {% endif %}
            </div>
          </aside>
        </div>
      </section>
      {% else %}
        <section class="panel"><p>No rooms are configured.</p></section>
      {% endif %}
    </main>
  </body>
</html>
"""


PUBLIC_TRANSCRIBE_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{ title }}</title>
    <style>
      * { box-sizing: border-box; }
      html, body { min-height: 100%; }
      body {
        margin: 0;
        background: #000;
        color: #fff;
        font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }
      main {
        min-height: 100vh;
        display: flex;
        align-items: flex-end;
        width: 100vw;
        padding: clamp(14px, 2.6vw, 42px);
      }
      .transcript {
        width: 100%;
        display: grid;
        gap: clamp(18px, 2.2vw, 34px);
      }
      .block,
      .empty {
        margin: 0;
        color: #fff;
        font-size: clamp(34px, 5.2vw, 88px);
        font-weight: 850;
        line-height: 1.1;
        letter-spacing: 0;
        overflow-wrap: anywhere;
        text-wrap: pretty;
      }
      .block:not(:last-child) { opacity: 0.6; }
      .empty { opacity: 0.72; }
      @media (max-width: 720px) {
        main { padding: 16px; }
        .block,
        .empty { font-size: clamp(28px, 11vw, 58px); }
      }
    </style>
  </head>
  <body>
    <main>
      <section
        id="transcript"
        class="transcript"
        aria-live="polite"
        data-room-slug="{{ room.slug }}"
        data-poll-ms="{{ poll_ms }}"
        data-render-lines="{{ render_lines }}"
      >
        <p class="empty" id="empty-state">Waiting for transcription.</p>
      </section>
      <script id="initial-segments" type="application/json">{{ segments|tojson }}</script>
    </main>
    <script>
      (() => {
        const transcript = document.getElementById("transcript");
        if (!transcript) return;
        const roomSlug = transcript.dataset.roomSlug;
        const pollMs = Number(transcript.dataset.pollMs || "1000");
        const renderBlocks = Math.max(1, Math.ceil(Number(transcript.dataset.renderLines || "18") / 3));
        const initialSegments = JSON.parse(document.getElementById("initial-segments")?.textContent || "[]");
        const segments = [];
        const seen = new Set();
        let lastId = 0;

        function normalizeText(text) {
          const normalized = String(text || "").replace(/\s+/g, " ").trim();
          return /[A-Za-z0-9\u00c0-\uffff]/.test(normalized) ? normalized : "";
        }

        function sentenceEnded(text) {
          return /[.!?]["')\]]?$/.test(text.trim());
        }

        function addSegment(segment) {
          const id = String(segment.id || "");
          const text = normalizeText(segment.text);
          if (!id || seen.has(id)) return false;
          seen.add(id);
          lastId = Math.max(lastId, Number(id));
          if (!text) return false;
          segments.push({ id, text });
          return true;
        }

        function buildBlocks() {
          const blocks = [];
          let current = [];
          let currentLength = 0;
          for (const segment of segments) {
            const nextLength = currentLength + (currentLength ? 1 : 0) + segment.text.length;
            const shouldStartBlock = current.length > 0 && (
              nextLength > 620 ||
              current.length >= 5 ||
              (currentLength >= 260 && sentenceEnded(current[current.length - 1].text))
            );
            if (shouldStartBlock) {
              blocks.push(current);
              current = [];
              currentLength = 0;
            }
            current.push(segment);
            currentLength += (currentLength ? 1 : 0) + segment.text.length;
          }
          if (current.length) blocks.push(current);
          return blocks.slice(-renderBlocks);
        }

        function render() {
          const blocks = buildBlocks();
          transcript.replaceChildren();
          if (!blocks.length) {
            const empty = document.createElement("p");
            empty.className = "empty";
            empty.id = "empty-state";
            empty.textContent = "Waiting for transcription.";
            transcript.appendChild(empty);
            return;
          }
          for (const blockSegments of blocks) {
            const block = document.createElement("p");
            block.className = "block";
            block.dataset.segmentIds = blockSegments.map((segment) => segment.id).join(",");
            block.textContent = blockSegments.map((segment) => segment.text).join(" ");
            transcript.appendChild(block);
          }
          window.scrollTo({ top: document.body.scrollHeight, behavior: "smooth" });
        }

        for (const segment of initialSegments) addSegment(segment);
        render();

        function appendSegment(segment) {
          if (addSegment(segment)) render();
        }

        async function poll() {
          try {
            const response = await fetch(`/api/public/transcription/${encodeURIComponent(roomSlug)}/segments?after_id=${lastId}`, { cache: "no-store" });
            if (response.ok) {
              const payload = await response.json();
              for (const segment of payload.segments || []) appendSegment(segment);
            }
          } finally {
            window.setTimeout(poll, Math.max(500, pollMs));
          }
        }

        window.setTimeout(poll, Math.max(500, pollMs));
      })();
    </script>
  </body>
</html>
"""


app = create_app()


if __name__ == "__main__":
    host = os.getenv("NTC_TRANSCRIPTION_HOST", "0.0.0.0")
    port = int(os.getenv("NTC_TRANSCRIPTION_PORT", "1975"))
    app.run(host=host, port=port)
