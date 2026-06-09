"""Public transcription display and private transcription controls for NTC."""

from __future__ import annotations

import hmac
import os
import sqlite3
import csv
import io
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, Response, abort, jsonify, redirect, render_template_string, request, send_file, session, url_for


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
BRAND_BACKGROUND_FILENAME = "ntc-embossed-background.jpg"
DEFAULT_BRAND_BACKGROUND_PATH = Path(__file__).resolve().parent / "assets" / BRAND_BACKGROUND_FILENAME


def _canonical_room_slug(room_slug: str | None) -> str:
    normalized = (room_slug or "").strip()
    return ROOM_SLUG_ALIASES.get(normalized, normalized)


def _csv_tuple(value: str, default: tuple[str, ...]) -> tuple[str, ...]:
    items = tuple(item.strip() for item in value.split(",") if item.strip())
    return items or default


def _source_status(transcription_enabled: bool, source_requested: bool, source_ingesting: bool) -> tuple[str, str]:
    if transcription_enabled:
        if source_ingesting:
            return "Live", "good"
        return "Starting", "warn"
    if source_ingesting or source_requested:
        return "Stopping", "warn"
    return "Idle", ""


class TranscriptionStore:
    def __init__(self, db_path: str | None):
        self.db_path = Path(db_path or "/app/data/ntccast.db")
        try:
            self._ensure_schema()
        except sqlite3.OperationalError as exc:
            if "unable to open database file" not in str(exc):
                raise

    def _connect(self, *, readonly: bool = True):
        if readonly:
            uri = f"file:{self.db_path}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, timeout=5)
        else:
            connection = sqlite3.connect(self.db_path, timeout=5)
        connection.row_factory = sqlite3.Row
        return connection

    def _ensure_schema(self):
        with self._connect(readonly=False) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS transcription_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    room_slug TEXT NOT NULL,
                    host_slug TEXT,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    trigger_mode TEXT NOT NULL DEFAULT 'manual',
                    started_by TEXT NOT NULL DEFAULT '',
                    ended_by TEXT NOT NULL DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_transcription_sessions_room_started
                ON transcription_sessions(room_slug, started_at DESC);
                """
            )

    def _table_columns(self, connection: sqlite3.Connection, table_name: str) -> set[str]:
        try:
            return {row["name"] for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()}
        except sqlite3.Error:
            return set()

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
            transcription_enabled = bool(row["transcription_enabled"])
            source_requested = bool(row["desired_active"])
            source_ingesting = bool(row["is_ingesting"])
            source_status_label, source_status_tone = _source_status(
                transcription_enabled,
                source_requested,
                source_ingesting,
            )
            rooms.append(
                {
                    "slug": row["slug"],
                    "label": row["label"] or row["slug"],
                    "room_enabled": bool(row["room_enabled"]),
                    "transcription_enabled": transcription_enabled,
                    "host_slug": row["host_slug"] or "",
                    "host_label": row["host_label"] or "No source host",
                    "host_enabled": bool(row["host_enabled"]) if row["host_slug"] else False,
                    "manual_mode": row["manual_mode"] or "",
                    "source_requested": source_requested,
                    "source_ingesting": source_ingesting,
                    "source_status_label": source_status_label,
                    "source_status_tone": source_status_tone,
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
            previous = connection.execute(
                "SELECT label, transcription_enabled FROM rooms WHERE slug = ?",
                (room_slug,),
            ).fetchone()
            was_enabled = bool(previous["transcription_enabled"]) if previous else False
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
            if enabled and not was_enabled:
                self._begin_transcription_session(
                    connection,
                    room_slug,
                    trigger_mode="manual",
                    actor="/transcription/settings",
                    started_at=timestamp,
                )
            elif not enabled and was_enabled:
                self._end_transcription_sessions(
                    connection,
                    room_slug,
                    trigger_mode="manual",
                    actor="/transcription/settings",
                    ended_at=timestamp,
                )
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

    def _begin_transcription_session(
        self,
        connection: sqlite3.Connection,
        room_slug: str,
        *,
        host_slug: str | None = None,
        trigger_mode: str = "manual",
        actor: str = "",
        started_at: str | None = None,
    ) -> int:
        active = connection.execute(
            """
            SELECT id
            FROM transcription_sessions
            WHERE room_slug = ?
              AND trigger_mode = ?
              AND ended_at IS NULL
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (room_slug, trigger_mode),
        ).fetchone()
        if active:
            return int(active["id"])
        cursor = connection.execute(
            """
            INSERT INTO transcription_sessions (
                room_slug, host_slug, started_at, trigger_mode, started_by
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                room_slug,
                host_slug,
                started_at or datetime.now(timezone.utc).isoformat(),
                trigger_mode,
                actor or trigger_mode,
            ),
        )
        return int(cursor.lastrowid)

    def _end_transcription_sessions(
        self,
        connection: sqlite3.Connection,
        room_slug: str,
        *,
        trigger_mode: str | None = None,
        actor: str = "",
        ended_at: str | None = None,
    ) -> int:
        where = ["room_slug = ?", "ended_at IS NULL"]
        where_params: list[str] = [room_slug]
        if trigger_mode:
            where.append("trigger_mode = ?")
            where_params.append(trigger_mode)
        timestamp = ended_at or datetime.now(timezone.utc).isoformat()
        cursor = connection.execute(
            f"""
            UPDATE transcription_sessions
            SET ended_at = ?,
                ended_by = ?
            WHERE {" AND ".join(where)}
            """,
            (timestamp, actor or trigger_mode or "system", *where_params),
        )
        return int(cursor.rowcount or 0)

    def list_transcript_archives(self, visible_room_slugs: tuple[str, ...], *, limit: int = 12):
        if not visible_room_slugs:
            return []
        placeholders = ",".join("?" for _ in visible_room_slugs)
        now = datetime.now(timezone.utc).isoformat()
        archives = []
        with self._connect(readonly=True) as connection:
            meeting_columns = self._table_columns(connection, "meeting_sessions")
            if meeting_columns:
                rows = connection.execute(
                    f"""
                    SELECT meeting_sessions.id,
                           meeting_sessions.room_slug,
                           meeting_sessions.host_slug,
                           meeting_sessions.started_at,
                           meeting_sessions.ended_at,
                           meeting_sessions.trigger_mode,
                           meeting_sessions.started_by,
                           meeting_sessions.ended_by,
                           rooms.label AS room_label
                    FROM meeting_sessions
                    JOIN rooms ON rooms.slug = meeting_sessions.room_slug
                    WHERE meeting_sessions.room_slug IN ({placeholders})
                    ORDER BY meeting_sessions.started_at DESC
                    LIMIT ?
                    """,
                    (*visible_room_slugs, max(1, min(100, int(limit or 12)))),
                ).fetchall()
                archives.extend(
                    self._archive_payload(connection, "meeting", row, now=now)
                    for row in rows
                )

            rows = connection.execute(
                f"""
                SELECT transcription_sessions.id,
                       transcription_sessions.room_slug,
                       transcription_sessions.host_slug,
                       transcription_sessions.started_at,
                       transcription_sessions.ended_at,
                       transcription_sessions.trigger_mode,
                       transcription_sessions.started_by,
                       transcription_sessions.ended_by,
                       rooms.label AS room_label
                FROM transcription_sessions
                JOIN rooms ON rooms.slug = transcription_sessions.room_slug
                WHERE transcription_sessions.room_slug IN ({placeholders})
                ORDER BY transcription_sessions.started_at DESC
                LIMIT ?
                """,
                (*visible_room_slugs, max(1, min(100, int(limit or 12)))),
            ).fetchall()
            archives.extend(
                self._archive_payload(connection, "transcription", row, now=now)
                for row in rows
            )

        archives.sort(key=lambda item: item["started_at"], reverse=True)
        return archives[: max(1, min(100, int(limit or 12)))]

    def get_transcript_archive(self, archive_kind: str, archive_id: int):
        kind = "meeting" if archive_kind == "meeting" else "transcription"
        table = "meeting_sessions" if kind == "meeting" else "transcription_sessions"
        with self._connect(readonly=True) as connection:
            if not self._table_columns(connection, table):
                return None
            row = connection.execute(
                f"""
                SELECT {table}.id,
                       {table}.room_slug,
                       {table}.host_slug,
                       {table}.started_at,
                       {table}.ended_at,
                       {table}.trigger_mode,
                       {table}.started_by,
                       {table}.ended_by,
                       rooms.label AS room_label
                FROM {table}
                JOIN rooms ON rooms.slug = {table}.room_slug
                WHERE {table}.id = ?
                """,
                (max(1, int(archive_id or 0)),),
            ).fetchone()
            if not row:
                return None
            archive = self._archive_payload(connection, kind, row, now=datetime.now(timezone.utc).isoformat())
            archive["transcripts"] = self._transcripts_between(
                connection,
                row["room_slug"],
                started_at=row["started_at"],
                ended_at=row["ended_at"] or datetime.now(timezone.utc).isoformat(),
            )
            return archive

    def _archive_payload(self, connection: sqlite3.Connection, kind: str, row: sqlite3.Row, *, now: str):
        ended_at = row["ended_at"] or now
        stats = connection.execute(
            """
            SELECT COUNT(*) AS segment_count,
                   COALESCE(SUM(LENGTH(text)), 0) AS character_count,
                   MIN(received_at) AS first_received_at,
                   MAX(received_at) AS last_received_at
            FROM transcript_segments
            WHERE room_slug = ?
              AND received_at >= ?
              AND received_at <= ?
            """,
            (row["room_slug"], row["started_at"], ended_at),
        ).fetchone()
        return {
            "kind": kind,
            "id": int(row["id"]),
            "room_slug": row["room_slug"],
            "room_label": row["room_label"],
            "host_slug": row["host_slug"] or "",
            "started_at": row["started_at"],
            "ended_at": row["ended_at"],
            "active": row["ended_at"] is None,
            "trigger_mode": row["trigger_mode"],
            "started_by": row["started_by"],
            "ended_by": row["ended_by"],
            "segment_count": int((stats or {})["segment_count"] or 0),
            "character_count": int((stats or {})["character_count"] or 0),
            "first_received_at": (stats or {})["first_received_at"],
            "last_received_at": (stats or {})["last_received_at"],
            "label": "Meeting" if kind == "meeting" else "Transcription",
        }

    def _transcripts_between(self, connection: sqlite3.Connection, room_slug: str, *, started_at: str, ended_at: str):
        columns = self._table_columns(connection, "transcript_segments")
        host_expr = "host_slug" if "host_slug" in columns else "''"
        provider_expr = "provider" if "provider" in columns else "''"
        model_expr = "model" if "model" in columns else "''"
        started_expr = "started_at" if "started_at" in columns else "received_at"
        ended_expr = "ended_at" if "ended_at" in columns else "received_at"
        rows = connection.execute(
            f"""
            SELECT id,
                   room_slug,
                   {host_expr} AS host_slug,
                   {provider_expr} AS provider,
                   {model_expr} AS model,
                   {started_expr} AS started_at,
                   {ended_expr} AS ended_at,
                   received_at,
                   text,
                   is_final
            FROM transcript_segments
            WHERE room_slug = ?
              AND received_at >= ?
              AND received_at <= ?
            ORDER BY received_at ASC, id ASC
            """,
            (room_slug, started_at, ended_at),
        ).fetchall()
        session_start = _parse_iso_datetime(started_at)
        transcripts = []
        for row in rows:
            received_at = row["received_at"]
            transcripts.append(
                {
                    "id": int(row["id"]),
                    "room_slug": row["room_slug"],
                    "host_slug": row["host_slug"],
                    "provider": row["provider"],
                    "model": row["model"],
                    "started_at": row["started_at"],
                    "ended_at": row["ended_at"],
                    "received_at": received_at,
                    "elapsed": _elapsed_label(session_start, _parse_iso_datetime(received_at)),
                    "text": row["text"],
                    "is_final": bool(row["is_final"]),
                }
            )
        return transcripts

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


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _elapsed_label(start: datetime | None, current: datetime | None) -> str:
    if not start or not current:
        return ""
    seconds = max(0, int((current - start).total_seconds()))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


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
        NTC_BRAND_BACKGROUND_PATH=os.getenv("NTC_BRAND_BACKGROUND_PATH", str(DEFAULT_BRAND_BACKGROUND_PATH)),
        SECRET_KEY=os.getenv("NTC_SECRET_KEY", "change-me"),
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
        return bool(session.get("ntc_transcription_settings"))

    def _require_settings_auth():
        if not _settings_authorized():
            return redirect(url_for("transcription_settings_login", next=request.full_path))
        return None

    def _settings_context(selected_room_slug: str | None = None):
        rooms = transcription_store.list_settings_rooms(_visible_rooms())
        selected_slug = _canonical_room_slug(selected_room_slug) or app.config["NTC_TRANSCRIPTION_DEFAULT_ROOM"]
        selected = next((room for room in rooms if room["slug"] == selected_slug), None) or (rooms[0] if rooms else None)
        return rooms, selected

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

    @app.get(f"/transcription/brand/{BRAND_BACKGROUND_FILENAME}", endpoint="ntc_brand_background")
    def ntc_brand_background():
        path = Path(app.config.get("NTC_BRAND_BACKGROUND_PATH") or DEFAULT_BRAND_BACKGROUND_PATH)
        if not path.exists() or not path.is_file():
            abort(404)
        return send_file(path, mimetype="image/jpeg", conditional=True, max_age=86400)

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

    @app.get("/transcription/settings/login")
    def transcription_settings_login():
        if _settings_authorized():
            return redirect(request.args.get("next") or url_for("transcription_settings"))
        return render_template_string(
            SETTINGS_LOGIN_TEMPLATE,
            title=f"{app.config['NTC_TRANSCRIPTION_TITLE']} Settings",
            error=request.args.get("error"),
            next_url=request.args.get("next") or url_for("transcription_settings"),
            brand_background_url=url_for("ntc_brand_background"),
        )

    @app.post("/transcription/settings/login")
    def transcription_settings_login_post():
        expected = app.config.get("NTC_TRANSCRIPTION_SETTINGS_PASSWORD", "")
        if not expected:
            return redirect(url_for("transcription_settings_login", error="Settings access is not configured yet."))
        if not hmac.compare_digest(request.form.get("password", ""), expected):
            return redirect(
                url_for(
                    "transcription_settings_login",
                    next=request.form.get("next") or url_for("transcription_settings"),
                    error="Password was not accepted.",
                )
            )
        session["ntc_transcription_settings"] = True
        session.modified = True
        return redirect(request.form.get("next") or url_for("transcription_settings"))

    @app.post("/transcription/settings/logout")
    def transcription_settings_logout():
        session.pop("ntc_transcription_settings", None)
        session.modified = True
        return redirect(url_for("transcription_settings_login"))

    @app.get("/transcription/settings")
    def transcription_settings():
        auth_response = _require_settings_auth()
        if auth_response:
            return auth_response
        rooms, selected = _settings_context(request.args.get("room"))
        return render_template_string(
            SETTINGS_TEMPLATE,
            title=f"{app.config['NTC_TRANSCRIPTION_TITLE']} Settings",
            rooms=rooms,
            selected=selected,
            transcript_archives=transcription_store.list_transcript_archives(_visible_rooms(), limit=8),
            language_options=TRANSLATION_LANGUAGE_OPTIONS,
            public_base=url_for("public_transcription"),
            logout_url=url_for("transcription_settings_logout"),
            settings_status_url=url_for("settings_status"),
            poll_ms=app.config["NTC_TRANSCRIPTION_POLL_MS"],
            message=request.args.get("message"),
            error=request.args.get("error"),
            brand_background_url=url_for("ntc_brand_background"),
        )

    @app.get("/api/internal/transcription/settings/status")
    def settings_status():
        if not _settings_authorized():
            return jsonify({"error": "settings auth required"}), 403
        rooms, selected = _settings_context(request.args.get("room"))
        return jsonify(
            {
                "rooms": rooms,
                "selected": selected,
                "selected_slug": selected["slug"] if selected else "",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
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

    @app.get("/transcription/settings/reports/<archive_kind>/<int:archive_id>")
    def transcription_report(archive_kind: str, archive_id: int):
        auth_response = _require_settings_auth()
        if auth_response:
            return auth_response
        archive = transcription_store.get_transcript_archive(archive_kind, archive_id)
        if not archive or archive["room_slug"] not in _visible_rooms():
            return jsonify({"error": "unknown transcript archive"}), 404
        return render_template_string(
            TRANSCRIPTION_REPORT_TEMPLATE,
            title=f"{archive['room_label']} Transcript",
            archive=archive,
            settings_url=url_for("transcription_settings", room=archive["room_slug"]),
            csv_url=url_for("transcription_report_csv", archive_kind=archive["kind"], archive_id=archive["id"]),
            brand_background_url=url_for("ntc_brand_background"),
        )

    @app.get("/transcription/settings/reports/<archive_kind>/<int:archive_id>.csv")
    def transcription_report_csv(archive_kind: str, archive_id: int):
        auth_response = _require_settings_auth()
        if auth_response:
            return auth_response
        archive = transcription_store.get_transcript_archive(archive_kind, archive_id)
        if not archive or archive["room_slug"] not in _visible_rooms():
            return jsonify({"error": "unknown transcript archive"}), 404
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Transcript", f"{archive['label']} #{archive['id']}"])
        writer.writerow(["Room", archive["room_label"]])
        writer.writerow(["Started", archive["started_at"]])
        writer.writerow(["Ended", archive["ended_at"] or "Active"])
        writer.writerow(["Segments", archive["segment_count"]])
        writer.writerow(["Characters", archive["character_count"]])
        writer.writerow([])
        writer.writerow(["Elapsed", "Received", "Start", "End", "Host", "Provider", "Model", "Text"])
        for transcript in archive["transcripts"]:
            writer.writerow(
                [
                    transcript["elapsed"],
                    transcript["received_at"],
                    transcript["started_at"],
                    transcript["ended_at"],
                    transcript["host_slug"],
                    transcript["provider"],
                    transcript["model"],
                    transcript["text"],
                ]
            )
        response = Response(output.getvalue(), mimetype="text/csv")
        response.headers["Content-Disposition"] = (
            f'attachment; filename="ntc-transcript-{archive["kind"]}-{archive["id"]}.csv"'
        )
        return response

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


SETTINGS_LOGIN_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{ title }}</title>
    <style>
      :root {
        --bg: #07121e;
        --panel: rgba(10, 21, 36, 0.94);
        --surface-2: rgba(18, 34, 53, 0.92);
        --text: #edf7ff;
        --muted: #9fb2c6;
        --line: rgba(143, 211, 255, 0.22);
        --line-strong: rgba(143, 211, 255, 0.42);
        --accent: #8fd3ff;
        --good: #74ddb4;
        --warn: #ffb770;
        --shadow: 0 24px 80px rgba(0, 0, 0, 0.38);
      }
      * { box-sizing: border-box; }
      [hidden] { display: none !important; }
      html {
        min-height: 100%;
        background: #050913;
      }
      body {
        margin: 0;
        font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
        color: var(--text);
        background:
          linear-gradient(180deg, rgba(5, 10, 18, 0.50), rgba(5, 10, 18, 0.88)),
          radial-gradient(circle at 12% 0%, rgba(143, 211, 255, 0.18), transparent 30rem),
          radial-gradient(circle at 96% 14%, rgba(116, 221, 180, 0.10), transparent 28rem),
          #050913;
        min-height: 100vh;
        min-height: 100svh;
        display: grid;
        place-items: center;
        padding: 16px;
        overflow-x: hidden;
        position: relative;
        isolation: isolate;
      }
      body::before {
        content: "";
        position: fixed;
        inset: 0;
        z-index: 0;
        pointer-events: none;
        background: url("{{ brand_background_url }}") center / cover no-repeat;
        opacity: 0.31;
        filter: saturate(1.08) contrast(1.04) brightness(0.9);
      }
      main {
        width: min(520px, calc(100vw - 32px));
        margin: 0 auto;
        padding: 0;
        position: relative;
        z-index: 1;
      }
      .shell {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 28px;
        padding: 30px;
        box-shadow: var(--shadow);
        backdrop-filter: blur(18px);
      }
      .eyebrow {
        display: inline-flex;
        width: fit-content;
        border-radius: 999px;
        padding: 0.22rem 0.58rem;
        border: 1px solid var(--line);
        background: rgba(143, 211, 255, 0.07);
        color: var(--accent);
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-size: 0.72rem;
        font-weight: 800;
      }
      h1 {
        margin: 0.98rem 0 0.45rem;
        font-size: clamp(2.25rem, 8vw, 3.55rem);
        line-height: 0.94;
        letter-spacing: 0;
      }
      p {
        margin: 0;
        color: var(--muted);
        line-height: 1.6;
      }
      .banner {
        margin-top: 1rem;
        border-radius: 16px;
        padding: 0.85rem 1rem;
        background: rgba(255, 183, 112, 0.1);
        color: var(--warn);
        font-weight: 700;
      }
      form {
        display: grid;
        gap: 0.85rem;
        margin-top: 1rem;
      }
      label {
        display: grid;
        gap: 0.35rem;
        font-weight: 700;
      }
      input, button { font: inherit; }
      input {
        width: 100%;
        border: 1px solid var(--line);
        border-radius: 16px;
        background: var(--surface-2);
        padding: 0.9rem 0.95rem;
        color: var(--text);
      }
      button {
        appearance: none;
        border: 1px solid rgba(143, 211, 255, 0.28);
        border-radius: 16px;
        padding: 0.9rem 1rem;
        background: rgba(143, 211, 255, 0.16);
        color: var(--text);
        font-weight: 850;
        cursor: pointer;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05);
      }
      button:hover { border-color: var(--line-strong); background: rgba(143, 211, 255, 0.21); }
      input:focus-visible,
      button:focus-visible {
        outline: none;
        border-color: var(--line-strong);
        box-shadow: 0 0 0 3px rgba(143, 211, 255, 0.16), inset 0 1px 0 rgba(255, 255, 255, 0.05);
      }
      @media (max-width: 640px) {
        main { width: min(100%, calc(100vw - 28px)); }
        .shell { border-radius: 24px; padding: 24px; }
      }
    </style>
  </head>
  <body>
    <main>
      <section class="shell">
        <div class="eyebrow">Control Panel</div>
        <h1>Transcription Settings</h1>
        <p>Sign in to manage room transcription and translation output.</p>
        {% if error %}
        <div class="banner">{{ error }}</div>
        {% endif %}
        <form method="post" action="{{ url_for('transcription_settings_login_post') }}">
          <input type="hidden" name="next" value="{{ next_url }}">
          <label>
            Password
            <input type="password" name="password" autocomplete="current-password" autofocus required>
          </label>
          <button type="submit">Open Settings</button>
        </form>
      </section>
    </main>
  </body>
</html>
"""


SETTINGS_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{ title }}</title>
    <style>
      :root {
        --bg: #07121e;
        --surface: rgba(10, 21, 36, 0.92);
        --surface-2: rgba(18, 34, 53, 0.9);
        --surface-3: rgba(6, 13, 24, 0.58);
        --text: #edf7ff;
        --muted: #9fb2c6;
        --line: rgba(143, 211, 255, 0.2);
        --line-strong: rgba(143, 211, 255, 0.36);
        --accent: #8fd3ff;
        --good: #74ddb4;
        --good-soft: rgba(116, 221, 180, 0.12);
        --warn: #ffb770;
        --warn-soft: rgba(255, 183, 112, 0.12);
        --bad: #ff9b9b;
        --bad-soft: rgba(255, 155, 155, 0.12);
        --shadow: 0 22px 70px rgba(0, 0, 0, 0.34);
        --mono: "IBM Plex Mono", "SFMono-Regular", Consolas, monospace;
      }
      * { box-sizing: border-box; }
      [hidden] { display: none !important; }
      html {
        min-height: 100%;
        background: #050913;
      }
      body {
        margin: 0;
        color: var(--text);
        font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
        background:
          linear-gradient(180deg, rgba(5, 10, 18, 0.50), rgba(5, 10, 18, 0.88)),
          radial-gradient(circle at 12% 0%, rgba(143, 211, 255, 0.18), transparent 30rem),
          radial-gradient(circle at 96% 14%, rgba(116, 221, 180, 0.10), transparent 28rem),
          #050913;
        min-height: 100vh;
        overflow-x: hidden;
        position: relative;
        isolation: isolate;
      }
      body::before {
        content: "";
        position: fixed;
        inset: 0;
        z-index: 0;
        pointer-events: none;
        background: url("{{ brand_background_url }}") center / cover no-repeat;
        opacity: 0.31;
        filter: saturate(1.08) contrast(1.04) brightness(0.9);
      }
      main {
        width: min(1320px, calc(100vw - 32px));
        margin: 0 auto;
        padding: 30px 0 44px;
        position: relative;
        z-index: 1;
      }
      .topbar {
        display: grid;
        grid-template-columns: auto 1fr auto;
        gap: 0.85rem;
        align-items: center;
        margin-bottom: 0.9rem;
      }
      .brand {
        display: grid;
        gap: 0.42rem;
        justify-items: center;
        text-align: center;
      }
      h1 {
        margin: 0;
        font-size: clamp(2.2rem, 5.2vw, 4rem);
        line-height: 0.94;
        letter-spacing: 0;
      }
      form { margin: 0; }
      button,
      input,
      select { font: inherit; }
      .eyebrow {
        display: inline-flex;
        width: fit-content;
        border-radius: 999px;
        padding: 0.22rem 0.58rem;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.02);
        color: var(--accent);
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        font-family: var(--mono);
      }
      .sub {
        color: var(--muted);
        margin-top: 0;
        line-height: 1.4;
      }
      .top-actions {
        display: flex;
        gap: 0.65rem;
        flex-wrap: wrap;
        justify-content: flex-end;
        justify-self: end;
      }
      .button {
        appearance: none;
        border: 1px solid var(--line);
        border-radius: 14px;
        background: var(--surface-2);
        color: var(--text);
        padding: 0.74rem 0.92rem;
        text-decoration: none;
        font-weight: 700;
        cursor: pointer;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        white-space: nowrap;
        transition: border-color 140ms ease, background 140ms ease, transform 140ms ease, color 140ms ease;
      }
      .button:hover,
      .button:focus-visible {
        transform: translateY(-1px);
        border-color: var(--line-strong);
      }
      .button.primary {
        background: linear-gradient(135deg, rgba(116, 221, 180, 0.22), rgba(143, 211, 255, 0.13));
        color: #dcfff0;
        border-color: rgba(116, 221, 180, 0.46);
      }
      .button.warning {
        background: var(--warn-soft);
        color: var(--warn);
        border-color: rgba(255, 209, 102, 0.34);
      }
      .tabs {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 0.28rem;
        width: min(620px, 100%);
        margin: 1rem auto 1.4rem;
        padding: 0.28rem;
        border: 1px solid var(--line);
        border-radius: 999px;
        background: rgba(5, 13, 24, 0.58);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
      }
      .tab {
        border: 1px solid transparent;
        border-radius: 999px;
        background: transparent;
        color: var(--text);
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 12px;
        padding: 0.58rem 0.82rem;
        text-decoration: none;
        min-height: 2.65rem;
        transition: border-color 140ms ease, background 140ms ease, transform 140ms ease, color 140ms ease;
      }
      .tab:hover,
      .tab:focus-visible {
        transform: translateY(-1px);
        border-color: var(--line-strong);
      }
      .tab.active {
        border-color: rgba(135, 214, 255, 0.42);
        background: linear-gradient(135deg, rgba(143, 211, 255, 0.18), rgba(143, 245, 200, 0.12));
        box-shadow: 0 10px 26px rgba(8, 19, 33, 0.26), inset 0 0 0 1px rgba(255, 255, 255, 0.04);
      }
      .tab-title {
        min-width: 0;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        font-weight: 850;
        text-align: center;
      }
      .dot {
        width: 0.58rem;
        height: 0.58rem;
        border-radius: 999px;
        background: var(--line);
        flex: 0 0 auto;
      }
      .dot.good { background: var(--good); }
      .dot.warn { background: var(--warn); }
      .notice {
        border-radius: 12px;
        padding: 0.82rem 0.95rem;
        margin-bottom: 1rem;
        background: var(--good-soft);
        border: 1px solid var(--line);
        color: var(--good);
        font-weight: 600;
      }
      .notice.error {
        background: var(--bad-soft);
        color: var(--bad);
      }
      .status-strip {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 0.75rem;
        margin-bottom: 1rem;
      }
      .status-tile,
      .card {
        border: 1px solid var(--line);
        border-radius: 24px;
        background: var(--surface);
        box-shadow: var(--shadow);
        backdrop-filter: blur(18px);
      }
      .status-tile {
        padding: 0.95rem;
        min-height: 82px;
      }
      .label {
        display: block;
        color: var(--muted);
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        font-family: var(--mono);
      }
      .status-value {
        display: block;
        margin-top: 8px;
        font-size: 1.15rem;
        font-weight: 800;
        line-height: 1.1;
        overflow-wrap: anywhere;
      }
      .status-value.source {
        display: inline-flex;
        width: fit-content;
        align-items: center;
        border: 1px solid var(--line);
        border-radius: 999px;
        padding: 0.34rem 0.68rem;
        background: rgba(143, 211, 255, 0.08);
        color: var(--text);
      }
      .status-value.source.good {
        border-color: rgba(116, 221, 180, 0.38);
        background: rgba(116, 221, 180, 0.12);
        color: #d8fff0;
      }
      .status-value.source.warn {
        border-color: rgba(255, 183, 112, 0.38);
        background: rgba(255, 183, 112, 0.12);
        color: var(--warn);
      }
      .layout {
        display: grid;
        grid-template-columns: minmax(0, 1.55fr) minmax(18rem, 1fr);
        gap: 1rem;
        align-items: start;
      }
      .stack {
        display: grid;
        gap: 1rem;
      }
      .card {
        padding: 1rem;
      }
      .card-head {
        display: flex;
        gap: 12px;
        justify-content: space-between;
        align-items: flex-start;
        flex-wrap: wrap;
      }
      .card h2 {
        margin: 0;
        font-size: 1.05rem;
        letter-spacing: 0;
      }
      .card p {
        margin: 0.28rem 0 0;
        color: var(--muted);
        line-height: 1.45;
      }
      .pill {
        display: inline-flex;
        align-items: center;
        min-height: 30px;
        padding: 6px 10px;
        border-radius: 999px;
        background: rgba(18, 34, 53, 0.78);
        color: #d8dde2;
        font-size: 0.82rem;
        font-weight: 800;
        white-space: nowrap;
      }
      .pill.good { background: var(--good-soft); color: var(--good); }
      .pill.warn { background: var(--warn-soft); color: var(--warn); }
      .pill.bad { background: var(--bad-soft); color: var(--bad); }
      .action-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 1rem;
        margin-top: 1rem;
        flex-wrap: wrap;
      }
      .action-row .button { min-width: 220px; }
      .switch-form {
        margin: 0;
      }
      .switch-control {
        appearance: none;
        border: 1px solid var(--line);
        border-radius: 14px;
        background: var(--surface-2);
        color: var(--text);
        min-height: 3.35rem;
        min-width: 10.5rem;
        padding: 0.72rem 0.85rem;
        display: inline-grid;
        grid-template-columns: 0.58rem 6.7rem;
        align-items: center;
        justify-items: start;
        justify-content: center;
        column-gap: 0.52rem;
        cursor: pointer;
        text-align: left;
        font-weight: 800;
        transition: border-color 140ms ease, background 140ms ease, color 140ms ease, transform 140ms ease;
      }
      .switch-control:hover,
      .switch-control:focus-visible {
        border-color: var(--line-strong);
        transform: translateY(-1px);
      }
      .switch-control.is-on {
        border-color: rgba(116, 221, 180, 0.42);
        background: rgba(116, 221, 180, 0.14);
        color: var(--good);
      }
      .switch-control.is-off {
        color: var(--muted);
      }
      .switch-copy {
        display: grid;
        gap: 0.1rem;
        min-width: 0;
        width: 6.7rem;
      }
      .switch-label {
        color: currentColor;
        font-size: 1rem;
        font-weight: 800;
        line-height: 1.1;
        text-transform: uppercase;
      }
      .switch-control.is-on .switch-label {
        color: currentColor;
      }
      .switch-caption {
        color: var(--muted);
        font-size: 0.72rem;
        font-weight: 700;
        line-height: 1.2;
        white-space: nowrap;
      }
      .switch-track {
        position: relative;
        width: 0.58rem;
        height: 0.58rem;
        border-radius: 999px;
        background: currentColor;
        opacity: 0.72;
      }
      .switch-knob {
        display: none;
      }
      .select-row {
        display: grid;
        grid-template-columns: minmax(160px, 1fr) auto;
        gap: 0.65rem;
        margin-top: 0.85rem;
      }
      select {
        min-height: 2.75rem;
        border-radius: 14px;
        border: 1px solid var(--line);
        background: var(--surface-2);
        color: var(--text);
        padding: 0 0.85rem;
        width: 100%;
      }
      .meta-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 0.75rem;
        margin-top: 0.85rem;
      }
      .metric {
        border: 1px solid var(--line);
        border-radius: 18px;
        padding: 0.9rem;
        background: rgba(6, 13, 20, 0.78);
      }
      .metric strong {
        display: block;
        font-size: 30px;
        line-height: 1;
      }
      .metric span {
        color: var(--muted);
        font-size: 13px;
        display: block;
        margin-top: 6px;
      }
      .details {
        display: grid;
        gap: 0.65rem;
        margin: 0.85rem 0 0;
      }
      .detail-row {
        display: grid;
        grid-template-columns: 110px minmax(0, 1fr);
        gap: 0.75rem;
        border-top: 1px solid var(--line);
        padding-top: 0.65rem;
      }
      .detail-row:first-child {
        border-top: 0;
        padding-top: 0;
      }
      .detail-row dt {
        color: var(--muted);
        font-size: 0.78rem;
        font-weight: 800;
      }
      .detail-row dd {
        margin: 0;
        min-width: 0;
        overflow-wrap: anywhere;
      }
      .detail-pill {
        display: inline-flex;
        width: fit-content;
        max-width: 100%;
        align-items: center;
        min-height: 30px;
        border: 1px solid var(--line);
        border-radius: 999px;
        padding: 0.34rem 0.58rem;
        background: rgba(143, 211, 255, 0.08);
        color: var(--text);
        font-weight: 850;
        line-height: 1.2;
        overflow-wrap: anywhere;
        white-space: normal;
      }
      .detail-pill.device {
        border-color: rgba(143, 211, 255, 0.34);
        color: #dff4ff;
      }
      .detail-pill.muted {
        border-color: var(--line);
        background: rgba(18, 34, 53, 0.68);
        color: var(--muted);
      }
      .archive-list {
        display: grid;
        gap: 0.65rem;
        margin-top: 0.85rem;
      }
      .archive-row {
        display: grid;
        grid-template-columns: minmax(0, 1fr) auto;
        gap: 0.75rem;
        align-items: center;
        border: 1px solid var(--line);
        border-radius: 18px;
        padding: 0.82rem;
        background: rgba(6, 13, 20, 0.64);
      }
      .archive-row strong {
        display: block;
        line-height: 1.2;
      }
      .archive-row span {
        display: block;
        margin-top: 0.25rem;
        color: var(--muted);
        font-size: 0.8rem;
        line-height: 1.35;
      }
      .archive-actions {
        display: flex;
        gap: 0.45rem;
        align-items: center;
      }
      .archive-actions .button {
        padding: 0.5rem 0.62rem;
        font-size: 0.78rem;
      }
      .empty {
        border: 1px dashed var(--line);
        border-radius: 20px;
        padding: 1.25rem;
        color: var(--muted);
      }
      @media (max-width: 1120px) {
        .layout {
          grid-template-columns: 1fr;
        }
      }
      @media (max-width: 720px) {
        main { width: min(100vw - 24px, 1120px); padding: 18px 0 2rem; }
        .topbar {
          grid-template-columns: auto minmax(0, 1fr) auto;
          gap: 0.45rem;
        }
        h1 { font-size: clamp(1.25rem, 6.4vw, 2.1rem); }
        .sub { font-size: 0.76rem; line-height: 1.25; }
        .eyebrow { font-size: 0.58rem; padding: 0.17rem 0.42rem; }
        .button { padding: 0.52rem 0.58rem; font-size: 0.78rem; border-radius: 12px; }
        .top-actions { gap: 0.4rem; }
        .status-strip,
        .meta-grid,
        .select-row { grid-template-columns: 1fr; }
        .tabs {
          grid-template-columns: repeat(2, minmax(0, 1fr));
          width: min(100%, 520px);
          margin-inline: auto;
        }
        .tab {
          gap: 0.45rem;
          padding: 0.52rem 0.58rem;
          min-height: 2.45rem;
        }
        .action-row .button { width: 100%; min-width: 0; }
        .select-row .button {
          min-height: 2.75rem;
          font-size: 0.95rem;
          font-weight: 800;
        }
        .switch-form,
        .switch-control { width: 100%; }
        .switch-control {
          grid-template-columns: 0.58rem 5rem;
        }
        .switch-copy {
          width: 5rem;
        }
        .archive-row {
          grid-template-columns: 1fr;
        }
        .archive-actions .button {
          flex: 1;
        }
        .detail-row { grid-template-columns: 1fr; gap: 4px; }
      }
    </style>
  </head>
  <body>
    <main
      id="settings-app"
      data-selected-room="{{ selected.slug if selected else '' }}"
      data-status-url="{{ settings_status_url }}"
      data-poll-ms="{{ poll_ms }}"
    >
      <header class="topbar">
        <a class="button" href="{{ public_base }}">Open Display</a>
        <div class="brand">
          <span class="eyebrow">Control Panel</span>
          <h1>Transcription Settings</h1>
          <div class="sub">Room source, transcription, and translation controls.</div>
        </div>
        <div class="top-actions">
          <form method="post" action="{{ logout_url }}">
            <button class="button" type="submit">Sign Out</button>
          </form>
        </div>
      </header>

      {% if message %}<div class="notice">{{ message }}</div>{% endif %}
      {% if error %}<div class="notice error">{{ error }}</div>{% endif %}

      <nav class="tabs">
        {% for room in rooms %}
          <a class="tab {% if selected and room.slug == selected.slug %}active{% endif %}" href="{{ url_for('transcription_settings', room=room.slug) }}" data-room-tab="{{ room.slug }}">
            <span class="tab-title">{{ room.label }}</span>
            <span class="dot {{ room.source_status_tone }}" data-room-dot></span>
          </a>
        {% endfor %}
      </nav>

      {% if selected %}
      <section class="status-strip" aria-label="Selected room status">
        <div class="status-tile">
          <span class="label">Transcription</span>
          <span class="status-value" data-status-field="transcription">{{ "On" if selected.transcription_enabled else "Off" }}</span>
        </div>
        <div class="status-tile">
          <span class="label">Source</span>
          <span class="status-value source {{ selected.source_status_tone }}" data-status-field="source">{{ selected.source_status_label }}</span>
        </div>
        <div class="status-tile">
          <span class="label">Ingest</span>
          <span class="status-value" data-status-field="ingest">{{ "Running" if selected.source_ingesting else "Stopped" }}</span>
        </div>
        <div class="status-tile">
          <span class="label">Output</span>
          <span class="status-value" data-status-field="output">
            {% if selected.translation_output_supported %}
              {{ "On" if selected.translation_output_enabled else "Off" }}
            {% else %}
              Unavailable
            {% endif %}
          </span>
        </div>
      </section>

      <div class="layout">
        <div class="stack">
          <section class="card">
            <div class="card-head">
              <div>
                <span class="label">Selected Room</span>
                <h2>Source Transcription</h2>
              </div>
            </div>
            <div class="action-row">
              <p data-source-summary>{{ selected.label }} audio is {{ "being transcribed" if selected.transcription_enabled else "not being transcribed" }}.</p>
              <form class="switch-form" method="post" action="{{ url_for('set_room_transcription', room_slug=selected.slug) }}">
                <input type="hidden" name="transcription_enabled" value="{{ "0" if selected.transcription_enabled else "1" }}" data-transcription-enabled-input>
                <button class="switch-control {% if selected.transcription_enabled %}is-on{% else %}is-off{% endif %}" type="submit" aria-label="{{ "Turn transcription off" if selected.transcription_enabled else "Turn transcription on" }}" data-transcription-switch>
                  <span class="switch-track" aria-hidden="true"><span class="switch-knob"></span></span>
                  <span class="switch-copy">
                    <span class="switch-label" data-transcription-switch-label>{{ "ON" if selected.transcription_enabled else "OFF" }}</span>
                    <span class="switch-caption" data-transcription-switch-caption>{{ "Transcribing" if selected.transcription_enabled else "Source idle" }}</span>
                  </span>
                </button>
              </form>
            </div>
          </section>

          <section class="card">
            <div class="card-head">
              <div>
                <span class="label">Mandarin Audio</span>
                <h2>Translation Output</h2>
              </div>
            </div>
            {% if selected.translation_output_supported %}
              <div class="action-row">
                <p data-translation-summary>{{ selected.translation_target_language_label }} output is {{ "armed" if selected.translation_output_enabled else "muted" }}.</p>
                <form class="switch-form" method="post" action="{{ url_for('set_translation_output', room_slug=selected.slug) }}">
                  <input type="hidden" name="translation_output_enabled" value="{{ "0" if selected.translation_output_enabled else "1" }}" data-translation-output-input>
                  <button class="switch-control {% if selected.translation_output_enabled %}is-on{% else %}is-off{% endif %}" type="submit" aria-label="{{ "Turn translation output off" if selected.translation_output_enabled else "Turn translation output on" }}" data-translation-switch>
                    <span class="switch-track" aria-hidden="true"><span class="switch-knob"></span></span>
                    <span class="switch-copy">
                      <span class="switch-label" data-translation-switch-label>{{ "ON" if selected.translation_output_enabled else "OFF" }}</span>
                      <span class="switch-caption" data-translation-switch-caption>{{ "Audio armed" if selected.translation_output_enabled else "Muted" }}</span>
                    </span>
                  </button>
                </form>
              </div>
              <form method="post" action="{{ url_for('set_translation_settings', room_slug=selected.slug) }}" class="select-row">
                <select name="target_language" aria-label="Translation target language">
                  {% for option in language_options %}
                    <option value="{{ option.code }}" {% if selected.translation_target_language == option.code %}selected{% endif %}>{{ option.label }}</option>
                  {% endfor %}
                </select>
                <button class="button" type="submit">Set Language</button>
              </form>
            {% else %}
              <p>{{ selected.label }} does not have translated room output configured.</p>
            {% endif %}
          </section>
        </div>

        <aside class="stack">
          <section class="card">
            <div class="card-head">
              <div>
                <span class="label">Room Agent</span>
                <h2>Source Status</h2>
              </div>
              <span class="pill {{ selected.source_status_tone }}" data-source-pill>{{ selected.source_status_label }}</span>
            </div>
            <dl class="details">
              <div class="detail-row">
                <dt>Host</dt>
                <dd><span class="detail-pill{% if not selected.host_slug %} muted{% endif %}" data-source-detail="host">{{ selected.host_label }}</span></dd>
              </div>
              <div class="detail-row">
                <dt>Device</dt>
                <dd><span class="detail-pill device{% if not selected.current_device %} muted{% endif %}" data-source-detail="device">{{ selected.current_device or "No input selected" }}</span></dd>
              </div>
              <div class="detail-row">
                <dt>Last Seen</dt>
                <dd data-source-detail="last_seen">{{ selected.last_seen_at or "Not seen yet" }}</dd>
              </div>
              <div class="detail-row" data-source-error-row {% if not selected.last_error %}hidden{% endif %}>
                <dt>Error</dt>
                <dd data-source-detail="error">{{ selected.last_error }}</dd>
              </div>
            </dl>
          </section>

          <section class="card">
            <div class="card-head">
              <div>
                <span class="label">Last {{ selected.stats.window_minutes }} Min</span>
                <h2>Recent Transcription Stats</h2>
              </div>
            </div>
            <div class="meta-grid">
              <div class="metric">
                <strong data-stat-field="segments">{{ selected.stats.segment_count }}</strong>
                <span data-stat-field="segments_label">segments in {{ selected.stats.window_minutes }} min</span>
              </div>
              <div class="metric">
                <strong data-stat-field="characters">{{ selected.stats.character_count }}</strong>
                <span>characters</span>
              </div>
            </div>
            <p data-stat-field="latest">{% if selected.stats.last_received_at %}Latest line {{ selected.stats.last_received_at }}{% else %}No recent transcript lines.{% endif %}</p>
          </section>

          <section class="card">
            <div class="card-head">
              <div>
                <span class="label">Transcript Archive</span>
                <h2>Recent Sessions</h2>
              </div>
            </div>
            {% if transcript_archives %}
              <div class="archive-list">
                {% for archive in transcript_archives %}
                  <div class="archive-row">
                    <div>
                      <strong>{{ archive.label }} #{{ archive.id }} &middot; {{ archive.room_label }}</strong>
                      <span>
                        {{ "Active" if archive.active else "Ended" }} &middot;
                        {{ archive.segment_count }} segments &middot;
                        {{ archive.character_count }} chars &middot;
                        {{ archive.started_at }}
                      </span>
                    </div>
                    <div class="archive-actions">
                      <a class="button" href="{{ url_for('transcription_report', archive_kind=archive.kind, archive_id=archive.id) }}">Open</a>
                      <a class="button" href="{{ url_for('transcription_report_csv', archive_kind=archive.kind, archive_id=archive.id) }}">CSV</a>
                    </div>
                  </div>
                {% endfor %}
              </div>
            {% else %}
              <p>No transcript sessions have been archived yet.</p>
            {% endif %}
          </section>
        </aside>
      </div>
      {% else %}
        <section class="empty">No rooms are configured.</section>
      {% endif %}
    </main>
    <script>
      (() => {
        const app = document.getElementById("settings-app");
        if (!app || !app.dataset.statusUrl) return;

        const pollMs = Math.max(750, Number(app.dataset.pollMs || "1000"));
        const selectedRoom = app.dataset.selectedRoom || "";
        const tones = ["good", "warn", "bad"];
        let inFlight = false;

        function setText(selector, value) {
          const element = document.querySelector(selector);
          if (element) element.textContent = value;
        }

        function setDetailPill(selector, value, muted) {
          const element = document.querySelector(selector);
          if (!element) return;
          element.textContent = value;
          element.classList.toggle("muted", !!muted);
        }

        function setTone(element, tone) {
          if (!element) return;
          element.classList.remove(...tones);
          if (tone) element.classList.add(tone);
        }

        function setSwitch(button, enabled) {
          if (!button) return;
          button.classList.toggle("is-on", enabled);
          button.classList.toggle("is-off", !enabled);
        }

        function updateRoomDots(rooms) {
          const roomMap = new Map((rooms || []).map((room) => [room.slug, room]));
          document.querySelectorAll("[data-room-tab]").forEach((tab) => {
            const room = roomMap.get(tab.dataset.roomTab || "");
            if (!room) return;
            setTone(tab.querySelector("[data-room-dot]"), room.source_status_tone || "");
          });
        }

        function updateSelectedRoom(room) {
          if (!room) return;

          setText('[data-status-field="transcription"]', room.transcription_enabled ? "On" : "Off");
          const statusSource = document.querySelector('[data-status-field="source"]');
          if (statusSource) {
            statusSource.textContent = room.source_status_label || "Idle";
            setTone(statusSource, room.source_status_tone || "");
          }
          setText('[data-status-field="ingest"]', room.source_ingesting ? "Running" : "Stopped");
          setText(
            '[data-status-field="output"]',
            room.translation_output_supported ? (room.translation_output_enabled ? "On" : "Off") : "Unavailable"
          );

          const sourcePill = document.querySelector("[data-source-pill]");
          if (sourcePill) {
            sourcePill.textContent = room.source_status_label || "Idle";
            setTone(sourcePill, room.source_status_tone || "");
          }

          setText("[data-source-summary]", `${room.label} audio is ${room.transcription_enabled ? "being transcribed" : "not being transcribed"}.`);
          setDetailPill('[data-source-detail="host"]', room.host_label || "No source host", !room.host_slug);
          setDetailPill('[data-source-detail="device"]', room.current_device || "No input selected", !room.current_device);
          setText('[data-source-detail="last_seen"]', room.last_seen_at || "Not seen yet");
          const errorRow = document.querySelector("[data-source-error-row]");
          const errorText = document.querySelector('[data-source-detail="error"]');
          if (errorRow) errorRow.hidden = !room.last_error;
          if (errorText) errorText.textContent = room.last_error || "";

          const transcriptionInput = document.querySelector("[data-transcription-enabled-input]");
          const transcriptionButton = document.querySelector("[data-transcription-switch]");
          if (transcriptionInput) transcriptionInput.value = room.transcription_enabled ? "0" : "1";
          if (transcriptionButton) {
            setSwitch(transcriptionButton, !!room.transcription_enabled);
            transcriptionButton.setAttribute("aria-label", room.transcription_enabled ? "Turn transcription off" : "Turn transcription on");
          }
          setText("[data-transcription-switch-label]", room.transcription_enabled ? "ON" : "OFF");
          setText("[data-transcription-switch-caption]", room.transcription_enabled ? "Transcribing" : "Source idle");

          const translationInput = document.querySelector("[data-translation-output-input]");
          const translationButton = document.querySelector("[data-translation-switch]");
          if (translationInput) translationInput.value = room.translation_output_enabled ? "0" : "1";
          if (translationButton) {
            setSwitch(translationButton, !!room.translation_output_enabled);
            translationButton.setAttribute("aria-label", room.translation_output_enabled ? "Turn translation output off" : "Turn translation output on");
          }
          setText("[data-translation-switch-label]", room.translation_output_enabled ? "ON" : "OFF");
          setText("[data-translation-switch-caption]", room.translation_output_enabled ? "Audio armed" : "Muted");
          setText(
            "[data-translation-summary]",
            `${room.translation_target_language_label || "Translation"} output is ${room.translation_output_enabled ? "armed" : "muted"}.`
          );

          const stats = room.stats || {};
          setText('[data-stat-field="segments"]', String(stats.segment_count ?? 0));
          setText('[data-stat-field="segments_label"]', `segments in ${stats.window_minutes ?? 180} min`);
          setText('[data-stat-field="characters"]', String(stats.character_count ?? 0));
          setText(
            '[data-stat-field="latest"]',
            stats.last_received_at ? `Latest line ${stats.last_received_at}` : "No recent transcript lines."
          );
        }

        async function refreshStatus() {
          if (inFlight || document.hidden) return;
          inFlight = true;
          try {
            const url = new URL(app.dataset.statusUrl, window.location.origin);
            if (selectedRoom) url.searchParams.set("room", selectedRoom);
            url.searchParams.set("_ts", String(Date.now()));
            const response = await fetch(url.toString(), { cache: "no-store" });
            if (!response.ok) return;
            const payload = await response.json();
            updateRoomDots(payload.rooms || []);
            updateSelectedRoom(payload.selected);
          } catch (error) {
            /* ignore transient status refresh failures */
          } finally {
            inFlight = false;
          }
        }

        window.setInterval(refreshStatus, pollMs);
        document.addEventListener("visibilitychange", () => {
          if (!document.hidden) refreshStatus();
        });
        refreshStatus();
      })();
    </script>
  </body>
</html>
"""


TRANSCRIPTION_REPORT_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{ title }}</title>
    <style>
      :root {
        --bg: #07121e;
        --surface: rgba(10, 21, 36, 0.94);
        --surface-2: rgba(18, 34, 53, 0.9);
        --text: #edf7ff;
        --muted: #9fb2c6;
        --line: rgba(143, 211, 255, 0.22);
        --line-strong: rgba(143, 211, 255, 0.38);
        --accent: #8fd3ff;
        --good: #74ddb4;
        --shadow: 0 22px 70px rgba(0, 0, 0, 0.34);
        --mono: "IBM Plex Mono", "SFMono-Regular", Consolas, monospace;
      }
      * { box-sizing: border-box; }
      html { min-height: 100%; background: #050913; }
      body {
        margin: 0;
        color: var(--text);
        font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
        background:
          linear-gradient(180deg, rgba(5, 10, 18, 0.58), rgba(5, 10, 18, 0.9)),
          #050913;
        min-height: 100vh;
        position: relative;
        isolation: isolate;
      }
      body::before {
        content: "";
        position: fixed;
        inset: 0;
        z-index: 0;
        pointer-events: none;
        background: url("{{ brand_background_url }}") center / cover no-repeat;
        opacity: 0.24;
        filter: saturate(1.08) contrast(1.04) brightness(0.9);
      }
      main {
        width: min(1120px, calc(100vw - 32px));
        margin: 0 auto;
        padding: 30px 0 48px;
        position: relative;
        z-index: 1;
      }
      .topbar {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 1rem;
        margin-bottom: 1rem;
      }
      h1 {
        margin: 0.25rem 0 0;
        font-size: clamp(2rem, 5vw, 3.5rem);
        line-height: 0.96;
      }
      .eyebrow,
      .label {
        color: var(--accent);
        font-family: var(--mono);
        font-size: 0.72rem;
        font-weight: 800;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }
      .sub {
        color: var(--muted);
        margin: 0.5rem 0 0;
      }
      .actions {
        display: flex;
        gap: 0.55rem;
        flex-wrap: wrap;
        justify-content: flex-end;
      }
      .button {
        appearance: none;
        border: 1px solid var(--line);
        border-radius: 14px;
        background: var(--surface-2);
        color: var(--text);
        padding: 0.72rem 0.9rem;
        text-decoration: none;
        font: inherit;
        font-weight: 800;
        cursor: pointer;
      }
      .button:hover,
      .button:focus-visible {
        border-color: var(--line-strong);
      }
      .summary {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 0.7rem;
        margin: 1rem 0;
      }
      .tile,
      .transcript-line {
        border: 1px solid var(--line);
        background: var(--surface);
        box-shadow: var(--shadow);
        backdrop-filter: blur(18px);
      }
      .tile {
        border-radius: 18px;
        padding: 0.9rem;
      }
      .tile strong {
        display: block;
        margin-top: 0.4rem;
        font-size: 1.08rem;
        overflow-wrap: anywhere;
      }
      .transcripts {
        display: grid;
        gap: 0.7rem;
        margin-top: 1rem;
      }
      .transcript-line {
        border-radius: 18px;
        padding: 0.9rem 1rem;
      }
      .line-meta {
        display: flex;
        gap: 0.55rem;
        flex-wrap: wrap;
        color: var(--muted);
        font-family: var(--mono);
        font-size: 0.76rem;
        margin-bottom: 0.45rem;
      }
      .elapsed {
        color: var(--good);
        font-weight: 900;
      }
      .line-text {
        font-size: 1.08rem;
        line-height: 1.48;
      }
      .empty {
        border: 1px dashed var(--line);
        border-radius: 18px;
        padding: 1rem;
        color: var(--muted);
      }
      @media (max-width: 780px) {
        main { width: min(100vw - 24px, 1120px); padding-top: 18px; }
        .topbar { display: grid; }
        .actions { justify-content: stretch; }
        .button { flex: 1; text-align: center; }
        .summary { grid-template-columns: 1fr; }
      }
      @media print {
        body { background: #fff; color: #111; }
        body::before,
        .actions { display: none !important; }
        main { width: 100%; padding: 0; }
        .eyebrow,
        .label,
        .sub,
        .line-meta,
        .elapsed { color: #333; }
        .tile,
        .transcript-line {
          background: #fff;
          border-color: #ccc;
          box-shadow: none;
          break-inside: avoid;
        }
      }
    </style>
  </head>
  <body>
    <main>
      <header class="topbar">
        <div>
          <div class="eyebrow">{{ archive.label }} Transcript</div>
          <h1>{{ archive.room_label }}</h1>
          <p class="sub">{{ archive.started_at }}{% if archive.ended_at %} to {{ archive.ended_at }}{% else %} to Active{% endif %}</p>
        </div>
        <div class="actions">
          <a class="button" href="{{ settings_url }}">Settings</a>
          <a class="button" href="{{ csv_url }}">Download CSV</a>
          <button class="button" type="button" onclick="window.print()">Print</button>
        </div>
      </header>

      <section class="summary" aria-label="Transcript summary">
        <div class="tile">
          <span class="label">Session</span>
          <strong>#{{ archive.id }} {{ archive.label }}</strong>
        </div>
        <div class="tile">
          <span class="label">Status</span>
          <strong>{{ "Active" if archive.active else "Ended" }}</strong>
        </div>
        <div class="tile">
          <span class="label">Segments</span>
          <strong>{{ archive.segment_count }}</strong>
        </div>
        <div class="tile">
          <span class="label">Characters</span>
          <strong>{{ archive.character_count }}</strong>
        </div>
      </section>

      {% if archive.transcripts %}
        <section class="transcripts">
          {% for transcript in archive.transcripts %}
            <article class="transcript-line">
              <div class="line-meta">
                <span class="elapsed">{{ transcript.elapsed }}</span>
                <span>{{ transcript.received_at }}</span>
                {% if transcript.provider %}<span>{{ transcript.provider }}</span>{% endif %}
                {% if transcript.model %}<span>{{ transcript.model }}</span>{% endif %}
              </div>
              <div class="line-text">{{ transcript.text }}</div>
            </article>
          {% endfor %}
        </section>
      {% else %}
        <section class="empty">No transcript lines were saved inside this session window.</section>
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
          const normalized = String(text || "").replace(/\\s+/g, " ").trim();
          return /[A-Za-z0-9\u00c0-\uffff]/.test(normalized) ? normalized : "";
        }

        function sentenceEnded(text) {
          return /[.!?]["')\\]]?$/.test(text.trim());
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
