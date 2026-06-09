import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ntc_transcription_app import create_app


def _create_test_db(path: Path):
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE rooms (
                slug TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                transcription_enabled INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE hosts (
                slug TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                room_slug TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                manual_mode TEXT NOT NULL DEFAULT 'auto',
                translation_output_enabled INTEGER NOT NULL DEFAULT 0,
                translation_target_language TEXT NOT NULL DEFAULT 'zh-CN',
                updated_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE source_runtime (
                host_slug TEXT PRIMARY KEY,
                desired_active INTEGER NOT NULL DEFAULT 0,
                is_ingesting INTEGER NOT NULL DEFAULT 0,
                current_device TEXT NOT NULL DEFAULT '',
                last_seen_at TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT ''
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE room_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_slug TEXT,
                host_slug TEXT,
                component TEXT NOT NULL,
                event_type TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{}',
                occurred_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE transcript_segments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_slug TEXT NOT NULL,
                received_at TEXT NOT NULL,
                text TEXT NOT NULL,
                is_final INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE meeting_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_slug TEXT NOT NULL,
                host_slug TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                trigger_mode TEXT NOT NULL DEFAULT 'system',
                started_by TEXT NOT NULL DEFAULT '',
                ended_by TEXT NOT NULL DEFAULT '',
                transcription_autostarted INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        connection.executemany(
            "INSERT INTO rooms (slug, label, enabled, transcription_enabled, updated_at) VALUES (?, ?, 1, 0, '')",
            [("room-a", "Room A"), ("room-b", "Room B"), ("diagnostics", "Diagnostics")],
        )
        connection.executemany(
            """
            INSERT INTO hosts (
                slug, label, room_slug, enabled, manual_mode, translation_output_enabled,
                translation_target_language, updated_at
            )
            VALUES (?, ?, ?, 1, 'auto', ?, 'zh-CN', '')
            """,
            [
                ("hp-envy-16-ad0xx", "HP Envy", "room-a", 0),
                ("hp-pavilion-14m-ba1xx", "HP Pavilion", "room-b", 0),
            ],
        )
        connection.executemany(
            """
            INSERT INTO source_runtime (
                host_slug, desired_active, is_ingesting, current_device, last_seen_at, last_error
            )
            VALUES (?, ?, ?, ?, ?, '')
            """,
            [
                ("hp-envy-16-ad0xx", 0, 0, "SQ 1&2", "2026-05-31T12:00:00+00:00"),
                ("hp-pavilion-14m-ba1xx", 0, 0, "SQ 3&4", "2026-05-31T12:00:00+00:00"),
            ],
        )


def _insert_segment(path: Path, room_slug: str, text: str, received_at: str = "2026-05-24T20:00:00+00:00") -> int:
    with sqlite3.connect(path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO transcript_segments (room_slug, received_at, text, is_final)
            VALUES (?, ?, ?, 1)
            """,
            (room_slug, received_at, text),
        )
        return int(cursor.lastrowid)


class TranscriptionTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "ntccast.db"
        _create_test_db(self.db_path)
        self.app = create_app(
            {
                "TESTING": True,
                "NTC_DB_PATH": str(self.db_path),
                "NTC_TRANSCRIPTION_VISIBLE_ROOMS": "room-a,room-b",
                "NTC_TRANSCRIPTION_DEFAULT_ROOM": "room-a",
                "NTC_TRANSCRIPTION_SETTINGS_PASSWORD": "settings-password",
                "SECRET_KEY": "test-secret",
            }
        )
        self.client = self.app.test_client()

    def tearDown(self):
        self.tempdir.cleanup()

    def test_healthz(self):
        response = self.client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])

    def test_public_page_is_transcript_only(self):
        _insert_segment(self.db_path, "room-a", "Public transcription line.")

        response = self.client.get("/transcription")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Public transcription line.", response.data)
        self.assertIn(b"background: #000", response.data)
        self.assertIn(b"width: 100%;", response.data)
        self.assertIn(b"initial-segments", response.data)
        self.assertIn(b"className = \"block\"", response.data)
        self.assertIn(b"/api/public/transcription/", response.data)
        self.assertNotIn(b"1260px", response.data)
        self.assertNotIn(b"class=\"line\"", response.data)
        self.assertNotIn(b"Translation Settings", response.data)
        self.assertNotIn(b"Live Caption Ingest", response.data)
        self.assertNotIn(b"Meeting live", response.data)
        self.assertNotIn(b"active_rooms", response.data)

    def _login_settings(self, password: str = "settings-password", *, follow_redirects: bool = True):
        return self.client.post(
            "/transcription/settings/login",
            data={"password": password, "next": "/transcription/settings"},
            follow_redirects=follow_redirects,
        )

    def test_settings_redirects_to_password_login(self):
        response = self.client.get("/transcription/settings")

        self.assertEqual(response.status_code, 302)
        self.assertIn("/transcription/settings/login", response.headers["Location"])

    def test_settings_login_page_uses_password_only(self):
        response = self.client.get("/transcription/settings/login")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Transcription Settings", response.data)
        self.assertIn(b'name="password"', response.data)
        self.assertNotIn(b'name="username"', response.data)
        self.assertIn(b"ntc-embossed-background.jpg", response.data)

    def test_settings_brand_asset_is_served(self):
        response = self.client.get("/transcription/brand/ntc-embossed-background.jpg")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "image/jpeg")
        response.close()

    def test_settings_login_rejects_wrong_password(self):
        response = self._login_settings("wrong-password")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Password was not accepted.", response.data)

    def test_settings_panel_shows_room_controls_and_stats(self):
        _insert_segment(self.db_path, "room-a", "Recent settings transcript.", received_at="2999-01-01T00:00:00+00:00")
        self._login_settings()

        response = self.client.get("/transcription/settings?room=room-a")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Transcription Settings", response.data)
        self.assertIn(b"Source Transcription", response.data)
        self.assertIn(b"Translation Output", response.data)
        self.assertIn(b"Recent Transcription Stats", response.data)
        self.assertIn(b"segments in", response.data)
        self.assertIn(b"Room source, transcription, and translation controls.", response.data)
        self.assertNotIn(b"WebCall stays separate", response.data)
        self.assertIn(b"Sign Out", response.data)
        self.assertIn(b"ntc-embossed-background.jpg", response.data)
        self.assertIn(b"switch-control", response.data)
        self.assertIn(b"detail-pill", response.data)
        self.assertIn(b'data-source-detail="device"', response.data)
        self.assertIn(b"data-status-url", response.data)
        self.assertIn(b"/api/internal/transcription/settings/status", response.data)

    def test_settings_can_toggle_room_transcription(self):
        self._login_settings()
        enabled = self.client.post(
            "/transcription/settings/rooms/room-a/transcription",
            data={"transcription_enabled": "1"},
            follow_redirects=True,
        )

        self.assertEqual(enabled.status_code, 200)
        with sqlite3.connect(self.db_path) as connection:
            self.assertEqual(
                connection.execute("SELECT transcription_enabled FROM rooms WHERE slug = 'room-a'").fetchone()[0],
                1,
            )

        disabled = self.client.post(
            "/transcription/settings/rooms/room-a/transcription",
            data={"transcription_enabled": "0"},
            follow_redirects=True,
        )

        self.assertEqual(disabled.status_code, 200)
        with sqlite3.connect(self.db_path) as connection:
            self.assertEqual(
                connection.execute("SELECT transcription_enabled FROM rooms WHERE slug = 'room-a'").fetchone()[0],
                0,
            )

    def test_manual_transcription_session_has_printable_report_and_csv(self):
        self._login_settings()
        self.client.post(
            "/transcription/settings/rooms/room-a/transcription",
            data={"transcription_enabled": "1"},
            follow_redirects=True,
        )
        received_at = datetime.now(timezone.utc).isoformat()
        _insert_segment(self.db_path, "room-a", "Archive transcript line.", received_at=received_at)
        with sqlite3.connect(self.db_path) as connection:
            session_id = connection.execute(
                """
                SELECT id
                FROM transcription_sessions
                WHERE room_slug = 'room-a'
                  AND ended_at IS NULL
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()[0]

        report = self.client.get(f"/transcription/settings/reports/transcription/{session_id}")
        csv_response = self.client.get(f"/transcription/settings/reports/transcription/{session_id}.csv")

        self.assertEqual(report.status_code, 200)
        self.assertIn(b"Archive transcript line.", report.data)
        self.assertIn(b"Download CSV", report.data)
        self.assertEqual(csv_response.status_code, 200)
        self.assertEqual(csv_response.mimetype, "text/csv")
        self.assertIn(b"Archive transcript line.", csv_response.data)

    def test_settings_marks_source_cleanup_as_stopping(self):
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                UPDATE source_runtime
                SET desired_active = 1, is_ingesting = 0
                WHERE host_slug = 'hp-envy-16-ad0xx'
                """
            )
        self._login_settings()

        response = self.client.get("/transcription/settings?room=room-a")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Stopping", response.data)

    def test_settings_status_api_requires_auth(self):
        response = self.client.get("/api/internal/transcription/settings/status?room=room-a")

        self.assertEqual(response.status_code, 403)

    def test_settings_status_api_reports_live_source_state(self):
        with sqlite3.connect(self.db_path) as connection:
            connection.execute("UPDATE rooms SET transcription_enabled = 1 WHERE slug = 'room-a'")
            connection.execute(
                """
                UPDATE source_runtime
                SET desired_active = 1, is_ingesting = 1
                WHERE host_slug = 'hp-envy-16-ad0xx'
                """
            )
        self._login_settings()

        response = self.client.get("/api/internal/transcription/settings/status?room=room-a")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["selected_slug"], "room-a")
        self.assertEqual(payload["selected"]["source_status_label"], "Live")
        self.assertEqual(payload["selected"]["source_status_tone"], "good")
        self.assertTrue(payload["selected"]["source_ingesting"])

    def test_settings_can_update_supported_translation_controls(self):
        self._login_settings()
        output = self.client.post(
            "/transcription/settings/rooms/room-a/translation-output",
            data={"translation_output_enabled": "1"},
            follow_redirects=True,
        )
        language = self.client.post(
            "/transcription/settings/rooms/room-a/translation-settings",
            data={"target_language": "es"},
            follow_redirects=True,
        )

        self.assertEqual(output.status_code, 200)
        self.assertEqual(language.status_code, 200)
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT translation_output_enabled, translation_target_language
                FROM hosts
                WHERE slug = 'hp-envy-16-ad0xx'
                """
            ).fetchone()
        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], "es")

    def test_public_api_returns_segments_after_id(self):
        first_id = _insert_segment(self.db_path, "room-a", "First line.")
        _insert_segment(self.db_path, "room-a", "Second line.")

        response = self.client.get(f"/api/public/transcription/room-a/segments?after_id={first_id}")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["room_slug"], "room-a")
        self.assertEqual([segment["text"] for segment in payload["segments"]], ["Second line."])

    def test_internal_api_returns_segments_for_translator(self):
        first_id = _insert_segment(self.db_path, "room-a", "First internal line.")
        _insert_segment(self.db_path, "room-a", "Second internal line.")

        response = self.client.get(f"/api/internal/transcription/room-a/segments?after_id={first_id}")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["room_slug"], "room-a")
        self.assertEqual([segment["text"] for segment in payload["segments"]], ["Second internal line."])

    def test_public_api_hides_internal_segment_fields(self):
        _insert_segment(self.db_path, "room-a", "Visible text.")

        response = self.client.get("/api/public/transcription/room-a/segments")

        self.assertEqual(response.status_code, 200)
        segment = response.get_json()["segments"][0]
        self.assertEqual(segment["text"], "Visible text.")
        self.assertNotIn("host_slug", segment)
        self.assertNotIn("provider", segment)
        self.assertNotIn("model", segment)

    def test_public_api_limits_stale_cursors_to_recent_window(self):
        for index in range(85):
            _insert_segment(self.db_path, "room-a", f"Public line {index}.")

        response = self.client.get("/api/public/transcription/room-a/segments?after_id=1")

        self.assertEqual(response.status_code, 200)
        texts = [segment["text"] for segment in response.get_json()["segments"]]
        self.assertEqual(len(texts), 80)
        self.assertEqual(texts[0], "Public line 5.")
        self.assertEqual(texts[-1], "Public line 84.")

    def test_unknown_and_hidden_rooms_return_404(self):
        hidden = self.client.get("/transcription/diagnostics")
        missing = self.client.get("/transcription/not-a-room")

        self.assertEqual(hidden.status_code, 404)
        self.assertEqual(missing.status_code, 404)

    def test_legacy_transcribe_page_redirects_to_transcription(self):
        response = self.client.get("/transcribe/room-a")

        self.assertEqual(response.status_code, 308)
        self.assertEqual(response.headers["Location"], "/transcription/room-a")


if __name__ == "__main__":
    unittest.main()
