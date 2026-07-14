import sqlite3
import tempfile
import time
import unittest
import math
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
                capture_mode TEXT NOT NULL DEFAULT 'auto',
                capture_sample_rate_hz INTEGER NOT NULL DEFAULT 48000,
                preferred_audio_pattern TEXT NOT NULL DEFAULT '',
                fallback_audio_pattern TEXT NOT NULL DEFAULT '',
                device_order_json TEXT NOT NULL DEFAULT '[]',
                heartbeat_token TEXT NOT NULL DEFAULT '',
                timezone TEXT NOT NULL DEFAULT 'America/New_York',
                notes TEXT NOT NULL DEFAULT '',
                translation_output_enabled INTEGER NOT NULL DEFAULT 0,
                translation_target_language TEXT NOT NULL DEFAULT 'zh-CN',
                updated_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_slug TEXT NOT NULL,
                day TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1
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
                device_list_json TEXT NOT NULL DEFAULT '[]',
                last_seen_at TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                last_error_changed_at TEXT NOT NULL DEFAULT '',
                stream_profile TEXT NOT NULL DEFAULT '',
                stream_channels INTEGER NOT NULL DEFAULT 1,
                sample_rate_hz INTEGER NOT NULL DEFAULT 48000,
                sample_bits INTEGER NOT NULL DEFAULT 0
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
                host_slug TEXT NOT NULL DEFAULT '',
                provider TEXT NOT NULL DEFAULT '',
                model TEXT NOT NULL DEFAULT '',
                started_at TEXT NOT NULL DEFAULT '',
                ended_at TEXT NOT NULL DEFAULT '',
                received_at TEXT NOT NULL,
                text TEXT NOT NULL,
                is_final INTEGER NOT NULL DEFAULT 1,
                source TEXT NOT NULL DEFAULT 'transcriber'
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
            [
                ("room-a", "Room A"),
                ("room-b", "Room B"),
                ("convention-laptop", "Convention Laptop"),
                ("diagnostics", "Diagnostics"),
            ],
        )
        connection.executemany(
            """
            INSERT INTO hosts (
                slug, label, room_slug, enabled, manual_mode, notes, translation_output_enabled,
                translation_target_language, heartbeat_token, updated_at
            )
            VALUES (?, ?, ?, 1, 'auto', ?, ?, 'zh-CN', ?, '')
            """,
            [
                ("ntc-dante-room-a", "NTC-Dante Room A", "room-a", "[source-only] Primary Dante bridge feed.", 0, "dante-a-token"),
                ("hp-envy-16-ad0xx", "HP Envy", "room-a", "", 0, "room-a-token"),
                ("ntc-dante-room-b", "NTC-Dante Room B", "room-b", "[source-only] Primary Dante bridge feed.", 0, "dante-b-token"),
                ("hp-pavilion-14m-ba1xx", "HP Pavilion", "room-b", "", 0, "room-b-token"),
                ("convention-laptop", "Convention Laptop", "convention-laptop", "[source-only] Temporary convention source.", 0, "convention-token"),
                ("iphone15pro", "iPhone 15 Pro", "convention-laptop", "[source-only] Browser microphone capture.", 0, "iphone-token"),
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
                ("ntc-dante-room-a", 0, 0, "", "2026-05-31T12:00:00+00:00"),
                ("hp-envy-16-ad0xx", 0, 0, "SQ 1&2", "2026-05-31T12:00:00+00:00"),
                ("ntc-dante-room-b", 0, 0, "", "2026-05-31T12:00:00+00:00"),
                ("hp-pavilion-14m-ba1xx", 0, 0, "SQ 3&4", "2026-05-31T12:00:00+00:00"),
                ("convention-laptop", 0, 0, "", "2026-05-31T12:00:00+00:00"),
                ("iphone15pro", 0, 0, "", "2026-05-31T12:00:00+00:00"),
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
                "NTC_TRANSCRIPTION_PROVIDER": "local_http",
                "NTC_TRANSCRIPTION_LOCAL_URL": "http://whisper.test/transcribe",
                "NTC_TRANSCRIPTION_SOURCE_PUBLIC_BASE_URL": "https://ntcnas.myftp.org/transcription",
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
        self.assertIn(b'<link rel="icon" href="data:,">', response.data)
        self.assertIn(b"className = \"block\"", response.data)
        self.assertIn(b"word-reveal", response.data)
        self.assertIn(b"word is-new", response.data)
        self.assertIn(b'data-word-delay-ms="64"', response.data)
        self.assertIn(b'data-word-delay-cap-ms="2400"', response.data)
        self.assertIn(b"function appendSegments", response.data)
        self.assertIn(b"animatedIds.add(id)", response.data)
        self.assertIn(b"wordDelayMs", response.data)
        self.assertIn(b"/api/public/transcription/", response.data)
        self.assertNotIn(b"1260px", response.data)
        self.assertNotIn(b"class=\"line\"", response.data)
        self.assertNotIn(b"Translation Settings", response.data)
        self.assertNotIn(b"Live Caption Ingest", response.data)
        self.assertNotIn(b"Meeting live", response.data)
        self.assertNotIn(b"active_rooms", response.data)

    def test_public_convention_alias_renders_convention_room(self):
        self.app.config["NTC_TRANSCRIPTION_VISIBLE_ROOMS"] = "room-a,room-b,convention-laptop"
        _insert_segment(self.db_path, "convention-laptop", "Convention transcription line.")

        response = self.client.get("/transcription/convention")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Convention transcription line.", response.data)
        self.assertIn(b'data-room-slug="convention-laptop"', response.data)

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
        self.assertIn(b"detail-pill active", response.data)
        self.assertIn(b'data-source-detail="device"', response.data)
        self.assertIn(b"data-status-url", response.data)
        self.assertIn(b"/api/internal/transcription/settings/status", response.data)
        self.assertIn(b"Schedule", response.data)
        self.assertIn(b"WebCall rows are imported read-only", response.data)
        self.assertIn(b"Transcription-only starts", response.data)

    def test_room_ab_schedule_imports_webcall_rows_readonly(self):
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO schedules (host_slug, day, start_time, end_time, enabled)
                VALUES ('ntc-dante-room-a', 'WED', '18:50', '21:00', 0)
                """
            )
            connection.execute(
                """
                INSERT INTO schedules (host_slug, day, start_time, end_time, enabled)
                VALUES ('hp-envy-16-ad0xx', 'WED', '19:00', '21:00', 1)
                """
            )
        self._login_settings()

        response = self.client.get("/transcription/settings?room=room-a")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"WebCall schedule", response.data)
        self.assertIn(b"Imported from WebCall. On/off is read-only here.", response.data)
        self.assertIn(b"Wednesday", response.data)
        self.assertIn(b"18:50", response.data)
        self.assertIn(b"disabled", response.data)
        self.assertIn(b"Transcription-only starts", response.data)
        schedule = self.app.transcription_store.list_room_schedule("room-a")
        self.assertEqual(
            [(row["source"], row["day"], row["start_time"], row["enabled"]) for row in schedule["webcall_rows"]],
            [("webcall", "WED", "18:50", False)],
        )

    def test_room_a_extra_schedule_does_not_change_webcall_schedule(self):
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO schedules (host_slug, day, start_time, end_time, enabled)
                VALUES ('ntc-dante-room-a', 'WED', '18:50', '21:00', 0)
                """
            )
        self._login_settings()

        response = self.client.post(
            "/transcription/settings/rooms/room-a/schedule",
            data={
                "schedule_count": "1",
                "schedule_day_0": "FRI",
                "schedule_start_0": "10:00",
                "schedule_end_0": "13:00",
                "schedule_enabled_0": "1",
            },
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Room A transcription-only schedule updated.", response.data)
        with sqlite3.connect(self.db_path) as connection:
            webcall_rows = connection.execute(
                """
                SELECT host_slug, day, start_time, end_time, enabled
                FROM schedules
                WHERE host_slug = 'ntc-dante-room-a'
                """
            ).fetchall()
            transcription_rows = connection.execute(
                """
                SELECT room_slug, host_slug, day, start_time, end_time, enabled
                FROM transcription_schedules
                WHERE room_slug = 'room-a'
                """
            ).fetchall()
        self.assertEqual(webcall_rows, [("ntc-dante-room-a", "WED", "18:50", "21:00", 0)])
        self.assertEqual(transcription_rows, [("room-a", "ntc-dante-room-a", "FRI", "10:00", "13:00", 1)])

    def test_room_a_transcription_only_schedule_autostarts_without_webcall(self):
        with sqlite3.connect(self.db_path) as connection:
            connection.execute("UPDATE rooms SET transcription_enabled = 1 WHERE slug = 'room-b'")
            connection.execute(
                """
                INSERT INTO transcription_schedules (room_slug, host_slug, day, start_time, end_time, enabled)
                VALUES ('room-a', 'ntc-dante-room-a', 'FRI', '10:00', '13:00', 1)
                """
            )

        started = self.app.extensions["ntc_transcription_scheduler_tick"](
            datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc)
        )

        self.assertEqual(len(started), 1)
        self.assertEqual(started[0]["room_slug"], "room-a")
        self.assertEqual(started[0]["schedule_source"], "transcription")
        with sqlite3.connect(self.db_path) as connection:
            rows = dict(
                connection.execute(
                    "SELECT slug, transcription_enabled FROM rooms WHERE slug IN ('room-a', 'room-b')"
                ).fetchall()
            )
        self.assertEqual(rows["room-a"], 1)
        self.assertEqual(rows["room-b"], 0)

    def test_settings_uses_room_abc_tabs_and_preferred_room_source(self):
        self.app.config["NTC_TRANSCRIPTION_VISIBLE_ROOMS"] = "room-a,room-b,convention-laptop"
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                UPDATE source_runtime
                SET desired_active = 0,
                    is_ingesting = 0,
                    current_device = 'CQ 1&2',
                    last_seen_at = '2999-01-01T00:00:00+00:00',
                    last_error = 'fallback should not become primary'
                WHERE host_slug = 'hp-pavilion-14m-ba1xx'
                """
            )
        self._login_settings()

        response = self.client.get("/transcription/api/internal/transcription/settings/status?room=room-b")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual([room["label"] for room in payload["rooms"]], ["Room A", "Room B", "Room C"])
        self.assertEqual(payload["selected"]["host_slug"], "ntc-dante-room-b")
        self.assertEqual(payload["selected"]["host_label"], "NTC-Dante Room B")
        self.assertEqual(payload["selected"]["last_error"], "")

    def test_room_c_schedule_can_be_updated_from_settings(self):
        self.app.config["NTC_TRANSCRIPTION_VISIBLE_ROOMS"] = "room-a,room-b,convention-laptop"
        self.app.config["NTC_TRANSCRIPTION_SCHEDULER_HOSTS"] = "convention-laptop"
        self._login_settings()

        response = self.client.post(
            "/transcription/settings/rooms/convention-laptop/schedule",
            data={
                "schedule_count": "2",
                "schedule_day_0": "THU",
                "schedule_start_0": "10:00",
                "schedule_end_0": "13:00",
                "schedule_enabled_0": "1",
                "schedule_day_1": "THU",
                "schedule_start_1": "19:00",
                "schedule_end_1": "22:00",
                "schedule_enabled_1": "1",
            },
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Room C transcription-only schedule updated.", response.data)
        self.assertIn(b"Room C Timing", response.data)
        self.assertIn(b"Soft End", response.data)
        with sqlite3.connect(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT room_slug, host_slug, day, start_time, end_time, enabled
                FROM transcription_schedules
                WHERE room_slug = 'convention-laptop'
                ORDER BY start_time
                """
            ).fetchall()
        self.assertEqual(
            rows,
            [
                ("convention-laptop", "convention-laptop", "THU", "10:00", "13:00", 1),
                ("convention-laptop", "convention-laptop", "THU", "19:00", "22:00", 1),
            ],
        )

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

    def test_enabling_one_transcription_source_disables_the_other_visible_sources(self):
        with sqlite3.connect(self.db_path) as connection:
            connection.execute("UPDATE rooms SET transcription_enabled = 1 WHERE slug IN ('room-a', 'room-b')")
        self._login_settings()

        response = self.client.post(
            "/transcription/settings/rooms/room-b/transcription",
            data={"transcription_enabled": "1"},
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)
        with sqlite3.connect(self.db_path) as connection:
            rows = dict(
                connection.execute(
                    "SELECT slug, transcription_enabled FROM rooms WHERE slug IN ('room-a', 'room-b', 'convention-laptop')"
                ).fetchall()
            )
        self.assertEqual(rows["room-a"], 0)
        self.assertEqual(rows["room-b"], 1)
        self.assertEqual(rows["convention-laptop"], 0)

    def test_convention_schedule_autostarts_without_soft_end_stop(self):
        self.app.config["NTC_TRANSCRIPTION_VISIBLE_ROOMS"] = "room-a,room-b,convention-laptop"
        self.app.config["NTC_TRANSCRIPTION_SCHEDULER_HOSTS"] = "convention-laptop"
        with sqlite3.connect(self.db_path) as connection:
            connection.execute("UPDATE rooms SET transcription_enabled = 1 WHERE slug = 'room-a'")
            connection.execute(
                """
                INSERT INTO transcription_schedules (room_slug, host_slug, day, start_time, end_time, enabled)
                VALUES ('convention-laptop', 'convention-laptop', 'THU', '19:00', '22:00', 1)
                """
            )

        started = self.app.extensions["ntc_transcription_scheduler_tick"](
            datetime(2026, 7, 9, 23, 0, tzinfo=timezone.utc)
        )
        after_end = self.app.extensions["ntc_transcription_scheduler_tick"](
            datetime(2026, 7, 10, 2, 15, tzinfo=timezone.utc)
        )

        self.assertEqual(len(started), 1)
        self.assertEqual(started[0]["room_slug"], "convention-laptop")
        self.assertEqual(started[0]["scheduled_start_at"], "2026-07-09T23:00:00+00:00")
        self.assertEqual(after_end, [])
        with sqlite3.connect(self.db_path) as connection:
            rows = dict(
                connection.execute(
                    "SELECT slug, transcription_enabled FROM rooms WHERE slug IN ('room-a', 'room-b', 'convention-laptop')"
                ).fetchall()
            )
            session = connection.execute(
                """
                SELECT started_at, trigger_mode, started_by, ended_at
                FROM transcription_sessions
                WHERE room_slug = 'convention-laptop'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        self.assertEqual(rows["room-a"], 0)
        self.assertEqual(rows["room-b"], 0)
        self.assertEqual(rows["convention-laptop"], 1)
        self.assertEqual(
            session,
            ("2026-07-09T23:00:00+00:00", "schedule", "transcription-scheduler", None),
        )

    def test_convention_schedule_creates_boundary_when_already_enabled(self):
        self.app.config["NTC_TRANSCRIPTION_VISIBLE_ROOMS"] = "room-a,room-b,convention-laptop"
        self.app.config["NTC_TRANSCRIPTION_SCHEDULER_HOSTS"] = "convention-laptop"
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                "UPDATE rooms SET transcription_enabled = 1 WHERE slug = 'convention-laptop'"
            )
            connection.execute(
                """
                INSERT INTO transcription_sessions (
                    room_slug, host_slug, started_at, trigger_mode, started_by
                )
                VALUES (
                    'convention-laptop',
                    'convention-laptop',
                    '2026-07-09T20:00:00+00:00',
                    'manual',
                    '/transcription/settings'
                )
                """
            )
            connection.execute(
                """
                INSERT INTO transcription_schedules (room_slug, host_slug, day, start_time, end_time, enabled)
                VALUES ('convention-laptop', 'convention-laptop', 'THU', '19:00', '22:00', 1)
                """
            )

        started = self.app.extensions["ntc_transcription_scheduler_tick"](
            datetime(2026, 7, 9, 23, 0, tzinfo=timezone.utc)
        )
        restarted_app = create_app(
            {
                "TESTING": True,
                "NTC_DB_PATH": str(self.db_path),
                "NTC_TRANSCRIPTION_VISIBLE_ROOMS": "room-a,room-b,convention-laptop",
                "NTC_TRANSCRIPTION_SCHEDULER_HOSTS": "convention-laptop",
                "SECRET_KEY": "test-secret",
            }
        )
        restarted = restarted_app.extensions["ntc_transcription_scheduler_tick"](
            datetime(2026, 7, 9, 23, 5, tzinfo=timezone.utc)
        )

        self.assertEqual(len(started), 1)
        self.assertEqual(len(restarted), 1)
        with sqlite3.connect(self.db_path) as connection:
            enabled = connection.execute(
                "SELECT transcription_enabled FROM rooms WHERE slug = 'convention-laptop'"
            ).fetchone()[0]
            sessions = connection.execute(
                """
                SELECT trigger_mode, started_by, ended_at
                FROM transcription_sessions
                WHERE room_slug = 'convention-laptop'
                ORDER BY id
                """
            ).fetchall()
        self.assertEqual(enabled, 1)
        self.assertEqual(len(sessions), 2)
        self.assertEqual(sessions[0][0], "manual")
        self.assertIsNotNone(sessions[0][2])
        self.assertEqual(sessions[1], ("schedule", "transcription-scheduler", None))

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

    def test_source_heartbeat_is_owned_by_transcription_api(self):
        with sqlite3.connect(self.db_path) as connection:
            connection.execute("UPDATE rooms SET transcription_enabled = 1 WHERE slug = 'room-a'")

        response = self.client.post(
            "/api/source/heartbeat",
            json={
                "host_slug": "hp-envy-16-ad0xx",
                "token": "room-a-token",
                "devices": ["SQ 1&2"],
                "current_device": "SQ 1&2",
                "is_ingesting": False,
                "stream_profile": "wav_pcm24",
                "stream_channels": 2,
                "sample_rate_hz": 48000,
                "sample_bits": 24,
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["project"], "ntc-transcription")
        self.assertTrue(payload["desired_active"])
        self.assertTrue(payload["transcription_desired_active"])
        self.assertFalse(payload["public_desired_active"])
        self.assertEqual(payload["room_slug"], "room-a")
        self.assertIn("https://ntcnas.myftp.org/transcription/api/source/ingest/hp-envy-16-ad0xx", payload["ingest_url"])
        with sqlite3.connect(self.db_path) as connection:
            runtime = connection.execute(
                """
                SELECT desired_active, current_device, stream_channels, sample_rate_hz, sample_bits
                FROM source_runtime
                WHERE host_slug = 'hp-envy-16-ad0xx'
                """
            ).fetchone()
        self.assertEqual(runtime, (1, "SQ 1&2", 2, 48000, 24))

    def test_source_heartbeat_rejects_bad_token(self):
        response = self.client.post(
            "/api/source/heartbeat",
            json={"host_slug": "hp-envy-16-ad0xx", "token": "wrong"},
        )

        self.assertEqual(response.status_code, 403)

    def test_browser_capture_page_renders_iphone_source(self):
        response = self.client.get("/transcription/capture/iphone15pro?token=iphone-token")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Room C Capture", response.data)
        self.assertIn(b"iPhone 15 Pro", response.data)
        self.assertIn(b"/transcription/api/source/browser/start/", response.data)
        self.assertIn(b"getUserMedia", response.data)
        self.assertIn(b"autoGainControl", response.data)
        self.assertIn(b"Start Capture", response.data)
        self.assertIn(b"toggleMute", response.data)
        self.assertIn(b"Mute Mic", response.data)
        self.assertIn(b"[Song]", response.data)
        self.assertIn(b"[Prophecy]", response.data)
        self.assertIn(b"[Announcements]", response.data)
        self.assertIn(b"Custom Marker", response.data)
        self.assertIn(b"insertCustomMarker", response.data)
        self.assertIn(b"Take ToneVision", response.data)
        self.assertIn(b"/transcription/api/source/browser/marker/", response.data)
        self.assertIn(b"/transcription/api/source/browser/tonevision/takeover/", response.data)
        self.assertIn(b"debug-line", response.data)
        self.assertIn(b"isSecureContext", response.data)
        self.assertIn(b"Open this page in Safari", response.data)
        self.assertIn(b'const hostSlug = "iphone15pro";', response.data)
        self.assertIn(b'const token = "iphone-token";', response.data)
        self.assertNotIn(b"&#34;iphone15pro&#34;", response.data)

    def test_tonevision_takeover_clears_and_starts_after_current_transcript(self):
        from tools import tonevision_bridge

        sent_payloads = []

        class FakeToneVisionWebSocket:
            def __init__(self, url, *, timeout):
                self.url = url
                self.timeout = timeout

            def connect(self):
                sent_payloads.append({"type": "connect"})

            def send_json(self, payload):
                sent_payloads.append(dict(payload))

            def close(self):
                sent_payloads.append({"type": "close"})

        original_ws = tonevision_bridge.ToneVisionWebSocket
        original_url = tonevision_bridge.tonevision_ws_url
        tonevision_bridge.ToneVisionWebSocket = FakeToneVisionWebSocket
        tonevision_bridge.tonevision_ws_url = lambda base_url, room_id, pin: "ws://tonevision.test/ws"
        self.app.config["NTC_TONEVISION_POLL_SECONDS"] = "0.05"
        try:
            _insert_segment(self.db_path, "convention-laptop", "Old transcript should not replay")

            response = self.client.post("/api/source/browser/tonevision/takeover/iphone15pro?token=iphone-token", json={})

            self.assertEqual(response.status_code, 200)
            deadline = time.time() + 2
            while time.time() < deadline and not any(payload.get("type") == "text" for payload in sent_payloads):
                time.sleep(0.02)
            text_payloads = [payload["text"] for payload in sent_payloads if payload.get("type") == "text"]
            self.assertEqual(text_payloads, [""])

            _insert_segment(self.db_path, "convention-laptop", "Fresh transcript should appear")
            deadline = time.time() + 2
            while time.time() < deadline:
                text_payloads = [payload["text"] for payload in sent_payloads if payload.get("type") == "text"]
                if any("Fresh transcript should appear" in text for text in text_payloads):
                    break
                time.sleep(0.02)

            self.assertTrue(any("Fresh transcript should appear" in text for text in text_payloads))
            self.assertFalse(any("Old transcript should not replay" in text for text in text_payloads))
        finally:
            tonevision_bridge.ToneVisionWebSocket = original_ws
            tonevision_bridge.tonevision_ws_url = original_url

    def test_browser_capture_start_rejects_bad_token(self):
        response = self.client.post(
            "/api/source/browser/start/iphone15pro?token=wrong",
            json={"sample_rate_hz": 48000},
        )

        self.assertEqual(response.status_code, 403)

    def test_browser_capture_start_and_stop_record_pcm16_runtime(self):
        with sqlite3.connect(self.db_path) as connection:
            connection.execute("UPDATE rooms SET transcription_enabled = 1 WHERE slug = 'convention-laptop'")

        started = self.client.post(
            "/api/source/browser/start/iphone15pro?token=iphone-token",
            json={"sample_rate_hz": 44100, "current_device": "iPhone microphone"},
        )
        chunk = self.client.post(
            "/api/source/browser/chunk/iphone15pro?token=iphone-token&sample_rate_hz=44100",
            data=(b"\x00\x00" * 2048),
            headers={"Content-Type": "application/octet-stream"},
        )
        stopped = self.client.post("/api/source/browser/stop/iphone15pro?token=iphone-token", json={})

        self.assertEqual(started.status_code, 200)
        self.assertEqual(chunk.status_code, 200)
        self.assertEqual(stopped.status_code, 200)
        payload = started.get_json()
        self.assertEqual(payload["stream_profile"], "browser_pcm16")
        self.assertEqual(payload["sample_rate_hz"], 44100)
        with sqlite3.connect(self.db_path) as connection:
            runtime = connection.execute(
                """
                SELECT desired_active, is_ingesting, current_device, stream_profile, stream_channels, sample_rate_hz, sample_bits
                FROM source_runtime
                WHERE host_slug = 'iphone15pro'
                """
            ).fetchone()
        self.assertEqual(runtime, (1, 0, "iPhone microphone", "browser_pcm16", 1, 44100, 16))

    def test_browser_capture_blocks_other_room_c_source_ingest(self):
        with sqlite3.connect(self.db_path) as connection:
            connection.execute("UPDATE rooms SET transcription_enabled = 1 WHERE slug = 'convention-laptop'")

        started = self.client.post(
            "/api/source/browser/start/iphone15pro?token=iphone-token",
            json={"sample_rate_hz": 48000, "current_device": "iPhone microphone"},
        )
        laptop_ingest = self.client.post(
            "/api/source/ingest/convention-laptop?token=convention-token",
            data=b"\x00" * 32,
            headers={"Content-Type": "application/octet-stream"},
        )
        self.client.post("/api/source/browser/stop/iphone15pro?token=iphone-token", json={})

        self.assertEqual(started.status_code, 200)
        self.assertEqual(laptop_ingest.status_code, 409)
        self.assertEqual(laptop_ingest.get_json()["active_host_slug"], "iphone15pro")

    def test_browser_capture_marker_inserts_transcript_segment(self):
        response = self.client.post(
            "/api/source/browser/marker/iphone15pro?token=iphone-token",
            json={"text": "annoucements"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["text"], "[Announcements]")
        self.assertGreater(payload["segment_id"], 0)
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                """
                SELECT room_slug, host_slug, provider, model, text, source
                FROM transcript_segments
                WHERE id = ?
                """,
                (payload["segment_id"],),
            ).fetchone()
        self.assertEqual(row, ("convention-laptop", "iphone15pro", "browser_capture", "marker", "[Announcements]", "manual_marker"))

    def test_local_http_transcription_falls_back_to_second_url(self):
        import ntc_transcription_source

        self.app.config["NTC_TRANSCRIPTION_LOCAL_URLS"] = (
            "http://primary-whisper.test/transcription,"
            "http://backup-whisper.test/transcription"
        )
        self.app.config["NTC_TRANSCRIPTION_LOCAL_URL"] = "http://legacy-whisper.test/transcription"
        with sqlite3.connect(self.db_path) as connection:
            connection.execute("UPDATE rooms SET transcription_enabled = 1 WHERE slug = 'convention-laptop'")

        calls = []

        class FakeResponse:
            def __init__(self, status_code, payload):
                self.status_code = status_code
                self._payload = payload
                self.text = str(payload)

            def json(self):
                return self._payload

        def fake_post(url, **kwargs):
            calls.append(url)
            if url == "http://primary-whisper.test/transcription":
                return FakeResponse(503, {"error": "busy"})
            return FakeResponse(200, {"text": "backup endpoint transcript"})

        original_post = ntc_transcription_source.requests.post
        ntc_transcription_source.requests.post = fake_post
        try:
            started = self.client.post(
                "/api/source/browser/start/iphone15pro?token=iphone-token",
                json={"sample_rate_hz": 16000, "current_device": "Test microphone"},
            )
            pcm16 = b"".join(
                int(10000 * math.sin(2 * math.pi * 440 * sample / 16000)).to_bytes(2, "little", signed=True)
                for sample in range(16000)
            )
            chunk = self.client.post(
                "/api/source/browser/chunk/iphone15pro?token=iphone-token&sample_rate_hz=16000",
                data=pcm16,
                headers={"Content-Type": "application/octet-stream"},
            )
            stopped = self.client.post("/api/source/browser/stop/iphone15pro?token=iphone-token", json={})

            self.assertEqual(started.status_code, 200)
            self.assertEqual(chunk.status_code, 200)
            self.assertEqual(stopped.status_code, 200)
            self.assertEqual(
                calls,
                [
                    "http://primary-whisper.test/transcription",
                    "http://backup-whisper.test/transcription",
                ],
            )
            with sqlite3.connect(self.db_path) as connection:
                row = connection.execute(
                    """
                    SELECT room_slug, host_slug, provider, model, text
                    FROM transcript_segments
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).fetchone()
            self.assertEqual(row, ("convention-laptop", "iphone15pro", "local_http", "local", "backup endpoint transcript"))
        finally:
            ntc_transcription_source.requests.post = original_post

    def test_browser_capture_marker_rejects_bad_token_and_text(self):
        bad_token = self.client.post(
            "/api/source/browser/marker/iphone15pro?token=wrong",
            json={"text": "[Song]"},
        )
        bad_marker = self.client.post(
            "/api/source/browser/marker/iphone15pro?token=iphone-token",
            json={"text": "[]"},
        )

        self.assertEqual(bad_token.status_code, 403)
        self.assertEqual(bad_marker.status_code, 400)

    def test_public_display_treats_bracket_markers_as_separate_blocks(self):
        self.app.config["NTC_TRANSCRIPTION_VISIBLE_ROOMS"] = "room-a,room-b,convention-laptop"
        _insert_segment(self.db_path, "convention-laptop", "Before marker", received_at="2026-05-24T20:00:00+00:00")
        _insert_segment(self.db_path, "convention-laptop", "[Announcements]", received_at="2026-05-24T20:00:01+00:00")
        _insert_segment(self.db_path, "convention-laptop", "After marker", received_at="2026-05-24T20:00:02+00:00")

        response = self.client.get("/transcription/convention-laptop")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"function isMarkerText", response.data)
        self.assertIn(b"block.classList.add(\"is-marker\")", response.data)
        self.assertIn(b"[Announcements]", response.data)

    def test_marker_segments_feed_public_api_and_tonevision_lines(self):
        from tools import tonevision_bridge

        self.app.config["NTC_TRANSCRIPTION_VISIBLE_ROOMS"] = "room-a,room-b,convention-laptop"
        before_id = _insert_segment(self.db_path, "convention-laptop", "Before marker", received_at="2026-05-24T20:00:00+00:00")
        marker_id = _insert_segment(self.db_path, "convention-laptop", "[Announcements]", received_at="2026-05-24T20:00:01+00:00")
        after_id = _insert_segment(self.db_path, "convention-laptop", "After marker", received_at="2026-05-24T20:00:02+00:00")

        response = self.client.get(f"/api/public/transcription/convention-laptop/segments?after_id={before_id - 1}")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        texts = [segment["text"] for segment in payload["segments"]]
        ids = [segment["id"] for segment in payload["segments"]]
        self.assertEqual(ids[-3:], [before_id, marker_id, after_id])
        self.assertEqual(texts[-3:], ["Before marker", "[Announcements]", "After marker"])

        tonevision_text = tonevision_bridge.trim_buffer(texts[-3:], 60000)
        self.assertEqual(tonevision_text, "Before marker\n[Announcements]\nAfter marker")

    def test_settings_collapses_multiple_hosts_per_room(self):
        with sqlite3.connect(self.db_path) as connection:
            connection.execute("UPDATE rooms SET transcription_enabled = 1 WHERE slug = 'room-a'")
            connection.execute(
                """
                UPDATE source_runtime
                SET desired_active = 1,
                    is_ingesting = 1,
                    current_device = 'Dante Room A 1-2',
                    last_seen_at = '2026-06-24T21:00:00+00:00',
                    last_error = ''
                WHERE host_slug = 'ntc-dante-room-a'
                """
            )
        self._login_settings()

        response = self.client.get("/transcription/settings?room=room-a")
        status = self.client.get("/api/internal/transcription/settings/status?room=room-a")
        toggle = self.client.post(
            "/transcription/settings/rooms/room-a/translation-output",
            data={"translation_output_enabled": "1"},
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data.count(b'data-room-tab="room-a"'), 1)
        self.assertEqual(response.data.count(b'data-room-tab="room-b"'), 1)
        self.assertIn(b"NTC-Dante Room A", response.data)
        self.assertIn(b"data-translation-switch", response.data)
        self.assertEqual(status.status_code, 200)
        payload = status.get_json()
        self.assertEqual([room["slug"] for room in payload["rooms"]].count("room-a"), 1)
        self.assertEqual(payload["selected"]["host_slug"], "ntc-dante-room-a")
        self.assertEqual(payload["selected"]["translation_host_slug"], "hp-envy-16-ad0xx")
        self.assertEqual(payload["selected"]["translation_host_label"], "HP Envy")
        self.assertTrue(payload["selected"]["translation_output_supported"])
        self.assertEqual(toggle.status_code, 200)
        with sqlite3.connect(self.db_path) as connection:
            hp_output = connection.execute(
                "SELECT translation_output_enabled FROM hosts WHERE slug = 'hp-envy-16-ad0xx'"
            ).fetchone()[0]
            dante_output = connection.execute(
                "SELECT translation_output_enabled FROM hosts WHERE slug = 'ntc-dante-room-a'"
            ).fetchone()[0]
        self.assertEqual(hp_output, 1)
        self.assertEqual(dante_output, 0)

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
