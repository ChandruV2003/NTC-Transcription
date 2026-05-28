import sqlite3
import tempfile
import unittest
from pathlib import Path

from ntc_transcription_app import create_app


def _create_test_db(path: Path):
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE rooms (
                slug TEXT PRIMARY KEY,
                label TEXT NOT NULL
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
        connection.executemany(
            "INSERT INTO rooms (slug, label) VALUES (?, ?)",
            [("room-a", "Room A"), ("room-b", "Room B"), ("diagnostics", "Diagnostics")],
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

        response = self.client.get("/transcribe")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Public transcription line.", response.data)
        self.assertIn(b"background: #000", response.data)
        self.assertIn(b"width: 100%;", response.data)
        self.assertIn(b"initial-segments", response.data)
        self.assertIn(b"className = \"block\"", response.data)
        self.assertIn(b"/api/public/transcribe/", response.data)
        self.assertNotIn(b"1260px", response.data)
        self.assertNotIn(b"class=\"line\"", response.data)
        self.assertNotIn(b"Translation Settings", response.data)
        self.assertNotIn(b"Live Caption Ingest", response.data)
        self.assertNotIn(b"Meeting live", response.data)
        self.assertNotIn(b"active_rooms", response.data)

    def test_public_api_returns_segments_after_id(self):
        first_id = _insert_segment(self.db_path, "room-a", "First line.")
        _insert_segment(self.db_path, "room-a", "Second line.")

        response = self.client.get(f"/api/public/transcribe/room-a/segments?after_id={first_id}")

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

        response = self.client.get("/api/public/transcribe/room-a/segments")

        self.assertEqual(response.status_code, 200)
        segment = response.get_json()["segments"][0]
        self.assertEqual(segment["text"], "Visible text.")
        self.assertNotIn("host_slug", segment)
        self.assertNotIn("provider", segment)
        self.assertNotIn("model", segment)

    def test_public_api_limits_stale_cursors_to_recent_window(self):
        for index in range(85):
            _insert_segment(self.db_path, "room-a", f"Public line {index}.")

        response = self.client.get("/api/public/transcribe/room-a/segments?after_id=1")

        self.assertEqual(response.status_code, 200)
        texts = [segment["text"] for segment in response.get_json()["segments"]]
        self.assertEqual(len(texts), 80)
        self.assertEqual(texts[0], "Public line 5.")
        self.assertEqual(texts[-1], "Public line 84.")

    def test_unknown_and_hidden_rooms_return_404(self):
        hidden = self.client.get("/transcribe/diagnostics")
        missing = self.client.get("/transcribe/not-a-room")

        self.assertEqual(hidden.status_code, 404)
        self.assertEqual(missing.status_code, 404)


if __name__ == "__main__":
    unittest.main()
