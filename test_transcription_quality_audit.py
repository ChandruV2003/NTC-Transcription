import unittest
from collections import Counter

from tools.transcription_quality_audit import Segment, audit_segments, score_segment


class TranscriptionQualityAuditTests(unittest.TestCase):
    def test_repeated_interjection_loop_is_likely_hallucination(self):
        text = " ".join(["Oh"] * 80)
        segment = Segment(
            source_id="sun_morning",
            scope="transcript_segments",
            segment_id=1,
            chunk_index=427,
            started_at_seconds=10248.0,
            ended_at_seconds=10273.0,
            text=text,
            row={},
        )

        finding = score_segment(segment, Counter({text.lower(): 3}), (text.lower(), text.lower()))

        self.assertEqual(finding.category, "likely_repetition_hallucination")
        self.assertIn("dominant_repeated_token", finding.flags)
        self.assertIn("interjection_loop_candidate", finding.flags)

    def test_repeated_worship_text_is_song_candidate(self):
        text = "Holy. You will always be. " * 12
        normalized = " ".join(["holy", "you", "will", "always", "be"] * 12)
        segment = Segment(
            source_id="fri_evening",
            scope="transcript_segments",
            segment_id=2,
            chunk_index=40,
            started_at_seconds=960.0,
            ended_at_seconds=985.0,
            text=text,
            row={},
        )

        finding = score_segment(segment, Counter({normalized: 3}), (normalized, normalized))

        self.assertEqual(finding.category, "song_or_worship_repetition_candidate")
        self.assertIn("repeated_ngram_loop", finding.flags)

    def test_normal_sentence_is_not_reported(self):
        segment = Segment(
            source_id="sun_morning",
            scope="transcript_segments",
            segment_id=3,
            chunk_index=1,
            started_at_seconds=0.0,
            ended_at_seconds=25.0,
            text="The Lord is good and his mercy endures forever.",
            row={},
        )

        findings = audit_segments([segment], min_score=3)

        self.assertEqual(findings, [])


if __name__ == "__main__":
    unittest.main()
