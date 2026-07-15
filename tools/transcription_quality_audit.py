#!/usr/bin/env python3
"""Audit NTC transcription output and seed a portable correction corpus.

The audit is intentionally model-agnostic. It reads transcript rows from a
SQLite database, flags likely hallucinations/repetition loops, and writes JSONL
records that can be reviewed and corrected by humans. Reviewed records become
portable training/correction material for prompts, second-pass repair, lyric
matching, or later fine-tuning.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


WORD_RE = re.compile(r"[a-z0-9']+", re.IGNORECASE)
KNOWN_LOOP_PHRASES = (
    "thanks for watching",
    "thank you for watching",
    "like and subscribe",
    "amara.org",
    "subtitles by",
)
INTERJECTION_WORDS = {
    "ah",
    "amen",
    "hallelujah",
    "huh",
    "oh",
    "o",
    "thank",
    "thanks",
    "you",
}


@dataclass
class Segment:
    source_id: str
    scope: str
    segment_id: int | None
    chunk_index: int | None
    started_at_seconds: float | None
    ended_at_seconds: float | None
    text: str
    row: dict[str, Any]


@dataclass
class Finding:
    source_id: str
    segment_key: str
    segment_id: int | None
    chunk_index: int | None
    started_at_seconds: float | None
    ended_at_seconds: float | None
    score: int
    severity: str
    category: str
    flags: list[str]
    word_count: int
    unique_ratio: float
    max_token_run: int
    dominant_token: str
    dominant_token_ratio: float
    text: str


def normalize_text(text: str) -> str:
    return " ".join(WORD_RE.findall((text or "").lower()))


def words(text: str) -> list[str]:
    return WORD_RE.findall((text or "").lower())


def max_run(tokens: list[str]) -> int:
    best = 0
    current = 0
    previous = None
    for token in tokens:
        if token == previous:
            current += 1
        else:
            previous = token
            current = 1
        best = max(best, current)
    return best


def repeated_ngram_score(tokens: list[str], size: int) -> int:
    if len(tokens) < size * 4:
        return 0
    ngrams = [" ".join(tokens[index:index + size]) for index in range(0, len(tokens) - size + 1)]
    counts = Counter(ngrams)
    return max(counts.values(), default=0)


def segment_key(source_id: str, chunk_index: int | None, segment_id: int | None, started_at_seconds: float | None) -> str:
    if chunk_index is not None:
        return f"{source_id}:{chunk_index}"
    if segment_id is not None:
        return f"{source_id}:id:{segment_id}"
    return f"{source_id}:t:{started_at_seconds or 0:.3f}"


def table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}


def select_segments(connection: sqlite3.Connection, table: str) -> list[Segment]:
    columns = table_columns(connection, table)
    if not columns:
        raise RuntimeError(f"table does not exist or has no columns: {table}")
    source_expr = "meeting_slug" if "meeting_slug" in columns else "room_slug" if "room_slug" in columns else "''"
    segment_id_expr = "id" if "id" in columns else "NULL"
    chunk_index_expr = "chunk_index" if "chunk_index" in columns else "id" if "id" in columns else "NULL"
    start_expr = "started_at_seconds" if "started_at_seconds" in columns else "NULL"
    end_expr = "ended_at_seconds" if "ended_at_seconds" in columns else "NULL"
    text_expr = "text" if "text" in columns else "''"
    order_expr = []
    if source_expr != "''":
        order_expr.append(source_expr)
    if chunk_index_expr != "NULL":
        order_expr.append(chunk_index_expr)
    elif segment_id_expr != "NULL":
        order_expr.append(segment_id_expr)
    order_sql = ", ".join(order_expr) or "rowid"
    rows = connection.execute(
        f"""
        SELECT
            {source_expr} AS source_id,
            {segment_id_expr} AS segment_id,
            {chunk_index_expr} AS chunk_index,
            {start_expr} AS started_at_seconds,
            {end_expr} AS ended_at_seconds,
            {text_expr} AS text,
            rowid AS sqlite_rowid
        FROM {table}
        ORDER BY {order_sql}
        """
    ).fetchall()
    segments = []
    for row in rows:
        source_id = str(row["source_id"] or "unknown")
        segments.append(
            Segment(
                source_id=source_id,
                scope=table,
                segment_id=int(row["segment_id"]) if row["segment_id"] is not None else None,
                chunk_index=int(row["chunk_index"]) if row["chunk_index"] is not None else None,
                started_at_seconds=float(row["started_at_seconds"]) if row["started_at_seconds"] is not None else None,
                ended_at_seconds=float(row["ended_at_seconds"]) if row["ended_at_seconds"] is not None else None,
                text=str(row["text"] or ""),
                row=dict(row),
            )
        )
    return segments


def score_segment(segment: Segment, duplicate_counts: Counter[str], adjacent_norms: tuple[str, str]) -> Finding:
    text = segment.text.strip()
    token_list = words(text)
    token_count = len(token_list)
    normalized = normalize_text(text)
    token_counts = Counter(token_list)
    dominant_token, dominant_count = token_counts.most_common(1)[0] if token_counts else ("", 0)
    unique_ratio = (len(token_counts) / token_count) if token_count else 0.0
    dominant_ratio = (dominant_count / token_count) if token_count else 0.0
    run = max_run(token_list)

    flags: list[str] = []
    score = 0
    if not text:
        flags.append("blank_text")
        score += 2
    if token_count >= 12 and unique_ratio <= 0.18:
        flags.append("low_word_diversity")
        score += 3
    if token_count >= 8 and dominant_ratio >= 0.70:
        flags.append("dominant_repeated_token")
        score += 4
    if run >= 6:
        flags.append("long_same_token_run")
        score += 4
    if repeated_ngram_score(token_list, 2) >= 6 or repeated_ngram_score(token_list, 3) >= 5:
        flags.append("repeated_ngram_loop")
        score += 3
    if normalized and duplicate_counts[normalized] >= 3:
        flags.append("same_text_repeated_in_scope")
        score += 3
    previous_norm, next_norm = adjacent_norms
    if normalized and (normalized == previous_norm or normalized == next_norm):
        flags.append("adjacent_duplicate_text")
        score += 2
    if token_count >= 8 and sum(1 for token in token_list if token in INTERJECTION_WORDS) / token_count >= 0.85:
        flags.append("interjection_loop_candidate")
        score += 3
    lowered = text.lower()
    if any(phrase in lowered for phrase in KNOWN_LOOP_PHRASES):
        flags.append("known_whisper_loop_phrase")
        score += 4

    severity = "none"
    if score >= 8:
        severity = "high"
    elif score >= 5:
        severity = "medium"
    elif score >= 2:
        severity = "low"

    worship_terms = {"amen", "glory", "god", "hallelujah", "holy", "jesus", "lord", "praise", "worship"}
    worship_ratio = (sum(1 for token in token_list if token in worship_terms) / token_count) if token_count else 0.0
    if "blank_text" in flags:
        category = "blank_or_silent_segment"
    elif "known_whisper_loop_phrase" in flags:
        category = "likely_hallucination"
    elif (
        "dominant_repeated_token" in flags
        and "same_text_repeated_in_scope" in flags
        and dominant_token in INTERJECTION_WORDS
    ):
        category = "likely_repetition_hallucination"
    elif "same_text_repeated_in_scope" in flags and "adjacent_duplicate_text" in flags and worship_ratio >= 0.20:
        category = "song_or_worship_repetition_candidate"
    elif "same_text_repeated_in_scope" in flags and "adjacent_duplicate_text" in flags:
        category = "repeated_transcript_candidate"
    elif "repeated_ngram_loop" in flags and worship_ratio >= 0.20:
        category = "song_or_worship_repetition_candidate"
    else:
        category = "quality_review_candidate"

    return Finding(
        source_id=segment.source_id,
        segment_key=segment_key(segment.source_id, segment.chunk_index, segment.segment_id, segment.started_at_seconds),
        segment_id=segment.segment_id,
        chunk_index=segment.chunk_index,
        started_at_seconds=segment.started_at_seconds,
        ended_at_seconds=segment.ended_at_seconds,
        score=score,
        severity=severity,
        category=category,
        flags=flags,
        word_count=token_count,
        unique_ratio=round(unique_ratio, 3),
        max_token_run=run,
        dominant_token=dominant_token,
        dominant_token_ratio=round(dominant_ratio, 3),
        text=text,
    )


def audit_segments(segments: list[Segment], min_score: int) -> list[Finding]:
    by_source: dict[str, list[Segment]] = defaultdict(list)
    for segment in segments:
        by_source[segment.source_id].append(segment)

    findings: list[Finding] = []
    for source_segments in by_source.values():
        normalized_texts = [normalize_text(segment.text) for segment in source_segments]
        duplicate_counts = Counter(text for text in normalized_texts if text)
        for index, segment in enumerate(source_segments):
            previous_norm = normalized_texts[index - 1] if index > 0 else ""
            next_norm = normalized_texts[index + 1] if index + 1 < len(normalized_texts) else ""
            finding = score_segment(segment, duplicate_counts, (previous_norm, next_norm))
            if finding.score >= min_score:
                findings.append(finding)
    findings.sort(key=lambda item: (item.source_id, item.chunk_index if item.chunk_index is not None else -1, item.segment_id or -1))
    return findings


def correction_record(finding: Finding, *, db_path: Path, corpus_version: str) -> dict[str, Any]:
    stable_id_raw = f"{db_path}:{finding.segment_key}:{finding.started_at_seconds}:{finding.text}"
    stable_id = hashlib.sha256(stable_id_raw.encode("utf-8")).hexdigest()[:24]
    return {
        "schema": "ntc.transcription.correction.v1",
        "corpus_version": corpus_version,
        "id": stable_id,
        "source": {
            "kind": "sqlite_transcript_segment",
            "db_path": str(db_path),
            "source_id": finding.source_id,
            "segment_key": finding.segment_key,
            "segment_id": finding.segment_id,
            "chunk_index": finding.chunk_index,
            "started_at_seconds": finding.started_at_seconds,
            "ended_at_seconds": finding.ended_at_seconds,
        },
        "audit": {
            "score": finding.score,
            "severity": finding.severity,
            "category": finding.category,
            "flags": finding.flags,
            "word_count": finding.word_count,
            "unique_ratio": finding.unique_ratio,
            "max_token_run": finding.max_token_run,
            "dominant_token": finding.dominant_token,
            "dominant_token_ratio": finding.dominant_token_ratio,
        },
        "hypothesis_text": finding.text,
        "corrected_text": "",
        "review": {
            "status": "needs_review",
            "reviewer": "",
            "reviewed_at": "",
            "notes": "",
            "action": "identify_song_correct_or_suppress" if finding.category == "song_or_worship_repetition_candidate" else "correct_or_suppress",
        },
        "domain": {
            "event_type": "",
            "speaker": "",
            "language": "en",
            "tags": [],
        },
    }


def write_outputs(findings: list[Finding], *, output_dir: Path, db_path: Path, corpus_version: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = output_dir / "quality_findings.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as handle:
        for finding in findings:
            handle.write(json.dumps(asdict(finding), ensure_ascii=False) + "\n")

    tsv_path = output_dir / "quality_findings.tsv"
    with tsv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["source_id", "segment_key", "score", "severity", "category", "flags", "time", "text"])
        for finding in findings:
            if finding.started_at_seconds is not None and finding.ended_at_seconds is not None:
                time_label = f"{finding.started_at_seconds:.3f}-{finding.ended_at_seconds:.3f}"
            else:
                time_label = ""
            writer.writerow([finding.source_id, finding.segment_key, finding.score, finding.severity, finding.category, ",".join(finding.flags), time_label, finding.text])

    corpus_path = output_dir / "correction_candidates.jsonl"
    with corpus_path.open("w", encoding="utf-8") as handle:
        for finding in findings:
            handle.write(json.dumps(correction_record(finding, db_path=db_path, corpus_version=corpus_version), ensure_ascii=False) + "\n")

    by_source: dict[str, dict[str, Any]] = defaultdict(lambda: {"findings": 0, "high": 0, "medium": 0, "low": 0})
    by_flag: Counter[str] = Counter()
    by_category: Counter[str] = Counter()
    for finding in findings:
        bucket = by_source[finding.source_id]
        bucket["findings"] += 1
        bucket[finding.severity] = bucket.get(finding.severity, 0) + 1
        by_flag.update(finding.flags)
        by_category[finding.category] += 1
    summary = {
        "finding_count": len(findings),
        "by_source": dict(sorted(by_source.items())),
        "by_flag": dict(by_flag.most_common()),
        "by_category": dict(by_category.most_common()),
        "outputs": {
            "quality_findings_jsonl": str(jsonl_path),
            "quality_findings_tsv": str(tsv_path),
            "correction_candidates_jsonl": str(corpus_path),
        },
    }
    (output_dir / "quality_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit transcription rows for likely hallucination/repetition issues.")
    parser.add_argument("--db", required=True, type=Path, help="SQLite DB containing transcript_segments or replay transcript rows.")
    parser.add_argument("--table", default="transcript_segments", help="Transcript table name.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for JSONL/TSV audit output.")
    parser.add_argument("--min-score", type=int, default=3, help="Minimum score to include as a finding.")
    parser.add_argument("--corpus-version", default="local-review", help="Label written into correction candidate records.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    connection = sqlite3.connect(args.db)
    connection.row_factory = sqlite3.Row
    segments = select_segments(connection, args.table)
    findings = audit_segments(segments, args.min_score)
    write_outputs(findings, output_dir=args.output_dir, db_path=args.db, corpus_version=args.corpus_version)
    print(json.dumps({"segments": len(segments), "findings": len(findings), "output_dir": str(args.output_dir)}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
