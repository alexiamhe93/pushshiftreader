"""
Dream-focused discovery and export workflows built on canonical Pushshift corpora.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .loader import load_corpus
from .reader import ReadProgress, read_zst_records
from .storage import read_parquet_records, write_json
from .utils import (
    discover_archives,
    ensure_directory,
    format_duration,
    format_size,
    get_months_in_range,
    iter_archive_pairs,
    timestamp_str,
)
from .writers import CsvWriter, JsonlWriter

logger = logging.getLogger(__name__)

_DREAM_PHRASES = [
    ("dreamed", re.compile(r"\b(i|we)\s+(had|have|keep having|was having|dreamed|dreamt)\b", re.IGNORECASE)),
    ("in_my_dream", re.compile(r"\b(in|inside)\s+my\s+dream\b", re.IGNORECASE)),
    ("last_night", re.compile(r"\blast\s+night(?:'s)?\s+dream\b", re.IGNORECASE)),
    ("nightmare", re.compile(r"\b(i|we)\s+had\s+(?:a\s+)?nightmare\b", re.IGNORECASE)),
]
_META_PATTERNS = [
    re.compile(r"\bwhat does (?:my|this) dream mean\b", re.IGNORECASE),
    re.compile(r"\bcan (?:someone|anyone) interpret\b", re.IGNORECASE),
    re.compile(r"\binterpret(?:ation|ing)?\b", re.IGNORECASE),
    re.compile(r"\bdream dictionary\b", re.IGNORECASE),
    re.compile(r"\bmeaning of (?:my|this) dream\b", re.IGNORECASE),
    re.compile(r"\bhelp me understand\b", re.IGNORECASE),
    re.compile(r"\banaly[sz]e (?:my|this) dream\b", re.IGNORECASE),
]
_FIRST_PERSON = re.compile(r"\b(i|me|my|mine|we|our|us)\b", re.IGNORECASE)
_NARRATIVE_MARKERS = re.compile(
    r"\b(was|were|went|saw|felt|tried|started|suddenly|then|next|walking|running|flying|woke)\b",
    re.IGNORECASE,
)
_DREAM_TOKENS = re.compile(r"\b(dream|dreams|dreamed|dreamt|dreaming|nightmare|nightmares)\b", re.IGNORECASE)
_REMOVED_TEXT = {"[deleted]", "[removed]", "deleted", "removed"}


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z0-9']+", text))


def _clean_submission_body(text: str) -> str:
    cleaned = _normalize_space((text or "").replace("&nbsp;", " "))
    return "" if cleaned.lower() in _REMOVED_TEXT else cleaned


def _compose_submission_text(title: str, body: str) -> str:
    pieces = []
    normalized_title = _normalize_space(title)
    normalized_body = _clean_submission_body(body)
    if normalized_title:
        pieces.append(normalized_title)
    if normalized_body:
        pieces.append(normalized_body)
    return "\n\n".join(pieces)


def _iso_timestamp(created_utc: object) -> str:
    try:
        created = int(created_utc or 0)
    except (TypeError, ValueError):
        return ""
    if created <= 0:
        return ""
    return datetime.fromtimestamp(created, tz=timezone.utc).isoformat()


@dataclass
class DreamSubmissionAssessment:
    matches: bool
    is_meta: bool
    word_count: int
    reason: str
    matched_phrase: str = ""


def assess_submission_for_dreams(record: dict, min_words: int = 20) -> DreamSubmissionAssessment:
    title = record.get("title") or ""
    body = _clean_submission_body(record.get("selftext") or "")
    text = _compose_submission_text(title, body)
    words = _word_count(text)
    is_meta = any(pattern.search(text) for pattern in _META_PATTERNS)

    if not body:
        return DreamSubmissionAssessment(False, is_meta, words, "empty_or_removed")

    if not bool(record.get("is_self", True)):
        return DreamSubmissionAssessment(False, is_meta, words, "link_post")

    if is_meta and words < max(min_words, 80):
        return DreamSubmissionAssessment(False, True, words, "meta_request")

    if words < min_words:
        return DreamSubmissionAssessment(False, is_meta, words, "too_short")

    matched_phrase = ""
    for label, pattern in _DREAM_PHRASES:
        if pattern.search(text):
            matched_phrase = label
            break

    has_first_person = bool(_FIRST_PERSON.search(text))
    has_narrative = bool(_NARRATIVE_MARKERS.search(text))
    dream_term_hits = len(_DREAM_TOKENS.findall(text))
    matches = bool(matched_phrase) or (dream_term_hits > 0 and has_first_person and has_narrative)

    if is_meta and not (matches and words >= 80 and has_first_person and has_narrative):
        return DreamSubmissionAssessment(False, True, words, "meta_request", matched_phrase)
    if not matches:
        return DreamSubmissionAssessment(False, is_meta, words, "not_firsthand", matched_phrase)
    return DreamSubmissionAssessment(True, is_meta, words, "matched", matched_phrase)


class _DiscoveryProgressReporter:
    def __init__(self, label: str, enabled: bool = True):
        self.label = label
        self.enabled = enabled
        self._last_percent = -1.0
        self._last_log_time = 0.0

    def callback(self, progress: ReadProgress) -> None:
        if not self.enabled:
            return
        now = time.time()
        percent = round(progress.percent, 1)
        should_log = (
            self._last_percent < 0
            or percent >= self._last_percent + 5.0
            or now - self._last_log_time >= 15.0
            or progress.is_final
        )
        if not should_log:
            return
        logger.info(
            "  %s: %5.1f%% scanned, %s lines",
            self.label,
            percent,
            f"{progress.lines_read:,}",
        )
        self._last_percent = percent
        self._last_log_time = now


class DreamSubredditDiscoverer:
    """
    Scan submission archives and rank subreddits likely to contain firsthand dream reports.
    """

    def __init__(
        self,
        archive_path: Path,
        output_path: Path,
        catalogue_path: Optional[Path] = None,
        show_progress: bool = True,
        progress_interval: int = 250000,
        min_words: int = 20,
        min_submissions: int = 10,
        min_matches: int = 3,
        top_n: int = 25,
    ):
        self.archive_path = Path(archive_path)
        self.output_path = Path(output_path)
        self.catalogue_path = Path(catalogue_path) if catalogue_path else None
        self.show_progress = show_progress
        self.progress_interval = max(1, progress_interval)
        self.min_words = max(1, min_words)
        self.min_submissions = max(1, min_submissions)
        self.min_matches = max(1, min_matches)
        self.top_n = max(1, top_n)

    def _catalogue_rows(self) -> Dict[str, dict]:
        if not self.catalogue_path:
            return {}
        index_path = self.catalogue_path / "subreddit_index.parquet"
        if not index_path.exists():
            raise ValueError(f"Catalogue index not found: {index_path}")
        return {
            (row.get("subreddit") or "").lower(): row
            for row in read_parquet_records(index_path)
            if row.get("subreddit")
        }

    def _candidate_rows(self, buckets: Dict[str, dict], catalogue_rows: Dict[str, dict]) -> List[dict]:
        rows: List[dict] = []
        for subreddit, bucket in buckets.items():
            if bucket["total_submissions"] < self.min_submissions:
                continue
            if bucket["matched_submissions"] < self.min_matches:
                continue

            total = bucket["total_submissions"]
            matched = bucket["matched_submissions"]
            meta = bucket["meta_filtered_submissions"]
            self_posts = bucket["self_posts"]
            avg_words = round(bucket["matched_word_total"] / matched, 2) if matched else 0.0
            matched_ratio = matched / total
            self_post_ratio = self_posts / total
            meta_ratio = meta / total
            score = round((matched_ratio * 100.0) + (self_post_ratio * 10.0) + (min(avg_words, 300.0) / 30.0) - (meta_ratio * 35.0), 4)

            row = {
                "subreddit": bucket["subreddit"],
                "total_submissions": total,
                "self_posts": self_posts,
                "matched_submissions": matched,
                "meta_filtered_submissions": meta,
                "matched_ratio": round(matched_ratio, 6),
                "self_post_ratio": round(self_post_ratio, 6),
                "meta_ratio": round(meta_ratio, 6),
                "avg_matched_words": avg_words,
                "discovery_score": score,
                "first_month": bucket["first_month"],
                "last_month": bucket["last_month"],
                "sample_titles": " | ".join(bucket["sample_titles"]),
            }
            catalogue_row = catalogue_rows.get(subreddit)
            if catalogue_row:
                row["catalogue_total_records"] = int(catalogue_row.get("total_records") or 0)
                row["catalogue_months_active"] = int(catalogue_row.get("months_active") or 0)
                row["catalogue_subscribers"] = int(catalogue_row.get("subreddit_subscribers") or 0)
            else:
                row["catalogue_total_records"] = ""
                row["catalogue_months_active"] = ""
                row["catalogue_subscribers"] = ""
            rows.append(row)

        rows.sort(
            key=lambda item: (
                item["discovery_score"],
                item["matched_submissions"],
                item["total_submissions"],
                item["subreddit"].lower(),
            ),
            reverse=True,
        )
        return rows

    def run(self, start_month: Optional[str] = None, end_month: Optional[str] = None) -> dict:
        started = time.time()
        ensure_directory(self.output_path)
        archives = discover_archives(self.archive_path)
        if not archives:
            raise ValueError(f"No archive files found in {self.archive_path}")

        months = set(get_months_in_range(archives, start_month, end_month))
        catalogue_rows = self._catalogue_rows()
        buckets: Dict[str, dict] = {}
        months_processed = 0

        for month, _comments_file, submissions_file in iter_archive_pairs(archives):
            if month not in months or submissions_file is None:
                continue
            months_processed += 1
            logger.info("Scanning %s (%s)", submissions_file.path.name, format_size(submissions_file.path.stat().st_size))
            reporter = _DiscoveryProgressReporter(submissions_file.path.name, enabled=self.show_progress)
            for record in read_zst_records(
                submissions_file.path,
                progress_callback=reporter.callback if self.show_progress else None,
                progress_interval=self.progress_interval,
            ):
                subreddit = (record.get("subreddit") or "").strip()
                if not subreddit:
                    continue
                key = subreddit.lower()
                bucket = buckets.get(key)
                if bucket is None:
                    bucket = {
                        "subreddit": subreddit,
                        "total_submissions": 0,
                        "self_posts": 0,
                        "matched_submissions": 0,
                        "meta_filtered_submissions": 0,
                        "matched_word_total": 0,
                        "sample_titles": [],
                        "first_month": month,
                        "last_month": month,
                    }
                    buckets[key] = bucket

                bucket["total_submissions"] += 1
                bucket["last_month"] = month
                if bool(record.get("is_self", True)):
                    bucket["self_posts"] += 1

                assessment = assess_submission_for_dreams(record, min_words=self.min_words)
                if assessment.is_meta:
                    bucket["meta_filtered_submissions"] += 1
                if not assessment.matches:
                    continue

                bucket["matched_submissions"] += 1
                bucket["matched_word_total"] += assessment.word_count
                title = _normalize_space(record.get("title") or "")
                if title and title not in bucket["sample_titles"] and len(bucket["sample_titles"]) < 3:
                    bucket["sample_titles"].append(title)

        rows = self._candidate_rows(buckets, catalogue_rows)
        candidates_path = self.output_path / "candidates.csv"
        shortlist_path = self.output_path / "shortlist.txt"
        metadata_path = self.output_path / "metadata.json"

        with CsvWriter(
            candidates_path,
            fields=[
                "subreddit",
                "discovery_score",
                "matched_submissions",
                "total_submissions",
                "matched_ratio",
                "self_posts",
                "self_post_ratio",
                "meta_filtered_submissions",
                "meta_ratio",
                "avg_matched_words",
                "first_month",
                "last_month",
                "catalogue_total_records",
                "catalogue_months_active",
                "catalogue_subscribers",
                "sample_titles",
            ],
            extra_fields=False,
        ) as writer:
            for row in rows:
                writer.write(row)

        shortlist_lines = [
            f"{index + 1}. r/{row['subreddit']} | score={row['discovery_score']} | matched={row['matched_submissions']}/{row['total_submissions']} | samples={row['sample_titles']}"
            for index, row in enumerate(rows[: self.top_n])
        ]
        shortlist_path.write_text("\n".join(shortlist_lines) + ("\n" if shortlist_lines else ""), encoding="utf-8")
        write_json(
            metadata_path,
            {
                "archive_path": str(self.archive_path),
                "catalogue_path": str(self.catalogue_path) if self.catalogue_path else "",
                "start_month": start_month or "",
                "end_month": end_month or "",
                "months_processed": months_processed,
                "candidates_written": len(rows),
                "top_n": self.top_n,
                "min_words": self.min_words,
                "min_submissions": self.min_submissions,
                "min_matches": self.min_matches,
                "completed_at": timestamp_str(),
                "duration_seconds": round(time.time() - started, 2),
            },
        )
        logger.info("Discovery complete: %s candidates in %s", f"{len(rows):,}", format_duration(time.time() - started))
        return {
            "months_processed": months_processed,
            "candidates_written": len(rows),
            "output_path": str(self.output_path),
        }


class DreamCorpusExporter:
    """
    Export extracted subreddit submissions into the flat dream-source schema.
    """

    def __init__(
        self,
        source_path: Path,
        output_path: Path,
        min_words: int = 20,
        include_nsfw: bool = False,
        subreddits: Optional[List[str]] = None,
    ):
        self.source_path = Path(source_path)
        self.output_path = Path(output_path)
        self.min_words = max(1, min_words)
        self.include_nsfw = include_nsfw
        self.subreddits = {item.lower() for item in (subreddits or [])}

    def _dataset_paths(self) -> List[Path]:
        if (self.source_path / "dataset.json").exists():
            return [self.source_path]

        datasets = []
        for candidate in sorted(self.source_path.iterdir()):
            if not candidate.is_dir():
                continue
            if not (candidate / "dataset.json").exists():
                continue
            if self.subreddits and candidate.name.lower() not in self.subreddits:
                continue
            datasets.append(candidate)
        if not datasets:
            raise ValueError(f"No extracted datasets found in {self.source_path}")
        return datasets

    def _normalize_submission(self, record: dict) -> Optional[dict]:
        if record.get("over_18") and not self.include_nsfw:
            return None
        assessment = assess_submission_for_dreams(record, min_words=self.min_words)
        if not assessment.matches:
            return None

        text = _compose_submission_text(record.get("title") or "", record.get("selftext") or "")
        permalink = record.get("permalink") or f"/r/{record.get('subreddit')}/comments/{record.get('id')}/"
        permalink = permalink if str(permalink).startswith("http") else f"https://www.reddit.com{permalink}"

        return {
            "answer_text": text,
            "survey": f"Reddit r/{record.get('subreddit') or 'unknown'}",
            "dream_entry_title": _normalize_space(record.get("title") or ""),
            "date": _iso_timestamp(record.get("created_utc")),
            "word_count": assessment.word_count,
            "source_type": "reddit_submission",
            "source_platform": "reddit",
            "source_subreddit": record.get("subreddit") or "",
            "source_post_id": record.get("id") or "",
            "source_permalink": permalink,
            "source_author": record.get("author") or "",
        }

    def run(self) -> dict:
        ensure_directory(self.output_path.parent)
        rows_written = 0
        datasets_processed = 0
        with JsonlWriter(self.output_path, compress=False) as writer:
            for dataset_path in self._dataset_paths():
                datasets_processed += 1
                dataset = load_corpus(dataset_path)
                logger.info("Exporting dream submissions from r/%s", dataset.subreddit)
                for submission in dataset.all_submissions(as_dict=True):
                    row = self._normalize_submission(submission)
                    if row is None:
                        continue
                    writer.write(row)
                    rows_written += 1
        return {
            "datasets_processed": datasets_processed,
            "rows_written": rows_written,
            "output_path": str(self.output_path),
        }
