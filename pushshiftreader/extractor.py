"""
Canonical whole-subreddit extraction into Parquet-backed research corpora.
"""

import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Pattern, Set

from .models import Comment, Submission
from .reader import ReadProgress, read_zst_records
from .storage import (
    AUTHOR_MONTH_SCHEMA,
    AUTHOR_SUMMARY_SCHEMA,
    COMMENT_FIELD_SPECS,
    COMMENT_SCHEMA,
    SUBMISSION_FIELD_SPECS,
    SUBMISSION_SCHEMA,
    CorpusLayout,
    DATASET_VERSION,
    ParquetRecordWriter,
    read_json,
    read_parquet_records,
    write_json,
    write_parquet_records,
)
from .utils import (
    ArchiveFile,
    discover_archives,
    format_duration,
    format_size,
    iter_archive_pairs,
    sanitize_subreddit_name,
    timestamp_str,
)

logger = logging.getLogger(__name__)


@dataclass
class ExtractionStats:
    subreddit: str
    month: str
    submissions_count: int = 0
    comments_count: int = 0
    authors_count: int = 0
    duration_seconds: float = 0.0

    def __str__(self) -> str:
        return (
            f"{self.subreddit}/{self.month}: "
            f"{self.submissions_count:,} submissions, "
            f"{self.comments_count:,} comments, "
            f"{self.authors_count:,} authors "
            f"({format_duration(self.duration_seconds)})"
        )


@dataclass
class ExtractionResult:
    subreddits: List[str]
    months_processed: int
    total_submissions: int
    total_comments: int
    duration_seconds: float
    stats: List[ExtractionStats] = field(default_factory=list)


def _compile_patterns(patterns: Optional[List[str]]) -> List[Pattern[str]]:
    return [re.compile(pattern, re.IGNORECASE) for pattern in (patterns or [])]


class _ArchiveProgressReporter:
    """Emit periodic archive scan progress without flooding logs."""

    def __init__(self, archive: ArchiveFile, enabled: bool = True):
        self.archive = archive
        self.enabled = enabled
        self._last_percent = -1.0
        self._last_log_time = 0.0
        self._completed = False
        self.matched_records = 0

    def tick_match(self) -> None:
        self.matched_records += 1

    def callback(self, progress: ReadProgress) -> None:
        if not self.enabled or self._completed:
            return

        now = time.time()
        percent = 100.0 if progress.is_final else min(round(progress.percent, 1), 99.9)
        should_log = (
            self._last_percent < 0
            or percent >= self._last_percent + 5.0
            or now - self._last_log_time >= 15.0
            or progress.is_final
        )
        if not should_log:
            return

        logger.info(
            "  %s: %5.1f%% scanned, %s lines, %s matches",
            self.archive.path.name,
            percent,
            f"{progress.lines_read:,}",
            f"{self.matched_records:,}",
        )
        self._last_percent = percent
        self._last_log_time = now
        if progress.is_final:
            self._completed = True


class SubredditExtractor:
    """
    Extract one or more full subreddit corpora into canonical monthly Parquet datasets.
    """

    def __init__(
        self,
        archive_path: Path,
        output_path: Path,
        subreddits: List[str],
        show_progress: bool = True,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        force: bool = False,
        comments_subdir: str = "comments",
        submissions_subdir: str = "submissions",
        workers: int = 1,
        progress_interval: int = 250000,
    ):
        self.archive_path = Path(archive_path)
        self.output_path = Path(output_path)
        self.subreddits = [sub.lower() for sub in subreddits]
        self.subreddit_set: Set[str] = set(self.subreddits)
        self.show_progress = show_progress
        self.force = force
        self.workers = workers
        self.progress_interval = max(1, progress_interval)

        self._include_patterns = _compile_patterns(include_patterns)
        self._exclude_patterns = _compile_patterns(exclude_patterns)

        if not self.archive_path.exists():
            raise ValueError(f"Archive path does not exist: {self.archive_path}")

        self.archives = discover_archives(
            self.archive_path,
            comments_subdir=comments_subdir,
            submissions_subdir=submissions_subdir,
        )
        if not self.archives:
            raise ValueError(f"No archive files found in {self.archive_path}")

        if self.workers != 1:
            logger.warning("Parallel extraction is not implemented yet; running sequentially")

    def _dataset_root(self, subreddit: str) -> Path:
        return self.output_path / sanitize_subreddit_name(subreddit)

    def _layout(self, subreddit: str) -> CorpusLayout:
        return CorpusLayout(self._dataset_root(subreddit))

    def _matches_subreddit(self, record: dict) -> bool:
        return record.get("subreddit", "").lower() in self.subreddit_set

    def _matches_keywords(self, record: dict, record_type: str) -> bool:
        if not self._include_patterns and not self._exclude_patterns:
            return True

        if record_type == "submissions":
            text = f"{record.get('title') or ''} {record.get('selftext') or ''}"
        else:
            text = record.get("body") or ""

        if self._include_patterns and not any(pattern.search(text) for pattern in self._include_patterns):
            return False
        if self._exclude_patterns and any(pattern.search(text) for pattern in self._exclude_patterns):
            return False
        return True

    def _month_done(self, subreddit: str, month: str) -> bool:
        return self._layout(subreddit).month_metadata_path(month).exists()

    def _update_author_stats(self, author_stats: Dict[str, dict], record: dict, record_type: str) -> None:
        author = record.get("author") or "[deleted]"
        score = int(record.get("score") or 0)
        created_utc = int(record.get("created_utc") or 0)

        entry = author_stats.get(author)
        if entry is None:
            entry = {
                "author": author,
                "comment_count": 0,
                "submission_count": 0,
                "comment_score_total": 0,
                "submission_score_total": 0,
                "avg_comment_score": None,
                "avg_submission_score": None,
                "first_seen_utc": created_utc,
                "last_seen_utc": created_utc,
            }
            author_stats[author] = entry

        if record_type == "submissions":
            entry["submission_count"] += 1
            entry["submission_score_total"] += score
            entry["avg_submission_score"] = entry["submission_score_total"] / entry["submission_count"]
        else:
            entry["comment_count"] += 1
            entry["comment_score_total"] += score
            entry["avg_comment_score"] = entry["comment_score_total"] / entry["comment_count"]

        if created_utc < entry["first_seen_utc"]:
            entry["first_seen_utc"] = created_utc
        if created_utc > entry["last_seen_utc"]:
            entry["last_seen_utc"] = created_utc

    def _record_to_canonical(self, record: dict, record_type: str) -> dict:
        if record_type == "submissions":
            return Submission.from_dict(record).to_dict(include_extra=False)
        return Comment.from_dict(record).to_dict(include_extra=False)

    def _aggregate_author_summary(self, subreddit: str) -> None:
        layout = self._layout(subreddit)
        monthly_files = sorted((layout.root / "authors").glob("*.parquet"))
        if not monthly_files:
            return

        summary: Dict[str, dict] = {}
        for file_path in monthly_files:
            for row in read_parquet_records(file_path):
                author = row["author"]
                entry = summary.get(author)
                if entry is None:
                    entry = {
                        "author": author,
                        "comment_count": 0,
                        "submission_count": 0,
                        "comment_score_total": 0,
                        "submission_score_total": 0,
                        "avg_comment_score": None,
                        "avg_submission_score": None,
                        "first_seen_utc": row.get("first_seen_utc") or 0,
                        "last_seen_utc": row.get("last_seen_utc") or 0,
                    }
                    summary[author] = entry

                entry["comment_count"] += int(row.get("comment_count") or 0)
                entry["submission_count"] += int(row.get("submission_count") or 0)
                entry["comment_score_total"] += int(row.get("comment_score_total") or 0)
                entry["submission_score_total"] += int(row.get("submission_score_total") or 0)

                first_seen = int(row.get("first_seen_utc") or 0)
                last_seen = int(row.get("last_seen_utc") or 0)
                if entry["first_seen_utc"] == 0 or (first_seen and first_seen < entry["first_seen_utc"]):
                    entry["first_seen_utc"] = first_seen
                if last_seen > entry["last_seen_utc"]:
                    entry["last_seen_utc"] = last_seen

        for row in summary.values():
            if row["comment_count"]:
                row["avg_comment_score"] = row["comment_score_total"] / row["comment_count"]
            if row["submission_count"]:
                row["avg_submission_score"] = row["submission_score_total"] / row["submission_count"]

        write_parquet_records(
            layout.authors_summary_path,
            sorted(summary.values(), key=lambda item: item["author"]),
            AUTHOR_SUMMARY_SCHEMA,
            [(field.name, field.type) for field in AUTHOR_SUMMARY_SCHEMA],
        )

    def _write_dataset_metadata(self, subreddit: str) -> None:
        layout = self._layout(subreddit)
        month_files = layout.month_metadata_files()
        if not month_files:
            return

        months: List[str] = []
        total_submissions = 0
        total_comments = 0
        total_authors = 0

        for file_path in month_files:
            payload = read_json(file_path)
            months.append(payload["month"])
            total_submissions += payload.get("submissions_count", 0)
            total_comments += payload.get("comments_count", 0)
            total_authors += payload.get("authors_count", 0)

        write_json(
            layout.dataset_metadata_path,
            {
                "dataset_version": DATASET_VERSION,
                "subreddit": subreddit,
                "months": sorted(months),
                "total_submissions": total_submissions,
                "total_comments": total_comments,
                "total_authors_across_months": total_authors,
                "filters": {
                    "include_patterns": [pattern.pattern for pattern in self._include_patterns],
                    "exclude_patterns": [pattern.pattern for pattern in self._exclude_patterns],
                },
                "updated_at": timestamp_str(),
            },
        )

    def _process_archive(
        self,
        archive: ArchiveFile,
        active_subreddits: Set[str],
        stats_by_subreddit: Dict[str, ExtractionStats],
        author_stats_by_subreddit: Dict[str, Dict[str, dict]],
    ) -> None:
        record_type = archive.file_type
        month = archive.month_str
        logger.info("Processing %s (%s)", archive.path.name, format_size(archive.path.stat().st_size))
        progress = _ArchiveProgressReporter(archive, enabled=self.show_progress)

        writers: Dict[str, ParquetRecordWriter] = {}
        try:
            for record in read_zst_records(
                archive.path,
                filter_fn=self._matches_subreddit,
                progress_callback=progress.callback if self.show_progress else None,
                progress_interval=self.progress_interval,
            ):
                subreddit = record.get("subreddit", "").lower()
                if subreddit not in active_subreddits:
                    continue
                if not self._matches_keywords(record, record_type):
                    continue

                canonical = self._record_to_canonical(record, record_type)

                if record_type == "submissions":
                    if subreddit not in writers:
                        writers[subreddit] = ParquetRecordWriter(
                            self._layout(subreddit).submissions_path(month),
                            SUBMISSION_SCHEMA,
                            SUBMISSION_FIELD_SPECS,
                        ).__enter__()
                    writers[subreddit].write(canonical)
                    stats_by_subreddit[subreddit].submissions_count += 1
                else:
                    if subreddit not in writers:
                        writers[subreddit] = ParquetRecordWriter(
                            self._layout(subreddit).comments_path(month),
                            COMMENT_SCHEMA,
                            COMMENT_FIELD_SPECS,
                        ).__enter__()
                    writers[subreddit].write(canonical)
                    stats_by_subreddit[subreddit].comments_count += 1

                progress.tick_match()
                self._update_author_stats(
                    author_stats_by_subreddit[subreddit],
                    record,
                    record_type,
                )
        finally:
            for writer in writers.values():
                writer.__exit__(None, None, None)

    def run(
        self,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
    ) -> ExtractionResult:
        started = time.time()

        archives = self.archives
        if start_month:
            archives = [archive for archive in archives if archive.month_str >= start_month]
        if end_month:
            archives = [archive for archive in archives if archive.month_str <= end_month]

        if not archives:
            logger.warning("No archives match the specified range")
            return ExtractionResult(
                subreddits=self.subreddits,
                months_processed=0,
                total_submissions=0,
                total_comments=0,
                duration_seconds=0.0,
            )

        all_stats: List[ExtractionStats] = []
        total_submissions = 0
        total_comments = 0
        months_processed = 0

        for month, comments_file, submissions_file in iter_archive_pairs(archives):
            active_subreddits = {
                subreddit
                for subreddit in self.subreddits
                if self.force or not self._month_done(subreddit, month)
            }
            if not active_subreddits:
                logger.info("Skipping %s: all target subreddits already processed", month)
                continue

            logger.info("=== Extracting %s ===", month)
            month_started = time.time()
            stats_by_subreddit = {
                subreddit: ExtractionStats(subreddit=subreddit, month=month)
                for subreddit in active_subreddits
            }
            author_stats_by_subreddit = {
                subreddit: {}
                for subreddit in active_subreddits
            }

            if submissions_file is not None:
                self._process_archive(
                    submissions_file,
                    active_subreddits,
                    stats_by_subreddit,
                    author_stats_by_subreddit,
                )

            if comments_file is not None:
                self._process_archive(
                    comments_file,
                    active_subreddits,
                    stats_by_subreddit,
                    author_stats_by_subreddit,
                )

            month_duration = time.time() - month_started
            for subreddit in sorted(active_subreddits):
                stats = stats_by_subreddit[subreddit]
                stats.authors_count = len(author_stats_by_subreddit[subreddit])
                stats.duration_seconds = month_duration
                all_stats.append(stats)
                total_submissions += stats.submissions_count
                total_comments += stats.comments_count

                layout = self._layout(subreddit)
                author_rows = sorted(
                    author_stats_by_subreddit[subreddit].values(),
                    key=lambda row: row["author"],
                )
                write_parquet_records(
                    layout.authors_path(month),
                    author_rows,
                    AUTHOR_MONTH_SCHEMA,
                    [(field.name, field.type) for field in AUTHOR_MONTH_SCHEMA],
                )
                write_json(
                    layout.month_metadata_path(month),
                    {
                        "month": month,
                        "subreddit": subreddit,
                        "dataset_version": DATASET_VERSION,
                        "submissions_count": stats.submissions_count,
                        "comments_count": stats.comments_count,
                        "authors_count": stats.authors_count,
                        "duration_seconds": round(month_duration, 3),
                        "source_archives": {
                            "comments": comments_file.path.name if comments_file else None,
                            "submissions": submissions_file.path.name if submissions_file else None,
                        },
                        "filters": {
                            "include_patterns": [pattern.pattern for pattern in self._include_patterns],
                            "exclude_patterns": [pattern.pattern for pattern in self._exclude_patterns],
                        },
                        "completed_at": timestamp_str(),
                    },
                )
                logger.info("  %s", stats)

            months_processed += 1

        for subreddit in self.subreddits:
            self._aggregate_author_summary(subreddit)
            self._write_dataset_metadata(subreddit)

        duration = time.time() - started
        logger.info(
            "Extraction complete: %s months, %s submissions, %s comments in %s",
            months_processed,
            f"{total_submissions:,}",
            f"{total_comments:,}",
            format_duration(duration),
        )

        return ExtractionResult(
            subreddits=self.subreddits,
            months_processed=months_processed,
            total_submissions=total_submissions,
            total_comments=total_comments,
            duration_seconds=duration,
            stats=all_stats,
        )
