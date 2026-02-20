"""
Subreddit extraction from Pushshift monthly archives.

Scans archive files and extracts records for specified subreddits,
writing to per-subreddit, per-month intermediate files.
"""

import csv
import json
import logging
import re
import time
from pathlib import Path
from typing import List, Set, Optional, Dict
from dataclasses import dataclass, field

from .reader import ZstReader, read_zst_records, ReadProgress
from .writers import JsonlWriter, CsvWriter, SUBMISSION_CSV_FIELDS, COMMENT_CSV_FIELDS
from .utils import (
    discover_archives, iter_archive_pairs, ensure_directory,
    sanitize_subreddit_name, format_size, format_duration, timestamp_str,
    ArchiveFile
)

logger = logging.getLogger(__name__)

# CSV columns for per-month author stats (written alongside comments/submissions)
_AUTHOR_MONTH_CSV_FIELDS = [
    'author', 'comment_count', 'submission_count',
    'comment_score_total', 'avg_comment_score',
    'submission_score_total', 'avg_submission_score',
    'first_seen_utc', 'last_seen_utc',
]

# CSV columns for subreddit-level aggregated author stats
_AUTHOR_SUBREDDIT_CSV_FIELDS = _AUTHOR_MONTH_CSV_FIELDS + ['months_active']


@dataclass
class ExtractionStats:
    """Statistics for an extraction run."""
    subreddit: str
    month: str
    submissions_count: int = 0
    comments_count: int = 0
    duration_seconds: float = 0.0

    def __str__(self) -> str:
        return (
            f"{self.subreddit}/{self.month}: "
            f"{self.submissions_count:,} submissions, "
            f"{self.comments_count:,} comments "
            f"({format_duration(self.duration_seconds)})"
        )


@dataclass
class ExtractionResult:
    """Result of a complete extraction run."""
    subreddits: List[str]
    months_processed: int
    total_submissions: int
    total_comments: int
    duration_seconds: float
    stats: List[ExtractionStats] = field(default_factory=list)


class SubredditExtractor:
    """
    Extract data for specific subreddits from Pushshift archives.

    Supports resumable extraction (skips months already extracted),
    keyword/regex filtering, and author-level statistics.

    Example:
        extractor = SubredditExtractor(
            archive_path="/data/reddit_dumps",
            output_path="./extracted",
            subreddits=["AskHistorians", "science"],
            include_patterns=["climate change", "global warming"],
            exclude_patterns=["\\[deleted\\]"],
        )
        result = extractor.run()
        print(f"Extracted {result.total_comments} comments")
    """

    def __init__(
        self,
        archive_path: Path,
        output_path: Path,
        subreddits: List[str],
        output_format: str = "both",  # "csv", "jsonl", or "both"
        comments_subdir: str = "comments",
        submissions_subdir: str = "submissions",
        show_progress: bool = True,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        force: bool = False,
    ):
        """
        Initialize the extractor.

        Args:
            archive_path: Path to directory containing comments/ and submissions/
            output_path: Path where extracted data will be written
            subreddits: List of subreddit names to extract (case-insensitive)
            output_format: Output format - "csv", "jsonl", or "both"
            comments_subdir: Name of comments subdirectory in archive
            submissions_subdir: Name of submissions subdirectory in archive
            show_progress: Whether to show progress bars (requires tqdm)
            include_patterns: Keep records whose text matches at least one pattern.
                Applies to title+selftext for submissions and body for comments.
                Patterns are treated as regular expressions (case-insensitive).
                If None or empty, all records pass the include check.
            exclude_patterns: Drop records whose text matches any of these patterns.
                Same field logic as include_patterns.
                If None or empty, no records are dropped.
            force: If True, re-extract even if metadata.json already exists for a
                subreddit/month. Default: False (skip already-completed months).
        """
        self.archive_path = Path(archive_path)
        self.output_path = Path(output_path)
        self.subreddits = [s.lower() for s in subreddits]
        self.subreddit_set: Set[str] = set(self.subreddits)
        self.output_format = output_format
        self.comments_subdir = comments_subdir
        self.submissions_subdir = submissions_subdir
        self.show_progress = show_progress
        self.force = force

        # Compile keyword patterns (case-insensitive)
        self._include_compiled = [
            re.compile(p, re.IGNORECASE) for p in (include_patterns or [])
        ]
        self._exclude_compiled = [
            re.compile(p, re.IGNORECASE) for p in (exclude_patterns or [])
        ]

        # Validate archive path
        if not self.archive_path.exists():
            raise ValueError(f"Archive path does not exist: {self.archive_path}")

        # Discover available archives
        self.archives = discover_archives(
            self.archive_path,
            comments_subdir,
            submissions_subdir
        )

        if not self.archives:
            raise ValueError(f"No archive files found in {self.archive_path}")

        logger.info(f"Found {len(self.archives)} archive files")

    def _matches_subreddit(self, record: dict) -> bool:
        """Check if a record belongs to one of our target subreddits."""
        subreddit = record.get('subreddit', '').lower()
        return subreddit in self.subreddit_set

    def _matches_keywords(self, record: dict, record_type: str) -> bool:
        """
        Check if a record passes the include/exclude keyword filters.

        Searches title+selftext for submissions, body for comments.
        Returns True if the record should be kept.

        - include_patterns: record must match at least one (if any are set)
        - exclude_patterns: record must not match any
        """
        if not self._include_compiled and not self._exclude_compiled:
            return True

        if record_type == 'submissions':
            text = (record.get('title') or '') + ' ' + (record.get('selftext') or '')
        else:
            text = record.get('body') or ''

        # Must match at least one include pattern (if any are specified)
        if self._include_compiled:
            if not any(p.search(text) for p in self._include_compiled):
                return False

        # Must not match any exclude pattern
        if self._exclude_compiled:
            if any(p.search(text) for p in self._exclude_compiled):
                return False

        return True

    def _is_month_done(self, subreddit: str, month: str) -> bool:
        """Check if a subreddit/month has already been extracted."""
        safe_name = sanitize_subreddit_name(subreddit)
        metadata_path = self.output_path / safe_name / month / 'metadata.json'
        return metadata_path.exists()

    def _get_output_dir(self, subreddit: str, month: str) -> Path:
        """Get output directory for a subreddit/month combination."""
        safe_name = sanitize_subreddit_name(subreddit)
        return ensure_directory(self.output_path / safe_name / month)

    def _create_writers(
        self,
        subreddit: str,
        month: str,
        record_type: str
    ) -> Dict[str, any]:
        """Create appropriate writers based on output format."""
        output_dir = self._get_output_dir(subreddit, month)
        writers = {}

        if record_type == 'submissions':
            if self.output_format in ('csv', 'both'):
                writers['csv'] = CsvWriter(
                    output_dir / 'submissions.csv',
                    fields=SUBMISSION_CSV_FIELDS
                )
            if self.output_format in ('jsonl', 'both'):
                writers['jsonl'] = JsonlWriter(
                    output_dir / 'submissions.jsonl.gz'
                )
        else:  # comments
            if self.output_format in ('csv', 'both'):
                writers['csv'] = CsvWriter(
                    output_dir / 'comments.csv',
                    fields=COMMENT_CSV_FIELDS
                )
            if self.output_format in ('jsonl', 'both'):
                writers['jsonl'] = JsonlWriter(
                    output_dir / 'comments.jsonl.gz'
                )

        return writers

    def _process_archive(
        self,
        archive: ArchiveFile,
        stats_by_sub: Dict[str, ExtractionStats],
        author_acc_by_sub: Dict[str, Dict],
        skip_subreddits: Set[str],
    ) -> int:
        """
        Process a single archive file, extracting matching records.

        Returns the total number of records extracted.
        """
        record_type = archive.file_type  # 'comments' or 'submissions'
        month = archive.month_str

        logger.info(f"Processing {archive.path.name} ({format_size(archive.path.stat().st_size)})")

        # Track writers per subreddit
        writers_by_sub: Dict[str, Dict[str, any]] = {}
        total_extracted = 0

        try:
            # Set up progress tracking
            if self.show_progress:
                try:
                    from tqdm import tqdm
                    pbar = tqdm(
                        total=archive.path.stat().st_size,
                        unit='B',
                        unit_scale=True,
                        desc=f"  {archive.path.name}"
                    )
                except ImportError:
                    pbar = None
            else:
                pbar = None

            last_bytes = 0

            def progress_callback(progress: ReadProgress):
                nonlocal last_bytes
                if pbar and progress.bytes_read > last_bytes:
                    pbar.update(progress.bytes_read - last_bytes)
                    last_bytes = progress.bytes_read

            # Stream through the archive
            for record in read_zst_records(
                archive.path,
                filter_fn=self._matches_subreddit,
                progress_callback=progress_callback if pbar else None,
                progress_interval=50000
            ):
                subreddit = record.get('subreddit', '').lower()

                # Skip subreddits already extracted in a previous run
                if subreddit in skip_subreddits:
                    continue

                # Apply keyword/regex filtering
                if not self._matches_keywords(record, record_type):
                    continue

                # Get or create writers for this subreddit
                if subreddit not in writers_by_sub:
                    writers_by_sub[subreddit] = self._create_writers(
                        subreddit, month, record_type
                    )
                    for w in writers_by_sub[subreddit].values():
                        w.__enter__()

                    if subreddit not in stats_by_sub:
                        stats_by_sub[subreddit] = ExtractionStats(
                            subreddit=subreddit,
                            month=month
                        )

                # Write to all configured formats
                for writer in writers_by_sub[subreddit].values():
                    writer.write(record)

                # Update extraction stats
                if record_type == 'submissions':
                    stats_by_sub[subreddit].submissions_count += 1
                else:
                    stats_by_sub[subreddit].comments_count += 1

                # Track author stats
                author = record.get('author') or '[deleted]'
                score = int(record.get('score') or 0)
                ts = int(record.get('created_utc') or 0)

                author_stats = author_acc_by_sub.setdefault(subreddit, {})
                entry = author_stats.get(author)
                if entry is None:
                    entry = {
                        'comment_count': 0,
                        'submission_count': 0,
                        'comment_score_total': 0,
                        'submission_score_total': 0,
                        'first_seen_utc': ts,
                        'last_seen_utc': ts,
                    }
                    author_stats[author] = entry

                if record_type == 'submissions':
                    entry['submission_count'] += 1
                    entry['submission_score_total'] += score
                else:
                    entry['comment_count'] += 1
                    entry['comment_score_total'] += score

                if ts < entry['first_seen_utc']:
                    entry['first_seen_utc'] = ts
                if ts > entry['last_seen_utc']:
                    entry['last_seen_utc'] = ts

                total_extracted += 1

            if pbar:
                pbar.close()

        finally:
            # Close all writers
            for sub_writers in writers_by_sub.values():
                for writer in sub_writers.values():
                    writer.__exit__(None, None, None)

        return total_extracted

    def _write_month_authors(
        self,
        subreddit: str,
        month: str,
        author_stats: Dict[str, Dict],
    ) -> None:
        """Write per-month author statistics to authors.csv in the month directory."""
        output_dir = self._get_output_dir(subreddit, month)
        file_path = output_dir / 'authors.csv'

        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(_AUTHOR_MONTH_CSV_FIELDS)
            for author in sorted(author_stats):
                s = author_stats[author]
                cc = s['comment_count']
                sc = s['submission_count']
                writer.writerow([
                    author,
                    cc,
                    sc,
                    s['comment_score_total'],
                    round(s['comment_score_total'] / cc, 2) if cc > 0 else '',
                    s['submission_score_total'],
                    round(s['submission_score_total'] / sc, 2) if sc > 0 else '',
                    s['first_seen_utc'],
                    s['last_seen_utc'],
                ])

    def _write_subreddit_authors(self, subreddit: str) -> None:
        """
        Aggregate all per-month authors.csv files into a subreddit-level authors.csv.

        Reads back previously written per-month files so the result is always
        correct even with incremental/resumed extractions (not just the current run).
        """
        safe_name = sanitize_subreddit_name(subreddit)
        sub_dir = self.output_path / safe_name

        if not sub_dir.exists():
            return

        global_stats: Dict[str, Dict] = {}

        for month_dir in sorted(sub_dir.iterdir()):
            if not month_dir.is_dir():
                continue
            authors_csv = month_dir / 'authors.csv'
            if not authors_csv.exists():
                continue

            month_str = month_dir.name
            with open(authors_csv, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    author = row['author']
                    entry = global_stats.get(author)
                    if entry is None:
                        entry = {
                            'comment_count': 0,
                            'submission_count': 0,
                            'comment_score_total': 0,
                            'submission_score_total': 0,
                            'first_seen_utc': None,
                            'last_seen_utc': None,
                            'months_active': set(),
                        }
                        global_stats[author] = entry

                    entry['comment_count'] += int(row.get('comment_count') or 0)
                    entry['submission_count'] += int(row.get('submission_count') or 0)
                    entry['comment_score_total'] += int(row.get('comment_score_total') or 0)
                    entry['submission_score_total'] += int(row.get('submission_score_total') or 0)

                    first = int(row['first_seen_utc']) if row.get('first_seen_utc') else None
                    last = int(row['last_seen_utc']) if row.get('last_seen_utc') else None
                    if first is not None:
                        if entry['first_seen_utc'] is None or first < entry['first_seen_utc']:
                            entry['first_seen_utc'] = first
                    if last is not None:
                        if entry['last_seen_utc'] is None or last > entry['last_seen_utc']:
                            entry['last_seen_utc'] = last

                    entry['months_active'].add(month_str)

        if not global_stats:
            return

        output_file = sub_dir / 'authors.csv'
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(_AUTHOR_SUBREDDIT_CSV_FIELDS)
            for author in sorted(global_stats):
                s = global_stats[author]
                cc = s['comment_count']
                sc = s['submission_count']
                writer.writerow([
                    author,
                    cc,
                    sc,
                    s['comment_score_total'],
                    round(s['comment_score_total'] / cc, 2) if cc > 0 else '',
                    s['submission_score_total'],
                    round(s['submission_score_total'] / sc, 2) if sc > 0 else '',
                    s['first_seen_utc'] or '',
                    s['last_seen_utc'] or '',
                    len(s['months_active']),
                ])

        logger.info(f"  Wrote subreddit authors: {len(global_stats):,} unique authors → {output_file}")

    def run(
        self,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None
    ) -> ExtractionResult:
        """
        Run the extraction.

        Args:
            start_month: Optional start month (YYYY-MM), inclusive
            end_month: Optional end month (YYYY-MM), inclusive

        Returns:
            ExtractionResult with statistics
        """
        start_time = time.time()

        logger.info(f"Extracting subreddits: {', '.join(self.subreddits)}")
        logger.info(f"Output path: {self.output_path}")

        if self._include_compiled:
            logger.info(f"Include patterns: {[p.pattern for p in self._include_compiled]}")
        if self._exclude_compiled:
            logger.info(f"Exclude patterns: {[p.pattern for p in self._exclude_compiled]}")
        if not self.force:
            logger.info("Resume mode: completed months will be skipped (use force=True to override)")

        # Filter archives by date range if specified
        archives_to_process = self.archives
        if start_month:
            archives_to_process = [
                a for a in archives_to_process
                if a.month_str >= start_month
            ]
        if end_month:
            archives_to_process = [
                a for a in archives_to_process
                if a.month_str <= end_month
            ]

        if not archives_to_process:
            logger.warning("No archives match the specified date range")
            return ExtractionResult(
                subreddits=self.subreddits,
                months_processed=0,
                total_submissions=0,
                total_comments=0,
                duration_seconds=0.0
            )

        logger.info(f"Processing {len(archives_to_process)} archive files")

        all_stats: List[ExtractionStats] = []
        months_seen = set()
        total_submissions = 0
        total_comments = 0

        for month, comments_file, submissions_file in iter_archive_pairs(archives_to_process):
            # Determine which subreddits to skip (already extracted in a prior run)
            skip_for_month: Set[str] = set()
            if not self.force:
                for sub in self.subreddits:
                    if self._is_month_done(sub, month):
                        skip_for_month.add(sub)
                        logger.info(f"  Skipping {sub}/{month}: already extracted")

            # If every target subreddit is done for this month, skip the archive entirely
            if skip_for_month >= self.subreddit_set:
                logger.info(f"All subreddits already extracted for {month}, skipping")
                continue

            month_start = time.time()
            stats_by_sub: Dict[str, ExtractionStats] = {}
            author_acc_by_sub: Dict[str, Dict] = {}

            logger.info(f"\n=== Processing {month} ===")

            # Process submissions first (typically smaller)
            if submissions_file:
                self._process_archive(submissions_file, stats_by_sub, author_acc_by_sub, skip_for_month)

            # Process comments
            if comments_file:
                self._process_archive(comments_file, stats_by_sub, author_acc_by_sub, skip_for_month)

            # Finalize month stats
            month_duration = time.time() - month_start
            for stats in stats_by_sub.values():
                stats.duration_seconds = month_duration
                total_submissions += stats.submissions_count
                total_comments += stats.comments_count
                all_stats.append(stats)
                logger.info(f"  {stats}")

            months_seen.add(month)

            # Write per-month outputs
            for subreddit, stats in stats_by_sub.items():
                self._write_month_metadata(subreddit, month, stats)
                if subreddit in author_acc_by_sub:
                    self._write_month_authors(subreddit, month, author_acc_by_sub[subreddit])

        total_duration = time.time() - start_time

        # Write subreddit-level outputs (aggregated from all per-month files)
        for subreddit in self.subreddits:
            self._write_subreddit_metadata(subreddit)
            self._write_subreddit_authors(subreddit)

        result = ExtractionResult(
            subreddits=self.subreddits,
            months_processed=len(months_seen),
            total_submissions=total_submissions,
            total_comments=total_comments,
            duration_seconds=total_duration,
            stats=all_stats
        )

        logger.info(f"\n=== Extraction Complete ===")
        logger.info(f"Processed {result.months_processed} months")
        logger.info(f"Total: {total_submissions:,} submissions, {total_comments:,} comments")
        logger.info(f"Duration: {format_duration(total_duration)}")

        return result

    def _write_month_metadata(
        self,
        subreddit: str,
        month: str,
        stats: ExtractionStats
    ):
        """Write metadata file for a subreddit/month."""
        output_dir = self._get_output_dir(subreddit, month)
        metadata = {
            'subreddit': subreddit,
            'month': month,
            'submissions_count': stats.submissions_count,
            'comments_count': stats.comments_count,
            'extracted_at': timestamp_str(),
            'extraction_duration_seconds': stats.duration_seconds,
            'include_patterns': [p.pattern for p in self._include_compiled],
            'exclude_patterns': [p.pattern for p in self._exclude_compiled],
        }

        with open(output_dir / 'metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)

    def _write_subreddit_metadata(self, subreddit: str):
        """
        Write top-level metadata for a subreddit.

        Aggregates counts from per-month metadata files so the result is
        always accurate, including months extracted in previous runs.
        """
        safe_name = sanitize_subreddit_name(subreddit)
        output_dir = self.output_path / safe_name

        if not output_dir.exists():
            return  # No data extracted for this subreddit

        all_months = []
        total_subs = 0
        total_comments = 0

        for month_dir in sorted(output_dir.iterdir()):
            if not month_dir.is_dir():
                continue
            meta_file = month_dir / 'metadata.json'
            if not meta_file.exists():
                continue
            try:
                with open(meta_file, 'r') as f:
                    month_meta = json.load(f)
                all_months.append(month_dir.name)
                total_subs += month_meta.get('submissions_count', 0)
                total_comments += month_meta.get('comments_count', 0)
            except (json.JSONDecodeError, KeyError):
                pass

        metadata = {
            'subreddit': subreddit,
            'months': all_months,
            'total_submissions': total_subs,
            'total_comments': total_comments,
            'extracted_at': timestamp_str(),
            'output_format': self.output_format,
            'include_patterns': [p.pattern for p in self._include_compiled],
            'exclude_patterns': [p.pattern for p in self._exclude_compiled],
        }

        with open(output_dir / 'metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
