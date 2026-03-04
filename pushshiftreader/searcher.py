"""
Cross-dataset word/pattern search across Pushshift monthly archives.

Scans all raw .zst archive files and extracts every comment or submission
whose text contains the given regex pattern, writing results to per-month
output files. Supports resumable runs and parallel workers.
"""

import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from .reader import read_zst_lines, ReadProgress
from .writers import JsonlWriter, CsvWriter, SUBMISSION_CSV_FIELDS, COMMENT_CSV_FIELDS
from .utils import (
    discover_archives, iter_archive_pairs, ensure_directory,
    format_size, format_duration, timestamp_str, ArchiveFile
)

logger = logging.getLogger(__name__)


@dataclass
class SearchStats:
    """Statistics for one month of a search run."""
    month: str
    comments_matched: int = 0
    submissions_matched: int = 0
    duration_seconds: float = 0.0

    def __str__(self) -> str:
        return (
            f"{self.month}: "
            f"{self.comments_matched:,} comments, "
            f"{self.submissions_matched:,} submissions matched "
            f"({format_duration(self.duration_seconds)})"
        )


@dataclass
class SearchResult:
    """Result of a complete search run."""
    pattern: str
    months_processed: int
    total_comments: int
    total_submissions: int
    duration_seconds: float
    stats: List[SearchStats] = field(default_factory=list)


@dataclass
class _SearchMonthJob:
    """Bundle of parameters for processing one month in a worker process."""
    month: str
    comments_path: Optional[Path]
    submissions_path: Optional[Path]
    output_path: Path
    pattern: str        # raw string — recompiled in each worker
    case_sensitive: bool
    search_comments: bool
    search_submissions: bool
    output_format: str


def _text_for_record(record: dict, record_type: str) -> str:
    """Extract searchable text from a record."""
    if record_type == 'comments':
        return record.get('body') or ''
    else:
        return ((record.get('title') or '') + ' ' + (record.get('selftext') or ''))


def _process_archive_file(
    file_path: Path,
    record_type: str,
    compiled_pattern: re.Pattern,
    output_dir: Path,
    output_format: str,
) -> int:
    """
    Stream through one archive file, writing matching records.

    Uses a two-stage filter:
      1. Regex search on the raw line string (avoids JSON parsing overhead for
         the vast majority of non-matching records).
      2. Field-specific check after JSON parsing (confirms the match is in the
         correct field: body for comments, title+selftext for submissions).

    Returns the number of matched records.
    """
    matched = 0
    writers: Dict[str, object] = {}

    try:
        if output_format in ('jsonl', 'both'):
            fname = f"{record_type}.jsonl.gz"
            w = JsonlWriter(output_dir / fname)
            w.__enter__()
            writers['jsonl'] = w
        if output_format in ('csv', 'both'):
            fname = f"{record_type}.csv"
            fields = COMMENT_CSV_FIELDS if record_type == 'comments' else SUBMISSION_CSV_FIELDS
            w = CsvWriter(output_dir / fname, fields=fields)
            w.__enter__()
            writers['csv'] = w

        for line, _ in read_zst_lines(file_path):
            # Stage 1: fast pre-filter on the raw JSON line.
            # The body/title text is embedded verbatim in the NDJSON line, so
            # this correctly skips lines that cannot possibly match.
            if not compiled_pattern.search(line):
                continue

            # Stage 2: parse JSON and check the correct text field.
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            text = _text_for_record(record, record_type)
            if not compiled_pattern.search(text):
                continue

            for w in writers.values():
                w.write(record)
            matched += 1

    finally:
        for w in writers.values():
            w.__exit__(None, None, None)

    return matched


def _run_search_month_job(job: _SearchMonthJob) -> Tuple[str, int, int]:
    """
    Module-level worker: process one month's archive files.

    Runs in a subprocess when workers > 1. Returns
    (month, comments_matched, submissions_matched).
    """
    import re as _re
    import time as _time

    flags = 0 if job.case_sensitive else _re.IGNORECASE
    compiled = _re.compile(job.pattern, flags)

    start = _time.time()
    comments_matched = 0
    submissions_matched = 0

    out_dir = ensure_directory(job.output_path / job.month)

    if job.search_comments and job.comments_path:
        comments_matched = _process_archive_file(
            job.comments_path, 'comments', compiled, out_dir, job.output_format
        )

    if job.search_submissions and job.submissions_path:
        submissions_matched = _process_archive_file(
            job.submissions_path, 'submissions', compiled, out_dir, job.output_format
        )

    duration = _time.time() - start
    _write_month_metadata(
        out_dir, job.month, job.pattern, comments_matched, submissions_matched, duration
    )

    return job.month, comments_matched, submissions_matched


def _write_month_metadata(
    out_dir: Path,
    month: str,
    pattern: str,
    comments_matched: int,
    submissions_matched: int,
    duration: float,
) -> None:
    metadata = {
        'month': month,
        'pattern': pattern,
        'comments_matched': comments_matched,
        'submissions_matched': submissions_matched,
        'duration_seconds': round(duration, 2),
        'completed_at': timestamp_str(),
    }
    with open(out_dir / 'metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)


class WordSearcher:
    """
    Search for a regex pattern across all Pushshift monthly archives.

    Extracts every comment and/or submission whose text matches the pattern,
    writing results to per-month output files. Runs are resumable: months
    already completed (metadata.json present) are skipped on restart.

    Example::

        searcher = WordSearcher(
            archive_path="/data/reddit_dumps",
            output_path="./search_results/nudge",
            pattern=r"nudg",          # matches nudge, nudging, nudged …
        )
        result = searcher.run()
        print(f"Found {result.total_comments:,} comments, "
              f"{result.total_submissions:,} submissions")

    Output layout::

        output_path/
            2020-01/
                comments.jsonl.gz       # all matching comments (raw records)
                submissions.jsonl.gz    # all matching submissions
                metadata.json           # completion marker + match counts
            2020-02/
                ...
            metadata.json               # summary for the whole search run

    Each output record is the raw JSON object from the Pushshift archive
    (all fields preserved, including subreddit, author, created_utc, etc.)
    """

    def __init__(
        self,
        archive_path: Path,
        output_path: Path,
        pattern: str,
        case_sensitive: bool = False,
        search_comments: bool = True,
        search_submissions: bool = True,
        output_format: str = "jsonl",   # "csv", "jsonl", or "both"
        workers: int = 1,
        force: bool = False,
        show_progress: bool = True,
        comments_subdir: str = "comments",
        submissions_subdir: str = "submissions",
    ):
        """
        Initialise the searcher.

        Args:
            archive_path: Root directory containing ``comments/`` and
                ``submissions/`` subdirectories of ``.zst`` archives.
            output_path: Directory where results will be written.
            pattern: Regular expression to search for (case-insensitive by
                default). Searches ``body`` for comments and
                ``title + selftext`` for submissions.
            case_sensitive: If ``True``, pattern matching is case-sensitive.
            search_comments: Include comment archives in the search.
            search_submissions: Include submission archives in the search.
            output_format: ``"jsonl"`` (default), ``"csv"``, or ``"both"``.
            workers: Number of parallel worker processes. ``-1`` uses all
                available CPU cores. Default is 1 (sequential).
            force: Re-process months that already have a ``metadata.json``
                completion marker. Default ``False`` (skip completed months).
            show_progress: Show a tqdm progress bar (requires ``tqdm``).
            comments_subdir: Name of the comments subdirectory.
            submissions_subdir: Name of the submissions subdirectory.
        """
        self.archive_path = Path(archive_path)
        self.output_path = Path(output_path)
        self.pattern = pattern
        self.case_sensitive = case_sensitive
        self.search_comments = search_comments
        self.search_submissions = search_submissions
        self.output_format = output_format
        self.force = force
        self.show_progress = show_progress

        if workers == -1:
            import os
            workers = os.cpu_count() or 1
        self.workers = max(1, workers)

        flags = 0 if case_sensitive else re.IGNORECASE
        self._compiled = re.compile(pattern, flags)

        if not self.archive_path.exists():
            raise ValueError(f"Archive path does not exist: {self.archive_path}")

        self.archives = discover_archives(
            self.archive_path, comments_subdir, submissions_subdir
        )
        if not self.archives:
            raise ValueError(f"No archive files found in {self.archive_path}")

        logger.info(f"WordSearcher: pattern={pattern!r}, {len(self.archives)} archive files")

    def _is_month_done(self, month: str) -> bool:
        return (self.output_path / month / 'metadata.json').exists()

    def _write_summary_metadata(
        self,
        months_processed: int,
        total_comments: int,
        total_submissions: int,
        duration: float,
    ) -> None:
        metadata = {
            'pattern': self.pattern,
            'case_sensitive': self.case_sensitive,
            'months_processed': months_processed,
            'total_comments_matched': total_comments,
            'total_submissions_matched': total_submissions,
            'duration_seconds': round(duration, 2),
            'completed_at': timestamp_str(),
        }
        with open(self.output_path / 'metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)

    def run(
        self,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
    ) -> SearchResult:
        """
        Run the search across all (or a range of) monthly archives.

        Args:
            start_month: Optional inclusive start month (``YYYY-MM``).
            end_month: Optional inclusive end month (``YYYY-MM``).

        Returns:
            :class:`SearchResult` with summary statistics.
        """
        ensure_directory(self.output_path)

        archives = self.archives
        if start_month:
            archives = [a for a in archives if a.month_str >= start_month]
        if end_month:
            archives = [a for a in archives if a.month_str <= end_month]

        if not archives:
            logger.warning("No archives match the specified date range")
            return SearchResult(
                pattern=self.pattern,
                months_processed=0,
                total_comments=0,
                total_submissions=0,
                duration_seconds=0.0,
            )

        if self.workers != 1:
            return self._run_parallel(archives)

        return self._run_sequential(archives)

    def _run_sequential(self, archives: List[ArchiveFile]) -> SearchResult:
        start_time = time.time()

        logger.info(f"Searching pattern={self.pattern!r} across {len(archives)} archive files")
        if not self.force:
            logger.info("Resume mode: completed months will be skipped (use force=True to override)")

        all_stats: List[SearchStats] = []
        months_seen: set = set()
        total_comments = 0
        total_submissions = 0

        for month, comments_file, submissions_file in iter_archive_pairs(archives):
            if not self.force and self._is_month_done(month):
                logger.info(f"Skipping {month}: already completed")
                continue

            logger.info(f"\n=== Searching {month} ===")
            month_start = time.time()
            out_dir = ensure_directory(self.output_path / month)

            comments_matched = 0
            submissions_matched = 0

            if self.search_submissions and submissions_file:
                logger.info(f"  Submissions: {format_size(submissions_file.path.stat().st_size)}")
                pbar = self._make_pbar(submissions_file)
                submissions_matched = self._process_file_with_progress(
                    submissions_file.path, 'submissions', out_dir, pbar
                )
                if pbar:
                    pbar.close()
                logger.info(f"  Submissions matched: {submissions_matched:,}")

            if self.search_comments and comments_file:
                logger.info(f"  Comments: {format_size(comments_file.path.stat().st_size)}")
                pbar = self._make_pbar(comments_file)
                comments_matched = self._process_file_with_progress(
                    comments_file.path, 'comments', out_dir, pbar
                )
                if pbar:
                    pbar.close()
                logger.info(f"  Comments matched: {comments_matched:,}")

            duration = time.time() - month_start
            _write_month_metadata(
                out_dir, month, self.pattern,
                comments_matched, submissions_matched, duration
            )

            stats = SearchStats(
                month=month,
                comments_matched=comments_matched,
                submissions_matched=submissions_matched,
                duration_seconds=duration,
            )
            all_stats.append(stats)
            total_comments += comments_matched
            total_submissions += submissions_matched
            months_seen.add(month)
            logger.info(f"  {stats}")

        total_duration = time.time() - start_time
        self._write_summary_metadata(
            len(months_seen), total_comments, total_submissions, total_duration
        )

        result = SearchResult(
            pattern=self.pattern,
            months_processed=len(months_seen),
            total_comments=total_comments,
            total_submissions=total_submissions,
            duration_seconds=total_duration,
            stats=all_stats,
        )
        logger.info(f"\n=== Search Complete ===")
        logger.info(f"Pattern: {self.pattern!r}")
        logger.info(f"Months processed: {result.months_processed}")
        logger.info(f"Matched: {total_comments:,} comments, {total_submissions:,} submissions")
        logger.info(f"Duration: {format_duration(total_duration)}")
        return result

    def _make_pbar(self, archive: ArchiveFile):
        """Create a tqdm progress bar for an archive file, or return None."""
        if not self.show_progress:
            return None
        try:
            from tqdm import tqdm
            return tqdm(
                total=archive.path.stat().st_size,
                unit='B',
                unit_scale=True,
                desc=f"  {archive.path.name}",
            )
        except ImportError:
            return None

    def _process_file_with_progress(
        self,
        file_path: Path,
        record_type: str,
        out_dir: Path,
        pbar,
    ) -> int:
        """Stream through one archive file, updating a progress bar."""
        matched = 0
        writers: Dict[str, object] = {}
        last_bytes = 0

        try:
            if self.output_format in ('jsonl', 'both'):
                w = JsonlWriter(out_dir / f"{record_type}.jsonl.gz")
                w.__enter__()
                writers['jsonl'] = w
            if self.output_format in ('csv', 'both'):
                fields = COMMENT_CSV_FIELDS if record_type == 'comments' else SUBMISSION_CSV_FIELDS
                w = CsvWriter(out_dir / f"{record_type}.csv", fields=fields)
                w.__enter__()
                writers['csv'] = w

            for line, bytes_read in read_zst_lines(file_path):
                if pbar and bytes_read > last_bytes:
                    pbar.update(bytes_read - last_bytes)
                    last_bytes = bytes_read

                # Stage 1: fast pre-filter on raw line
                if not self._compiled.search(line):
                    continue

                # Stage 2: parse and check correct text field
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                text = _text_for_record(record, record_type)
                if not self._compiled.search(text):
                    continue

                for w in writers.values():
                    w.write(record)
                matched += 1

        finally:
            for w in writers.values():
                w.__exit__(None, None, None)

        return matched

    def _run_parallel(self, archives: List[ArchiveFile]) -> SearchResult:
        """Run the search across multiple processes, one month per worker."""
        from concurrent.futures import ProcessPoolExecutor, as_completed

        start_time = time.time()
        logger.info(
            f"Searching pattern={self.pattern!r} across {len(archives)} archive files "
            f"({self.workers} workers)"
        )

        jobs: List[_SearchMonthJob] = []
        for month, comments_file, submissions_file in iter_archive_pairs(archives):
            if not self.force and self._is_month_done(month):
                logger.info(f"Skipping {month}: already completed")
                continue
            jobs.append(_SearchMonthJob(
                month=month,
                comments_path=comments_file.path if comments_file else None,
                submissions_path=submissions_file.path if submissions_file else None,
                output_path=self.output_path,
                pattern=self.pattern,
                case_sensitive=self.case_sensitive,
                search_comments=self.search_comments,
                search_submissions=self.search_submissions,
                output_format=self.output_format,
            ))

        if not jobs:
            logger.info("All months already completed, nothing to do")
            return SearchResult(
                pattern=self.pattern,
                months_processed=0,
                total_comments=0,
                total_submissions=0,
                duration_seconds=0.0,
            )

        logger.info(f"Processing {len(jobs)} months across {self.workers} workers")

        all_stats: List[SearchStats] = []
        months_seen: set = set()
        total_comments = 0
        total_submissions = 0

        pbar = None
        if self.show_progress:
            try:
                from tqdm import tqdm
                pbar = tqdm(total=len(jobs), unit='month', desc='Searching')
            except ImportError:
                pass

        try:
            with ProcessPoolExecutor(max_workers=self.workers) as pool:
                futures = {pool.submit(_run_search_month_job, job): job for job in jobs}
                for future in as_completed(futures):
                    job = futures[future]
                    try:
                        month, comments_matched, submissions_matched = future.result()
                    except Exception as e:
                        logger.error(f"Error processing {job.month}: {e}")
                        raise

                    stats = SearchStats(
                        month=month,
                        comments_matched=comments_matched,
                        submissions_matched=submissions_matched,
                    )
                    all_stats.append(stats)
                    total_comments += comments_matched
                    total_submissions += submissions_matched
                    months_seen.add(month)
                    logger.info(f"  {stats}")

                    if pbar:
                        pbar.update(1)
        finally:
            if pbar:
                pbar.close()

        total_duration = time.time() - start_time
        self._write_summary_metadata(
            len(months_seen), total_comments, total_submissions, total_duration
        )

        result = SearchResult(
            pattern=self.pattern,
            months_processed=len(months_seen),
            total_comments=total_comments,
            total_submissions=total_submissions,
            duration_seconds=total_duration,
            stats=sorted(all_stats, key=lambda s: s.month),
        )
        logger.info(f"\n=== Search Complete ===")
        logger.info(f"Pattern: {self.pattern!r}")
        logger.info(f"Months processed: {result.months_processed}")
        logger.info(f"Matched: {total_comments:,} comments, {total_submissions:,} submissions")
        logger.info(f"Duration: {format_duration(total_duration)}")
        return result
