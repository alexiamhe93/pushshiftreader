"""
Archive catalogue builder — streaming pass over all archives to produce
per-(subreddit, month) stats.

Performs a single pass over each monthly archive pair, accumulating
comment/submission counts and unique author sets per subreddit, then writes
one CSV row per subreddit per month.  Author sets are discarded after each
month so peak memory stays flat.
"""

import csv
import time
import logging
from pathlib import Path
from typing import Dict, Optional, Set

from .utils import (
    discover_archives,
    iter_archive_pairs,
    get_months_in_range,
    ensure_directory,
    format_duration,
)
from .reader import read_zst_records

logger = logging.getLogger(__name__)

CATALOGUE_FIELDS = ['subreddit', 'month', 'n_submissions', 'n_comments', 'n_unique_authors']


class ArchiveCatalogue:
    """
    Build a per-(subreddit, month) statistics catalogue from raw Pushshift archives.

    Performs a single streaming pass over each monthly archive pair,
    accumulating counts per subreddit, then writes one CSV row per subreddit
    per month.  Author sets are discarded after each month, keeping memory flat.

    Supports resumable processing: on startup the existing output CSV is read to
    find already-processed months, which are then skipped.

    Example::

        cat = ArchiveCatalogue(
            archive_path="/path/to/dumps",
            output_path="catalogue.csv",
        )
        cat.run(start_month="2013-01", end_month="2013-06")
    """

    def __init__(
        self,
        archive_path,
        output_path,
        show_progress: bool = True,
    ):
        self.archive_path = Path(archive_path)
        self.output_path = Path(output_path)
        self.show_progress = show_progress

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        min_activity: int = 1,
    ) -> dict:
        """
        Run the catalogue pass over all monthly archives.

        Args:
            start_month: Optional start month (YYYY-MM), inclusive.
            end_month:   Optional end month (YYYY-MM), inclusive.
            min_activity: Skip subreddits whose total records
                          (submissions + comments) in a month is below this
                          threshold.  Default 1 (keep all).

        Returns:
            Summary dict with keys ``months_processed``, ``subreddits_seen``,
            and ``rows_written``.
        """
        start_time = time.time()

        archives = discover_archives(self.archive_path)
        if not archives:
            logger.warning(f"No archives found in {self.archive_path}")
            return {'months_processed': 0, 'subreddits_seen': 0, 'rows_written': 0}

        months = get_months_in_range(archives, start_month, end_month)
        completed = self._completed_months()

        pending = [m for m in months if m not in completed]
        skipped = len(months) - len(pending)

        if skipped:
            logger.info(f"Skipping {skipped} already-catalogued month(s)")

        if not pending:
            logger.info("All months already catalogued — nothing to do")
            return {'months_processed': 0, 'subreddits_seen': 0, 'rows_written': 0}

        logger.info(f"Cataloguing {len(pending)} month(s) → {self.output_path}")

        # Ensure the output directory exists
        ensure_directory(self.output_path.parent)

        # Open CSV in append mode; write header only when starting fresh
        file_is_new = not self.output_path.exists() or self.output_path.stat().st_size == 0
        fh = open(self.output_path, 'a', newline='', encoding='utf-8')
        writer = csv.DictWriter(fh, fieldnames=CATALOGUE_FIELDS)
        if file_is_new:
            writer.writeheader()

        months_processed = 0
        total_rows = 0
        all_subreddits: Set[str] = set()

        try:
            for month, comments_file, submissions_file in iter_archive_pairs(archives):
                if month not in pending:
                    continue

                if self.show_progress:
                    print(f"  [{month}] processing ...", flush=True)

                month_data = self._process_month(month, comments_file, submissions_file)

                # Write one row per subreddit, applying min_activity filter
                rows_this_month = 0
                for subreddit, stats in sorted(month_data.items()):
                    if stats['n_submissions'] + stats['n_comments'] < min_activity:
                        continue
                    writer.writerow({
                        'subreddit': subreddit,
                        'month': month,
                        'n_submissions': stats['n_submissions'],
                        'n_comments': stats['n_comments'],
                        'n_unique_authors': len(stats['authors']),
                    })
                    rows_this_month += 1
                    all_subreddits.add(subreddit)

                fh.flush()
                months_processed += 1
                total_rows += rows_this_month

                if self.show_progress:
                    print(
                        f"  [{month}] done — {rows_this_month:,} subreddits written",
                        flush=True,
                    )
        finally:
            fh.close()

        duration = time.time() - start_time
        logger.info(
            f"Catalogue complete: {months_processed} months, "
            f"{len(all_subreddits):,} subreddits, "
            f"{total_rows:,} rows in {format_duration(duration)}"
        )

        return {
            'months_processed': months_processed,
            'subreddits_seen': len(all_subreddits),
            'rows_written': total_rows,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _completed_months(self) -> Set[str]:
        """
        Read the existing output CSV (if any) and return the set of
        already-processed months so they can be skipped on resume.
        """
        if not self.output_path.exists():
            return set()

        done: Set[str] = set()
        try:
            with open(self.output_path, newline='', encoding='utf-8') as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    if 'month' in row and row['month']:
                        done.add(row['month'])
        except Exception as exc:
            logger.warning(f"Could not read existing catalogue ({self.output_path}): {exc}")

        return done

    def _process_month(
        self,
        month: str,
        comments_file,
        submissions_file,
    ) -> Dict[str, dict]:
        """
        Stream one month's archives and return per-subreddit stats.

        Returns:
            Dict mapping ``subreddit_name`` → ``{'n_submissions': int,
            'n_comments': int, 'authors': set}``.
        """
        data: Dict[str, dict] = {}

        def _bucket(name: str) -> dict:
            if name not in data:
                data[name] = {'n_submissions': 0, 'n_comments': 0, 'authors': set()}
            return data[name]

        def _progress(prog):
            if self.show_progress:
                print(
                    f"\r    {prog.percent:5.1f}%  {prog.lines_read:>12,} lines",
                    end='',
                    flush=True,
                )

        # Stream submissions
        if submissions_file is not None:
            logger.debug(f"  Streaming submissions: {submissions_file.path.name}")
            if self.show_progress:
                print(f"    submissions: {submissions_file.path.name}", flush=True)
            for record in read_zst_records(submissions_file.path, progress_callback=_progress):
                sub = record.get('subreddit', '')
                if not sub:
                    continue
                bucket = _bucket(sub)
                bucket['n_submissions'] += 1
                author = record.get('author', '')
                if author and author != '[deleted]':
                    bucket['authors'].add(author)
            if self.show_progress:
                print()

        # Stream comments
        if comments_file is not None:
            logger.debug(f"  Streaming comments: {comments_file.path.name}")
            if self.show_progress:
                print(f"    comments: {comments_file.path.name}", flush=True)
            for record in read_zst_records(comments_file.path, progress_callback=_progress):
                sub = record.get('subreddit', '')
                if not sub:
                    continue
                bucket = _bucket(sub)
                bucket['n_comments'] += 1
                author = record.get('author', '')
                if author and author != '[deleted]':
                    bucket['authors'].add(author)
            if self.show_progress:
                print()

        return data
