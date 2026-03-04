"""
Archive catalogue builder — streaming pass over all archives to produce
per-(subreddit, month) stats.

Performs a single pass over each monthly archive pair, accumulating
comment/submission counts and unique author sets per subreddit, then writes
one CSV row per subreddit per month.  Author sets are discarded after each
month so peak memory stays flat.

Also provides SubredditIndex, which aggregates across all months to produce
a single CSV with one row per subreddit, including NSFW flag and subscriber
counts sourced from submission metadata.
"""

import csv
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set

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


# ---------------------------------------------------------------------------
# SubredditIndex
# ---------------------------------------------------------------------------

# Fields written to the per-month intermediate CSVs
_INDEX_MONTH_FIELDS = [
    'subreddit', 'subreddit_id',
    'n_submissions', 'n_comments',
    'over_18', 'subreddit_subscribers',
]

# Fields in the final aggregated CSV
INDEX_FIELDS = [
    'subreddit', 'subreddit_id',
    'n_submissions', 'n_comments',
    'first_month', 'last_month', 'months_active',
    'over_18', 'subreddit_subscribers',
]


class SubredditIndex:
    """
    Build a per-subreddit aggregated index from raw Pushshift archives.

    Performs a single streaming pass over all monthly archive pairs and
    produces a CSV with **one row per subreddit** containing totals and
    metadata aggregated across all time.

    Runs are resumable: per-month intermediate files are written to a
    ``<stem>_months/`` sibling directory.  On restart, already-processed
    months are skipped automatically.  Call :meth:`run` to process archives
    and write the final CSV in one step.

    Output columns:

    ``subreddit``, ``subreddit_id``, ``n_submissions``, ``n_comments``,
    ``first_month``, ``last_month``, ``months_active``,
    ``over_18``, ``subreddit_subscribers``

    - ``over_18`` — ``True`` if any submission ever carried the NSFW flag.
    - ``subreddit_subscribers`` — highest subscriber count observed across all
      records (sourced from submission metadata).

    Example::

        idx = SubredditIndex(
            archive_path="/path/to/dumps",
            output_path="./subreddits.csv",
        )
        result = idx.run(start_month="2020-01", end_month="2022-12")
        print(f"Found {result['subreddits']:,} subreddits")
        # → subreddits.csv  (one row per subreddit)
    """

    def __init__(
        self,
        archive_path: Path,
        output_path: Path,
        show_progress: bool = True,
    ):
        """
        Initialise the index builder.

        Args:
            archive_path: Root directory containing ``comments/`` and
                ``submissions/`` subdirectories of ``.zst`` archives.
            output_path: Destination CSV file (e.g. ``./subreddits.csv``).
                Intermediate per-month files are stored alongside it in a
                ``<stem>_months/`` directory.
            show_progress: Print progress to stdout while processing.
        """
        self.archive_path = Path(archive_path)
        self.output_path = Path(output_path)
        self.show_progress = show_progress

        # Intermediate per-month files live next to the output CSV
        stem = self.output_path.stem
        self._months_dir = self.output_path.parent / f"{stem}_months"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        min_records: int = 1,
    ) -> dict:
        """
        Stream archives, build per-subreddit stats, write the output CSV.

        Already-processed months (those with an intermediate file in the
        ``_months/`` directory) are skipped automatically, making runs
        resumable.

        Args:
            start_month: Optional inclusive start month (``YYYY-MM``).
            end_month:   Optional inclusive end month (``YYYY-MM``).
            min_records: Subreddits with fewer total records
                (submissions + comments across all time) than this threshold
                are excluded from the output.  Default ``1`` (keep all).

        Returns:
            Dict with keys ``months_processed``, ``subreddits``,
            and ``output_path``.
        """
        start_time = time.time()

        archives = discover_archives(self.archive_path)
        if not archives:
            logger.warning(f"No archives found in {self.archive_path}")
            return {'months_processed': 0, 'subreddits': 0, 'output_path': str(self.output_path)}

        months = get_months_in_range(archives, start_month, end_month)
        ensure_directory(self._months_dir)

        completed = self._completed_months()
        pending = [m for m in months if m not in completed]
        skipped = len(months) - len(pending)

        if skipped:
            logger.info(f"Skipping {skipped} already-indexed month(s)")
        if not pending:
            logger.info("All months already indexed — aggregating existing data")
        else:
            logger.info(f"Indexing {len(pending)} month(s)")

        # --- Streaming pass: write one intermediate CSV per month ----------
        for month, comments_file, submissions_file in iter_archive_pairs(archives):
            if month not in pending:
                continue

            if self.show_progress:
                print(f"  [{month}] processing ...", flush=True)

            month_data = self._process_month(month, comments_file, submissions_file)
            self._write_month_intermediate(month, month_data)

            if self.show_progress:
                print(f"  [{month}] done — {len(month_data):,} subreddits", flush=True)

        # --- Aggregation pass: merge all intermediates → final CSV ---------
        subreddit_count = self._aggregate(min_records)

        duration = time.time() - start_time
        logger.info(
            f"SubredditIndex complete: {len(pending)} months processed, "
            f"{subreddit_count:,} subreddits → {self.output_path} "
            f"({format_duration(duration)})"
        )

        return {
            'months_processed': len(pending),
            'subreddits': subreddit_count,
            'output_path': str(self.output_path),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _completed_months(self) -> Set[str]:
        """Return set of months that already have an intermediate file."""
        done: Set[str] = set()
        if self._months_dir.exists():
            for p in self._months_dir.glob("*.csv"):
                # Filename is YYYY-MM.csv
                done.add(p.stem)
        return done

    def _process_month(
        self,
        month: str,
        comments_file,
        submissions_file,
    ) -> Dict[str, dict]:
        """
        Stream one month's archives and accumulate per-subreddit stats.

        Returns a dict mapping subreddit name → stats dict with keys:
        ``subreddit_id``, ``n_submissions``, ``n_comments``,
        ``over_18``, ``subreddit_subscribers``.
        """
        data: Dict[str, dict] = {}

        def _bucket(name: str) -> dict:
            if name not in data:
                data[name] = {
                    'subreddit_id': '',
                    'n_submissions': 0,
                    'n_comments': 0,
                    'over_18': False,
                    'subreddit_subscribers': 0,
                }
            return data[name]

        def _progress(prog):
            if self.show_progress:
                print(
                    f"\r    {prog.percent:5.1f}%  {prog.lines_read:>12,} lines",
                    end='', flush=True,
                )

        if submissions_file is not None:
            if self.show_progress:
                print(f"    submissions: {submissions_file.path.name}", flush=True)
            for record in read_zst_records(submissions_file.path, progress_callback=_progress):
                sub = record.get('subreddit', '')
                if not sub:
                    continue
                bucket = _bucket(sub)
                bucket['n_submissions'] += 1

                # Capture subreddit-level metadata from submission records
                if not bucket['subreddit_id']:
                    bucket['subreddit_id'] = record.get('subreddit_id', '')
                if record.get('over_18'):
                    bucket['over_18'] = True
                subs = record.get('subreddit_subscribers') or 0
                try:
                    subs = int(subs)
                except (TypeError, ValueError):
                    subs = 0
                if subs > bucket['subreddit_subscribers']:
                    bucket['subreddit_subscribers'] = subs

            if self.show_progress:
                print()

        if comments_file is not None:
            if self.show_progress:
                print(f"    comments: {comments_file.path.name}", flush=True)
            for record in read_zst_records(comments_file.path, progress_callback=_progress):
                sub = record.get('subreddit', '')
                if not sub:
                    continue
                bucket = _bucket(sub)
                bucket['n_comments'] += 1

                if not bucket['subreddit_id']:
                    bucket['subreddit_id'] = record.get('subreddit_id', '')

            if self.show_progress:
                print()

        return data

    def _write_month_intermediate(self, month: str, data: Dict[str, dict]) -> None:
        """Write per-subreddit stats for one month to an intermediate CSV."""
        out_path = self._months_dir / f"{month}.csv"
        with open(out_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=_INDEX_MONTH_FIELDS)
            writer.writeheader()
            for subreddit in sorted(data):
                s = data[subreddit]
                writer.writerow({
                    'subreddit': subreddit,
                    'subreddit_id': s['subreddit_id'],
                    'n_submissions': s['n_submissions'],
                    'n_comments': s['n_comments'],
                    'over_18': s['over_18'],
                    'subreddit_subscribers': s['subreddit_subscribers'],
                })

    def _aggregate(self, min_records: int) -> int:
        """
        Read all intermediate monthly CSVs and aggregate into the final
        per-subreddit output CSV.

        Returns the number of subreddits written.
        """
        # Accumulate across all intermediate files
        agg: Dict[str, dict] = {}

        month_files = sorted(self._months_dir.glob("*.csv"))
        if not month_files:
            logger.warning("No intermediate month files found; output will be empty.")

        for month_path in month_files:
            month = month_path.stem  # YYYY-MM
            with open(month_path, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    sub = row['subreddit']
                    if sub not in agg:
                        agg[sub] = {
                            'subreddit_id': row.get('subreddit_id', ''),
                            'n_submissions': 0,
                            'n_comments': 0,
                            'months': [],
                            'over_18': False,
                            'subreddit_subscribers': 0,
                        }
                    entry = agg[sub]

                    entry['n_submissions'] += int(row.get('n_submissions') or 0)
                    entry['n_comments'] += int(row.get('n_comments') or 0)
                    entry['months'].append(month)

                    if not entry['subreddit_id'] and row.get('subreddit_id'):
                        entry['subreddit_id'] = row['subreddit_id']

                    if row.get('over_18') in ('True', 'true', '1', True):
                        entry['over_18'] = True

                    subs = int(row.get('subreddit_subscribers') or 0)
                    if subs > entry['subreddit_subscribers']:
                        entry['subreddit_subscribers'] = subs

        # Write final CSV
        ensure_directory(self.output_path.parent)
        written = 0
        with open(self.output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=INDEX_FIELDS)
            writer.writeheader()
            for subreddit in sorted(agg):
                entry = agg[subreddit]
                total = entry['n_submissions'] + entry['n_comments']
                if total < min_records:
                    continue
                months_sorted = sorted(entry['months'])
                writer.writerow({
                    'subreddit': subreddit,
                    'subreddit_id': entry['subreddit_id'],
                    'n_submissions': entry['n_submissions'],
                    'n_comments': entry['n_comments'],
                    'first_month': months_sorted[0] if months_sorted else '',
                    'last_month': months_sorted[-1] if months_sorted else '',
                    'months_active': len(months_sorted),
                    'over_18': entry['over_18'],
                    'subreddit_subscribers': entry['subreddit_subscribers'],
                })
                written += 1

        return written
