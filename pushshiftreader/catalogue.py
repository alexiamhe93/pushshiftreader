"""
Canonical archive-wide catalogue and subreddit index builders.
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

from .reader import ReadProgress, read_zst_records
from .storage import (
    CATALOGUE_MONTH_SCHEMA,
    CATALOGUE_VERSION,
    SUBREDDIT_INDEX_SCHEMA,
    CatalogueLayout,
    read_json,
    read_parquet_records,
    write_json,
    write_parquet_records,
)
from .utils import discover_archives, format_duration, format_size, get_months_in_range, iter_archive_pairs, timestamp_str

logger = logging.getLogger(__name__)


CATALOGUE_MONTH_SPECS = [(field.name, field.type) for field in CATALOGUE_MONTH_SCHEMA]
SUBREDDIT_INDEX_SPECS = [(field.name, field.type) for field in SUBREDDIT_INDEX_SCHEMA]


class _CatalogueProgressReporter:
    """Periodic progress logs for raw archive scans."""

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
            or progress.bytes_read >= progress.total_bytes
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


class ArchiveCatalogue:
    """
    Build a canonical per-(subreddit, month) summary dataset from raw archives.

    Output layout:

    - ``months/YYYY-MM.parquet``: one row per subreddit for the month
    - ``months/YYYY-MM.json``: completion marker + month metadata
    - ``catalogue.parquet``: all month rows combined
    - ``subreddit_index.parquet``: one row per subreddit aggregated across months
    - ``catalogue.json``: top-level run metadata
    """

    def __init__(
        self,
        archive_path: Path,
        output_path: Path,
        show_progress: bool = True,
        progress_interval: int = 250000,
    ):
        self.archive_path = Path(archive_path)
        self.output_path = Path(output_path)
        self.layout = CatalogueLayout(self.output_path)
        self.show_progress = show_progress
        self.progress_interval = max(1, progress_interval)

    def _completed_months(self) -> Set[str]:
        return {path.stem for path in self.layout.month_metadata_files()}

    def _bucket(self, data: Dict[str, dict], subreddit: str) -> dict:
        if subreddit not in data:
            data[subreddit] = {
                "subreddit": subreddit,
                "subreddit_id": "",
                "n_submissions": 0,
                "n_comments": 0,
                "authors": set(),
                "over_18": False,
                "subreddit_subscribers": 0,
            }
        return data[subreddit]

    def _scan_archive(self, archive, data: Dict[str, dict]) -> None:
        reporter = _CatalogueProgressReporter(archive.path.name, enabled=self.show_progress)
        logger.info("Processing %s (%s)", archive.path.name, format_size(archive.path.stat().st_size))

        for record in read_zst_records(
            archive.path,
            progress_callback=reporter.callback if self.show_progress else None,
            progress_interval=self.progress_interval,
        ):
            subreddit = (record.get("subreddit") or "").strip()
            if not subreddit:
                continue

            bucket = self._bucket(data, subreddit)
            if archive.file_type == "submissions":
                bucket["n_submissions"] += 1
                if not bucket["subreddit_id"]:
                    bucket["subreddit_id"] = record.get("subreddit_id") or ""
                if record.get("over_18"):
                    bucket["over_18"] = True
                try:
                    subscribers = int(record.get("subreddit_subscribers") or 0)
                except (TypeError, ValueError):
                    subscribers = 0
                if subscribers > bucket["subreddit_subscribers"]:
                    bucket["subreddit_subscribers"] = subscribers
            else:
                bucket["n_comments"] += 1
                if not bucket["subreddit_id"]:
                    bucket["subreddit_id"] = record.get("subreddit_id") or ""

            author = record.get("author") or ""
            if author and author != "[deleted]":
                bucket["authors"].add(author)

    def _process_month(self, month: str, comments_file, submissions_file, min_activity: int) -> List[dict]:
        data: Dict[str, dict] = {}

        if submissions_file is not None:
            self._scan_archive(submissions_file, data)
        if comments_file is not None:
            self._scan_archive(comments_file, data)

        rows: List[dict] = []
        for subreddit, bucket in sorted(data.items()):
            total_records = bucket["n_submissions"] + bucket["n_comments"]
            if total_records < min_activity:
                continue
            rows.append(
                {
                    "subreddit": subreddit,
                    "month": month,
                    "subreddit_id": bucket["subreddit_id"],
                    "n_submissions": bucket["n_submissions"],
                    "n_comments": bucket["n_comments"],
                    "n_unique_authors": len(bucket["authors"]),
                    "total_records": total_records,
                    "over_18": bucket["over_18"],
                    "subreddit_subscribers": bucket["subreddit_subscribers"],
                }
            )
        return rows

    def _combine_catalogue(self) -> List[dict]:
        rows: List[dict] = []
        for month_path in sorted((self.output_path / "months").glob("*.parquet")):
            rows.extend(read_parquet_records(month_path))

        write_parquet_records(
            self.layout.combined_catalogue_path,
            rows,
            CATALOGUE_MONTH_SCHEMA,
            CATALOGUE_MONTH_SPECS,
        )
        return rows

    def _write_index_from_rows(self, rows: List[dict], min_records: int = 1) -> int:
        aggregated: Dict[str, dict] = {}

        for row in rows:
            subreddit = row["subreddit"]
            entry = aggregated.get(subreddit)
            if entry is None:
                entry = {
                    "subreddit": subreddit,
                    "subreddit_id": row.get("subreddit_id") or "",
                    "n_submissions": 0,
                    "n_comments": 0,
                    "total_records": 0,
                    "months": [],
                    "over_18": False,
                    "subreddit_subscribers": 0,
                }
                aggregated[subreddit] = entry

            entry["n_submissions"] += int(row.get("n_submissions") or 0)
            entry["n_comments"] += int(row.get("n_comments") or 0)
            entry["total_records"] += int(row.get("total_records") or 0)
            entry["months"].append(row["month"])
            if not entry["subreddit_id"] and row.get("subreddit_id"):
                entry["subreddit_id"] = row["subreddit_id"]
            if row.get("over_18"):
                entry["over_18"] = True
            subscribers = int(row.get("subreddit_subscribers") or 0)
            if subscribers > entry["subreddit_subscribers"]:
                entry["subreddit_subscribers"] = subscribers

        index_rows: List[dict] = []
        for subreddit, entry in sorted(aggregated.items()):
            if entry["total_records"] < min_records:
                continue
            months = sorted(set(entry["months"]))
            index_rows.append(
                {
                    "subreddit": subreddit,
                    "subreddit_id": entry["subreddit_id"],
                    "n_submissions": entry["n_submissions"],
                    "n_comments": entry["n_comments"],
                    "total_records": entry["total_records"],
                    "first_month": months[0] if months else "",
                    "last_month": months[-1] if months else "",
                    "months_active": len(months),
                    "over_18": entry["over_18"],
                    "subreddit_subscribers": entry["subreddit_subscribers"],
                }
            )

        write_parquet_records(
            self.layout.subreddit_index_path,
            index_rows,
            SUBREDDIT_INDEX_SCHEMA,
            SUBREDDIT_INDEX_SPECS,
        )
        return len(index_rows)

    def run(
        self,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
        min_activity: int = 1,
    ) -> dict:
        started = time.time()
        archives = discover_archives(self.archive_path)
        if not archives:
            logger.warning("No archives found in %s", self.archive_path)
            return {"months_processed": 0, "subreddits_seen": 0, "rows_written": 0, "output_path": str(self.output_path)}

        months = get_months_in_range(archives, start_month, end_month)
        pending = [month for month in months if month not in self._completed_months()]
        if not pending:
            logger.info("All months already catalogued; rebuilding combined outputs")
        else:
            logger.info("Cataloguing %s month(s) into %s", len(pending), self.output_path)

        months_processed = 0
        rows_written = 0
        subreddits_seen: Set[str] = set()

        for month, comments_file, submissions_file in iter_archive_pairs(archives):
            if month not in pending:
                continue

            logger.info("=== Cataloguing %s ===", month)
            month_started = time.time()
            rows = self._process_month(month, comments_file, submissions_file, min_activity=min_activity)
            write_parquet_records(
                self.layout.month_data_path(month),
                rows,
                CATALOGUE_MONTH_SCHEMA,
                CATALOGUE_MONTH_SPECS,
            )
            write_json(
                self.layout.month_metadata_path(month),
                {
                    "month": month,
                    "catalogue_version": CATALOGUE_VERSION,
                    "rows_written": len(rows),
                    "subreddits_seen": len({row["subreddit"] for row in rows}),
                    "duration_seconds": round(time.time() - month_started, 3),
                    "completed_at": timestamp_str(),
                },
            )
            months_processed += 1
            rows_written += len(rows)
            subreddits_seen.update(row["subreddit"] for row in rows)
            logger.info("  %s rows written for %s (%s)", f"{len(rows):,}", month, format_duration(time.time() - month_started))

        combined_rows = self._combine_catalogue()
        index_count = self._write_index_from_rows(combined_rows)
        duration = time.time() - started

        write_json(
            self.layout.metadata_path,
            {
                "catalogue_version": CATALOGUE_VERSION,
                "months_processed": months_processed,
                "rows_written": len(combined_rows),
                "subreddits_seen": len({row["subreddit"] for row in combined_rows}),
                "subreddit_index_rows": index_count,
                "archive_path": str(self.archive_path),
                "completed_at": timestamp_str(),
                "duration_seconds": round(duration, 3),
            },
        )

        logger.info(
            "Catalogue complete: %s months, %s rows, %s indexed subreddits in %s",
            months_processed,
            f"{len(combined_rows):,}",
            f"{index_count:,}",
            format_duration(duration),
        )
        return {
            "months_processed": months_processed,
            "subreddits_seen": len({row["subreddit"] for row in combined_rows}),
            "rows_written": len(combined_rows),
            "output_path": str(self.output_path),
        }


class SubredditIndex:
    """
    Rebuild a per-subreddit aggregate index from an existing catalogue root.
    """

    def __init__(self, catalogue_path: Path):
        self.catalogue_path = Path(catalogue_path)
        self.layout = CatalogueLayout(self.catalogue_path)

    def build(self, min_records: int = 1) -> dict:
        rows = read_parquet_records(self.layout.combined_catalogue_path)
        if not rows:
            rows = []
            for month_file in sorted((self.catalogue_path / "months").glob("*.parquet")):
                rows.extend(read_parquet_records(month_file))

        aggregated: Dict[str, dict] = {}
        for row in rows:
            subreddit = row["subreddit"]
            entry = aggregated.get(subreddit)
            if entry is None:
                entry = {
                    "subreddit": subreddit,
                    "subreddit_id": row.get("subreddit_id") or "",
                    "n_submissions": 0,
                    "n_comments": 0,
                    "total_records": 0,
                    "months": [],
                    "over_18": False,
                    "subreddit_subscribers": 0,
                }
                aggregated[subreddit] = entry

            entry["n_submissions"] += int(row.get("n_submissions") or 0)
            entry["n_comments"] += int(row.get("n_comments") or 0)
            entry["total_records"] += int(row.get("total_records") or 0)
            entry["months"].append(row["month"])
            if row.get("over_18"):
                entry["over_18"] = True
            subscribers = int(row.get("subreddit_subscribers") or 0)
            if subscribers > entry["subreddit_subscribers"]:
                entry["subreddit_subscribers"] = subscribers

        index_rows = []
        for subreddit, entry in sorted(aggregated.items()):
            if entry["total_records"] < min_records:
                continue
            months = sorted(set(entry["months"]))
            index_rows.append(
                {
                    "subreddit": subreddit,
                    "subreddit_id": entry["subreddit_id"],
                    "n_submissions": entry["n_submissions"],
                    "n_comments": entry["n_comments"],
                    "total_records": entry["total_records"],
                    "first_month": months[0] if months else "",
                    "last_month": months[-1] if months else "",
                    "months_active": len(months),
                    "over_18": entry["over_18"],
                    "subreddit_subscribers": entry["subreddit_subscribers"],
                }
            )

        write_parquet_records(
            self.layout.subreddit_index_path,
            index_rows,
            SUBREDDIT_INDEX_SCHEMA,
            SUBREDDIT_INDEX_SPECS,
        )
        return {
            "subreddits": len(index_rows),
            "output_path": str(self.layout.subreddit_index_path),
        }
