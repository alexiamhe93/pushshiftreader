"""
Cross-subreddit author index built from canonical corpus author summaries.
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

from .storage import (
    CROSSSUB_ACTIVITY_SCHEMA,
    CROSSSUB_SUMMARY_SCHEMA,
    CROSSSUB_VERSION,
    CrossSubLayout,
    CorpusLayout,
    read_json,
    read_parquet_records,
    write_json,
    write_parquet_records,
)
from .utils import ensure_directory, format_duration, timestamp_str

logger = logging.getLogger(__name__)

_ACTIVITY_SPECS = [(field.name, field.type) for field in CROSSSUB_ACTIVITY_SCHEMA]
_SUMMARY_SPECS = [(field.name, field.type) for field in CROSSSUB_SUMMARY_SCHEMA]


class CrossSubIndex:
    """
    Find authors active across multiple extracted subreddit corpora.

    Reads ``authors_summary.parquet`` from each corpus root and writes:

    - ``author_activity.parquet``: one row per author × subreddit
    - ``author_summary.parquet``: one row per author aggregated across subreddits
    - ``crosssub.json``: run metadata
    """

    def __init__(self, extracted_paths: List[Path]):
        self.extracted_paths = [Path(path) for path in extracted_paths]
        self._index: Dict[str, Dict[str, dict]] = {}
        self._labels: Dict[str, str] = {}

    @classmethod
    def from_directory(
        cls,
        extracted_dir: Path,
        subreddits: Optional[List[str]] = None,
    ) -> "CrossSubIndex":
        extracted_dir = Path(extracted_dir)
        filter_set = {item.lower() for item in subreddits} if subreddits else None
        paths: List[Path] = []

        for candidate in sorted(extracted_dir.iterdir()):
            if not candidate.is_dir():
                continue
            if not (candidate / "authors_summary.parquet").exists():
                continue
            if filter_set is None or candidate.name.lower() in filter_set:
                paths.append(candidate)

        if not paths:
            logger.warning("No corpus directories with authors_summary.parquet found in %s", extracted_dir)
        return cls(paths)

    def build(self, min_subreddits: int = 2) -> "CrossSubIndex":
        started = time.time()
        combined: Dict[str, Dict[str, dict]] = {}

        for path in self.extracted_paths:
            subreddit = path.name
            display_subreddit = self._display_subreddit(path)
            authors_summary_path = path / "authors_summary.parquet"
            if not authors_summary_path.exists():
                logger.warning("No authors_summary.parquet in %s, skipping", path)
                continue

            rows = read_parquet_records(authors_summary_path)
            n_authors = 0
            for row in rows:
                author = (row.get("author") or "").strip()
                if not author or author == "[deleted]":
                    continue
                if author not in combined:
                    combined[author] = {}
                combined[author][subreddit] = dict(row)
                n_authors += 1

            self._labels[subreddit] = display_subreddit
            logger.info("  Read %s authors from r/%s", f"{n_authors:,}", display_subreddit)

        self._index = {
            author: by_subreddit
            for author, by_subreddit in combined.items()
            if len(by_subreddit) >= min_subreddits
        }
        logger.info(
            "Cross-sub index built: %s authors in >= %s subreddits (%s)",
            f"{len(self._index):,}",
            min_subreddits,
            format_duration(time.time() - started),
        )
        return self

    def save(self, output_dir: Path) -> dict:
        layout = CrossSubLayout(ensure_directory(Path(output_dir)))
        activity_rows: List[dict] = []
        summary_rows: List[dict] = []

        for author in sorted(self._index):
            subreddits = self._index[author]
            total_comments = 0
            total_submissions = 0
            first_seen_utc = None
            last_seen_utc = None

            for subreddit, stats in sorted(subreddits.items()):
                display_subreddit = self._labels.get(subreddit, subreddit)
                row = {
                    "author": author,
                    "subreddit": display_subreddit,
                    "comment_count": int(stats.get("comment_count") or 0),
                    "submission_count": int(stats.get("submission_count") or 0),
                    "comment_score_total": int(stats.get("comment_score_total") or 0),
                    "avg_comment_score": stats.get("avg_comment_score"),
                    "submission_score_total": int(stats.get("submission_score_total") or 0),
                    "avg_submission_score": stats.get("avg_submission_score"),
                    "first_seen_utc": int(stats.get("first_seen_utc") or 0),
                    "last_seen_utc": int(stats.get("last_seen_utc") or 0),
                }
                activity_rows.append(row)

                total_comments += row["comment_count"]
                total_submissions += row["submission_count"]
                if first_seen_utc is None or (row["first_seen_utc"] and row["first_seen_utc"] < first_seen_utc):
                    first_seen_utc = row["first_seen_utc"]
                if last_seen_utc is None or row["last_seen_utc"] > last_seen_utc:
                    last_seen_utc = row["last_seen_utc"]

            summary_rows.append(
                {
                    "author": author,
                    "n_subreddits": len(subreddits),
                    "subreddits": "|".join(
                        sorted(
                            (self._labels.get(subreddit, subreddit) for subreddit in subreddits),
                            key=str.lower,
                        )
                    ),
                    "total_comments": total_comments,
                    "total_submissions": total_submissions,
                    "first_seen_utc": first_seen_utc or 0,
                    "last_seen_utc": last_seen_utc or 0,
                }
            )

        write_parquet_records(
            layout.activity_path,
            activity_rows,
            CROSSSUB_ACTIVITY_SCHEMA,
            _ACTIVITY_SPECS,
        )
        write_parquet_records(
            layout.summary_path,
            summary_rows,
            CROSSSUB_SUMMARY_SCHEMA,
            _SUMMARY_SPECS,
        )
        write_json(
            layout.metadata_path,
            {
                "crosssub_version": CROSSSUB_VERSION,
                "authors": len(summary_rows),
                "pairs": len(activity_rows),
                "source_paths": [str(path) for path in self.extracted_paths],
                "completed_at": timestamp_str(),
            },
        )

        logger.info("Saved %s authors (%s pairs) to %s", f"{len(summary_rows):,}", f"{len(activity_rows):,}", output_dir)
        return {"authors": len(summary_rows), "pairs": len(activity_rows)}

    def _display_subreddit(self, path: Path) -> str:
        layout = CorpusLayout(path)

        for month_file in sorted((path / "submissions").glob("*.parquet")):
            rows = read_parquet_records(month_file)
            if rows and rows[0].get("subreddit"):
                return rows[0]["subreddit"]

        for month_file in sorted((path / "comments").glob("*.parquet")):
            rows = read_parquet_records(month_file)
            if rows and rows[0].get("subreddit"):
                return rows[0]["subreddit"]

        if layout.dataset_metadata_path.exists():
            payload = read_json(layout.dataset_metadata_path)
            if payload.get("subreddit"):
                return payload["subreddit"]

        return path.name
