"""
Keyword-set tracking over canonical subreddit corpora.
"""

from dataclasses import dataclass, field
import json
import logging
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Pattern, Tuple

from .models import Comment, Submission
from .storage import (
    COMMENT_FIELD_SPECS,
    MATCH_COMMENT_SCHEMA,
    MATCH_SUBMISSION_SCHEMA,
    MONTHLY_COUNT_SCHEMA,
    SUBMISSION_FIELD_SPECS,
    TERM_COUNT_SCHEMA,
    TrackingLayout,
    TRACKING_VERSION,
    read_json,
    read_parquet_records,
    write_json,
    write_parquet_records,
)
from .utils import format_duration, timestamp_str

logger = logging.getLogger(__name__)


@dataclass
class KeywordSet:
    name: str
    terms: List[str] = field(default_factory=list)
    regexes: List[str] = field(default_factory=list)
    case_sensitive: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "KeywordSet":
        name = str(data.get("name") or "").strip()
        if not name:
            raise ValueError("Keyword-set config entries require a non-empty 'name'")
        return cls(
            name=name,
            terms=[str(item) for item in data.get("terms", [])],
            regexes=[str(item) for item in data.get("regexes", [])],
            case_sensitive=bool(data.get("case_sensitive", False)),
        )

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "terms": list(self.terms),
            "regexes": list(self.regexes),
            "case_sensitive": self.case_sensitive,
        }

    def compiled_patterns(self) -> List[Tuple[str, Pattern[str]]]:
        flags = 0 if self.case_sensitive else re.IGNORECASE
        compiled: List[Tuple[str, Pattern[str]]] = []
        for term in self.terms:
            compiled.append((term, re.compile(re.escape(term), flags)))
        for pattern in self.regexes:
            compiled.append((pattern, re.compile(pattern, flags)))
        return compiled


@dataclass
class TrackingStats:
    month: str
    comments_matched: int = 0
    submissions_matched: int = 0
    duration_seconds: float = 0.0


@dataclass
class TrackingResult:
    months_processed: int
    total_comments: int
    total_submissions: int
    duration_seconds: float
    stats: List[TrackingStats] = field(default_factory=list)


def load_keyword_sets(config_path: Path) -> List[KeywordSet]:
    config_path = Path(config_path)
    with open(config_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    keyword_sets = payload.get("keyword_sets")
    if not isinstance(keyword_sets, list) or not keyword_sets:
        raise ValueError("Keyword config must contain a non-empty 'keyword_sets' list")

    return [KeywordSet.from_dict(item) for item in keyword_sets]


def merge_keyword_sets(*collections: Iterable[KeywordSet]) -> List[KeywordSet]:
    merged: Dict[str, KeywordSet] = {}

    for collection in collections:
        for item in collection:
            existing = merged.get(item.name)
            if existing is None:
                merged[item.name] = KeywordSet(
                    name=item.name,
                    terms=list(item.terms),
                    regexes=list(item.regexes),
                    case_sensitive=item.case_sensitive,
                )
                continue

            existing.terms = sorted(set(existing.terms) | set(item.terms))
            existing.regexes = sorted(set(existing.regexes) | set(item.regexes))
            existing.case_sensitive = existing.case_sensitive or item.case_sensitive

    return [merged[name] for name in sorted(merged)]


class KeywordTracker:
    """
    Track named keyword sets across one extracted subreddit corpus.
    """

    def __init__(
        self,
        dataset_path: Path,
        output_path: Path,
        keyword_sets: Iterable[KeywordSet],
        force: bool = False,
    ):
        self.dataset_path = Path(dataset_path)
        self.output_path = Path(output_path)
        self.keyword_sets = list(keyword_sets)
        self.force = force

        if not self.dataset_path.exists():
            raise ValueError(f"Dataset path does not exist: {self.dataset_path}")
        if not self.keyword_sets:
            raise ValueError("At least one keyword set must be provided")

        names = [item.name for item in self.keyword_sets]
        if len(set(names)) != len(names):
            raise ValueError("Keyword set names must be unique")

        self.layout = TrackingLayout(self.output_path)
        self._compiled = {
            item.name: item.compiled_patterns()
            for item in self.keyword_sets
        }

    def _dataset_metadata(self) -> Dict[str, object]:
        metadata_path = self.dataset_path / "dataset.json"
        if metadata_path.exists():
            return read_json(metadata_path)
        return {
            "subreddit": self.dataset_path.name,
            "months": [],
        }

    def _month_done(self, month: str) -> bool:
        return self.layout.month_metadata_path(month).exists()

    def _scan_records(
        self,
        month: str,
        records: List[dict],
        record_type: str,
    ) -> Tuple[List[dict], List[dict], List[dict], int]:
        monthly_counts = defaultdict(lambda: {"matched_records": 0, "total_matches": 0})
        term_counts = defaultdict(lambda: {"matched_records": 0, "total_matches": 0})
        matches: List[dict] = []
        matched_records = 0

        for record in records:
            subreddit = record.get("subreddit", "")
            text = (
                record.get("body") or ""
                if record_type == "comment"
                else f"{record.get('title') or ''} {record.get('selftext') or ''}"
            )

            for keyword_set in self.keyword_sets:
                matched_terms: List[str] = []
                total_matches = 0

                for label, pattern in self._compiled[keyword_set.name]:
                    matches_for_term = len(pattern.findall(text))
                    if matches_for_term:
                        matched_terms.append(label)
                        total_matches += matches_for_term

                        term_bucket = term_counts[(month, subreddit, record_type, keyword_set.name, label)]
                        term_bucket["matched_records"] += 1
                        term_bucket["total_matches"] += matches_for_term

                if not matched_terms:
                    continue

                row = {
                    **record,
                    "record_type": record_type,
                    "keyword_set": keyword_set.name,
                    "matched_terms": json.dumps(sorted(matched_terms), ensure_ascii=False),
                    "match_count": total_matches,
                }
                matches.append(row)
                matched_records += 1

                month_bucket = monthly_counts[(month, subreddit, record_type, keyword_set.name)]
                month_bucket["matched_records"] += 1
                month_bucket["total_matches"] += total_matches

        monthly_rows = [
            {
                "month": key[0],
                "subreddit": key[1],
                "record_type": key[2],
                "keyword_set": key[3],
                "matched_records": value["matched_records"],
                "total_matches": value["total_matches"],
            }
            for key, value in sorted(monthly_counts.items())
        ]
        term_rows = [
            {
                "month": key[0],
                "subreddit": key[1],
                "record_type": key[2],
                "keyword_set": key[3],
                "term": key[4],
                "matched_records": value["matched_records"],
                "total_matches": value["total_matches"],
            }
            for key, value in sorted(term_counts.items())
        ]

        return matches, monthly_rows, term_rows, matched_records

    def _combine_aggregate_files(self) -> None:
        monthly_rows: List[dict] = []
        term_rows: List[dict] = []

        monthly_dir = self.output_path / "aggregates" / "monthly"
        if monthly_dir.exists():
            for file_path in sorted(monthly_dir.glob("*.parquet")):
                monthly_rows.extend(read_parquet_records(file_path))

        terms_dir = self.output_path / "aggregates" / "terms"
        if terms_dir.exists():
            for file_path in sorted(terms_dir.glob("*.parquet")):
                term_rows.extend(read_parquet_records(file_path))

        write_parquet_records(
            self.layout.combined_monthly_counts_path,
            monthly_rows,
            MONTHLY_COUNT_SCHEMA,
            [(field.name, field.type) for field in MONTHLY_COUNT_SCHEMA],
        )
        write_parquet_records(
            self.layout.combined_term_counts_path,
            term_rows,
            TERM_COUNT_SCHEMA,
            [(field.name, field.type) for field in TERM_COUNT_SCHEMA],
        )

    def run(
        self,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
    ) -> TrackingResult:
        metadata = self._dataset_metadata()
        months = list(metadata.get("months", []))
        if start_month:
            months = [month for month in months if month >= start_month]
        if end_month:
            months = [month for month in months if month <= end_month]

        started = time.time()
        stats: List[TrackingStats] = []
        total_comments = 0
        total_submissions = 0

        for month in months:
            if not self.force and self._month_done(month):
                logger.info("Skipping %s: keyword tracking already complete", month)
                continue

            logger.info("=== Tracking %s ===", month)
            month_started = time.time()
            comments_path = self.dataset_path / "comments" / f"{month}.parquet"
            submissions_path = self.dataset_path / "submissions" / f"{month}.parquet"

            comment_records = read_parquet_records(comments_path)
            submission_records = read_parquet_records(submissions_path)

            comment_matches, comment_monthly_rows, comment_term_rows, comments_matched = self._scan_records(
                month,
                comment_records,
                "comment",
            )
            submission_matches, submission_monthly_rows, submission_term_rows, submissions_matched = self._scan_records(
                month,
                submission_records,
                "submission",
            )

            write_parquet_records(
                self.layout.comments_match_path(month),
                comment_matches,
                MATCH_COMMENT_SCHEMA,
                COMMENT_FIELD_SPECS + [
                    ("record_type", MATCH_COMMENT_SCHEMA.field("record_type").type),
                    ("keyword_set", MATCH_COMMENT_SCHEMA.field("keyword_set").type),
                    ("matched_terms", MATCH_COMMENT_SCHEMA.field("matched_terms").type),
                    ("match_count", MATCH_COMMENT_SCHEMA.field("match_count").type),
                ],
            )
            write_parquet_records(
                self.layout.submissions_match_path(month),
                submission_matches,
                MATCH_SUBMISSION_SCHEMA,
                SUBMISSION_FIELD_SPECS + [
                    ("record_type", MATCH_SUBMISSION_SCHEMA.field("record_type").type),
                    ("keyword_set", MATCH_SUBMISSION_SCHEMA.field("keyword_set").type),
                    ("matched_terms", MATCH_SUBMISSION_SCHEMA.field("matched_terms").type),
                    ("match_count", MATCH_SUBMISSION_SCHEMA.field("match_count").type),
                ],
            )
            write_parquet_records(
                self.layout.monthly_counts_path(month),
                comment_monthly_rows + submission_monthly_rows,
                MONTHLY_COUNT_SCHEMA,
                [(field.name, field.type) for field in MONTHLY_COUNT_SCHEMA],
            )
            write_parquet_records(
                self.layout.term_counts_path(month),
                comment_term_rows + submission_term_rows,
                TERM_COUNT_SCHEMA,
                [(field.name, field.type) for field in TERM_COUNT_SCHEMA],
            )

            duration = time.time() - month_started
            write_json(
                self.layout.month_metadata_path(month),
                {
                    "month": month,
                    "dataset_path": str(self.dataset_path),
                    "tracking_version": TRACKING_VERSION,
                    "comments_matched": comments_matched,
                    "submissions_matched": submissions_matched,
                    "keyword_sets": [
                        item.to_dict()
                        for item in self.keyword_sets
                    ],
                    "completed_at": timestamp_str(),
                    "duration_seconds": round(duration, 3),
                },
            )

            stats.append(
                TrackingStats(
                    month=month,
                    comments_matched=comments_matched,
                    submissions_matched=submissions_matched,
                    duration_seconds=duration,
                )
            )
            total_comments += comments_matched
            total_submissions += submissions_matched

        self._combine_aggregate_files()
        duration = time.time() - started
        write_json(
            self.layout.metadata_path,
            {
                "tracking_version": TRACKING_VERSION,
                "dataset_path": str(self.dataset_path),
                "dataset_subreddit": metadata.get("subreddit"),
                "months_processed": len(stats),
                "total_comments": total_comments,
                "total_submissions": total_submissions,
                "duration_seconds": round(duration, 3),
                "keyword_sets": [
                    item.to_dict()
                    for item in self.keyword_sets
                ],
                "completed_at": timestamp_str(),
            },
        )

        logger.info(
            "Keyword tracking complete: %s months, %s comments, %s submissions in %s",
            len(stats),
            f"{total_comments:,}",
            f"{total_submissions:,}",
            format_duration(duration),
        )
        return TrackingResult(
            months_processed=len(stats),
            total_comments=total_comments,
            total_submissions=total_submissions,
            duration_seconds=duration,
            stats=stats,
        )
