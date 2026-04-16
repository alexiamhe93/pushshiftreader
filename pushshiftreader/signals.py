"""
Signal detection over canonical thread-aware corpora.

Signals are stored sparsely: only records where at least one detector fires are
written to the per-month Parquet file.
"""

import logging
import time
from abc import ABC
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from .models import Comment, Submission, Thread
from .storage import (
    CorpusLayout,
    _annotation_to_arrow,
    read_json,
    read_parquet_records,
    schema_from_specs,
    write_json,
    write_parquet_records,
)
from .trees import load_threads
from .utils import format_duration, timestamp_str

logger = logging.getLogger(__name__)

_BASE_SIGNAL_SPECS = [
    ("record_id", _annotation_to_arrow(str)),
    ("record_type", _annotation_to_arrow(str)),
]
_BASE_FIELDS = ["record_id", "record_type"]


@dataclass
class SignalRunResult:
    months_processed: int
    rows_written: int
    duration_seconds: float
    rows_by_month: Dict[str, int] = field(default_factory=dict)


class Detector(ABC):
    """Base class for signal detectors."""

    def __init__(self, name: str):
        self.name = name

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        return False

    def detect_submission(self, submission: Submission, thread: Thread, depth: int = 0) -> bool:
        return False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"


class RegexDetector(Detector):
    """Fire when a regex matches configured text fields."""

    def __init__(
        self,
        name: str,
        pattern: str,
        record_type: str = "comment",
        fields: Optional[List[str]] = None,
        case_sensitive: bool = False,
    ):
        super().__init__(name)
        import re

        flags = 0 if case_sensitive else re.IGNORECASE
        self._pattern = re.compile(pattern, flags)
        self._record_type = record_type
        self._fields = fields

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        if self._record_type not in ("comment", "both"):
            return False
        fields = self._fields or ["body"]
        return any(self._pattern.search(getattr(comment, field, "") or "") for field in fields)

    def detect_submission(self, submission: Submission, thread: Thread, depth: int = 0) -> bool:
        if self._record_type not in ("submission", "both"):
            return False
        fields = self._fields or ["title", "selftext"]
        return any(self._pattern.search(getattr(submission, field, "") or "") for field in fields)


class ScoreDetector(Detector):
    """Fire when scores fall within configured bounds."""

    def __init__(
        self,
        name: str,
        min_score: Optional[int] = None,
        max_score: Optional[int] = None,
        record_type: str = "both",
    ):
        super().__init__(name)
        if min_score is None and max_score is None:
            raise ValueError("At least one of min_score or max_score must be set")
        self._min = min_score
        self._max = max_score
        self._record_type = record_type

    def _check(self, score: int) -> bool:
        if self._min is not None and score < self._min:
            return False
        if self._max is not None and score > self._max:
            return False
        return True

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        if self._record_type not in ("comment", "both"):
            return False
        return self._check(comment.score)

    def detect_submission(self, submission: Submission, thread: Thread, depth: int = 0) -> bool:
        if self._record_type not in ("submission", "both"):
            return False
        return self._check(submission.score)


class AuthorIsOPDetector(Detector):
    """Fire when the comment author matches the OP."""

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        op = thread.submission.author
        return bool(op and op != "[deleted]" and comment.author == op)


class SignalDetector:
    """
    Run detectors over per-month thread data and persist sparse Parquet signal rows.
    """

    def __init__(self, extracted_path: Path, detectors: List[Detector]):
        self.extracted_path = Path(extracted_path)
        self.layout = CorpusLayout(self.extracted_path)
        self.detectors = list(detectors)

        if not self.extracted_path.exists():
            raise ValueError(f"Extracted path does not exist: {self.extracted_path}")
        if not self.detectors:
            raise ValueError("At least one Detector must be provided")

        names = [detector.name for detector in self.detectors]
        if len(set(names)) != len(names):
            raise ValueError(f"Duplicate detector names: {sorted(name for name in names if names.count(name) > 1)}")

    def _get_months(self) -> List[str]:
        if self.layout.dataset_metadata_path.exists():
            metadata = read_json(self.layout.dataset_metadata_path)
            return [month for month in metadata.get("months", []) if self.layout.threads_path(month).exists()]

        threads_dir = self.extracted_path / "threads"
        if not threads_dir.exists():
            return []
        return sorted(path.stem for path in threads_dir.glob("*.parquet"))

    def _signal_specs(self):
        bool_type = _annotation_to_arrow(bool)
        return _BASE_SIGNAL_SPECS + [(detector.name, bool_type) for detector in self.detectors]

    def run_month(self, month: str, force: bool = False) -> int:
        threads_path = self.layout.threads_path(month)
        if not threads_path.exists():
            logger.warning("No thread table for %s; run TreeBuilder first", month)
            return 0

        signals_path = self.layout.signals_path(month)
        if signals_path.exists() and not force:
            existing = read_parquet_records(signals_path)
            logger.info("Skipping %s: signals already exist", month)
            return len(existing)

        started = time.time()
        logger.info("Detecting signals for %s", month)
        rows: List[dict] = []

        for thread in load_threads(threads_path):
            submission_signals = {
                detector.name: detector.detect_submission(thread.submission, thread)
                for detector in self.detectors
            }
            if any(submission_signals.values()):
                rows.append(
                    {
                        "record_id": thread.submission.id,
                        "record_type": "submission",
                        **submission_signals,
                    }
                )

            for comment, depth in thread.walk():
                comment_signals = {
                    detector.name: detector.detect_comment(comment, thread, depth)
                    for detector in self.detectors
                }
                if any(comment_signals.values()):
                    rows.append(
                        {
                            "record_id": comment.id,
                            "record_type": "comment",
                            **comment_signals,
                        }
                    )

        signal_specs = self._signal_specs()
        signal_schema = schema_from_specs(signal_specs)
        write_parquet_records(
            signals_path,
            rows,
            signal_schema,
            signal_specs,
        )
        write_json(
            self.layout.signals_metadata_path(month),
            {
                "month": month,
                "rows_written": len(rows),
                "detectors": [detector.name for detector in self.detectors],
                "completed_at": timestamp_str(),
                "duration_seconds": round(time.time() - started, 3),
            },
        )
        logger.info("  %s signal rows written (%s)", f"{len(rows):,}", format_duration(time.time() - started))
        return len(rows)

    def run_all_months(self, force: bool = False) -> Dict[str, int]:
        months = self._get_months()
        if not months:
            logger.warning("No months with thread tables found in %s", self.extracted_path)
            return {}

        results: Dict[str, int] = {}
        total_started = time.time()
        for month in months:
            results[month] = self.run_month(month, force=force)

        write_json(
            self.layout.signals_summary_path,
            {
                "months_processed": len(results),
                "rows_written": sum(results.values()),
                "detectors": [detector.name for detector in self.detectors],
                "completed_at": timestamp_str(),
                "duration_seconds": round(time.time() - total_started, 3),
            },
        )
        logger.info(
            "Signal detection complete: %s months, %s rows in %s",
            len(results),
            f"{sum(results.values()):,}",
            format_duration(time.time() - total_started),
        )
        return results
