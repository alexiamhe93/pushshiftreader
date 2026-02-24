"""
Signal detection for extracted Reddit data.

Runs Detector instances over thread data (threads.jsonl.gz produced by
TreeBuilder) and writes a per-month signals.csv with one boolean column
per detector.  Only rows where at least one signal fires are written
(sparse output) — a record absent from signals.csv implicitly has all
signals set to False.

Example usage::

    from pushshiftreader import SignalDetector, Detector, RegexDetector

    class DeltaDetector(Detector):
        def detect_comment(self, comment, thread, depth=0):
            return 'Δ' in comment.body or '!delta' in comment.body.lower()

    sd = SignalDetector(
        "./extracted/ChangeMyView",
        detectors=[
            DeltaDetector("delta_awarded"),
            RegexDetector("cites_source", r"https?://|\\[\\d+\\]"),
            AuthorIsOPDetector("op_replied"),
        ],
    )
    sd.run_all_months()
"""

import csv
import logging
import time
from abc import ABC
from pathlib import Path
from typing import Dict, List, Optional

from .models import Comment, Submission, Thread
from .trees import load_threads
from .utils import format_duration

logger = logging.getLogger(__name__)

_BASE_FIELDS = ['record_id', 'record_type']


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

class Detector(ABC):
    """
    Base class for signal detectors.

    Subclass this and implement ``detect_comment`` and/or
    ``detect_submission``.  Whichever method you don't override returns
    ``False`` by default (the signal does not fire for that record type).

    The ``name`` attribute becomes the column header in ``signals.csv``,
    so it should be a valid Python identifier (letters, digits, underscores).

    Example::

        class LongCommentDetector(Detector):
            def detect_comment(self, comment, thread, depth=0):
                return len(comment.body) > 2000
    """

    def __init__(self, name: str):
        """
        Args:
            name: Identifier for this signal.  Used as the column name in
                ``signals.csv``.  Must be unique within a ``SignalDetector``.
        """
        self.name = name

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        """Return True if this signal fires for the given comment."""
        return False

    def detect_submission(self, submission: Submission, thread: Thread, depth: int = 0) -> bool:
        """Return True if this signal fires for the given submission."""
        return False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"


# ---------------------------------------------------------------------------
# Built-in general-purpose detectors
# ---------------------------------------------------------------------------

class RegexDetector(Detector):
    """
    Fires when any of the specified text fields match a regular expression.

    Defaults to searching ``body`` for comments and ``title`` + ``selftext``
    for submissions.  Pass explicit ``fields`` to override.

    Example::

        # Fire when a comment body cites a URL
        RegexDetector("cites_url", r"https?://", record_type="comment")

        # Fire when a submission title contains a question mark
        RegexDetector("question_title", r"\\?", fields=["title"],
                      record_type="submission")

        # Fire on both record types
        RegexDetector("mentions_study", r"\\bstudy\\b", record_type="both")
    """

    def __init__(
        self,
        name: str,
        pattern: str,
        record_type: str = "comment",
        fields: Optional[List[str]] = None,
        case_sensitive: bool = False,
    ):
        """
        Args:
            name: Signal identifier.
            pattern: Regular expression to match.
            record_type: Which record type(s) to apply to:
                ``"comment"``, ``"submission"``, or ``"both"``.
            fields: Field names to search.  Defaults to ``["body"]`` for
                comments and ``["title", "selftext"]`` for submissions.
            case_sensitive: Whether the match is case-sensitive
                (default: ``False``).
        """
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
        return any(
            self._pattern.search(getattr(comment, f, '') or '')
            for f in fields
        )

    def detect_submission(self, submission: Submission, thread: Thread, depth: int = 0) -> bool:
        if self._record_type not in ("submission", "both"):
            return False
        fields = self._fields or ["title", "selftext"]
        return any(
            self._pattern.search(getattr(submission, f, '') or '')
            for f in fields
        )


class ScoreDetector(Detector):
    """
    Fires when a record's score falls within the specified bounds.

    Either ``min_score`` or ``max_score`` (or both) must be provided.

    Example::

        # Comments with score >= 100
        ScoreDetector("highly_upvoted", min_score=100, record_type="comment")

        # Controversial comments (negative score)
        ScoreDetector("negative_score", max_score=-1, record_type="comment")

        # Submissions scoring between 10 and 99
        ScoreDetector("mid_range_submission", min_score=10, max_score=99,
                      record_type="submission")
    """

    def __init__(
        self,
        name: str,
        min_score: Optional[int] = None,
        max_score: Optional[int] = None,
        record_type: str = "both",
    ):
        """
        Args:
            name: Signal identifier.
            min_score: Inclusive lower bound on score.  ``None`` = no lower bound.
            max_score: Inclusive upper bound on score.  ``None`` = no upper bound.
            record_type: ``"comment"``, ``"submission"``, or ``"both"``.
        """
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
    """
    Fires when the comment's author is the original poster (OP) of the thread.

    Useful for identifying when OP participates in the discussion below their
    own post (e.g. defending a position, acknowledging a counter-argument).

    Deleted/unknown authors (``"[deleted]"``) never match.

    Example::

        AuthorIsOPDetector("op_comment")
    """

    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool:
        op = thread.submission.author
        if not op or op == "[deleted]":
            return False
        return comment.author == op


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class SignalDetector:
    """
    Run a set of Detector instances over extracted thread data.

    Reads ``threads.jsonl.gz`` for each month (produced by
    :class:`~pushshiftreader.TreeBuilder`), applies all detectors to every
    comment and submission in each thread, and writes ``signals.csv`` with
    one boolean column per detector.

    Only rows where at least one signal fires are written.  A record absent
    from ``signals.csv`` implicitly has all signals set to ``False``.

    ``TreeBuilder`` must have been run on the subreddit before calling
    :meth:`run_month` or :meth:`run_all_months`.

    Example::

        from pushshiftreader import (
            SignalDetector, Detector, RegexDetector,
            ScoreDetector, AuthorIsOPDetector,
        )

        class DeltaDetector(Detector):
            def detect_comment(self, comment, thread, depth=0):
                return 'Δ' in comment.body or '!delta' in comment.body.lower()

        sd = SignalDetector(
            "./extracted/ChangeMyView",
            detectors=[
                DeltaDetector("delta_awarded"),
                AuthorIsOPDetector("op_comment"),
                ScoreDetector("top_comment", min_score=100, record_type="comment"),
            ],
        )
        results = sd.run_all_months()
        # results == {"2013-01": 42, "2013-02": 87, ...}
    """

    def __init__(self, extracted_path: Path, detectors: List[Detector]):
        """
        Args:
            extracted_path: Path to the extracted subreddit directory
                (the directory that contains per-month subdirectories).
            detectors: List of :class:`Detector` instances to apply.
                Names must be unique across the list.
        """
        self.extracted_path = Path(extracted_path)
        self.detectors = list(detectors)

        if not self.extracted_path.exists():
            raise ValueError(f"Extracted path does not exist: {self.extracted_path}")
        if not self.detectors:
            raise ValueError("At least one Detector must be provided")

        names = [d.name for d in self.detectors]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"Duplicate detector names: {sorted(dupes)}")

    def _get_months(self) -> List[str]:
        """Return sorted list of month directories that contain threads.jsonl.gz."""
        months = []
        for item in sorted(self.extracted_path.iterdir()):
            if item.is_dir() and '-' in item.name:
                if (item / 'threads.jsonl.gz').exists():
                    months.append(item.name)
        return months

    def run_month(self, month: str) -> int:
        """
        Run all detectors on one month's threads and write ``signals.csv``.

        Args:
            month: Month string in ``YYYY-MM`` format.

        Returns:
            Number of rows written to ``signals.csv`` (sparse — only rows
            where at least one signal fired).
        """
        threads_path = self.extracted_path / month / 'threads.jsonl.gz'

        if not threads_path.exists():
            logger.warning(
                f"No threads.jsonl.gz found for {month} — "
                "run TreeBuilder.build_month() first"
            )
            return 0

        start = time.time()
        logger.info(f"Detecting signals for {month}")

        fieldnames = _BASE_FIELDS + [d.name for d in self.detectors]
        output_path = self.extracted_path / month / 'signals.csv'
        rows_written = 0

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for thread in load_threads(threads_path):
                # --- submission ---
                sub_signals = {
                    d.name: d.detect_submission(thread.submission, thread)
                    for d in self.detectors
                }
                if any(sub_signals.values()):
                    writer.writerow({
                        'record_id': thread.submission.id,
                        'record_type': 'submission',
                        **sub_signals,
                    })
                    rows_written += 1

                # --- every comment in the thread (depth-first) ---
                for comment, depth in thread.walk():
                    comment_signals = {
                        d.name: d.detect_comment(comment, thread, depth)
                        for d in self.detectors
                    }
                    if any(comment_signals.values()):
                        writer.writerow({
                            'record_id': comment.id,
                            'record_type': 'comment',
                            **comment_signals,
                        })
                        rows_written += 1

        duration = time.time() - start
        logger.info(
            f"  {rows_written:,} signal rows written "
            f"({format_duration(duration)})"
        )
        return rows_written

    def run_all_months(self) -> Dict[str, int]:
        """
        Run detectors on all months that have ``threads.jsonl.gz``.

        Returns:
            Dict mapping each month string to the number of signal rows written.
        """
        months = self._get_months()
        if not months:
            logger.warning(
                f"No months with threads.jsonl.gz found in {self.extracted_path}. "
                "Run TreeBuilder first."
            )
            return {}

        logger.info(
            f"Running {len(self.detectors)} detector(s) "
            f"[{', '.join(d.name for d in self.detectors)}] "
            f"over {len(months)} months"
        )

        results: Dict[str, int] = {}
        total_start = time.time()

        for month in months:
            results[month] = self.run_month(month)

        total_rows = sum(results.values())
        total_duration = time.time() - total_start
        logger.info(
            f"Done: {total_rows:,} signal rows across {len(months)} months "
            f"({format_duration(total_duration)})"
        )
        return results
