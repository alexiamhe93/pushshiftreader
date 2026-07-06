"""
Scale-tiered extraction units behind one resolver.

Three scales, one API, one provenance contract:

- ``TurnSpec``     — a single comment or submission by id.
- ``ThreadSpec``   — a submission plus its full comment tree.
- ``SubredditSliceSpec`` — one subreddit's corpus over an optional TimeWindow.

A :class:`Selector` resolves specs against an extracted corpus (preferred:
cheap Parquet scans) or raw ``.zst`` archives (fallback: streaming scan with
a fast substring pre-filter). Every resolution returns the matching records
*and* a manifest recording exactly which files were read.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .models import Comment, CommentNode, Submission, Thread
from .provenance import Manifest, manifest_run
from .reader import read_zst_lines
from .storage import CorpusLayout, read_json, read_parquet_records
from .utils import discover_archives
from .windows import TimeWindow

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TurnSpec:
    """A single comment or submission. ``record_type`` narrows the search."""

    record_id: str
    record_type: Optional[str] = None  # "comment" | "submission" | None (either)


@dataclass(frozen=True)
class ThreadSpec:
    """A submission and its full comment tree."""

    submission_id: str


@dataclass(frozen=True)
class SubredditSliceSpec:
    """One subreddit corpus over an optional time window."""

    subreddit: Optional[str] = None  # None = the corpus the Selector points at
    window: Optional[TimeWindow] = None


@dataclass
class Selection:
    """Resolved records plus the manifest describing how they were obtained."""

    records: List[Dict[str, Any]]  # each tagged with "record_type"
    manifest: Manifest

    def to_thread(self) -> Optional[Thread]:
        """Build a nested :class:`Thread` model from thread-shaped records."""
        submission = None
        comments: List[Comment] = []
        for record in self.records:
            if record.get("record_type") == "submission":
                submission = Submission.from_dict(
                    {k: v for k, v in record.items() if k != "record_type"}
                )
            else:
                comments.append(
                    Comment.from_dict({k: v for k, v in record.items() if k != "record_type"})
                )
        if submission is None:
            return None

        nodes = {comment.id: CommentNode(comment=comment) for comment in comments}
        top_level: List[CommentNode] = []
        for comment in comments:
            parent_id = comment.parent_comment_id
            if parent_id and parent_id in nodes:
                nodes[parent_id].replies.append(nodes[comment.id])
            else:
                top_level.append(nodes[comment.id])
        top_level.sort(key=lambda node: node.comment.created_utc)
        return Thread(submission=submission, comments=top_level)

    def write(self, output_dir: Union[str, Path]) -> Path:
        """Write records as JSONL plus the manifest; returns the records path."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        records_path = output_dir / "records.jsonl"
        with open(records_path, "w", encoding="utf-8") as handle:
            for record in self.records:
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        self.manifest.add_output(records_path)
        self.manifest.write(output_dir)
        return records_path


def _tag(record: Dict[str, Any], record_type: str) -> Dict[str, Any]:
    tagged = dict(record)
    tagged["record_type"] = record_type
    return tagged


class Selector:
    """
    Resolve extraction specs against a corpus and/or raw archives.

    Args:
        corpus_root: Path to one extracted subreddit corpus (CorpusLayout).
        archive_path: Root of raw ``comments/`` + ``submissions/`` archives.

    At least one source must be given. The corpus is always tried first.
    """

    def __init__(
        self,
        corpus_root: Optional[Union[str, Path]] = None,
        archive_path: Optional[Union[str, Path]] = None,
    ):
        if corpus_root is None and archive_path is None:
            raise ValueError("Provide corpus_root and/or archive_path")
        self.corpus_root = Path(corpus_root) if corpus_root else None
        self.layout = CorpusLayout(self.corpus_root) if self.corpus_root else None
        self.archive_path = Path(archive_path) if archive_path else None

    # ---- months / archives -------------------------------------------------

    def _corpus_months(self) -> List[str]:
        assert self.layout is not None
        if self.layout.dataset_metadata_path.exists():
            return list(read_json(self.layout.dataset_metadata_path).get("months", []))
        return [path.stem for path in self.layout.month_metadata_files()]

    def _months_in_window(self, window: Optional[TimeWindow]) -> List[str]:
        months = self._corpus_months()
        if window is None:
            return months
        return [month for month in months if window.overlaps_month(month)]

    # ---- public API ----------------------------------------------------------

    def resolve(self, spec) -> Selection:
        if isinstance(spec, TurnSpec):
            return self._resolve_turn(spec)
        if isinstance(spec, ThreadSpec):
            return self._resolve_thread(spec)
        if isinstance(spec, SubredditSliceSpec):
            return self._resolve_slice(spec)
        raise TypeError(f"Unknown spec type: {type(spec).__name__}")

    # ---- turn ----------------------------------------------------------------

    def _resolve_turn(self, spec: TurnSpec) -> Selection:
        with manifest_run(
            "extract-turn",
            params={"record_id": spec.record_id, "record_type": spec.record_type},
        ) as manifest:
            record = None
            if self.layout is not None:
                record = self._turn_from_corpus(spec, manifest)
            if record is None and self.archive_path is not None:
                record = self._turn_from_archives(spec, manifest)
            records = [record] if record else []
            manifest.counts = {"records": len(records)}
        return Selection(records=records, manifest=manifest)

    def _turn_from_corpus(self, spec: TurnSpec, manifest: Manifest) -> Optional[Dict[str, Any]]:
        assert self.layout is not None
        sources = []
        if spec.record_type in (None, "comment"):
            sources.append(("comment", self.layout.comments_path))
        if spec.record_type in (None, "submission"):
            sources.append(("submission", self.layout.submissions_path))

        for month in self._corpus_months():
            for record_type, path_fn in sources:
                path = path_fn(month)
                if not path.exists():
                    continue
                manifest.add_input(path)
                for row in read_parquet_records(path):
                    if row.get("id") == spec.record_id:
                        return _tag(row, record_type)
        return None

    def _turn_from_archives(self, spec: TurnSpec, manifest: Manifest) -> Optional[Dict[str, Any]]:
        assert self.archive_path is not None
        needle = f'"id":"{spec.record_id}"'
        needle_spaced = f'"id": "{spec.record_id}"'
        wanted_types = (
            {"comments", "submissions"}
            if spec.record_type is None
            else {spec.record_type + "s"}
        )
        for archive in discover_archives(self.archive_path):
            if archive.file_type not in wanted_types:
                continue
            manifest.add_input(archive.path)
            record_type = "comment" if archive.file_type == "comments" else "submission"
            for line, _ in read_zst_lines(archive.path):
                if needle not in line and needle_spaced not in line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("id") == spec.record_id:
                    return _tag(record, record_type)
        return None

    # ---- thread ----------------------------------------------------------------

    def _resolve_thread(self, spec: ThreadSpec) -> Selection:
        link_id = f"t3_{spec.submission_id}"
        with manifest_run(
            "extract-thread",
            params={"submission_id": spec.submission_id},
        ) as manifest:
            records: List[Dict[str, Any]] = []
            if self.layout is not None:
                records = self._thread_from_corpus(spec, link_id, manifest)
            if not records and self.archive_path is not None:
                records = self._thread_from_archives(spec, link_id, manifest)
            manifest.counts = {
                "records": len(records),
                "comments": sum(1 for r in records if r["record_type"] == "comment"),
            }
        return Selection(records=records, manifest=manifest)

    def _thread_from_corpus(
        self, spec: ThreadSpec, link_id: str, manifest: Manifest
    ) -> List[Dict[str, Any]]:
        assert self.layout is not None
        records: List[Dict[str, Any]] = []
        for month in self._corpus_months():
            submissions_path = self.layout.submissions_path(month)
            if submissions_path.exists():
                manifest.add_input(submissions_path)
                for row in read_parquet_records(submissions_path):
                    if row.get("id") == spec.submission_id:
                        records.append(_tag(row, "submission"))
            comments_path = self.layout.comments_path(month)
            if comments_path.exists():
                manifest.add_input(comments_path)
                for row in read_parquet_records(comments_path):
                    if (row.get("link_id") or "") == link_id:
                        records.append(_tag(row, "comment"))
        if not any(r["record_type"] == "submission" for r in records):
            return []
        return records

    def _thread_from_archives(
        self, spec: ThreadSpec, link_id: str, manifest: Manifest
    ) -> List[Dict[str, Any]]:
        assert self.archive_path is not None
        records: List[Dict[str, Any]] = []
        submission_needles = (f'"id":"{spec.submission_id}"', f'"id": "{spec.submission_id}"')
        for archive in discover_archives(self.archive_path):
            manifest.add_input(archive.path)
            if archive.file_type == "submissions":
                for line, _ in read_zst_lines(archive.path):
                    if not any(needle in line for needle in submission_needles):
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if record.get("id") == spec.submission_id:
                        records.append(_tag(record, "submission"))
            else:
                for line, _ in read_zst_lines(archive.path):
                    if link_id not in line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if (record.get("link_id") or "") == link_id:
                        records.append(_tag(record, "comment"))
        if not any(r["record_type"] == "submission" for r in records):
            return []
        return records

    # ---- subreddit slice ---------------------------------------------------------

    def _resolve_slice(self, spec: SubredditSliceSpec) -> Selection:
        if self.layout is None:
            raise ValueError(
                "SubredditSliceSpec requires corpus_root; extract the subreddit "
                "first with SubredditExtractor (archive-scale extraction is its job)"
            )
        window = spec.window
        with manifest_run(
            "extract-slice",
            params={
                "corpus_root": str(self.corpus_root),
                "subreddit": spec.subreddit,
                "window": window.to_dict() if window else None,
            },
        ) as manifest:
            records: List[Dict[str, Any]] = []
            for month in self._months_in_window(window):
                for record_type, path_fn in (
                    ("submission", self.layout.submissions_path),
                    ("comment", self.layout.comments_path),
                ):
                    path = path_fn(month)
                    if not path.exists():
                        continue
                    manifest.add_input(path)
                    for row in read_parquet_records(path):
                        if spec.subreddit and (row.get("subreddit") or "").lower() != spec.subreddit.lower():
                            continue
                        if window is not None and not window.contains(row.get("created_utc") or 0):
                            continue
                        records.append(_tag(row, record_type))
            manifest.counts = {
                "records": len(records),
                "submissions": sum(1 for r in records if r["record_type"] == "submission"),
                "comments": sum(1 for r in records if r["record_type"] == "comment"),
            }
        return Selection(records=records, manifest=manifest)
