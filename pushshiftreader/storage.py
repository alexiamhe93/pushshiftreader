"""
Canonical dataset storage and Parquet helpers.

This module defines the package's research-facing storage contract:

- extracted subreddit corpora are stored under a dataset root
- monthly submissions/comments/authors/thread rows are stored as Parquet
- metadata and month checkpoints are stored as JSON
- keyword-tracking runs use a separate, but similar, layout
"""

from dataclasses import fields
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union, get_args, get_origin

from .models import Comment, Submission
from .utils import ensure_directory

DATASET_VERSION = "1.0"
TRACKING_VERSION = "1.0"
CATALOGUE_VERSION = "1.0"
CROSSSUB_VERSION = "1.0"


def _load_pyarrow():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ImportError(
            "pyarrow is required for canonical Parquet storage. "
            "Install it with: pip install pyarrow"
        ) from exc
    return pa, pq


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path = Path(path)
    ensure_directory(path.parent)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def read_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _strip_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin is Union:
        args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _annotation_to_arrow(annotation: Any):
    pa, _ = _load_pyarrow()
    annotation = _strip_optional(annotation)
    origin = get_origin(annotation)

    if annotation is bool:
        return pa.bool_()
    if annotation is int:
        return pa.int64()
    if annotation is float:
        return pa.float64()
    if annotation is str:
        return pa.string()

    if origin in (list, List, dict, Dict, tuple, Tuple):
        return pa.string()

    return pa.string()


def _field_specs(model_cls) -> List[Tuple[str, Any]]:
    specs = []
    for item in fields(model_cls):
        if item.name == "_extra":
            continue
        specs.append((item.name, _annotation_to_arrow(item.type)))
    return specs


SUBMISSION_FIELD_SPECS = _field_specs(Submission)
COMMENT_FIELD_SPECS = _field_specs(Comment)
SUBMISSION_FIELDS = [name for name, _ in SUBMISSION_FIELD_SPECS]
COMMENT_FIELDS = [name for name, _ in COMMENT_FIELD_SPECS]


def schema_from_specs(specs: List[Tuple[str, Any]]):
    pa, _ = _load_pyarrow()
    return pa.schema([pa.field(name, dtype) for name, dtype in specs])


SUBMISSION_SCHEMA = schema_from_specs(SUBMISSION_FIELD_SPECS)
COMMENT_SCHEMA = schema_from_specs(COMMENT_FIELD_SPECS)

AUTHOR_MONTH_SCHEMA = schema_from_specs([
    ("author", _annotation_to_arrow(str)),
    ("comment_count", _annotation_to_arrow(int)),
    ("submission_count", _annotation_to_arrow(int)),
    ("comment_score_total", _annotation_to_arrow(int)),
    ("avg_comment_score", _annotation_to_arrow(float)),
    ("submission_score_total", _annotation_to_arrow(int)),
    ("avg_submission_score", _annotation_to_arrow(float)),
    ("first_seen_utc", _annotation_to_arrow(int)),
    ("last_seen_utc", _annotation_to_arrow(int)),
])

AUTHOR_SUMMARY_SCHEMA = AUTHOR_MONTH_SCHEMA

THREAD_ROW_SCHEMA = schema_from_specs(
    COMMENT_FIELD_SPECS + [
        ("submission_id", _annotation_to_arrow(str)),
        ("parent_comment_id", _annotation_to_arrow(str)),
        ("parent_author", _annotation_to_arrow(str)),
        ("submission_author", _annotation_to_arrow(str)),
        ("submission_title", _annotation_to_arrow(str)),
        ("submission_created_utc", _annotation_to_arrow(int)),
        ("depth", _annotation_to_arrow(int)),
        ("thread_size", _annotation_to_arrow(int)),
        ("time_since_submission", _annotation_to_arrow(int)),
        ("missing_parent", _annotation_to_arrow(bool)),
        ("is_top_level", _annotation_to_arrow(bool)),
    ]
)

MATCH_COMMENT_SCHEMA = schema_from_specs(
    COMMENT_FIELD_SPECS + [
        ("record_type", _annotation_to_arrow(str)),
        ("keyword_set", _annotation_to_arrow(str)),
        ("matched_terms", _annotation_to_arrow(str)),
        ("match_count", _annotation_to_arrow(int)),
    ]
)

MATCH_SUBMISSION_SCHEMA = schema_from_specs(
    SUBMISSION_FIELD_SPECS + [
        ("record_type", _annotation_to_arrow(str)),
        ("keyword_set", _annotation_to_arrow(str)),
        ("matched_terms", _annotation_to_arrow(str)),
        ("match_count", _annotation_to_arrow(int)),
    ]
)

MONTHLY_COUNT_SCHEMA = schema_from_specs([
    ("month", _annotation_to_arrow(str)),
    ("subreddit", _annotation_to_arrow(str)),
    ("record_type", _annotation_to_arrow(str)),
    ("keyword_set", _annotation_to_arrow(str)),
    ("matched_records", _annotation_to_arrow(int)),
    ("total_matches", _annotation_to_arrow(int)),
])

TERM_COUNT_SCHEMA = schema_from_specs([
    ("month", _annotation_to_arrow(str)),
    ("subreddit", _annotation_to_arrow(str)),
    ("record_type", _annotation_to_arrow(str)),
    ("keyword_set", _annotation_to_arrow(str)),
    ("term", _annotation_to_arrow(str)),
    ("matched_records", _annotation_to_arrow(int)),
    ("total_matches", _annotation_to_arrow(int)),
])

CATALOGUE_MONTH_SCHEMA = schema_from_specs([
    ("subreddit", _annotation_to_arrow(str)),
    ("month", _annotation_to_arrow(str)),
    ("subreddit_id", _annotation_to_arrow(str)),
    ("n_submissions", _annotation_to_arrow(int)),
    ("n_comments", _annotation_to_arrow(int)),
    ("n_unique_authors", _annotation_to_arrow(int)),
    ("total_records", _annotation_to_arrow(int)),
    ("over_18", _annotation_to_arrow(bool)),
    ("subreddit_subscribers", _annotation_to_arrow(int)),
])

SUBREDDIT_INDEX_SCHEMA = schema_from_specs([
    ("subreddit", _annotation_to_arrow(str)),
    ("subreddit_id", _annotation_to_arrow(str)),
    ("n_submissions", _annotation_to_arrow(int)),
    ("n_comments", _annotation_to_arrow(int)),
    ("total_records", _annotation_to_arrow(int)),
    ("first_month", _annotation_to_arrow(str)),
    ("last_month", _annotation_to_arrow(str)),
    ("months_active", _annotation_to_arrow(int)),
    ("over_18", _annotation_to_arrow(bool)),
    ("subreddit_subscribers", _annotation_to_arrow(int)),
])

CROSSSUB_ACTIVITY_SCHEMA = schema_from_specs([
    ("author", _annotation_to_arrow(str)),
    ("subreddit", _annotation_to_arrow(str)),
    ("comment_count", _annotation_to_arrow(int)),
    ("submission_count", _annotation_to_arrow(int)),
    ("comment_score_total", _annotation_to_arrow(int)),
    ("avg_comment_score", _annotation_to_arrow(float)),
    ("submission_score_total", _annotation_to_arrow(int)),
    ("avg_submission_score", _annotation_to_arrow(float)),
    ("first_seen_utc", _annotation_to_arrow(int)),
    ("last_seen_utc", _annotation_to_arrow(int)),
])

CROSSSUB_SUMMARY_SCHEMA = schema_from_specs([
    ("author", _annotation_to_arrow(str)),
    ("n_subreddits", _annotation_to_arrow(int)),
    ("subreddits", _annotation_to_arrow(str)),
    ("total_comments", _annotation_to_arrow(int)),
    ("total_submissions", _annotation_to_arrow(int)),
    ("first_seen_utc", _annotation_to_arrow(int)),
    ("last_seen_utc", _annotation_to_arrow(int)),
])


def _coerce_scalar(value: Any, dtype) -> Any:
    pa, _ = _load_pyarrow()

    if pa.types.is_string(dtype):
        if value is None:
            return None
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return str(value)

    if value in (None, ""):
        return None

    if pa.types.is_boolean(dtype):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in ("true", "1", "yes"):
                return True
            if lowered in ("false", "0", "no"):
                return False
            return None
        return bool(value)

    if pa.types.is_integer(dtype):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    if pa.types.is_floating(dtype):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return value


def normalize_row(record: Dict[str, Any], specs: List[Tuple[str, Any]]) -> Dict[str, Any]:
    return {
        name: _coerce_scalar(record.get(name), dtype)
        for name, dtype in specs
    }


def empty_table(schema):
    pa, _ = _load_pyarrow()
    arrays = [pa.array([], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


class ParquetRecordWriter:
    """
    Buffered Parquet writer for predictable, schema-driven row storage.

    The writer uses a fixed schema so archive extraction can stream records
    directly to disk without keeping whole months in memory.
    """

    def __init__(self, file_path: Path, schema, specs: List[Tuple[str, Any]], batch_size: int = 5000):
        self.file_path = Path(file_path)
        self.schema = schema
        self.specs = specs
        self.batch_size = batch_size
        self._buffer: List[Dict[str, Any]] = []
        self._writer = None

    def __enter__(self):
        ensure_directory(self.file_path.parent)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
        if self._writer is not None:
            self._writer.close()
        return False

    def write(self, record: Dict[str, Any]) -> None:
        self._buffer.append(normalize_row(record, self.specs))
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return

        pa, pq = _load_pyarrow()
        table = pa.Table.from_pylist(self._buffer, schema=self.schema)

        if self._writer is None:
            self._writer = pq.ParquetWriter(
                self.file_path,
                self.schema,
                compression="zstd",
            )
        self._writer.write_table(table)
        self._buffer.clear()


def write_parquet_records(
    file_path: Path,
    records: Iterable[Dict[str, Any]],
    schema,
    specs: List[Tuple[str, Any]],
) -> None:
    rows = [normalize_row(record, specs) for record in records]
    pa, pq = _load_pyarrow()
    ensure_directory(Path(file_path).parent)
    table = pa.Table.from_pylist(rows, schema=schema) if rows else empty_table(schema)
    pq.write_table(table, file_path, compression="zstd")


def read_parquet_records(file_path: Path) -> List[Dict[str, Any]]:
    pa, pq = _load_pyarrow()
    file_path = Path(file_path)
    if not file_path.exists():
        return []
    return pq.read_table(file_path).to_pylist()


class CorpusLayout:
    """Directory layout for one canonical subreddit corpus."""

    def __init__(self, root: Path):
        self.root = Path(root)

    @property
    def dataset_metadata_path(self) -> Path:
        return self.root / "dataset.json"

    @property
    def authors_summary_path(self) -> Path:
        return self.root / "authors_summary.parquet"

    def submissions_path(self, month: str) -> Path:
        return self.root / "submissions" / f"{month}.parquet"

    def comments_path(self, month: str) -> Path:
        return self.root / "comments" / f"{month}.parquet"

    def authors_path(self, month: str) -> Path:
        return self.root / "authors" / f"{month}.parquet"

    def threads_path(self, month: str) -> Path:
        return self.root / "threads" / f"{month}.parquet"

    def signals_path(self, month: str) -> Path:
        return self.root / "signals" / f"{month}.parquet"

    def signals_metadata_path(self, month: str) -> Path:
        return self.root / "signals" / f"{month}.json"

    @property
    def signals_summary_path(self) -> Path:
        return self.root / "signals.json"

    def month_metadata_path(self, month: str) -> Path:
        return self.root / "months" / f"{month}.json"

    def month_metadata_files(self) -> List[Path]:
        months_dir = self.root / "months"
        if not months_dir.exists():
            return []
        return sorted(months_dir.glob("*.json"))


class TrackingLayout:
    """Directory layout for a keyword-tracking run."""

    def __init__(self, root: Path):
        self.root = Path(root)

    @property
    def metadata_path(self) -> Path:
        return self.root / "tracking.json"

    def comments_match_path(self, month: str) -> Path:
        return self.root / "matches" / "comments" / f"{month}.parquet"

    def submissions_match_path(self, month: str) -> Path:
        return self.root / "matches" / "submissions" / f"{month}.parquet"

    def monthly_counts_path(self, month: str) -> Path:
        return self.root / "aggregates" / "monthly" / f"{month}.parquet"

    def term_counts_path(self, month: str) -> Path:
        return self.root / "aggregates" / "terms" / f"{month}.parquet"

    def month_metadata_path(self, month: str) -> Path:
        return self.root / "months" / f"{month}.json"

    @property
    def combined_monthly_counts_path(self) -> Path:
        return self.root / "aggregates" / "monthly_counts.parquet"

    @property
    def combined_term_counts_path(self) -> Path:
        return self.root / "aggregates" / "term_counts.parquet"

    def month_metadata_files(self) -> List[Path]:
        months_dir = self.root / "months"
        if not months_dir.exists():
            return []
        return sorted(months_dir.glob("*.json"))


class CatalogueLayout:
    """Directory layout for archive-wide subreddit/month catalogue outputs."""

    def __init__(self, root: Path):
        self.root = Path(root)

    @property
    def metadata_path(self) -> Path:
        return self.root / "catalogue.json"

    def month_data_path(self, month: str) -> Path:
        return self.root / "months" / f"{month}.parquet"

    def month_metadata_path(self, month: str) -> Path:
        return self.root / "months" / f"{month}.json"

    @property
    def combined_catalogue_path(self) -> Path:
        return self.root / "catalogue.parquet"

    @property
    def subreddit_index_path(self) -> Path:
        return self.root / "subreddit_index.parquet"

    def month_metadata_files(self) -> List[Path]:
        months_dir = self.root / "months"
        if not months_dir.exists():
            return []
        return sorted(months_dir.glob("*.json"))


class CrossSubLayout:
    """Directory layout for cross-subreddit author overlap outputs."""

    def __init__(self, root: Path):
        self.root = Path(root)

    @property
    def metadata_path(self) -> Path:
        return self.root / "crosssub.json"

    @property
    def activity_path(self) -> Path:
        return self.root / "author_activity.parquet"

    @property
    def summary_path(self) -> Path:
        return self.root / "author_summary.parquet"
