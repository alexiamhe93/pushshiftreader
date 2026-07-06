"""
Time-windowed slicing of canonical corpora.

Time is a first-class axis for diachronic work: this module turns a monthly
extracted corpus into per-epoch (month / quarter / year) Parquet slices under
``<corpus>/slices/<granularity>/<epoch>/``, so downstream per-period analysis
(diachronic embeddings, temporal search) reads one directory per epoch.
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Union

from .provenance import Manifest, manifest_run
from .storage import CorpusLayout, read_json, write_json
from .utils import ensure_directory, format_duration

logger = logging.getLogger(__name__)

GRANULARITIES = ("month", "quarter", "year")


def epoch_of(created_utc: int, granularity: str = "month") -> str:
    """Calendar epoch label (UTC) for a unix timestamp."""
    moment = datetime.fromtimestamp(int(created_utc), tz=timezone.utc)
    return _epoch_label(moment.year, moment.month, granularity)


def month_to_epoch(month: str, granularity: str = "month") -> str:
    """Map a ``YYYY-MM`` month string to its epoch label."""
    year, mon = int(month[:4]), int(month[5:7])
    return _epoch_label(year, mon, granularity)


def _epoch_label(year: int, month: int, granularity: str) -> str:
    if granularity == "month":
        return f"{year}-{month:02d}"
    if granularity == "quarter":
        return f"{year}-Q{(month - 1) // 3 + 1}"
    if granularity == "year":
        return str(year)
    raise ValueError(f"Unknown granularity {granularity!r}; expected one of {GRANULARITIES}")


def _month_bounds_utc(month: str) -> "tuple[int, int]":
    """(start, end) unix timestamps for a YYYY-MM month; end is exclusive."""
    year, mon = int(month[:4]), int(month[5:7])
    start = datetime(year, mon, 1, tzinfo=timezone.utc)
    if mon == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, mon + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp()), int(end.timestamp())


@dataclass(frozen=True)
class TimeWindow:
    """
    Half-open time window ``[start_utc, end_utc)`` over ``created_utc``.

    Either bound may be ``None`` (unbounded on that side).
    """

    start_utc: Optional[int] = None
    end_utc: Optional[int] = None

    @classmethod
    def from_months(cls, start_month: Optional[str] = None, end_month: Optional[str] = None) -> "TimeWindow":
        """Build a window from inclusive ``YYYY-MM`` month bounds."""
        start = _month_bounds_utc(start_month)[0] if start_month else None
        end = _month_bounds_utc(end_month)[1] if end_month else None
        return cls(start_utc=start, end_utc=end)

    def contains(self, created_utc: int) -> bool:
        ts = int(created_utc)
        if self.start_utc is not None and ts < self.start_utc:
            return False
        if self.end_utc is not None and ts >= self.end_utc:
            return False
        return True

    def overlaps_month(self, month: str) -> bool:
        """Whether any part of a YYYY-MM month falls inside the window."""
        month_start, month_end = _month_bounds_utc(month)
        if self.start_utc is not None and month_end <= self.start_utc:
            return False
        if self.end_utc is not None and month_start >= self.end_utc:
            return False
        return True

    def to_dict(self) -> Dict[str, Optional[int]]:
        return {"start_utc": self.start_utc, "end_utc": self.end_utc}


# Epoch is an alias that reads better where the label (not the bounds) is meant.
Epoch = str


@dataclass
class SliceResult:
    granularity: str
    epochs: List[str]
    output_root: Path
    rows_written: Dict[str, int]
    manifest: Manifest


def _corpus_months(layout: CorpusLayout) -> List[str]:
    if layout.dataset_metadata_path.exists():
        return list(read_json(layout.dataset_metadata_path).get("months", []))
    return [path.stem for path in layout.month_metadata_files()]


def slice_corpus(
    corpus_root: Union[str, Path],
    granularity: str = "month",
    output_root: Optional[Union[str, Path]] = None,
    window: Optional[TimeWindow] = None,
    force: bool = False,
) -> SliceResult:
    """
    Re-bucket a monthly extracted corpus into per-epoch Parquet slices.

    Writes ``<output_root>/<epoch>/{submissions,comments}.parquet`` plus a
    per-run manifest. ``output_root`` defaults to
    ``<corpus_root>/slices/<granularity>``. Existing epochs are skipped
    unless ``force`` — the slice layout is resumable like every other run
    output in the package.
    """
    import pyarrow.parquet as pq
    import pyarrow as pa

    if granularity not in GRANULARITIES:
        raise ValueError(f"Unknown granularity {granularity!r}; expected one of {GRANULARITIES}")

    corpus_root = Path(corpus_root)
    layout = CorpusLayout(corpus_root)
    months = [m for m in _corpus_months(layout) if window is None or window.overlaps_month(m)]
    if not months:
        raise ValueError(f"No corpus months found under {corpus_root}")

    output_root = Path(output_root) if output_root else corpus_root / "slices" / granularity
    started = time.time()

    epochs: Dict[str, List[str]] = {}
    for month in sorted(months):
        epochs.setdefault(month_to_epoch(month, granularity), []).append(month)

    rows_written: Dict[str, int] = {}

    with manifest_run(
        "slice",
        params={
            "corpus_root": str(corpus_root),
            "granularity": granularity,
            "window": window.to_dict() if window else None,
            "months": sorted(months),
        },
    ) as manifest:
        for epoch, epoch_months in epochs.items():
            epoch_dir = output_root / epoch
            done_marker = epoch_dir / "epoch.json"
            if done_marker.exists() and not force:
                logger.info("Skipping epoch %s: already sliced", epoch)
                continue
            ensure_directory(epoch_dir)

            epoch_rows = 0
            for record_type, path_fn in (
                ("submissions", layout.submissions_path),
                ("comments", layout.comments_path),
            ):
                tables = []
                for month in epoch_months:
                    month_path = path_fn(month)
                    if month_path.exists():
                        manifest.add_input(month_path)
                        tables.append(pq.read_table(month_path))
                if not tables:
                    continue
                combined = pa.concat_tables(tables)
                if window is not None and (window.start_utc is not None or window.end_utc is not None):
                    mask = [window.contains(ts) for ts in combined.column("created_utc").to_pylist()]
                    combined = combined.filter(pa.array(mask))
                out_path = epoch_dir / f"{record_type}.parquet"
                pq.write_table(combined, out_path, compression="zstd")
                manifest.add_output(out_path)
                epoch_rows += combined.num_rows

            write_json(
                done_marker,
                {
                    "epoch": epoch,
                    "granularity": granularity,
                    "months": epoch_months,
                    "rows": epoch_rows,
                },
            )
            rows_written[epoch] = epoch_rows

        manifest.counts = {
            "epochs": len(epochs),
            "epochs_written": len(rows_written),
            "rows_written": sum(rows_written.values()),
        }
    manifest.write(output_root)

    logger.info(
        "Sliced %s into %s %s epochs (%s rows) in %s",
        corpus_root.name,
        len(epochs),
        granularity,
        f"{sum(rows_written.values()):,}",
        format_duration(time.time() - started),
    )
    return SliceResult(
        granularity=granularity,
        epochs=sorted(epochs),
        output_root=output_root,
        rows_written=rows_written,
        manifest=manifest,
    )
