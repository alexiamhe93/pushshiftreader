"""
Change-point / novelty detection over keyword-tracking term series.

Consumes the ``term_counts`` time series a :class:`~.tracking.KeywordTracker`
run produces and flags months where a term's usage inflects: the monthly
count sits ``threshold`` standard deviations above a trailing baseline
(rolling z-score — deliberately simple; swap in PELT/ruptures downstream if
a dependency is ever warranted).

Inflection terms feed the ``first_appearance`` sampler: detect *when* a term
takes off, then pull its earliest instances — the entry point to the
rare-emergent-sense problem.
"""

import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional, Tuple, Union

from .provenance import Manifest, manifest_run
from .storage import TrackingLayout, read_parquet_records, write_json

logger = logging.getLogger(__name__)


@dataclass
class EmergencePoint:
    """One flagged inflection: term usage jumped against its own baseline."""

    term: str
    month: str
    count: int
    baseline_mean: float
    baseline_std: float
    zscore: float
    first_month: str  # first month the term appears at all — seed for first_appearance


@dataclass
class EmergenceResult:
    points: List[EmergencePoint]
    series: Dict[str, List[Tuple[str, int]]]
    manifest: Manifest

    def write(self, output_dir: Union[str, Path]) -> Path:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        points_path = output_dir / "emergence.json"
        write_json(
            points_path,
            {
                "points": [asdict(point) for point in self.points],
                "terms": sorted(self.series),
            },
        )
        self.manifest.add_output(points_path)
        self.manifest.write(output_dir)
        return points_path


def _month_range(months: List[str]) -> List[str]:
    """All calendar months from min to max of the observed months."""
    if not months:
        return []
    start, end = min(months), max(months)
    year, mon = int(start[:4]), int(start[5:7])
    out = []
    while True:
        label = f"{year}-{mon:02d}"
        out.append(label)
        if label == end:
            break
        mon += 1
        if mon > 12:
            mon, year = 1, year + 1
    return out


def build_term_series(
    term_rows: List[Dict[str, Any]],
    count_field: str = "matched_records",
) -> Dict[str, List[Tuple[str, int]]]:
    """
    Collapse tracking ``term_counts`` rows into per-term monthly series.

    Counts are summed across record types and subreddits; months with no
    occurrences are zero-filled across the full observed span (a burst after
    absence must register against a baseline that includes the absence).
    """
    by_term: Dict[str, Dict[str, int]] = {}
    all_months: List[str] = []
    for row in term_rows:
        term = str(row.get("term") or "")
        month = str(row.get("month") or "")
        if not term or not month:
            continue
        all_months.append(month)
        by_term.setdefault(term, {})
        by_term[term][month] = by_term[term].get(month, 0) + int(row.get(count_field) or 0)

    span = _month_range(sorted(set(all_months)))
    return {
        term: [(month, counts.get(month, 0)) for month in span]
        for term, counts in by_term.items()
    }


def detect_emergence(
    series: Dict[str, List[Tuple[str, int]]],
    window: int = 6,
    threshold: float = 3.0,
    min_count: int = 5,
    std_floor: float = 1.0,
) -> List[EmergencePoint]:
    """
    Rolling z-score change-point detection over per-term monthly series.

    For each month with at least ``window`` preceding months, flag when
    ``(count - baseline_mean) / max(baseline_std, std_floor) >= threshold``
    and the count itself is at least ``min_count`` (guards against 0→2 noise).
    """
    points: List[EmergencePoint] = []
    for term in sorted(series):
        term_series = series[term]
        counts = [count for _, count in term_series]
        first_month = next((month for month, count in term_series if count > 0), "")
        for index in range(window, len(term_series)):
            month, count = term_series[index]
            baseline = counts[index - window: index]
            base_mean = mean(baseline)
            base_std = max(pstdev(baseline), std_floor)
            zscore = (count - base_mean) / base_std
            if zscore >= threshold and count >= min_count:
                points.append(
                    EmergencePoint(
                        term=term,
                        month=month,
                        count=count,
                        baseline_mean=round(base_mean, 3),
                        baseline_std=round(base_std, 3),
                        zscore=round(zscore, 3),
                        first_month=first_month,
                    )
                )
    points.sort(key=lambda point: (-point.zscore, point.term, point.month))
    return points


def run_emergence(
    tracking_root: Union[str, Path],
    window: int = 6,
    threshold: float = 3.0,
    min_count: int = 5,
    keyword_set: Optional[str] = None,
) -> EmergenceResult:
    """
    Detect inflection terms from a completed keyword-tracking run.

    Reads the combined ``term_counts.parquet``; optionally restricts to one
    keyword set. Returns flagged points plus the zero-filled series (so the
    downstream analysis can plot exactly what the detector saw).
    """
    tracking_root = Path(tracking_root)
    layout = TrackingLayout(tracking_root)
    term_counts_path = layout.combined_term_counts_path
    if not term_counts_path.exists():
        raise ValueError(f"No combined term counts at {term_counts_path}; run tracking first")

    with manifest_run(
        "emergence",
        params={
            "tracking_root": str(tracking_root),
            "window": window,
            "threshold": threshold,
            "min_count": min_count,
            "keyword_set": keyword_set,
        },
    ) as manifest:
        manifest.add_input(term_counts_path)
        rows = read_parquet_records(term_counts_path)
        if keyword_set:
            rows = [row for row in rows if row.get("keyword_set") == keyword_set]
        series = build_term_series(rows)
        points = detect_emergence(
            series, window=window, threshold=threshold, min_count=min_count
        )
        manifest.counts = {
            "terms": len(series),
            "months": len(next(iter(series.values()))) if series else 0,
            "inflections": len(points),
        }
    return EmergenceResult(points=points, series=series, manifest=manifest)
