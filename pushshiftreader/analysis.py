"""
Small analysis helpers for quick inspection of extracted corpora and tracking runs.
"""

from pathlib import Path
from typing import Any, Dict, Optional

from .loader import load_corpus


def build_smoke_report(
    dataset_path: Path,
    tracking_path: Optional[Path] = None,
    top_n: int = 10,
) -> Dict[str, Any]:
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "pandas is required for smoke analysis. Install it with: pip install pandas"
        ) from exc

    dataset = load_corpus(dataset_path)
    comments_df = dataset.comments_dataframe(include_threads=False)
    submissions_df = dataset.submissions_dataframe()
    authors_summary_path = Path(dataset_path) / "authors_summary.parquet"
    authors_df = pd.read_parquet(authors_summary_path) if authors_summary_path.exists() else pd.DataFrame()

    monthly_summary = pd.DataFrame(
        [dataset.month_stats(month) for month in dataset.months]
    ).sort_values("month")

    report: Dict[str, Any] = {
        "subreddit": dataset.subreddit,
        "months": list(dataset.months),
        "monthly_summary": monthly_summary,
        "top_comment_authors": (
            authors_df.sort_values("comment_count", ascending=False)
            .head(top_n)[["author", "comment_count", "submission_count"]]
            if not authors_df.empty
            else pd.DataFrame(columns=["author", "comment_count", "submission_count"])
        ),
        "comment_columns": list(comments_df.columns),
        "submission_columns": list(submissions_df.columns),
        "comments_rows": len(comments_df),
        "submissions_rows": len(submissions_df),
    }

    if tracking_path:
        tracking_root = Path(tracking_path)
        monthly_counts_path = tracking_root / "aggregates" / "monthly_counts.parquet"
        term_counts_path = tracking_root / "aggregates" / "term_counts.parquet"

        report["tracking_monthly_counts"] = (
            pd.read_parquet(monthly_counts_path)
            .sort_values(["month", "record_type", "keyword_set"])
            if monthly_counts_path.exists()
            else pd.DataFrame()
        )
        report["tracking_top_terms"] = (
            pd.read_parquet(term_counts_path)
            .sort_values("total_matches", ascending=False)
            .head(top_n)
            if term_counts_path.exists()
            else pd.DataFrame()
        )

    return report
