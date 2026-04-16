"""
Research-facing loaders for canonical subreddit corpora.
"""

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from .models import Comment, Submission, Thread
from .storage import CorpusLayout, read_json, read_parquet_records
from .trees import load_threads
from .utils import ensure_directory

logger = logging.getLogger(__name__)


@dataclass
class CorpusMetadata:
    subreddit: str
    months: List[str]
    total_submissions: int
    total_comments: int
    total_authors_across_months: int
    dataset_version: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CorpusMetadata":
        return cls(
            subreddit=data.get("subreddit", ""),
            months=list(data.get("months", [])),
            total_submissions=int(data.get("total_submissions") or 0),
            total_comments=int(data.get("total_comments") or 0),
            total_authors_across_months=int(data.get("total_authors_across_months") or 0),
            dataset_version=data.get("dataset_version", ""),
        )


class CorpusDataset:
    """
    Load and analyze one canonical subreddit corpus.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        if not self.path.exists():
            raise ValueError(f"Dataset path does not exist: {self.path}")
        self.layout = CorpusLayout(self.path)

        if self.layout.dataset_metadata_path.exists():
            self.metadata = CorpusMetadata.from_dict(read_json(self.layout.dataset_metadata_path))
        else:
            months = [
                file_path.stem
                for file_path in self.layout.month_metadata_files()
            ]
            self.metadata = CorpusMetadata(
                subreddit=self.path.name,
                months=sorted(months),
                total_submissions=0,
                total_comments=0,
                total_authors_across_months=0,
                dataset_version="",
            )

    @property
    def subreddit(self) -> str:
        return self.metadata.subreddit

    @property
    def months(self) -> List[str]:
        return list(self.metadata.months)

    def _iter_model_records(self, section: str, month: Optional[str], as_dict: bool):
        months = [month] if month else self.months
        for current_month in months:
            if section == "submissions":
                records = read_parquet_records(self.layout.submissions_path(current_month))
                for record in records:
                    yield record if as_dict else Submission.from_dict(record)
            elif section == "comments":
                records = read_parquet_records(self.layout.comments_path(current_month))
                for record in records:
                    yield record if as_dict else Comment.from_dict(record)
            elif section == "threads":
                threads_path = self.layout.threads_path(current_month)
                if threads_path.exists():
                    yield from load_threads(threads_path)

    def submissions(self, month: Optional[str] = None, as_dict: bool = False):
        return self._iter_model_records("submissions", month, as_dict)

    def comments(self, month: Optional[str] = None, as_dict: bool = False):
        return self._iter_model_records("comments", month, as_dict)

    def threads(self, month: Optional[str] = None) -> Iterator[Thread]:
        return self._iter_model_records("threads", month, False)

    def all_submissions(self, as_dict: bool = False):
        return self.submissions(month=None, as_dict=as_dict)

    def all_comments(self, as_dict: bool = False):
        return self.comments(month=None, as_dict=as_dict)

    def all_threads(self) -> Iterator[Thread]:
        return self.threads(month=None)

    def submission_count(self, month: Optional[str] = None) -> int:
        return sum(1 for _ in self.submissions(month=month))

    def comment_count(self, month: Optional[str] = None) -> int:
        return sum(1 for _ in self.comments(month=month))

    def get_submission(self, submission_id: str) -> Optional[Submission]:
        for submission in self.submissions():
            if submission.id == submission_id:
                return submission
        return None

    def get_thread(self, submission_id: str) -> Optional[Thread]:
        for thread in self.threads():
            if thread.submission.id == submission_id:
                return thread
        return None

    def comments_dataframe(self, month: Optional[str] = None, include_threads: bool = True):
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError(
                "pandas is required for DataFrame export. Install it with: pip install pandas"
            ) from exc

        frames = []
        months = [month] if month else self.months
        for current_month in months:
            if include_threads and self.layout.threads_path(current_month).exists():
                df = pd.read_parquet(self.layout.threads_path(current_month))
            else:
                df = pd.read_parquet(self.layout.comments_path(current_month))
            signals_path = self.layout.signals_path(current_month)
            if signals_path.exists():
                signals_df = pd.read_parquet(signals_path)
                comment_signals = (
                    signals_df[signals_df["record_type"] == "comment"]
                    .drop(columns=["record_type"])
                    .rename(columns={"record_id": "id"})
                )
                if not comment_signals.empty:
                    df = df.merge(comment_signals, on="id", how="left")
            if not df.empty:
                df.insert(0, "month", current_month)
                frames.append(df)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def submissions_dataframe(self, month: Optional[str] = None):
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError(
                "pandas is required for DataFrame export. Install it with: pip install pandas"
            ) from exc

        frames = []
        months = [month] if month else self.months
        for current_month in months:
            if not self.layout.submissions_path(current_month).exists():
                continue
            df = pd.read_parquet(self.layout.submissions_path(current_month))
            signals_path = self.layout.signals_path(current_month)
            if signals_path.exists():
                signals_df = pd.read_parquet(signals_path)
                submission_signals = (
                    signals_df[signals_df["record_type"] == "submission"]
                    .drop(columns=["record_type"])
                    .rename(columns={"record_id": "id"})
                )
                if not submission_signals.empty:
                    df = df.merge(submission_signals, on="id", how="left")
            if not df.empty:
                df.insert(0, "month", current_month)
                frames.append(df)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def month_stats(self, month: str) -> Dict[str, Any]:
        metadata_path = self.layout.month_metadata_path(month)
        if metadata_path.exists():
            return read_json(metadata_path)
        return {
            "month": month,
            "submissions_count": self.submission_count(month),
            "comments_count": self.comment_count(month),
        }

    def export_comment_graph(self, output_dir: Path, month: Optional[str] = None) -> Dict[str, int]:
        output_dir = ensure_directory(Path(output_dir))
        node_fields = ["node_id", "type", "author", "score", "created_utc", "depth"]
        edge_fields = ["source", "target", "time_delta"]

        nodes_path = output_dir / "comment_graph_nodes.csv"
        edges_path = output_dir / "comment_graph_edges.csv"
        total_nodes = 0
        total_edges = 0

        with open(nodes_path, "w", newline="", encoding="utf-8") as nodes_handle, open(
            edges_path, "w", newline="", encoding="utf-8"
        ) as edges_handle:
            node_writer = csv.DictWriter(nodes_handle, fieldnames=node_fields)
            edge_writer = csv.DictWriter(edges_handle, fieldnames=edge_fields)
            node_writer.writeheader()
            edge_writer.writeheader()

            for thread in self.threads(month):
                nodes, edges = thread.to_comment_graph()
                node_writer.writerows(nodes)
                edge_writer.writerows(edges)
                total_nodes += len(nodes)
                total_edges += len(edges)

        return {"nodes": total_nodes, "edges": total_edges}

    def export_author_graph(self, output_dir: Path, month: Optional[str] = None) -> Dict[str, int]:
        output_dir = ensure_directory(Path(output_dir))
        all_node_stats: Dict[str, Dict[str, Any]] = {}
        all_edge_stats: Dict[tuple, Dict[str, Any]] = {}

        for thread in self.threads(month):
            node_stats, edge_stats = thread.to_author_graph()

            for author, stats in node_stats.items():
                if author not in all_node_stats:
                    all_node_stats[author] = dict(stats)
                else:
                    current = all_node_stats[author]
                    current["comment_count"] += stats["comment_count"]
                    current["total_score"] += stats["total_score"]
                    if stats["first_seen_utc"] < current["first_seen_utc"]:
                        current["first_seen_utc"] = stats["first_seen_utc"]
                    if stats["last_seen_utc"] > current["last_seen_utc"]:
                        current["last_seen_utc"] = stats["last_seen_utc"]

            for key, stats in edge_stats.items():
                if key not in all_edge_stats:
                    all_edge_stats[key] = dict(stats)
                else:
                    current = all_edge_stats[key]
                    current["weight"] += stats["weight"]
                    if stats["first_interaction_utc"] < current["first_interaction_utc"]:
                        current["first_interaction_utc"] = stats["first_interaction_utc"]

        nodes_path = output_dir / "author_graph_nodes.csv"
        edges_path = output_dir / "author_graph_edges.csv"
        with open(nodes_path, "w", newline="", encoding="utf-8") as nodes_handle:
            writer = csv.DictWriter(
                nodes_handle,
                fieldnames=["author", "comment_count", "total_score", "first_seen_utc", "last_seen_utc"],
            )
            writer.writeheader()
            for author, stats in sorted(all_node_stats.items()):
                writer.writerow({"author": author, **stats})

        with open(edges_path, "w", newline="", encoding="utf-8") as edges_handle:
            writer = csv.DictWriter(
                edges_handle,
                fieldnames=["source", "target", "weight", "first_interaction_utc"],
            )
            writer.writeheader()
            for (source, target), stats in sorted(all_edge_stats.items()):
                writer.writerow({"source": source, "target": target, **stats})

        return {"nodes": len(all_node_stats), "edges": len(all_edge_stats)}


SubredditData = CorpusDataset
SubredditMetadata = CorpusMetadata


def load_corpus(path: Path) -> CorpusDataset:
    return CorpusDataset(path)


def load_subreddit(path: Path) -> CorpusDataset:
    return CorpusDataset(path)
