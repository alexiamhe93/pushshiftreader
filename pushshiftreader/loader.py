"""
Loader for extracted subreddit data.

Provides a clean API for accessing submissions, comments, and threads
from extracted and processed subreddit data.
"""

import csv
import gzip
import json
import logging
from pathlib import Path
from typing import Iterator, List, Optional, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

from .models import Submission, Comment, Thread, CommentNode
from .trees import load_threads


@dataclass
class SubredditMetadata:
    """Metadata for an extracted subreddit."""
    subreddit: str
    months: List[str]
    total_submissions: int
    total_comments: int
    extracted_at: str
    output_format: str
    
    @classmethod
    def from_dict(cls, data: dict) -> "SubredditMetadata":
        return cls(
            subreddit=data.get('subreddit', ''),
            months=data.get('months', []),
            total_submissions=data.get('total_submissions', 0),
            total_comments=data.get('total_comments', 0),
            extracted_at=data.get('extracted_at', ''),
            output_format=data.get('output_format', '')
        )


class SubredditData:
    """
    Interface for accessing extracted subreddit data.
    
    Example:
        data = SubredditData("./extracted/AskHistorians")
        
        # Get available months
        print(data.months)
        
        # Iterate over submissions for a month
        for sub in data.submissions("2023-01"):
            print(sub.title)
        
        # Iterate over all comments
        for comment in data.all_comments():
            print(comment.body)
        
        # Access threaded data
        for thread in data.threads("2023-01"):
            print(f"{thread.submission.title}: {thread.comment_count} comments")
    """
    
    def __init__(self, path: Path):
        """
        Initialize subreddit data accessor.
        
        Args:
            path: Path to extracted subreddit directory
        """
        self.path = Path(path)
        
        if not self.path.exists():
            raise ValueError(f"Path does not exist: {self.path}")
        
        # Load metadata
        metadata_path = self.path / 'metadata.json'
        if metadata_path.exists():
            with open(metadata_path) as f:
                self.metadata = SubredditMetadata.from_dict(json.load(f))
        else:
            # Discover months manually
            months = self._discover_months()
            self.metadata = SubredditMetadata(
                subreddit=self.path.name,
                months=months,
                total_submissions=0,
                total_comments=0,
                extracted_at='',
                output_format='unknown'
            )
    
    def _discover_months(self) -> List[str]:
        """Discover available months from directory structure."""
        months = []
        for item in self.path.iterdir():
            if item.is_dir() and '-' in item.name:
                try:
                    year, month = item.name.split('-')
                    if len(year) == 4 and len(month) == 2:
                        months.append(item.name)
                except ValueError:
                    continue
        return sorted(months)
    
    @property
    def subreddit(self) -> str:
        """Subreddit name."""
        return self.metadata.subreddit
    
    @property
    def months(self) -> List[str]:
        """List of available months (YYYY-MM format)."""
        return self.metadata.months
    
    def _month_path(self, month: str) -> Path:
        """Get path for a specific month."""
        return self.path / month
    
    def _read_csv(self, file_path: Path) -> Iterator[Dict[str, Any]]:
        """Read records from a CSV file."""
        if not file_path.exists():
            return
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row
    
    def _read_jsonl_gz(self, file_path: Path) -> Iterator[dict]:
        """Read records from a gzipped JSONL file."""
        if not file_path.exists():
            return
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)
    
    def submissions(
        self,
        month: Optional[str] = None,
        as_dict: bool = False
    ) -> Iterator[Submission]:
        """
        Iterate over submissions.
        
        Args:
            month: Specific month (YYYY-MM) or None for all months
            as_dict: If True, yield raw dicts instead of Submission objects
        
        Yields:
            Submission objects (or dicts if as_dict=True)
        """
        months = [month] if month else self.months
        
        for m in months:
            month_path = self._month_path(m)
            
            # Prefer JSONL (preserves all fields)
            jsonl_path = month_path / 'submissions.jsonl.gz'
            if jsonl_path.exists():
                for record in self._read_jsonl_gz(jsonl_path):
                    yield record if as_dict else Submission.from_dict(record)
            else:
                # Fall back to CSV
                csv_path = month_path / 'submissions.csv'
                for record in self._read_csv(csv_path):
                    yield record if as_dict else Submission.from_dict(record)
    
    def comments(
        self,
        month: Optional[str] = None,
        as_dict: bool = False
    ) -> Iterator[Comment]:
        """
        Iterate over comments.
        
        Args:
            month: Specific month (YYYY-MM) or None for all months
            as_dict: If True, yield raw dicts instead of Comment objects
        
        Yields:
            Comment objects (or dicts if as_dict=True)
        """
        months = [month] if month else self.months
        
        for m in months:
            month_path = self._month_path(m)
            
            # Prefer JSONL
            jsonl_path = month_path / 'comments.jsonl.gz'
            if jsonl_path.exists():
                for record in self._read_jsonl_gz(jsonl_path):
                    yield record if as_dict else Comment.from_dict(record)
            else:
                # Fall back to CSV
                csv_path = month_path / 'comments.csv'
                for record in self._read_csv(csv_path):
                    yield record if as_dict else Comment.from_dict(record)
    
    def all_submissions(self, as_dict: bool = False) -> Iterator[Submission]:
        """Iterate over all submissions across all months."""
        return self.submissions(month=None, as_dict=as_dict)
    
    def all_comments(self, as_dict: bool = False) -> Iterator[Comment]:
        """Iterate over all comments across all months."""
        return self.comments(month=None, as_dict=as_dict)
    
    def threads(self, month: Optional[str] = None) -> Iterator[Thread]:
        """
        Iterate over threads (requires tree building to have been run).
        
        Args:
            month: Specific month or None for all months
        
        Yields:
            Thread objects with nested comment structure
        """
        months = [month] if month else self.months
        
        for m in months:
            threads_path = self._month_path(m) / 'threads.jsonl.gz'
            if threads_path.exists():
                yield from load_threads(threads_path)
    
    def all_threads(self) -> Iterator[Thread]:
        """Iterate over all threads across all months."""
        return self.threads(month=None)
    
    def get_submission(self, submission_id: str) -> Optional[Submission]:
        """
        Find a specific submission by ID.
        
        Note: This scans through files, so it's not efficient for
        multiple lookups. Consider loading into a dict if you need
        to look up many submissions.
        """
        for sub in self.all_submissions():
            if sub.id == submission_id:
                return sub
        return None
    
    def get_thread(self, submission_id: str) -> Optional[Thread]:
        """
        Find a specific thread by submission ID.
        
        Note: This scans through files, so it's not efficient for
        multiple lookups.
        """
        for thread in self.all_threads():
            if thread.submission.id == submission_id:
                return thread
        return None
    
    def submission_count(self, month: Optional[str] = None) -> int:
        """Count submissions (optionally for a specific month)."""
        return sum(1 for _ in self.submissions(month))
    
    def comment_count(self, month: Optional[str] = None) -> int:
        """Count comments (optionally for a specific month)."""
        return sum(1 for _ in self.comments(month))
    
    def comments_dataframe(self, month: Optional[str] = None, signals: bool = True):
        """
        Return a pandas DataFrame of comments across one or all months.

        Sources from ``threads.jsonl.gz`` so that thread-level features
        (depth, thread size, time since submission, submission metadata) can
        be joined onto each comment row.  Run :class:`~pushshiftreader.TreeBuilder`
        first; months without ``threads.jsonl.gz`` are skipped with a warning.

        When ``signals=True`` and a ``signals.csv`` exists for the month,
        signal columns are left-joined onto the DataFrame automatically.
        Records absent from ``signals.csv`` receive ``False`` / ``NaN`` for
        all signal columns.

        Args:
            month: Specific month (``YYYY-MM``) or ``None`` for all months.
            signals: Join signal columns from ``signals.csv`` when present.

        Returns:
            ``pandas.DataFrame`` with one row per comment, plus a leading
            ``month`` column.  Returns an empty DataFrame if no data is found.

        Requires ``pandas`` (``pip install pandas``).
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for DataFrame export. "
                "Install it with: pip install pandas"
            )

        months = [month] if month else self.months
        dfs = []

        for m in months:
            threads_path = self._month_path(m) / 'threads.jsonl.gz'
            if not threads_path.exists():
                logger.warning(
                    f"No threads.jsonl.gz for {m} — "
                    "run TreeBuilder.build_month() first, skipping"
                )
                continue

            month_frames = []
            for thread in load_threads(threads_path):
                df = thread.to_dataframe()
                if not df.empty:
                    month_frames.append(df)

            if not month_frames:
                continue

            month_df = pd.concat(month_frames, ignore_index=True)
            month_df.insert(0, 'month', m)

            if signals:
                signals_path = self._month_path(m) / 'signals.csv'
                if signals_path.exists():
                    sig_df = pd.read_csv(signals_path)
                    comment_sigs = (
                        sig_df[sig_df['record_type'] == 'comment']
                        .drop(columns=['record_type'])
                        .rename(columns={'record_id': 'id'})
                    )
                    if not comment_sigs.empty:
                        month_df = month_df.merge(comment_sigs, on='id', how='left')

            dfs.append(month_df)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    def submissions_dataframe(self, month: Optional[str] = None, signals: bool = True):
        """
        Return a pandas DataFrame of submissions across one or all months.

        When ``signals=True`` and a ``signals.csv`` exists for the month,
        submission-type signal columns are left-joined automatically.

        Args:
            month: Specific month (``YYYY-MM``) or ``None`` for all months.
            signals: Join signal columns from ``signals.csv`` when present.

        Returns:
            ``pandas.DataFrame`` with one row per submission, plus a leading
            ``month`` column.  Returns an empty DataFrame if no data is found.

        Requires ``pandas`` (``pip install pandas``).
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for DataFrame export. "
                "Install it with: pip install pandas"
            )

        months = [month] if month else self.months
        dfs = []

        for m in months:
            rows = [sub.to_dict(include_extra=False) for sub in self.submissions(m)]
            if not rows:
                continue

            month_df = pd.DataFrame(rows)
            month_df.insert(0, 'month', m)

            if signals:
                signals_path = self._month_path(m) / 'signals.csv'
                if signals_path.exists():
                    sig_df = pd.read_csv(signals_path)
                    sub_sigs = (
                        sig_df[sig_df['record_type'] == 'submission']
                        .drop(columns=['record_type'])
                        .rename(columns={'record_id': 'id'})
                    )
                    if not sub_sigs.empty:
                        month_df = month_df.merge(sub_sigs, on='id', how='left')

            dfs.append(month_df)

        if not dfs:
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    def export_comment_graph(
        self,
        output_dir: Path,
        month: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Export a comment/conversation graph to CSV files.

        Writes two files to ``output_dir``:

        - ``comment_graph_nodes.csv`` — one row per submission or comment
        - ``comment_graph_edges.csv`` — one directed edge per reply

        **Node columns:** ``node_id``, ``type``, ``author``, ``score``,
        ``created_utc``, ``depth``

        **Edge columns:** ``source``, ``target``, ``time_delta``
        (seconds from the parent post's timestamp to the child's)

        Node and edge rows are streamed directly to disk as each thread is
        processed, so memory usage stays flat regardless of data volume.
        Requires ``TreeBuilder`` to have been run first.

        Args:
            output_dir: Directory to write the two CSV files into.
            month: Specific month (``YYYY-MM``) or ``None`` for all months.

        Returns:
            Dict with ``nodes`` and ``edges`` counts.
        """
        from .utils import ensure_directory
        output_dir = ensure_directory(Path(output_dir))

        months = [month] if month else self.months

        node_fields = ['node_id', 'type', 'author', 'score', 'created_utc', 'depth']
        edge_fields = ['source', 'target', 'time_delta']

        nodes_path = output_dir / 'comment_graph_nodes.csv'
        edges_path = output_dir / 'comment_graph_edges.csv'

        total_nodes = 0
        total_edges = 0

        with (
            open(nodes_path, 'w', newline='', encoding='utf-8') as nf,
            open(edges_path, 'w', newline='', encoding='utf-8') as ef,
        ):
            node_writer = csv.DictWriter(nf, fieldnames=node_fields)
            edge_writer = csv.DictWriter(ef, fieldnames=edge_fields)
            node_writer.writeheader()
            edge_writer.writeheader()

            for m in months:
                threads_path = self._month_path(m) / 'threads.jsonl.gz'
                if not threads_path.exists():
                    logger.warning(
                        f"No threads.jsonl.gz for {m} — "
                        "run TreeBuilder first, skipping"
                    )
                    continue

                for thread in load_threads(threads_path):
                    nodes, edges = thread.to_comment_graph()
                    node_writer.writerows(nodes)
                    edge_writer.writerows(edges)
                    total_nodes += len(nodes)
                    total_edges += len(edges)

        logger.info(
            f"Comment graph: {total_nodes:,} nodes, {total_edges:,} edges → {output_dir}"
        )
        return {'nodes': total_nodes, 'edges': total_edges}

    def export_author_graph(
        self,
        output_dir: Path,
        month: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Export an author-interaction graph to CSV files.

        Writes two files to ``output_dir``:

        - ``author_graph_nodes.csv`` — one row per unique author
        - ``author_graph_edges.csv`` — one directed edge per unique
          (replier, replied-to) pair, with interaction count as weight

        **Node columns:** ``author``, ``comment_count``, ``total_score``,
        ``first_seen_utc``, ``last_seen_utc``

        **Edge columns:** ``source``, ``target``, ``weight``,
        ``first_interaction_utc``

        Deleted/unknown authors are excluded.  Author stats and interaction
        counts are accumulated across all threads before writing.
        Requires ``TreeBuilder`` to have been run first.

        Args:
            output_dir: Directory to write the two CSV files into.
            month: Specific month (``YYYY-MM``) or ``None`` for all months.

        Returns:
            Dict with ``nodes`` and ``edges`` counts.
        """
        from .utils import ensure_directory
        output_dir = ensure_directory(Path(output_dir))

        months = [month] if month else self.months

        # Accumulate across all threads
        all_node_stats: Dict[str, Dict] = {}
        all_edge_stats: Dict[tuple, Dict] = {}

        for m in months:
            threads_path = self._month_path(m) / 'threads.jsonl.gz'
            if not threads_path.exists():
                logger.warning(
                    f"No threads.jsonl.gz for {m} — "
                    "run TreeBuilder first, skipping"
                )
                continue

            for thread in load_threads(threads_path):
                node_stats, edge_stats = thread.to_author_graph()

                for author, stats in node_stats.items():
                    if author not in all_node_stats:
                        all_node_stats[author] = dict(stats)
                    else:
                        s = all_node_stats[author]
                        s['comment_count'] += stats['comment_count']
                        s['total_score'] += stats['total_score']
                        if stats['first_seen_utc'] < s['first_seen_utc']:
                            s['first_seen_utc'] = stats['first_seen_utc']
                        if stats['last_seen_utc'] > s['last_seen_utc']:
                            s['last_seen_utc'] = stats['last_seen_utc']

                for (src, tgt), stats in edge_stats.items():
                    key = (src, tgt)
                    if key not in all_edge_stats:
                        all_edge_stats[key] = dict(stats)
                    else:
                        e = all_edge_stats[key]
                        e['weight'] += stats['weight']
                        if stats['first_interaction_utc'] < e['first_interaction_utc']:
                            e['first_interaction_utc'] = stats['first_interaction_utc']

        # Write to CSV
        node_fields = ['author', 'comment_count', 'total_score',
                       'first_seen_utc', 'last_seen_utc']
        edge_fields = ['source', 'target', 'weight', 'first_interaction_utc']

        nodes_path = output_dir / 'author_graph_nodes.csv'
        edges_path = output_dir / 'author_graph_edges.csv'

        with open(nodes_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=node_fields)
            writer.writeheader()
            for author, stats in all_node_stats.items():
                writer.writerow({'author': author, **stats})

        with open(edges_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=edge_fields)
            writer.writeheader()
            for (src, tgt), stats in all_edge_stats.items():
                writer.writerow({'source': src, 'target': tgt, **stats})

        logger.info(
            f"Author graph: {len(all_node_stats):,} nodes, "
            f"{len(all_edge_stats):,} edges → {output_dir}"
        )
        return {'nodes': len(all_node_stats), 'edges': len(all_edge_stats)}

    def month_stats(self, month: str) -> Dict[str, Any]:
        """Get statistics for a specific month."""
        month_path = self._month_path(month)
        metadata_path = month_path / 'metadata.json'
        
        if metadata_path.exists():
            with open(metadata_path) as f:
                return json.load(f)
        
        # Calculate manually if no metadata
        return {
            'month': month,
            'submissions_count': self.submission_count(month),
            'comments_count': self.comment_count(month)
        }


def load_subreddit(path: Path) -> SubredditData:
    """
    Load extracted subreddit data.
    
    This is the main entry point for accessing extracted data.
    
    Args:
        path: Path to extracted subreddit directory
    
    Returns:
        SubredditData accessor object
    
    Example:
        data = load_subreddit("./extracted/AskHistorians")
        for thread in data.threads("2023-01"):
            print(thread.submission.title)
    """
    return SubredditData(path)
