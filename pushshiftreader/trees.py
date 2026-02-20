"""
Comment tree reconstruction.

Builds nested comment structures from flat comment data using
SQLite as an efficient index for parent-child relationships.
"""

import json
import gzip
import sqlite3
import logging
import time
from pathlib import Path
from typing import List, Dict, Optional, Iterator
from contextlib import contextmanager

from .models import Submission, Comment, Thread, CommentNode
from .writers import StreamingThreadWriter
from .utils import ensure_directory, format_duration

logger = logging.getLogger(__name__)


class TreeBuilder:
    """
    Build comment trees from extracted subreddit data.
    
    Uses SQLite to index comments by their IDs, then constructs
    nested thread structures by resolving parent-child relationships.
    
    Example:
        builder = TreeBuilder("./extracted/AskHistorians")
        builder.build_all_months()
    """
    
    def __init__(
        self,
        extracted_path: Path,
        db_path: Optional[Path] = None
    ):
        """
        Initialize tree builder.
        
        Args:
            extracted_path: Path to extracted subreddit data
            db_path: Optional path for SQLite index (default: in-memory)
        """
        self.extracted_path = Path(extracted_path)
        self.db_path = db_path or ":memory:"
        
        if not self.extracted_path.exists():
            raise ValueError(f"Extracted path does not exist: {self.extracted_path}")
    
    def _get_months(self) -> List[str]:
        """Get list of available months in extracted data."""
        months = []
        for item in self.extracted_path.iterdir():
            if item.is_dir() and '-' in item.name:
                # Check if it looks like a month directory (YYYY-MM)
                try:
                    year, month = item.name.split('-')
                    if len(year) == 4 and len(month) == 2:
                        months.append(item.name)
                except ValueError:
                    continue
        return sorted(months)
    
    def _load_jsonl_gz(self, file_path: Path) -> Iterator[dict]:
        """Load records from a gzipped JSONL file."""
        if not file_path.exists():
            return
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)
    
    def _load_submissions(self, month_path: Path) -> Dict[str, Submission]:
        """Load submissions from a month directory, indexed by ID."""
        submissions = {}
        
        # Try JSONL first (has all fields)
        jsonl_path = month_path / 'submissions.jsonl.gz'
        if jsonl_path.exists():
            for record in self._load_jsonl_gz(jsonl_path):
                sub = Submission.from_dict(record)
                submissions[sub.id] = sub
        
        return submissions
    
    def _load_comments_to_db(
        self,
        month_path: Path,
        conn: sqlite3.Connection
    ) -> int:
        """Load comments into SQLite for indexing. Returns count."""
        cursor = conn.cursor()
        count = 0
        
        jsonl_path = month_path / 'comments.jsonl.gz'
        if not jsonl_path.exists():
            return 0
        
        for record in self._load_jsonl_gz(jsonl_path):
            cursor.execute('''
                INSERT OR REPLACE INTO comments (id, parent_id, link_id, data)
                VALUES (?, ?, ?, ?)
            ''', (
                record.get('id', ''),
                record.get('parent_id', ''),
                record.get('link_id', ''),
                json.dumps(record)
            ))
            count += 1
            
            if count % 10000 == 0:
                conn.commit()
        
        conn.commit()
        return count
    
    @contextmanager
    def _create_index_db(self):
        """Create and manage SQLite index database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS comments (
                id TEXT PRIMARY KEY,
                parent_id TEXT,
                link_id TEXT,
                data TEXT
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_link_id ON comments(link_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_parent_id ON comments(parent_id)')
        conn.commit()
        
        try:
            yield conn
        finally:
            conn.close()
    
    def _build_thread(
        self,
        submission: Submission,
        conn: sqlite3.Connection
    ) -> Thread:
        """Build a complete thread for a submission."""
        cursor = conn.cursor()
        
        # Get all comments for this submission
        link_id = f"t3_{submission.id}"
        cursor.execute(
            'SELECT id, parent_id, data FROM comments WHERE link_id = ?',
            (link_id,)
        )
        
        # Index comments by ID
        comments_by_id: Dict[str, Comment] = {}
        parent_map: Dict[str, str] = {}  # child_id -> parent_id
        
        for row in cursor.fetchall():
            comment_id, parent_id, data_json = row
            comment = Comment.from_dict(json.loads(data_json))
            comments_by_id[comment_id] = comment
            parent_map[comment_id] = parent_id
        
        # Build tree structure
        nodes_by_id: Dict[str, CommentNode] = {}
        
        # Create nodes for all comments
        for comment_id, comment in comments_by_id.items():
            nodes_by_id[comment_id] = CommentNode(comment=comment)
        
        # Link children to parents
        top_level_nodes: List[CommentNode] = []
        
        for comment_id, node in nodes_by_id.items():
            parent_id = parent_map.get(comment_id, '')
            
            if parent_id.startswith('t1_'):
                # Parent is another comment
                parent_comment_id = parent_id[3:]
                if parent_comment_id in nodes_by_id:
                    nodes_by_id[parent_comment_id].replies.append(node)
                else:
                    # Parent comment not in our data (maybe deleted)
                    top_level_nodes.append(node)
            else:
                # Top-level comment (parent is submission)
                top_level_nodes.append(node)
        
        # Sort top-level comments by created_utc
        top_level_nodes.sort(key=lambda n: n.comment.created_utc)
        
        # Sort replies recursively
        def sort_replies(node: CommentNode):
            node.replies.sort(key=lambda n: n.comment.created_utc)
            for reply in node.replies:
                sort_replies(reply)
        
        for node in top_level_nodes:
            sort_replies(node)
        
        return Thread(submission=submission, comments=top_level_nodes)
    
    def build_month(self, month: str) -> int:
        """
        Build trees for a specific month.
        
        Args:
            month: Month string (YYYY-MM)
        
        Returns:
            Number of threads built
        """
        month_path = self.extracted_path / month
        if not month_path.exists():
            logger.warning(f"Month directory not found: {month_path}")
            return 0
        
        start_time = time.time()
        logger.info(f"Building trees for {month}")
        
        # Load submissions
        submissions = self._load_submissions(month_path)
        logger.info(f"  Loaded {len(submissions)} submissions")
        
        if not submissions:
            return 0
        
        # Create index and load comments
        with self._create_index_db() as conn:
            comment_count = self._load_comments_to_db(month_path, conn)
            logger.info(f"  Indexed {comment_count} comments")
            
            # Build threads
            output_path = month_path / 'threads.jsonl.gz'
            thread_count = 0
            
            with StreamingThreadWriter(output_path) as writer:
                for submission_id, submission in submissions.items():
                    thread = self._build_thread(submission, conn)
                    writer.write(thread)
                    thread_count += 1
                    
                    if thread_count % 1000 == 0:
                        logger.debug(f"  Built {thread_count} threads...")
        
        duration = time.time() - start_time
        logger.info(f"  Built {thread_count} threads in {format_duration(duration)}")
        
        return thread_count
    
    def build_all_months(self) -> Dict[str, int]:
        """
        Build trees for all available months.
        
        Returns:
            Dict mapping month to thread count
        """
        months = self._get_months()
        logger.info(f"Building trees for {len(months)} months")
        
        results = {}
        total_start = time.time()
        
        for month in months:
            results[month] = self.build_month(month)
        
        total_duration = time.time() - total_start
        total_threads = sum(results.values())
        
        logger.info(f"\nTotal: {total_threads} threads in {format_duration(total_duration)}")
        
        return results


def load_threads(threads_path: Path) -> Iterator[Thread]:
    """
    Load threads from a threads.jsonl.gz file.
    
    Args:
        threads_path: Path to threads.jsonl.gz file
    
    Yields:
        Thread objects
    """
    with gzip.open(threads_path, 'rt', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            
            data = json.loads(line)
            
            # Reconstruct submission
            submission = Submission.from_dict(data['submission'])
            
            # Reconstruct comment tree
            def build_node(node_data: dict) -> CommentNode:
                comment = Comment.from_dict(node_data['comment'])
                replies = [build_node(r) for r in node_data.get('replies', [])]
                return CommentNode(comment=comment, replies=replies)
            
            comments = [build_node(n) for n in data.get('comments', [])]
            
            yield Thread(submission=submission, comments=comments)
