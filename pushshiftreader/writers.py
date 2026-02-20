"""
Writers for outputting extracted data to various formats.

Supports:
- CSV (flat format for easy analysis)
- Gzipped JSONL (compressed, preserves all fields)
- Gzipped JSON (for nested thread structures)
"""

import csv
import gzip
import json
from pathlib import Path
from typing import List, Iterator, Optional, Any, Set
from contextlib import contextmanager

from .models import Submission, Comment, Thread


# Default field ordering for CSV output (most useful fields first)
SUBMISSION_CSV_FIELDS = [
    'id', 'created_utc', 'author', 'subreddit', 'title', 'selftext',
    'score', 'num_comments', 'url', 'domain', 'permalink',
    'is_self', 'over_18', 'spoiler', 'stickied', 'locked',
    'upvote_ratio', 'gilded', 'distinguished',
    'author_flair_text', 'link_flair_text',
    'subreddit_id', 'subreddit_subscribers',
    'edited', 'archived', 'removed_by_category',
    'total_awards_received', 'crosspost_parent'
]

COMMENT_CSV_FIELDS = [
    'id', 'created_utc', 'author', 'subreddit', 'body',
    'score', 'link_id', 'parent_id', 'permalink',
    'controversiality', 'stickied', 'distinguished', 'is_submitter',
    'gilded', 'author_flair_text',
    'subreddit_id', 'edited', 'archived', 'locked',
    'removed_by_category', 'total_awards_received'
]


class JsonlWriter:
    """
    Writer for gzipped JSONL (newline-delimited JSON) files.
    
    Each line contains a complete JSON object. Preserves all fields.
    """
    
    def __init__(self, file_path: Path, compress: bool = True):
        """
        Initialize writer.
        
        Args:
            file_path: Output file path (.jsonl or .jsonl.gz)
            compress: Whether to gzip compress the output
        """
        self.file_path = Path(file_path)
        self.compress = compress
        self._file = None
        self._count = 0
    
    def __enter__(self):
        if self.compress:
            self._file = gzip.open(self.file_path, 'wt', encoding='utf-8')
        else:
            self._file = open(self.file_path, 'w', encoding='utf-8')
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file:
            self._file.close()
        return False
    
    def write(self, record: dict):
        """Write a single record as a JSON line."""
        self._file.write(json.dumps(record, ensure_ascii=False))
        self._file.write('\n')
        self._count += 1
    
    def write_submission(self, submission: Submission):
        """Write a Submission object."""
        self.write(submission.to_dict())
    
    def write_comment(self, comment: Comment):
        """Write a Comment object."""
        self.write(comment.to_dict())
    
    @property
    def count(self) -> int:
        """Number of records written."""
        return self._count


class CsvWriter:
    """
    Writer for CSV files with configurable field selection.
    
    Handles dynamic field discovery if fields aren't pre-specified.
    """
    
    def __init__(
        self,
        file_path: Path,
        fields: Optional[List[str]] = None,
        extra_fields: bool = True
    ):
        """
        Initialize CSV writer.
        
        Args:
            file_path: Output file path
            fields: List of fields to include (in order). If None, auto-detect.
            extra_fields: If True and fields is specified, also include any
                         extra fields found in records (appended at end)
        """
        self.file_path = Path(file_path)
        self.fields = fields
        self.extra_fields = extra_fields
        self._file = None
        self._writer = None
        self._seen_fields: Set[str] = set()
        self._count = 0
        self._header_written = False
    
    def __enter__(self):
        self._file = open(self.file_path, 'w', newline='', encoding='utf-8')
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file:
            self._file.close()
        return False
    
    def _get_value(self, record: dict, field: str) -> str:
        """Extract field value, handling nested dicts and special types."""
        value = record.get(field, '')
        
        if value is None:
            return ''
        elif isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        else:
            return str(value)
    
    def write(self, record: dict):
        """Write a single record."""
        # First record: determine fields and write header
        if not self._header_written:
            if self.fields is None:
                # Auto-detect fields from first record
                self.fields = list(record.keys())
            
            self._writer = csv.writer(self._file)
            
            # If extra_fields enabled, we'll track and append new fields
            if self.extra_fields:
                self._seen_fields = set(self.fields)
            
            self._writer.writerow(self.fields)
            self._header_written = True
        
        # Track any new fields (for logging purposes)
        if self.extra_fields:
            for key in record.keys():
                if key not in self._seen_fields:
                    self._seen_fields.add(key)
        
        # Write row
        row = [self._get_value(record, f) for f in self.fields]
        self._writer.writerow(row)
        self._count += 1
    
    def write_submission(self, submission: Submission):
        """Write a Submission object."""
        self.write(submission.to_dict())
    
    def write_comment(self, comment: Comment):
        """Write a Comment object."""
        self.write(comment.to_dict())
    
    @property
    def count(self) -> int:
        """Number of records written."""
        return self._count


class ThreadWriter:
    """
    Writer for threads in gzipped JSON format.
    
    Writes complete thread structures with nested comments.
    """
    
    def __init__(self, file_path: Path):
        """
        Initialize thread writer.
        
        Args:
            file_path: Output file path (will be .json.gz)
        """
        self.file_path = Path(file_path)
        self._threads: List[dict] = []
        self._count = 0
    
    def write(self, thread: Thread):
        """Add a thread to be written."""
        self._threads.append(thread.to_dict())
        self._count += 1
    
    def save(self):
        """Write all threads to the output file."""
        with gzip.open(self.file_path, 'wt', encoding='utf-8') as f:
            json.dump(self._threads, f, ensure_ascii=False)
    
    @property
    def count(self) -> int:
        """Number of threads written."""
        return self._count


class StreamingThreadWriter:
    """
    Writer for threads that streams to gzipped JSONL.
    
    More memory-efficient than ThreadWriter for large outputs.
    Each line is a complete thread JSON object.
    """
    
    def __init__(self, file_path: Path):
        self.file_path = Path(file_path)
        self._file = None
        self._count = 0
    
    def __enter__(self):
        self._file = gzip.open(self.file_path, 'wt', encoding='utf-8')
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file:
            self._file.close()
        return False
    
    def write(self, thread: Thread):
        """Write a single thread."""
        self._file.write(json.dumps(thread.to_dict(), ensure_ascii=False))
        self._file.write('\n')
        self._count += 1
    
    @property
    def count(self) -> int:
        return self._count


def write_submissions_csv(
    submissions: Iterator[Submission],
    output_path: Path,
    fields: Optional[List[str]] = None
) -> int:
    """
    Convenience function to write submissions to CSV.
    
    Args:
        submissions: Iterator of Submission objects
        output_path: Output file path
        fields: Optional field list (defaults to SUBMISSION_CSV_FIELDS)
    
    Returns:
        Number of submissions written
    """
    if fields is None:
        fields = SUBMISSION_CSV_FIELDS
    
    with CsvWriter(output_path, fields=fields) as writer:
        for sub in submissions:
            writer.write_submission(sub)
        return writer.count


def write_comments_csv(
    comments: Iterator[Comment],
    output_path: Path,
    fields: Optional[List[str]] = None
) -> int:
    """
    Convenience function to write comments to CSV.
    
    Args:
        comments: Iterator of Comment objects
        output_path: Output file path
        fields: Optional field list (defaults to COMMENT_CSV_FIELDS)
    
    Returns:
        Number of comments written
    """
    if fields is None:
        fields = COMMENT_CSV_FIELDS
    
    with CsvWriter(output_path, fields=fields) as writer:
        for comment in comments:
            writer.write_comment(comment)
        return writer.count
