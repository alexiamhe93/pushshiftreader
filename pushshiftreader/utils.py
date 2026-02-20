"""
Utility functions for logging, progress tracking, and path handling.
"""

import re
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional, Iterator
from dataclasses import dataclass


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[Path] = None,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Set up logging for the pushshiftreader package.
    
    Args:
        level: Logging level (default INFO)
        log_file: Optional file to write logs to
        format_string: Custom format string
    
    Returns:
        Configured logger
    """
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    logger = logging.getLogger("pushshiftreader")
    logger.setLevel(level)
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # Console handler
    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(logging.Formatter(format_string))
    logger.addHandler(console)
    
    # File handler if requested
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(format_string))
        logger.addHandler(file_handler)
    
    return logger


@dataclass
class ArchiveFile:
    """Represents a Pushshift archive file."""
    path: Path
    year: int
    month: int
    file_type: str  # 'comments' or 'submissions'
    
    @property
    def month_str(self) -> str:
        """Return YYYY-MM format string."""
        return f"{self.year}-{self.month:02d}"
    
    @property
    def size_mb(self) -> float:
        """Return file size in megabytes."""
        return self.path.stat().st_size / (1024 * 1024)
    
    def __lt__(self, other: "ArchiveFile") -> bool:
        """Sort by date, then by type."""
        return (self.year, self.month, self.file_type) < (other.year, other.month, other.file_type)


def parse_archive_filename(filename: str) -> Optional[Tuple[str, int, int]]:
    """
    Parse a Pushshift archive filename.
    
    Args:
        filename: Filename like "RC_2023-01.zst" or "RS_2023-01.zst"
    
    Returns:
        Tuple of (type, year, month) or None if not a valid archive file
        type is 'comments' for RC_ files, 'submissions' for RS_ files
    """
    # Pattern: RC_YYYY-MM.zst or RS_YYYY-MM.zst
    pattern = r'^(RC|RS)_(\d{4})-(\d{2})\.zst$'
    match = re.match(pattern, filename)
    
    if not match:
        return None
    
    prefix, year, month = match.groups()
    file_type = 'comments' if prefix == 'RC' else 'submissions'
    
    return file_type, int(year), int(month)


def discover_archives(
    archive_path: Path,
    comments_subdir: str = "comments",
    submissions_subdir: str = "submissions"
) -> List[ArchiveFile]:
    """
    Discover all archive files in the given directory structure.
    
    Expects structure:
        archive_path/
            comments/
                RC_2023-01.zst
                RC_2023-02.zst
                ...
            submissions/
                RS_2023-01.zst
                RS_2023-02.zst
                ...
    
    Args:
        archive_path: Root path containing comments/ and submissions/ folders
        comments_subdir: Name of comments subdirectory
        submissions_subdir: Name of submissions subdirectory
    
    Returns:
        Sorted list of ArchiveFile objects
    """
    archive_path = Path(archive_path)
    archives = []
    
    # Scan comments directory
    comments_dir = archive_path / comments_subdir
    if comments_dir.exists():
        for file_path in comments_dir.glob("RC_*.zst"):
            parsed = parse_archive_filename(file_path.name)
            if parsed:
                file_type, year, month = parsed
                archives.append(ArchiveFile(
                    path=file_path,
                    year=year,
                    month=month,
                    file_type=file_type
                ))
    
    # Scan submissions directory
    submissions_dir = archive_path / submissions_subdir
    if submissions_dir.exists():
        for file_path in submissions_dir.glob("RS_*.zst"):
            parsed = parse_archive_filename(file_path.name)
            if parsed:
                file_type, year, month = parsed
                archives.append(ArchiveFile(
                    path=file_path,
                    year=year,
                    month=month,
                    file_type=file_type
                ))
    
    return sorted(archives)


def get_months_in_range(
    archives: List[ArchiveFile],
    start_month: Optional[str] = None,
    end_month: Optional[str] = None
) -> List[str]:
    """
    Get list of unique months available in archives, optionally filtered.
    
    Args:
        archives: List of ArchiveFile objects
        start_month: Optional start month (YYYY-MM format)
        end_month: Optional end month (YYYY-MM format)
    
    Returns:
        Sorted list of month strings (YYYY-MM format)
    """
    months = set()
    for archive in archives:
        months.add(archive.month_str)
    
    months = sorted(months)
    
    if start_month:
        months = [m for m in months if m >= start_month]
    if end_month:
        months = [m for m in months if m <= end_month]
    
    return months


def iter_archive_pairs(
    archives: List[ArchiveFile]
) -> Iterator[Tuple[str, Optional[ArchiveFile], Optional[ArchiveFile]]]:
    """
    Iterate over archives grouped by month, yielding comment/submission pairs.
    
    Args:
        archives: List of ArchiveFile objects
    
    Yields:
        Tuples of (month_str, comments_file, submissions_file)
        Either file may be None if not present for that month
    """
    # Group by month
    by_month = {}
    for archive in archives:
        month_str = archive.month_str
        if month_str not in by_month:
            by_month[month_str] = {'comments': None, 'submissions': None}
        by_month[month_str][archive.file_type] = archive
    
    # Yield in sorted order
    for month_str in sorted(by_month.keys()):
        files = by_month[month_str]
        yield month_str, files['comments'], files['submissions']


def format_size(size_bytes: int) -> str:
    """Format byte size as human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def format_duration(seconds: float) -> str:
    """Format duration in seconds as human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        mins = seconds / 60
        return f"{mins:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def sanitize_subreddit_name(name: str) -> str:
    """
    Sanitize a subreddit name for use as a directory name.
    
    Removes leading r/ if present, converts to lowercase,
    and replaces problematic characters.
    """
    # Remove r/ prefix if present
    if name.lower().startswith("r/"):
        name = name[2:]
    
    # Subreddit names are generally safe, but let's be careful
    # Replace any path separators or problematic chars
    name = name.replace("/", "_").replace("\\", "_")
    
    return name


def ensure_directory(path: Path) -> Path:
    """Ensure a directory exists, creating it if necessary."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def timestamp_str() -> str:
    """Return current timestamp as ISO format string."""
    return datetime.now().isoformat()
