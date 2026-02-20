"""
Low-level reader for .zst compressed NDJSON files.

Handles streaming decompression with proper memory management
for the large window sizes used in Pushshift dumps.
"""

import json
import zstandard
import logging
from pathlib import Path
from typing import Iterator, Tuple, Optional, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ReadProgress:
    """Progress information for file reading."""
    bytes_read: int
    total_bytes: int
    lines_read: int
    errors: int
    
    @property
    def percent(self) -> float:
        if self.total_bytes == 0:
            return 0.0
        return (self.bytes_read / self.total_bytes) * 100


def _read_and_decode(
    reader,
    chunk_size: int,
    max_window_size: int,
    previous_chunk: Optional[bytes] = None,
    bytes_read: int = 0
) -> str:
    """
    Recursively read and decode chunks, handling Unicode boundary issues.
    
    Sometimes a UTF-8 character gets split across chunk boundaries,
    causing decode errors. This function handles that by reading
    additional chunks until decoding succeeds.
    """
    chunk = reader.read(chunk_size)
    bytes_read += len(chunk)
    
    if previous_chunk is not None:
        chunk = previous_chunk + chunk
    
    try:
        return chunk.decode('utf-8')
    except UnicodeDecodeError:
        if bytes_read > max_window_size:
            raise UnicodeDecodeError(
                'utf-8', chunk, 0, len(chunk),
                f"Unable to decode after reading {bytes_read:,} bytes"
            )
        logger.debug(f"Decode error at {bytes_read:,} bytes, reading more")
        return _read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_zst_lines(
    file_path: Path,
    chunk_size: int = 2**27,  # 128MB chunks
    max_window_size: int = 2**31  # 2GB max window for decompression
) -> Iterator[Tuple[str, int]]:
    """
    Stream lines from a .zst compressed file.
    
    Args:
        file_path: Path to the .zst file
        chunk_size: Size of chunks to read at a time
        max_window_size: Maximum decompression window size
    
    Yields:
        Tuples of (line, bytes_position) for progress tracking
    """
    file_path = Path(file_path)
    file_size = file_path.stat().st_size
    
    with open(file_path, 'rb') as fh:
        # Create decompressor with large window size for Pushshift files
        dctx = zstandard.ZstdDecompressor(max_window_size=max_window_size)
        reader = dctx.stream_reader(fh)
        
        buffer = ''
        bytes_read = 0
        
        while True:
            chunk = _read_and_decode(reader, chunk_size, max_window_size * 2)
            
            if not chunk:
                # End of file - yield any remaining content
                if buffer.strip():
                    yield buffer.strip(), file_size
                break
            
            # Update position (approximate, based on compressed bytes)
            bytes_read = fh.tell()
            
            # Split into lines
            lines = (buffer + chunk).split('\n')
            
            # Keep incomplete last line in buffer
            buffer = lines.pop()
            
            # Yield complete lines
            for line in lines:
                if line.strip():
                    yield line, bytes_read


def read_zst_records(
    file_path: Path,
    filter_fn: Optional[Callable[[dict], bool]] = None,
    progress_callback: Optional[Callable[[ReadProgress], None]] = None,
    progress_interval: int = 10000
) -> Iterator[dict]:
    """
    Stream parsed JSON records from a .zst compressed NDJSON file.
    
    Args:
        file_path: Path to the .zst file
        filter_fn: Optional function to filter records (return True to keep)
        progress_callback: Optional callback for progress updates
        progress_interval: How often to call progress callback (in lines)
    
    Yields:
        Parsed JSON dictionaries
    """
    file_path = Path(file_path)
    total_bytes = file_path.stat().st_size
    
    lines_read = 0
    errors = 0
    last_bytes = 0
    
    for line, bytes_read in read_zst_lines(file_path):
        lines_read += 1
        
        try:
            record = json.loads(line)
        except json.JSONDecodeError as e:
            errors += 1
            logger.warning(f"JSON decode error at line {lines_read}: {e}")
            continue
        
        # Apply filter if provided
        if filter_fn is not None and not filter_fn(record):
            continue
        
        yield record
        
        # Progress callback
        if progress_callback and lines_read % progress_interval == 0:
            progress = ReadProgress(
                bytes_read=bytes_read,
                total_bytes=total_bytes,
                lines_read=lines_read,
                errors=errors
            )
            progress_callback(progress)
    
    # Final progress update
    if progress_callback:
        progress = ReadProgress(
            bytes_read=total_bytes,
            total_bytes=total_bytes,
            lines_read=lines_read,
            errors=errors
        )
        progress_callback(progress)


def count_records(
    file_path: Path,
    filter_fn: Optional[Callable[[dict], bool]] = None,
    progress_callback: Optional[Callable[[ReadProgress], None]] = None
) -> Tuple[int, int]:
    """
    Count records in a .zst file, optionally with a filter.
    
    Args:
        file_path: Path to the .zst file
        filter_fn: Optional filter function
        progress_callback: Optional progress callback
    
    Returns:
        Tuple of (total_records, matched_records)
    """
    total = 0
    matched = 0
    
    for record in read_zst_records(file_path, progress_callback=progress_callback):
        total += 1
        if filter_fn is None or filter_fn(record):
            matched += 1
    
    return total, matched


class ZstReader:
    """
    Context manager for reading .zst files with progress tracking.
    
    Example:
        with ZstReader("RC_2023-01.zst") as reader:
            for record in reader:
                process(record)
            print(f"Read {reader.lines_read} lines")
    """
    
    def __init__(
        self,
        file_path: Path,
        filter_fn: Optional[Callable[[dict], bool]] = None,
        show_progress: bool = False
    ):
        self.file_path = Path(file_path)
        self.filter_fn = filter_fn
        self.show_progress = show_progress
        
        self.lines_read = 0
        self.errors = 0
        self.records_yielded = 0
        self._iterator = None
        self._pbar = None
    
    def __enter__(self):
        if self.show_progress:
            try:
                from tqdm import tqdm
                total_bytes = self.file_path.stat().st_size
                self._pbar = tqdm(
                    total=total_bytes,
                    unit='B',
                    unit_scale=True,
                    desc=self.file_path.name
                )
            except ImportError:
                logger.warning("tqdm not installed, progress bar disabled")
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._pbar:
            self._pbar.close()
        return False
    
    def __iter__(self):
        last_bytes = 0
        
        for line, bytes_read in read_zst_lines(self.file_path):
            self.lines_read += 1
            
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                self.errors += 1
                continue
            
            if self.filter_fn is not None and not self.filter_fn(record):
                continue
            
            self.records_yielded += 1
            
            # Update progress bar
            if self._pbar and bytes_read > last_bytes:
                self._pbar.update(bytes_read - last_bytes)
                last_bytes = bytes_read
            
            yield record
        
        # Ensure progress bar completes
        if self._pbar:
            total = self.file_path.stat().st_size
            if last_bytes < total:
                self._pbar.update(total - last_bytes)
