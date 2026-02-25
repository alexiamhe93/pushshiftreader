"""
pushshiftreader - Extract and analyze Reddit data from Pushshift archives.

A Python module for working with Pushshift Reddit data dumps, providing:
- Streaming decompression of .zst archives
- Subreddit-specific data extraction  
- Comment tree reconstruction
- CSV and compressed JSON output

Quick Start:
    from pushshiftreader import SubredditExtractor, TreeBuilder, load_subreddit
    
    # Extract subreddit data from archives
    extractor = SubredditExtractor(
        archive_path="/path/to/reddit_dumps",
        output_path="./extracted",
        subreddits=["AskHistorians", "science"]
    )
    extractor.run()
    
    # Build comment trees
    builder = TreeBuilder("./extracted/AskHistorians")
    builder.build_all_months()
    
    # Load and analyze
    data = load_subreddit("./extracted/AskHistorians")
    for thread in data.threads("2023-01"):
        print(f"{thread.submission.title}: {thread.comment_count} comments")

Command Line:
    python -m pushshiftreader extract --archive /path/to/dumps --output ./out --subreddits AskHistorians
    python -m pushshiftreader build-trees ./out/AskHistorians
    python -m pushshiftreader info ./out/AskHistorians

Signal Detection:
    from pushshiftreader import SignalDetector, Detector, RegexDetector

    class DeltaDetector(Detector):
        def detect_comment(self, comment, thread, depth=0):
            return 'Δ' in comment.body or '!delta' in comment.body.lower()

    sd = SignalDetector(
        "./extracted/ChangeMyView",
        detectors=[DeltaDetector("delta_awarded")],
    )
    sd.run_all_months()

Signal Detection Presets:
    from pushshiftreader import get_detectors, SignalDetector

    # Built-in presets: 'general', 'cmv'/'changemyview', 'aita'/'amitheasshole'
    sd = SignalDetector("./extracted/ChangeMyView",
                        detectors=get_detectors('cmv'))
    sd.run_all_months()

Graph Export:
    data = load_subreddit("./extracted/ChangeMyView")
    data.export_comment_graph("./graphs/")   # comment_graph_nodes.csv + edges.csv
    data.export_author_graph("./graphs/")    # author_graph_nodes.csv + edges.csv
"""

__version__ = "0.3.0"
__author__ = "Your Name"

# Core data models
from .models import (
    Submission,
    Comment,
    Thread,
    CommentNode
)

# Main extraction and processing classes
from .extractor import (
    SubredditExtractor,
    ExtractionResult,
    ExtractionStats
)

from .trees import (
    TreeBuilder,
    load_threads
)

# Data loading
from .loader import (
    load_subreddit,
    SubredditData,
    SubredditMetadata
)

# Low-level reader (for custom processing)
from .reader import (
    read_zst_records,
    read_zst_lines,
    ZstReader,
    ReadProgress
)

# Writers (for custom output)
from .writers import (
    CsvWriter,
    JsonlWriter,
    StreamingThreadWriter,
    SUBMISSION_CSV_FIELDS,
    COMMENT_CSV_FIELDS
)

# Signal detection
from .signals import (
    Detector,
    SignalDetector,
    RegexDetector,
    ScoreDetector,
    AuthorIsOPDetector,
)

# Signal detection presets
from .presets import (
    get_detectors,
    StickiedCommentDetector,
    ModDistinguishedDetector,
    ContentRemovedDetector,
    AuthorDeletedDetector,
    TopLevelCommentDetector,
    DepthDetector,
    DeltaAwardedDetector,
    AITAVerdictDetector,
)

# Archive catalogue and cross-subreddit index
from .catalogue import ArchiveCatalogue
from .crosssub import CrossSubIndex

# Utilities
from .utils import (
    setup_logging,
    discover_archives,
    ArchiveFile
)

__all__ = [
    # Version
    '__version__',
    
    # Models
    'Submission',
    'Comment', 
    'Thread',
    'CommentNode',
    
    # Main classes
    'SubredditExtractor',
    'TreeBuilder',
    'SubredditData',
    'SignalDetector',

    # Signal detectors (built-in)
    'Detector',
    'RegexDetector',
    'ScoreDetector',
    'AuthorIsOPDetector',

    # Signal detector presets
    'get_detectors',
    'StickiedCommentDetector',
    'ModDistinguishedDetector',
    'ContentRemovedDetector',
    'AuthorDeletedDetector',
    'TopLevelCommentDetector',
    'DepthDetector',
    'DeltaAwardedDetector',
    'AITAVerdictDetector',
    
    # Result types
    'ExtractionResult',
    'ExtractionStats',
    'SubredditMetadata',
    
    # Higher-level analysis classes
    'ArchiveCatalogue',
    'CrossSubIndex',

    # Functions
    'load_subreddit',
    'load_threads',
    'read_zst_records',
    'read_zst_lines',
    'setup_logging',
    'discover_archives',
    
    # Low-level classes
    'ZstReader',
    'ReadProgress',
    'CsvWriter',
    'JsonlWriter',
    'StreamingThreadWriter',
    'ArchiveFile',
    
    # Constants
    'SUBMISSION_CSV_FIELDS',
    'COMMENT_CSV_FIELDS',
]
