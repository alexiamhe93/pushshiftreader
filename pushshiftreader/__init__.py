"""
pushshiftreader

Canonical subreddit extraction, keyword-set tracking, and thread-aware analysis
for Pushshift Reddit archives.
"""

__version__ = "1.0.0"
__author__ = "alexiamhe93"

from .analysis import build_smoke_report
from .catalogue import ArchiveCatalogue, SubredditIndex
from .crosssub import CrossSubIndex
from .dreams import DreamCorpusExporter, DreamSubredditDiscoverer, assess_submission_for_dreams
from .extractor import ExtractionResult, ExtractionStats, SubredditExtractor
from .loader import (
    CorpusDataset,
    CorpusMetadata,
    SubredditData,
    SubredditMetadata,
    load_corpus,
    load_subreddit,
)
from .models import Comment, CommentNode, Submission, Thread
from .provenance import Manifest, fingerprint_file, manifest_run, write_manifest
from .selection import Selection, Selector, SubredditSliceSpec, ThreadSpec, TurnSpec
from .windows import GRANULARITIES, TimeWindow, epoch_of, month_to_epoch, slice_corpus
from .presets import (
    AITAVerdictDetector,
    AuthorDeletedDetector,
    ContentRemovedDetector,
    DeltaAwardedDetector,
    DepthDetector,
    ModDistinguishedDetector,
    StickiedCommentDetector,
    TopLevelCommentDetector,
    get_detectors,
)
from .reddit_api import (
    DEFAULT_SUBREDDITS,
    RedditAPICredentials,
    RedditAPIClient,
    RedditAPIError,
    RedditCommentFetcher,
    RedditSubmissionScraper,
    clean_subreddit_name,
    flatten_comment_listing,
    normalize_comment,
    normalize_submission,
)
from .reader import ReadProgress, ZstReader, count_records, read_zst_lines, read_zst_records
from .searcher import SearchResult, SearchStats, WordSearcher, assemble_search_results
from .signals import AuthorIsOPDetector, Detector, RegexDetector, ScoreDetector, SignalDetector
from .storage import (
    COMMENT_FIELDS,
    COMMENT_SCHEMA,
    DATASET_VERSION,
    SUBMISSION_FIELDS,
    SUBMISSION_SCHEMA,
    TRACKING_VERSION,
    CorpusLayout,
    TrackingLayout,
)
from .tracking import (
    KeywordSet,
    KeywordTracker,
    TrackingResult,
    TrackingStats,
    load_keyword_sets,
    merge_keyword_sets,
)
from .trees import TreeBuilder, load_threads
from .utils import ArchiveFile, discover_archives, setup_logging

__all__ = [
    "__version__",
    "ArchiveFile",
    "ArchiveCatalogue",
    "AITAVerdictDetector",
    "AuthorDeletedDetector",
    "AuthorIsOPDetector",
    "build_smoke_report",
    "Comment",
    "CommentNode",
    "ContentRemovedDetector",
    "CorpusDataset",
    "CorpusLayout",
    "CorpusMetadata",
    "CrossSubIndex",
    "DATASET_VERSION",
    "DeltaAwardedDetector",
    "DepthDetector",
    "Detector",
    "DEFAULT_SUBREDDITS",
    "DreamCorpusExporter",
    "DreamSubredditDiscoverer",
    "ExtractionResult",
    "ExtractionStats",
    "GRANULARITIES",
    "KeywordSet",
    "KeywordTracker",
    "Manifest",
    "Selection",
    "Selector",
    "SubredditSliceSpec",
    "ThreadSpec",
    "TimeWindow",
    "TurnSpec",
    "ModDistinguishedDetector",
    "ReadProgress",
    "RedditAPICredentials",
    "RedditAPIClient",
    "RedditAPIError",
    "RedditCommentFetcher",
    "RedditSubmissionScraper",
    "RegexDetector",
    "ScoreDetector",
    "SearchResult",
    "SearchStats",
    "SignalDetector",
    "WordSearcher",
    "assemble_search_results",
    "Submission",
    "StickiedCommentDetector",
    "SubredditData",
    "SubredditIndex",
    "SubredditExtractor",
    "SubredditMetadata",
    "Thread",
    "TopLevelCommentDetector",
    "TrackingLayout",
    "TRACKING_VERSION",
    "TrackingResult",
    "TrackingStats",
    "TreeBuilder",
    "ZstReader",
    "COMMENT_FIELDS",
    "COMMENT_SCHEMA",
    "SUBMISSION_FIELDS",
    "SUBMISSION_SCHEMA",
    "count_records",
    "discover_archives",
    "epoch_of",
    "fingerprint_file",
    "manifest_run",
    "month_to_epoch",
    "slice_corpus",
    "write_manifest",
    "load_keyword_sets",
    "get_detectors",
    "load_corpus",
    "load_subreddit",
    "load_threads",
    "merge_keyword_sets",
    "read_zst_lines",
    "read_zst_records",
    "assess_submission_for_dreams",
    "clean_subreddit_name",
    "flatten_comment_listing",
    "normalize_comment",
    "normalize_submission",
    "setup_logging",
]
