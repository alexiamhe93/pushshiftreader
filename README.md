# pushshiftreader

A Python module for extracting and analyzing Reddit data from Pushshift archives.

## Features

- **Stream large archives** — Memory-efficient processing of multi-gigabyte .zst files
- **Extract by subreddit** — Pull specific subreddits from the full archive
- **Resumable extraction** — Re-run safely; completed months are skipped automatically
- **Keyword/regex filtering** — Filter posts and comments by content at extraction time
- **Author statistics** — Per-author activity counts, scores, and date ranges written to CSV
- **Reconstruct comment trees** — Rebuild threaded conversations with nested replies
- **Multiple output formats** — CSV for easy analysis, compressed JSON for full fidelity
- **Simple API** — Clean Python interface for research workflows

## Installation

```bash
# Clone or download the package
cd pushshiftreader

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install with progress bars
pip install -e ".[progress]"

# Or minimal install
pip install -e .
```

## Quick Start

### 1. Extract Subreddit Data

Extract specific subreddits from the monthly Pushshift archives:

```python
from pushshiftreader import SubredditExtractor

extractor = SubredditExtractor(
    archive_path="/path/to/reddit_dumps",  # Contains comments/ and submissions/
    output_path="./extracted",
    subreddits=["AskHistorians", "AskScience"]
)

result = extractor.run()
print(f"Extracted {result.total_comments:,} comments")
```

Or use the command line:

```bash
pushshiftreader extract \
    --archive /path/to/reddit_dumps \
    --output ./extracted \
    --subreddits AskHistorians AskScience
```

### 2. Resume an Interrupted Extraction

If a run is interrupted, just re-run the same command. Any month that already has a
`metadata.json` is skipped automatically — only missing months are processed:

```python
# Safe to re-run; completed months are skipped
result = extractor.run()
```

```bash
# Same command — completed months are skipped
pushshiftreader extract \
    --archive /path/to/reddit_dumps \
    --output ./extracted \
    --subreddits AskHistorians

# Force a full re-extraction, overwriting existing data
pushshiftreader extract ... --force
```

### 3. Filter by Keyword or Regex

Keep only records whose text matches your patterns. Filters are applied during streaming,
so non-matching records never touch disk.

- **`include_patterns`** — record must match at least one pattern to be kept
- **`exclude_patterns`** — record is dropped if it matches any pattern
- Searches `title` + `selftext` for submissions; `body` for comments
- All patterns are case-insensitive regular expressions

```python
extractor = SubredditExtractor(
    archive_path="/path/to/reddit_dumps",
    output_path="./extracted",
    subreddits=["science"],
    include_patterns=["climate change", "global warming", r"\bCO2\b"],
    exclude_patterns=[r"\[deleted\]", "off.topic"],
)
result = extractor.run()
```

```bash
pushshiftreader extract \
    --archive /path/to/reddit_dumps \
    --output ./extracted \
    --subreddits science \
    --include "climate change" "global warming" \
    --exclude "\[deleted\]"
```

The patterns used are recorded in every `metadata.json` file for reproducibility.

### 4. Author Statistics

After each extraction, `authors.csv` files are written automatically — no extra steps needed.

**Per-month** (`<subreddit>/<YYYY-MM>/authors.csv`):

| column | description |
|---|---|
| `author` | Reddit username |
| `comment_count` | Comments in that month |
| `submission_count` | Submissions in that month |
| `comment_score_total` | Sum of comment scores |
| `avg_comment_score` | Average comment score |
| `submission_score_total` | Sum of submission scores |
| `avg_submission_score` | Average submission score |
| `first_seen_utc` | Earliest activity timestamp |
| `last_seen_utc` | Latest activity timestamp |

**Subreddit-level** (`<subreddit>/authors.csv`) — aggregated across all extracted months,
including those from previous runs:

Same columns as above, plus `months_active` (number of distinct months with activity).

### 5. Build Comment Trees

Reconstruct threaded conversations:

```python
from pushshiftreader import TreeBuilder

builder = TreeBuilder("./extracted/AskHistorians")
builder.build_all_months()
```

Command line:

```bash
pushshiftreader build-trees ./extracted/AskHistorians
```

### 6. Load and Analyze

```python
from pushshiftreader import load_subreddit

data = load_subreddit("./extracted/AskHistorians")

# See what's available
print(f"Months: {data.months}")
print(f"Total submissions: {data.metadata.total_submissions:,}")

# Iterate over submissions
for submission in data.submissions("2023-01"):
    print(f"{submission.created_datetime}: {submission.title}")

# Iterate over comments
for comment in data.comments("2023-01"):
    if comment.score > 100:
        print(f"{comment.author}: {comment.body[:100]}...")

# Work with threaded data
for thread in data.threads("2023-01"):
    print(f"\n{thread.submission.title}")
    print(f"  {thread.comment_count} comments")

    # Walk the comment tree
    for comment, depth in thread.walk():
        indent = "  " * (depth + 1)
        print(f"{indent}{comment.author}: {comment.body[:50]}...")
```

## Archive Structure

The extractor expects Pushshift archives organized as:

```
reddit_dumps/
├── comments/
│   ├── RC_2005-12.zst
│   ├── RC_2006-01.zst
│   └── ...
└── submissions/
    ├── RS_2005-12.zst
    ├── RS_2006-01.zst
    └── ...
```

## Output Structure

Extracted data is organized by subreddit and month:

```
extracted/
└── AskHistorians/
    ├── metadata.json           # Subreddit-level stats (aggregated from all months)
    ├── authors.csv             # Per-author stats aggregated across all months
    ├── 2023-01/
    │   ├── metadata.json       # Month stats + patterns used
    │   ├── authors.csv         # Per-author stats for this month
    │   ├── submissions.csv     # Flat CSV
    │   ├── submissions.jsonl.gz # Compressed JSON lines
    │   ├── comments.csv
    │   ├── comments.jsonl.gz
    │   └── threads.jsonl.gz    # Nested thread structures (after build-trees)
    ├── 2023-02/
    │   └── ...
    └── ...
```

## API Reference

### Core Classes

#### `SubredditExtractor`

Extracts subreddit data from Pushshift archives.

```python
extractor = SubredditExtractor(
    archive_path: Path,                    # Root archive directory
    output_path: Path,                     # Where to write extracted data
    subreddits: List[str],                 # Subreddits to extract
    output_format: str = "both",           # "csv", "jsonl", or "both"
    show_progress: bool = True,
    include_patterns: List[str] = None,    # Keep records matching any pattern
    exclude_patterns: List[str] = None,    # Drop records matching any pattern
    force: bool = False,                   # Re-extract already-completed months
)

result = extractor.run(
    start_month: str = None,  # Optional "YYYY-MM" filter
    end_month: str = None
)
```

#### `TreeBuilder`

Reconstructs comment trees from extracted data.

```python
builder = TreeBuilder(
    extracted_path: Path,     # Path to extracted subreddit
    db_path: Path = None      # SQLite index location (default: in-memory)
)

builder.build_month("2023-01")  # Build specific month
builder.build_all_months()       # Build all months
```

#### `SubredditData`

Interface for accessing extracted data.

```python
data = load_subreddit("./extracted/AskHistorians")

data.months                      # List of available months
data.submissions(month=None)     # Iterator over submissions
data.comments(month=None)        # Iterator over comments
data.threads(month=None)         # Iterator over threads
data.get_submission(id)          # Find specific submission
data.get_thread(id)              # Find specific thread
```

### Data Models

#### `Submission`

```python
submission.id                    # Submission ID
submission.title                 # Post title
submission.selftext              # Post body (for self posts)
submission.author                # Author username
submission.subreddit             # Subreddit name
submission.score                 # Net upvotes
submission.num_comments          # Comment count
submission.created_utc           # Unix timestamp
submission.created_datetime      # Python datetime
submission.url_permalink         # Full Reddit URL
submission.is_deleted            # Was deleted by author
submission.is_removed            # Was removed by mods
```

#### `Comment`

```python
comment.id                       # Comment ID
comment.body                     # Comment text
comment.author                   # Author username
comment.score                    # Net upvotes
comment.link_id                  # Parent submission (t3_...)
comment.parent_id                # Parent comment/submission
comment.is_top_level             # Direct reply to submission
comment.submission_id            # Submission ID (no prefix)
comment.parent_comment_id        # Parent comment ID if reply
comment.created_datetime         # Python datetime
comment.url_permalink            # Full Reddit URL
```

#### `Thread`

```python
thread.submission                # Submission object
thread.comments                  # List of top-level CommentNodes
thread.all_comments              # Flat list of all comments
thread.comment_count             # Total comment count
thread.walk()                    # Iterator of (comment, depth) tuples
```

## Command Line

```bash
# List available archives
pushshiftreader list-archives /path/to/dumps -v

# Extract subreddits (completed months are skipped automatically)
pushshiftreader extract \
    --archive /path/to/dumps \
    --output ./extracted \
    --subreddits AskHistorians science \
    --format both \
    --start-month 2020-01 \
    --end-month 2023-12

# Filter by keyword/regex during extraction
pushshiftreader extract \
    --archive /path/to/dumps \
    --output ./extracted \
    --subreddits science \
    --include "climate" "CO2" \
    --exclude "\[deleted\]"

# Force re-extraction of already-completed months
pushshiftreader extract ... --force

# Build comment trees
pushshiftreader build-trees ./extracted/AskHistorians

# View extracted data info
pushshiftreader info ./extracted/AskHistorians -v
```

## Tips for Large Archives

1. **Extract to SSD** — Tree building does heavy random access
2. **Process in batches** — Use `--start-month` and `--end-month` to chunk work
3. **Resume freely** — Completed months are always skipped; no need to track progress manually
4. **Monitor disk space** — Extracted data is ~7x larger than compressed
5. **Use JSONL format** — If you only need Python access, skip CSV with `--format jsonl`
6. **Filter early** — Use `--include`/`--exclude` to reduce output size when studying a specific topic

## File Structure

```
pushshiftreader/
├── __init__.py          # Package exports and documentation
├── __main__.py          # CLI entry point
├── models.py            # Submission, Comment, Thread, CommentNode dataclasses
├── reader.py            # .zst streaming decompression
├── utils.py             # Logging, path handling, archive discovery
├── writers.py           # CSV and compressed JSON output
├── extractor.py         # Main subreddit extraction engine
├── trees.py             # Comment tree reconstruction with SQLite
├── loader.py            # Clean API for loading extracted data
├── cli.py               # Command-line interface
pyproject.toml           # Package configuration
```

## License

MIT License
