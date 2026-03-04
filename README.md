# pushshiftreader

A Python module for extracting and analyzing Reddit data from Pushshift archives.

## Features

- **Stream large archives** — Memory-efficient processing of multi-gigabyte .zst files
- **Extract by subreddit** — Pull specific subreddits from the full archive
- **Parallel extraction** — Process multiple months simultaneously with `workers=N` or `--workers -1`
- **Resumable extraction** — Re-run safely; completed months are skipped automatically
- **Keyword/regex filtering** — Filter posts and comments by content at extraction time
- **Author statistics** — Per-author activity counts, scores, and date ranges written to CSV
- **Reconstruct comment trees** — Rebuild threaded conversations with nested replies
- **Signal detection** — Annotate threads with custom boolean signals; ships with built-in detectors for regex, score thresholds, and OP participation
- **Signal detection presets** — One-call `get_detectors(preset)` factory with ready-made detectors for mod/admin actions, thread dynamics, CMV delta awards, and AITA verdicts
- **DataFrame export** — One-line export to pandas with thread-level features (depth, thread size, response time, submission metadata) joined in; optional signals join
- **Graph export** — Export conversation graphs (comment→reply) and author-interaction graphs (who replies to whom) as node/edge CSV files
- **Multiple output formats** — CSV for easy analysis, compressed JSON for full fidelity
- **Simple API** — Clean Python interface for research workflows
- **Archive catalogue** — Single streaming pass over raw dumps to build a per-(subreddit, month) stats table (post counts + unique authors); resumable
- **Cross-subreddit author index** — Find authors active across multiple subreddits from already-extracted `authors.csv` files; no re-streaming required
- **Cross-dataset word search** — Search any regex pattern across the entire archive (all subreddits, all months) and collect every matching comment or post with full metadata; resumable and parallelisable
- **Subreddit index** — One-row-per-subreddit CSV from the raw archives: post/comment counts, first/last active month, NSFW flag, and peak subscriber count; resumable

## Installation

```bash
# Clone or download the package
cd pushshiftreader

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install with progress bars
pip install -e ".[progress]"

# Install with DataFrame export support
pip install -e ".[analysis]"

# Install everything
pip install -e ".[all]"

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

### 3. Speed Up Extraction with Parallel Workers

For large archives, process multiple months at the same time by setting `workers`:

```python
extractor = SubredditExtractor(
    archive_path="/path/to/reddit_dumps",
    output_path="./extracted",
    subreddits=["ChangeMyView"],
    workers=-1,   # use all available CPU cores
)
result = extractor.run()
```

```bash
# Use all CPU cores
pushshiftreader extract \
    --archive /path/to/reddit_dumps \
    --output ./extracted \
    --subreddits ChangeMyView \
    --workers -1

# Or specify an exact count
pushshiftreader extract ... --workers 8
```

Each month is processed in an independent worker process. The sequential code path (the default, `workers=1`) is unchanged — per-file progress bars still work as before. With `workers > 1`, a per-month completion bar is shown instead.

Resumability works normally in parallel mode: already-completed months are detected before any workers are launched and simply skipped.

### 4. Filter by Keyword or Regex

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

### 5. Author Statistics

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

### 6. Build Comment Trees

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

### 7. Detect Signals

After building trees, run custom detectors over thread data to produce a
`signals.csv` per month.  Each detector adds one boolean column; only rows
where at least one signal fires are written (sparse output, easy to join with
`comments.csv`).

**Subclass `Detector` to define your own signal:**

```python
from pushshiftreader import SignalDetector, Detector, RegexDetector, AuthorIsOPDetector

class DeltaDetector(Detector):
    """Fires when a comment contains a delta award (CMV convention)."""
    def detect_comment(self, comment, thread, depth=0):
        return 'Δ' in comment.body or '!delta' in comment.body.lower()

sd = SignalDetector(
    "./extracted/ChangeMyView",
    detectors=[
        DeltaDetector("delta_awarded"),
        AuthorIsOPDetector("op_comment"),
        RegexDetector("cites_source", r"https?://"),
    ],
)
results = sd.run_all_months()
# writes extracted/ChangeMyView/YYYY-MM/signals.csv for each month
```

**Built-in detectors:**

| Class | Description |
|---|---|
| `RegexDetector(name, pattern, record_type, fields)` | Fires if any text field matches the regex |
| `ScoreDetector(name, min_score, max_score, record_type)` | Fires if score is within the given bounds |
| `AuthorIsOPDetector(name)` | Fires if the commenter is the submission's OP |

**`signals.csv` format** (one row per record where at least one signal fired):

| column | description |
|---|---|
| `record_id` | Comment or submission ID |
| `record_type` | `"comment"` or `"submission"` |
| `<signal_name>` | One boolean column per detector |

Join with `comments.csv` on `record_id`; a missing row means all signals are `False`.

### 8. Signal Detection Presets

Instead of defining detectors from scratch, use `get_detectors()` to get a
ready-made list for common scenarios:

```python
from pushshiftreader import get_detectors, SignalDetector

# General preset: mod/admin actions + thread dynamics (works for any subreddit)
sd = SignalDetector("./extracted/AskHistorians",
                    detectors=get_detectors('general'))
sd.run_all_months()

# CMV preset: general + delta award detection
sd = SignalDetector("./extracted/ChangeMyView",
                    detectors=get_detectors('cmv'))
sd.run_all_months()

# AITA preset: general + verdict keyword detection
sd = SignalDetector("./extracted/AmITheAsshole",
                    detectors=get_detectors('aita'))
sd.run_all_months()
```

**Available presets:**

| Preset | Alias | Signals included |
|---|---|---|
| `'general'` | — | `stickied_comment`, `mod_distinguished`, `content_removed`, `author_deleted`, `top_level_comment`, `op_comment` |
| `'cmv'` | `'changemyview'` | All general signals + `delta_awarded` |
| `'aita'` | `'amitheasshole'` | All general signals + `aita_verdict` |

**All preset detector classes** (can be used individually too):

| Class | Signal fires when… |
|---|---|
| `StickiedCommentDetector` | Comment is stickied |
| `ModDistinguishedDetector` | Comment/submission is distinguished by mod or admin |
| `ContentRemovedDetector` | Body is `[removed]` or `removed_by_category` is set |
| `AuthorDeletedDetector` | Author is `[deleted]` or body is `[deleted]` |
| `TopLevelCommentDetector` | Comment is a direct reply to the submission |
| `DepthDetector(name, min_depth, max_depth)` | Comment depth falls within the given range |
| `DeltaAwardedDetector` | Comment contains `Δ` or `!delta` (CMV) |
| `AITAVerdictDetector` | Comment contains `NTA`/`YTA`/`ESH`/`NAH`/`INFO` (AITA) |

### 9. Export to pandas DataFrame

After building trees, export comments or submissions to a pandas DataFrame
with thread-level features automatically joined in.

```python
from pushshiftreader import load_subreddit

data = load_subreddit("./extracted/ChangeMyView")

# All comments across all months — with depth, thread size, response time,
# and submission metadata joined in per row.
# If signals.csv exists for a month it is joined in automatically.
df = data.comments_dataframe()

# Single month
df = data.comments_dataframe("2019-06")

# Submissions DataFrame
subs = data.submissions_dataframe()

# Skip signals join
df = data.comments_dataframe(signals=False)
```

**Columns added on top of standard comment fields:**

| column | description |
|---|---|
| `month` | Source month (``YYYY-MM``) |
| `depth` | Depth in reply tree (0 = top-level comment) |
| `thread_size` | Total comments in the thread |
| `time_since_submission` | Seconds between submission post time and comment post time |
| `submission_id` | Parent submission ID |
| `submission_title` | Parent submission title |
| `submission_score` | Parent submission score |
| `submission_author` | Parent submission author |
| `<signal_name>` | Boolean signal columns, if ``signals.csv`` exists |

You can also convert a single `Thread` object directly:

```python
for thread in data.threads("2019-06"):
    df = thread.to_dataframe()   # same columns as above, no 'month'
    high_scoring = df[df['score'] > 50]
```

### 10. Export Graphs

Export conversation structure or author-interaction networks as node/edge CSV files,
ready for Gephi, NetworkX, or any graph tool.

**Comment/conversation graph** — nodes are submissions and comments, edges are replies:

```python
from pushshiftreader import load_subreddit

data = load_subreddit("./extracted/ChangeMyView")

# Writes comment_graph_nodes.csv and comment_graph_edges.csv
data.export_comment_graph("./graphs/")

# Single month
data.export_comment_graph("./graphs/", month="2019-06")
```

**Author-interaction graph** — nodes are usernames, edges represent reply relationships:

```python
# Writes author_graph_nodes.csv and author_graph_edges.csv
data.export_author_graph("./graphs/")
```

Both methods also work on individual `Thread` objects (returning lists of dicts rather than writing to disk):

```python
for thread in data.threads("2019-06"):
    nodes, edges = thread.to_comment_graph()
    nodes, edges = thread.to_author_graph()
```

**Output file schemas:**

`comment_graph_nodes.csv`:

| column | description |
|---|---|
| `node_id` | `t3_<id>` for submissions, `t1_<id>` for comments |
| `type` | `submission` or `comment` |
| `author` | Reddit username |
| `score` | Net upvotes |
| `created_utc` | Unix timestamp |
| `depth` | 0 = submission root, 1 = top-level reply, etc. |

`comment_graph_edges.csv`:

| column | description |
|---|---|
| `source` | Parent node ID |
| `target` | Child node ID |
| `time_delta` | Seconds from parent post time to child post time |

`author_graph_nodes.csv`:

| column | description |
|---|---|
| `author` | Reddit username |
| `comment_count` | Total comments in the dataset |
| `total_score` | Sum of comment scores |
| `first_seen_utc` / `last_seen_utc` | Activity window |

`author_graph_edges.csv`:

| column | description |
|---|---|
| `source` | Replying author |
| `target` | Author being replied to |
| `weight` | Number of times source replied to target |
| `first_interaction_utc` | Timestamp of first interaction |

### 11. Load and Analyze

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

### 12. Build an Archive Catalogue

Scan all raw archives and produce a single CSV table with per-(subreddit, month)
record counts and unique author totals.  The pass is resumable — re-running
skips already-processed months automatically.

```python
from pushshiftreader import ArchiveCatalogue

cat = ArchiveCatalogue(
    archive_path="/path/to/dumps",
    output_path="catalogue.csv",
)

result = cat.run(
    start_month="2013-01",   # optional
    end_month="2013-06",     # optional
    min_activity=10,         # skip subreddits with < 10 records in a month
)
print(f"Wrote {result['rows_written']:,} rows for {result['subreddits_seen']:,} subreddits")
```

Output CSV columns: `subreddit, month, n_submissions, n_comments, n_unique_authors`

### 13. Cross-Subreddit Author Index

Find authors who are active across multiple extracted subreddits.  Reads the
already-aggregated `authors.csv` from each subreddit directory — no
re-streaming of archives.

```python
from pushshiftreader import CrossSubIndex

# Auto-discover all subreddit dirs that have an authors.csv
idx = CrossSubIndex.from_directory("./extracted")
idx.build(min_subreddits=2)   # keep authors in >= 2 subreddits
result = idx.save("./crosssub/")
print(f"{result['authors']:,} authors across {result['pairs']:,} subreddit pairs")
```

Or target specific subreddits:

```python
idx = CrossSubIndex.from_directory(
    "./extracted",
    subreddits=["AskHistorians", "AskScience", "science"],
)
idx.build(min_subreddits=2).save("./crosssub/")
```

**Output files** (written to `output_dir/`):

| File | Description |
|---|---|
| `author_activity.csv` | One row per author × subreddit pair with activity stats |
| `author_summary.csv` | One row per author — totals + pipe-separated subreddit list |

`author_summary.csv` columns: `author, n_subreddits, subreddits, total_comments,
total_submissions, total_months_active, first_seen_utc, last_seen_utc`

### 14. Build a Subreddit Index

`SubredditIndex` makes a single streaming pass over the raw archives and
produces one CSV file listing **every subreddit** in the dataset with
aggregated statistics.

```python
from pushshiftreader import SubredditIndex

idx = SubredditIndex(
    archive_path="/path/to/reddit_dumps",
    output_path="./subreddits.csv",
)

result = idx.run()
print(f"Found {result['subreddits']:,} subreddits → {result['output_path']}")
```

**Date-range filtering** and **minimum activity threshold:**

```python
result = idx.run(
    start_month="2015-01",
    end_month="2022-12",
    min_records=10,   # omit subreddits with fewer than 10 total posts/comments
)
```

**Resumable** — intermediate per-month files are written to
`subreddits_months/` alongside the output CSV.  Re-running automatically
skips already-processed months:

```python
# Safe to re-run; completed months are skipped
result = idx.run()
```

**Output columns:**

| column | description |
|---|---|
| `subreddit` | Subreddit name |
| `subreddit_id` | Reddit's internal subreddit ID |
| `n_submissions` | Total submissions across all time |
| `n_comments` | Total comments across all time |
| `first_month` | Earliest month with any activity (YYYY-MM) |
| `last_month` | Latest month with any activity (YYYY-MM) |
| `months_active` | Number of distinct months with activity |
| `over_18` | `True` if any submission was ever NSFW-tagged |
| `subreddit_subscribers` | Peak subscriber count observed across all records |

### 15. Search Across All Archives

`WordSearcher` scans every monthly archive file (all subreddits, all time) for a
regex pattern and collects every matching comment or submission.  Useful for
tracking how a word or phrase has been used across Reddit over time.

```python
from pushshiftreader import WordSearcher

searcher = WordSearcher(
    archive_path="/path/to/reddit_dumps",
    output_path="./search_results/nudge",
    pattern=r"nudg",          # matches nudge, nudged, nudging, nudger …
)

result = searcher.run()
print(f"Found {result.total_comments:,} comments, "
      f"{result.total_submissions:,} submissions "
      f"across {result.months_processed} months")
```

**Date-range filtering:**

```python
result = searcher.run(start_month="2015-01", end_month="2020-12")
```

**Parallel workers:**

```python
searcher = WordSearcher(
    archive_path="/path/to/reddit_dumps",
    output_path="./search_results/nudge",
    pattern=r"nudg",
    workers=-1,   # use all CPU cores
)
result = searcher.run()
```

**Resumable** — like all other tools, runs are interruptible.  Each month
writes a `metadata.json` completion marker; on restart, already-done months
are skipped:

```python
# Just re-run the same searcher — completed months are skipped automatically
result = searcher.run()

# Force a full re-search (overwrites existing output)
searcher = WordSearcher(..., force=True)
```

**Output layout:**

```
search_results/nudge/
├── metadata.json           # Summary: pattern, total counts, duration
├── comments.csv            # Assembled flat CSV (after assemble_results)
├── submissions.csv         # Assembled flat CSV (after assemble_results)
├── 2015-01/
│   ├── comments.jsonl.gz   # All matching comments (raw Pushshift records)
│   ├── submissions.jsonl.gz
│   └── metadata.json       # Month completion marker + match counts
├── 2015-02/
│   └── ...
```

Every output record is the raw JSON object from the archive — all fields are
preserved (subreddit, author, score, created\_utc, body/title, etc.).

**Assemble results into flat CSVs:**

After a search (or at any point during an interrupted run), call
`assemble_results()` to merge all per-month files into two flat CSVs in the
same directory:

```python
counts = searcher.assemble_results()
# writes search_results/nudge/comments.csv
#        search_results/nudge/submissions.csv
print(counts)  # {'comments': 42318, 'submissions': 1204}
```

Or call the standalone function on any existing output directory:

```python
from pushshiftreader import assemble_search_results

assemble_search_results("./search_results/nudge")
```

Each CSV has a leading `month` (YYYY-MM) column, followed by the standard
comment or submission field set. Only months with a `metadata.json` completion
marker are included, so it is safe to call mid-way through an interrupted run.

**Constructor options:**

```python
WordSearcher(
    archive_path: Path,              # Root archive directory
    output_path: Path,               # Where to write results
    pattern: str,                    # Regex pattern
    case_sensitive: bool = False,    # Default: case-insensitive
    search_comments: bool = True,
    search_submissions: bool = True,
    output_format: str = "jsonl",    # "csv", "jsonl", or "both"
    workers: int = 1,                # Parallel processes (-1 = all cores)
    force: bool = False,
    show_progress: bool = True,
)
```

**Performance note:** `WordSearcher` uses a two-stage filter — the raw NDJSON
line is checked against the regex *before* JSON parsing.  Because the body/title
text is embedded verbatim in the line, this skips JSON parsing for the vast
majority of non-matching records, making the search significantly faster than
parsing everything first.

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
    │   ├── threads.jsonl.gz    # Nested thread structures (after build-trees)
    │   └── signals.csv         # Signal detection results (after signal detection)
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
    workers: int = 1,                      # Parallel worker processes (-1 = all cores)
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

#### `SignalDetector`

Runs detectors over thread data and writes `signals.csv`.

```python
sd = SignalDetector(
    extracted_path: Path,          # Path to extracted subreddit directory
    detectors: List[Detector],     # One or more Detector instances (names must be unique)
)

sd.run_month("2023-01")            # Detect signals for one month
sd.run_all_months()                # Detect signals for all months with threads
```

#### `Detector`

Abstract base class for custom signals.  Override either or both methods:

```python
class MyDetector(Detector):
    def detect_comment(self, comment: Comment, thread: Thread, depth: int = 0) -> bool: ...
    def detect_submission(self, submission: Submission, thread: Thread, depth: int = 0) -> bool: ...
```

The `depth` parameter (0 = top-level comment) is passed automatically by `SignalDetector`.

#### `SubredditIndex`

Builds a per-subreddit aggregated index from all raw Pushshift archives.

```python
idx = SubredditIndex(
    archive_path: Path,          # Root archive directory
    output_path: Path,           # Destination CSV (e.g. "subreddits.csv")
    show_progress: bool = True,
)

result = idx.run(
    start_month: str = None,     # Optional "YYYY-MM" filter
    end_month: str = None,
    min_records: int = 1,        # Exclude subreddits below this activity threshold
)
# result['subreddits']        → number of subreddits written
# result['months_processed']  → months processed in this run
# result['output_path']       → path to the final CSV
```

Intermediate per-month files are stored in `<output_path.stem>_months/`
alongside the output CSV and are used for resumability.

#### `WordSearcher`

Searches all Pushshift archives for a regex pattern and collects matching records.

```python
searcher = WordSearcher(
    archive_path: Path,              # Root archive directory
    output_path: Path,               # Where to write results
    pattern: str,                    # Regex pattern (case-insensitive by default)
    case_sensitive: bool = False,
    search_comments: bool = True,
    search_submissions: bool = True,
    output_format: str = "jsonl",    # "csv", "jsonl", or "both"
    workers: int = 1,                # -1 = all CPU cores
    force: bool = False,
    show_progress: bool = True,
)

result = searcher.run(
    start_month: str = None,   # Optional "YYYY-MM" filter
    end_month: str = None,
)
# result.total_comments, result.total_submissions, result.months_processed
# result.stats  → List[SearchStats] (per-month counts)

counts = searcher.assemble_results()
# Merges all per-month files → output_path/comments.csv + submissions.csv
# counts == {'comments': N, 'submissions': M}
```

#### `assemble_search_results(output_path)`

Standalone version of `WordSearcher.assemble_results()`. Reads all completed
month subdirectories in `output_path` and writes two flat CSVs:

- `output_path/comments.csv` — `month` + all comment fields
- `output_path/submissions.csv` — `month` + all submission fields

Prefers `.jsonl.gz` per month; falls back to `.csv`. Safe to call on a
partial/interrupted search — only months with `metadata.json` are included.

```python
from pushshiftreader import assemble_search_results

counts = assemble_search_results("./search_results/nudge")
# {'comments': 42318, 'submissions': 1204}
```

#### `get_detectors(preset='general')`

Factory returning a ready-to-use list of `Detector` instances for a named preset.
Presets: `'general'`, `'cmv'`/`'changemyview'`, `'aita'`/`'amitheasshole'`.

#### `SubredditData`

Interface for accessing extracted data.

```python
data = load_subreddit("./extracted/AskHistorians")

data.months                                  # List of available months
data.submissions(month=None)                 # Iterator over submissions
data.comments(month=None)                    # Iterator over comments
data.threads(month=None)                     # Iterator over threads
data.get_submission(id)                      # Find specific submission
data.get_thread(id)                          # Find specific thread
data.comments_dataframe(month, signals)      # pandas DataFrame of comments
data.submissions_dataframe(month, signals)   # pandas DataFrame of submissions
data.export_comment_graph(output_dir, month) # write conversation graph CSVs
data.export_author_graph(output_dir, month)  # write author-interaction graph CSVs
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
thread.to_dataframe()            # pandas DataFrame with thread features joined
thread.to_comment_graph()        # (nodes, edges) lists for conversation graph
thread.to_author_graph()         # (node_stats, edge_stats) for author graph
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

# Parallel extraction — use all CPU cores
pushshiftreader extract \
    --archive /path/to/dumps \
    --output ./extracted \
    --subreddits AskHistorians \
    --workers -1

# Build comment trees
pushshiftreader build-trees ./extracted/AskHistorians

# View extracted data info
pushshiftreader info ./extracted/AskHistorians -v

# Build an archive catalogue (all subreddits, one month range)
pushshiftreader catalogue \
    --archive /path/to/dumps \
    --output catalogue.csv \
    --start-month 2013-01 \
    --end-month 2013-12 \
    --min-activity 10

# Resume an interrupted catalogue run (already-processed months are skipped)
pushshiftreader catalogue --archive /path/to/dumps --output catalogue.csv

# Cross-subreddit author index (all extracted subreddits)
pushshiftreader cross-sub-index \
    --extracted ./extracted \
    --output ./crosssub/

# Limit to specific subreddits and require >= 3 common subreddits
pushshiftreader cross-sub-index \
    --extracted ./extracted \
    --subreddits AskHistorians AskScience science \
    --output ./crosssub/ \
    --min-subreddits 3
```

## Tips for Large Archives

1. **Use parallel workers** — `--workers -1` saturates all CPU cores; the bottleneck for large archives is .zst decompression, which scales well across months
2. **Extract to SSD** — Tree building does heavy random access
3. **Process in batches** — Use `--start-month` and `--end-month` to chunk work
4. **Resume freely** — Completed months are always skipped; no need to track progress manually
5. **Monitor disk space** — Extracted data is ~7x larger than compressed
6. **Use JSONL format** — If you only need Python access, skip CSV with `--format jsonl`
7. **Filter early** — Use `--include`/`--exclude` to reduce output size when studying a specific topic

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
├── signals.py           # Signal detection framework and built-in detectors
├── presets.py           # Built-in detector presets and get_detectors() factory
├── loader.py            # Clean API for loading extracted data
├── catalogue.py         # Archive catalogue builder (ArchiveCatalogue, SubredditIndex)
├── crosssub.py          # Cross-subreddit author index (CrossSubIndex)
├── searcher.py          # Cross-dataset word/pattern search (WordSearcher)
├── cli.py               # Command-line interface
pyproject.toml           # Package configuration
```

## License

MIT License
