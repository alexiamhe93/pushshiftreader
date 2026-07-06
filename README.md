# pushshiftreader

`pushshiftreader` is a research-oriented Python package for working with Reddit Pushshift archives.

The package is built around a canonical Parquet pipeline:

- extract full subreddit corpora from raw monthly `.zst` archives
- build lightweight archive-wide catalogue tables for longitudinal discovery
- track named keyword sets across time
- reconstruct thread-aware comment tables
- detect sparse analytical signals over submissions and comments
- build cross-subreddit author-overlap indexes from extracted corpora

It is designed for social science and computational research workflows where reproducibility, resumability, and downstream analysis matter more than ad hoc one-off scripts.

## Current Scope

- Canonical storage is Parquet plus JSON metadata.
- Extraction is resumable at the month level.
- Tracking is monthly by default and produces both matched corpora and aggregate tables.
- Thread building and signal detection run on extracted corpora, not raw archives.
- Catalogue and cross-subreddit indexing are first-class workflows.
- Progress logging during extraction is periodic and now emits a single final `100%` update per archive.

## Installation

```bash
git clone https://github.com/alexiamhe93/pushshiftreader.git
cd pushshiftreader

python -m venv .venv
source .venv/bin/activate

pip install -e .
```

Useful extras:

```bash
pip install -e ".[analysis]"   # pandas helpers
pip install -e ".[progress]"   # tqdm support
pip install -e ".[all]"        # everything
```

## Live Reddit API Scraping

For recent Reddit data, use `scrape-reddit`. It authenticates with a Reddit app through OAuth client credentials and writes newline-delimited JSON files for submissions and comments.

If you prefer not to put credentials in your shell, run `notebooks/reddit_api_scrape_wales.ipynb`. It prompts for the Reddit secret with a hidden `getpass` field and then calls the same CLI under the hood.

Set credentials in your shell:

```bash
export REDDIT_CLIENT_ID="your-app-client-id"
export REDDIT_CLIENT_SECRET="your-app-secret"
export REDDIT_USER_AGENT="macos:pushshiftreader:0.1 by u/your_username"
export REDDIT_AUTH_MODE="client_credentials"
```

If your Reddit app is an installed app, use `REDDIT_AUTH_MODE=installed_client` and leave `REDDIT_CLIENT_SECRET` blank.

Scrape the Welsh target subreddits (`r/wales`, `r/Cardiff`, `r/southwales`, `r/northwales`):

```bash
pushshiftreader scrape-reddit \
  --output ./runs/reddit-api/wales-communities \
  --sort new \
  --days 30 \
  --max-submissions 100
```

This creates one folder per subreddit:

```text
runs/reddit-api/wales-communities/
  metadata.json
  wales/submissions.jsonl
  wales/comments.jsonl
  Cardiff/submissions.jsonl
  Cardiff/comments.jsonl
```

Useful options:

- `--subreddits wales Cardiff southwales northwales` overrides the defaults.
- `--comments-limit 500` controls the maximum comment listing size per submission.
- `--comment-depth 3` limits nested comment depth.
- `--skip-comments` downloads submissions only, which is best for a broad first pass.
- `--days 30` keeps submissions from the past 30 days.
- `--since 2026-04-01 --until 2026-04-30` keeps an explicit UTC date range.
- `--include-raw` preserves each API object under a `raw` field.

Live API scrape output is JSONL rather than the Pushshift Parquet layout. The notebook includes a CSV export cell that combines all subreddit submission rows into `all_submissions.csv` and all comment rows into `all_comments.csv`.

## Raw Archive Layout

The package expects a raw archive root like this:

```text
/path/to/reddit/
  comments/
    RC_2012-01.zst
    RC_2012-02.zst
  submissions/
    RS_2012-01.zst
    RS_2012-02.zst
```

## Quick Start

### 1. Extract a subreddit corpus

```bash
pushshiftreader extract \
  --archive /path/to/reddit \
  --output ./runs/extracted \
  --subreddits science \
  --start-month 2012-01 \
  --end-month 2012-03
```

This writes a canonical corpus at `./runs/extracted/science`.

Extraction is safe to rerun. Completed months are skipped unless you pass `--force`.

You can also filter during extraction:

```bash
pushshiftreader extract \
  --archive /path/to/reddit \
  --output ./runs/extracted \
  --subreddits science \
  --include "climate change" "global warming" \
  --exclude "\\[deleted\\]"
```

### 2. Inspect the extracted corpus

```bash
pushshiftreader info ./runs/extracted/science
pushshiftreader analyze-smoke ./runs/extracted/science
```

### 3. Track keyword sets across time

CLI-only tracking:

```bash
pushshiftreader track ./runs/extracted/science \
  --output ./runs/tracking/science-climate \
  --term-set climate=climate,warming \
  --regex-set climate=carbon\\s+tax
```

Config-driven tracking:

```json
{
  "keyword_sets": [
    {
      "name": "climate",
      "terms": ["climate", "warming"],
      "regexes": ["carbon\\s+tax"]
    },
    {
      "name": "policy",
      "terms": ["policy", "regulation"]
    }
  ]
}
```

```bash
pushshiftreader track ./runs/extracted/science \
  --output ./runs/tracking/science-topics \
  --keyword-config ./keyword_sets.json
```

### 4. Build thread-aware comment tables

```bash
pushshiftreader build-threads ./runs/extracted/science
```

This creates monthly Parquet tables with thread-aware fields such as:

- `submission_id`
- `parent_comment_id`
- `parent_author`
- `submission_author`
- `depth`
- `thread_size`
- `time_since_submission`
- `missing_parent`
- `is_top_level`

### 5. Detect signals

Use built-in presets:

```bash
pushshiftreader detect-signals ./runs/extracted/science \
  --preset general
```

Or add custom regex signals:

```bash
pushshiftreader detect-signals ./runs/extracted/science \
  --preset general \
  --regex-signal mentions_climate=both:climate \
  --regex-signal cites_url=comment:https?://
```

Supported presets:

- `general`
- `cmv` / `changemyview`
- `aita` / `amitheasshole`

### 6. Build a raw-archive catalogue

The catalogue is the high-level longitudinal map of the archive.

```bash
pushshiftreader catalogue \
  --archive /path/to/reddit \
  --output ./runs/catalogue \
  --start-month 2012-01 \
  --end-month 2012-03
```

Then rebuild or filter the subreddit-level index:

```bash
pushshiftreader subreddit-index ./runs/catalogue --min-records 1000
```

### 7. Discover dream-report subreddits

For dream-corpus enrichment, scan submissions and rank communities that look like firsthand dream-report spaces:

```bash
pushshiftreader discover-dream-subreddits \
  --archive /path/to/reddit \
  --catalogue ./runs/catalogue \
  --output ./runs/dream-discovery \
  --start-month 2024-01 \
  --end-month 2024-12
```

This writes:

- `candidates.csv`: ranked machine-readable candidate table
- `shortlist.txt`: quick human-readable shortlist
- `metadata.json`: run metadata

### 8. Build a cross-subreddit author index

Once you have multiple extracted corpora in one directory:

```text
./runs/extracted/
  askreddit/
  science/
  changemyview/
```

you can build an author overlap index without rescanning the raw archives:

```bash
pushshiftreader cross-sub-index \
  --extracted ./runs/extracted \
  --output ./runs/crosssub \
  --min-subreddits 2
```

### 9. Export extracted dream submissions for CassiusDay

After extracting approved dream subreddits, convert their submission corpora into the flat dream-source schema used by `CassiusDay_program`:

```bash
pushshiftreader export-dreams \
  --source ./runs/extracted \
  --subreddits DreamJournal LucidDreaming \
  --output ./runs/dream_exports/reddit_dreams.jsonl
```

The export is JSONL and preserves provenance fields such as subreddit, post ID, permalink, and author.

## Output Layout

### Extracted corpus

```text
science/
  dataset.json
  authors_summary.parquet
  months/
    2012-01.json
  submissions/
    2012-01.parquet
  comments/
    2012-01.parquet
  authors/
    2012-01.parquet
  threads/
    2012-01.parquet
  signals/
    2012-01.parquet
    2012-01.json
  signals.json
```

### Tracking run

```text
science-climate/
  tracking.json
  months/
    2012-01.json
  matches/
    comments/2012-01.parquet
    submissions/2012-01.parquet
  aggregates/
    monthly/2012-01.parquet
    terms/2012-01.parquet
    monthly_counts.parquet
    term_counts.parquet
```

### Catalogue run

```text
catalogue/
  catalogue.json
  catalogue.parquet
  subreddit_index.parquet
  months/
    2012-01.parquet
    2012-01.json
```

### Cross-subreddit index

```text
crosssub/
  crosssub.json
  author_activity.parquet
  author_summary.parquet
```

## Python API

### Extract

```python
from pushshiftreader import SubredditExtractor

result = SubredditExtractor(
    archive_path="/path/to/reddit",
    output_path="./runs/extracted",
    subreddits=["science"],
).run(start_month="2012-01", end_month="2012-03")

print(result.total_comments)
```

### Load a corpus

```python
from pushshiftreader import load_corpus

dataset = load_corpus("./runs/extracted/science")
print(dataset.subreddit)
print(dataset.months)

comments_df = dataset.comments_dataframe()
submissions_df = dataset.submissions_dataframe()
```

If signal tables exist, `comments_dataframe()` and `submissions_dataframe()` automatically left-join the signal columns.

### Track keyword sets

```python
from pushshiftreader import KeywordSet, KeywordTracker

tracker = KeywordTracker(
    dataset_path="./runs/extracted/science",
    output_path="./runs/tracking/science-climate",
    keyword_sets=[
        KeywordSet(name="climate", terms=["climate", "warming"]),
        KeywordSet(name="policy", regexes=[r"carbon\s+tax"]),
    ],
)

result = tracker.run()
print(result.total_comments, result.total_submissions)
```

### Build threads

```python
from pushshiftreader import TreeBuilder

TreeBuilder("./runs/extracted/science").build_all_months()
```

### Detect signals

```python
from pushshiftreader import SignalDetector, RegexDetector, get_detectors

detector = SignalDetector(
    "./runs/extracted/science",
    detectors=get_detectors("general") + [
        RegexDetector("mentions_climate", r"climate", record_type="both"),
    ],
)

detector.run_all_months()
```

### Build a smoke report

```python
from pushshiftreader import build_smoke_report

report = build_smoke_report(
    dataset_path="./runs/extracted/science",
    tracking_path="./runs/tracking/science-climate",
)

print(report["monthly_summary"])
```

## CLI Reference

```text
pushshiftreader extract
pushshiftreader catalogue
pushshiftreader subreddit-index
pushshiftreader track
pushshiftreader build-threads
pushshiftreader detect-signals
pushshiftreader cross-sub-index
pushshiftreader info
pushshiftreader analyze-smoke
pushshiftreader list-archives
```

Use `pushshiftreader <command> --help` for full argument details.

## Notes

- The package currently optimizes for correctness, resumability, and research-friendly outputs rather than maximal ingestion speed.
- Canonical internal storage is Parquet. CSV is no longer the core storage contract.
- The `workers` argument is present on extraction, but parallel extraction is not implemented yet; non-default values currently fall back to sequential execution with a warning.
- Some older utility modules from earlier versions of the package have been replaced by the new canonical pipeline.

## Verification

The current codebase is covered by fixture-driven tests for:

- extraction and resume behavior
- progress logging
- catalogue building
- keyword tracking
- thread reconstruction
- signal detection
- cross-subreddit author overlap
- CLI smoke paths

Run the suite with:

```bash
python -m pytest
```
