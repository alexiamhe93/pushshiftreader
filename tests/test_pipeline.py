import csv
import json
from pathlib import Path

import zstandard

from pushshiftreader import (
    ArchiveCatalogue,
    AuthorIsOPDetector,
    CrossSubIndex,
    DreamCorpusExporter,
    DreamSubredditDiscoverer,
    KeywordSet,
    KeywordTracker,
    RedditSubmissionScraper,
    RedditCommentFetcher,
    RegexDetector,
    SignalDetector,
    SubredditIndex,
    SubredditExtractor,
    TreeBuilder,
    build_smoke_report,
    load_corpus,
)
from pushshiftreader.cli import main as cli_main
from pushshiftreader.storage import TrackingLayout, read_json, read_parquet_records


def _write_zst_jsonl(path: Path, records):
    compressor = zstandard.ZstdCompressor()
    payload = "\n".join(json.dumps(record) for record in records).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as handle:
        handle.write(compressor.compress(payload))


def _archive_fixture(root: Path) -> Path:
    archive_root = root / "archives"

    _write_zst_jsonl(
        archive_root / "submissions" / "RS_2020-01.zst",
        [
            {
                "id": "s1",
                "subreddit": "science",
                "subreddit_id": "t5_science",
                "author": "alice",
                "title": "Climate policy and science",
                "selftext": "Climate change and warming are central topics.",
                "created_utc": 1577836800,
                "score": 10,
                "num_comments": 2,
                "permalink": "/r/science/comments/s1/",
            },
            {
                "id": "s_noise",
                "subreddit": "AskReddit",
                "subreddit_id": "t5_askreddit",
                "author": "alice",
                "title": "Unrelated",
                "selftext": "Ignore this",
                "created_utc": 1577836801,
                "score": 1,
                "num_comments": 0,
                "permalink": "/r/AskReddit/comments/s_noise/",
            },
        ],
    )
    _write_zst_jsonl(
        archive_root / "comments" / "RC_2020-01.zst",
        [
            {
                "id": "c1",
                "subreddit": "science",
                "subreddit_id": "t5_science",
                "author": "bob",
                "body": "Climate change is real.",
                "created_utc": 1577836900,
                "score": 4,
                "link_id": "t3_s1",
                "parent_id": "t3_s1",
            },
            {
                "id": "c2",
                "subreddit": "science",
                "subreddit_id": "t5_science",
                "author": "carol",
                "body": "Yes, warming is accelerating.",
                "created_utc": 1577837000,
                "score": 3,
                "link_id": "t3_s1",
                "parent_id": "t1_c1",
            },
            {
                "id": "c_noise",
                "subreddit": "AskReddit",
                "subreddit_id": "t5_askreddit",
                "author": "alice",
                "body": "Ignore this",
                "created_utc": 1577837100,
                "score": 1,
                "link_id": "t3_s_noise",
                "parent_id": "t3_s_noise",
            },
        ],
    )

    _write_zst_jsonl(
        archive_root / "submissions" / "RS_2020-02.zst",
        [
            {
                "id": "s2",
                "subreddit": "science",
                "subreddit_id": "t5_science",
                "author": "dora",
                "title": "Energy policy",
                "selftext": "A carbon tax can change incentives.",
                "created_utc": 1580515200,
                "score": 9,
                "num_comments": 2,
                "permalink": "/r/science/comments/s2/",
            }
        ],
    )
    _write_zst_jsonl(
        archive_root / "comments" / "RC_2020-02.zst",
        [
            {
                "id": "c4",
                "subreddit": "science",
                "subreddit_id": "t5_science",
                "author": "erin",
                "body": "A carbon tax now.",
                "created_utc": 1580515300,
                "score": 5,
                "link_id": "t3_s2",
                "parent_id": "t3_s2",
            },
            {
                "id": "c5",
                "subreddit": "science",
                "subreddit_id": "t5_science",
                "author": "frank",
                "body": "This climate policy needs discussion.",
                "created_utc": 1580515400,
                "score": 2,
                "link_id": "t3_s2",
                "parent_id": "t1_missing",
            },
        ],
    )

    return archive_root


def _dream_archive_fixture(root: Path) -> Path:
    archive_root = root / "dream_archives"

    _write_zst_jsonl(
        archive_root / "submissions" / "RS_2024-01.zst",
        [
            {
                "id": "dj1",
                "subreddit": "DreamJournal",
                "subreddit_id": "t5_dreamjournal",
                "author": "sleepwalker",
                "title": "I had the strangest dream about a flooded city",
                "selftext": "Last night I dreamed I was walking through a flooded city and then I started floating over the streets.",
                "created_utc": 1704067200,
                "score": 21,
                "num_comments": 4,
                "permalink": "/r/DreamJournal/comments/dj1/",
                "is_self": True,
                "over_18": False,
            },
            {
                "id": "dj2",
                "subreddit": "DreamJournal",
                "subreddit_id": "t5_dreamjournal",
                "author": "sleepwalker",
                "title": "Dream where my childhood house kept changing rooms",
                "selftext": "In my dream I was back in my childhood house. Then every room kept shifting while I tried to find my sister.",
                "created_utc": 1704068200,
                "score": 13,
                "num_comments": 2,
                "permalink": "/r/DreamJournal/comments/dj2/",
                "is_self": True,
                "over_18": False,
            },
            {
                "id": "dj_meta",
                "subreddit": "DreamJournal",
                "subreddit_id": "t5_dreamjournal",
                "author": "meaningseeker",
                "title": "What does this dream mean?",
                "selftext": "Can someone interpret this dream for me?",
                "created_utc": 1704069200,
                "score": 3,
                "num_comments": 1,
                "permalink": "/r/DreamJournal/comments/dj_meta/",
                "is_self": True,
                "over_18": False,
            },
            {
                "id": "sleep1",
                "subreddit": "sleep",
                "subreddit_id": "t5_sleep",
                "author": "tireduser",
                "title": "Cannot stay asleep",
                "selftext": "I keep waking up after three hours and need advice.",
                "created_utc": 1704070200,
                "score": 8,
                "num_comments": 6,
                "permalink": "/r/sleep/comments/sleep1/",
                "is_self": True,
                "over_18": False,
            },
            {
                "id": "ask1",
                "subreddit": "AskReddit",
                "subreddit_id": "t5_askreddit",
                "author": "questioner",
                "title": "What is your favorite pillow?",
                "selftext": "I am shopping for a better one.",
                "created_utc": 1704071200,
                "score": 5,
                "num_comments": 3,
                "permalink": "/r/AskReddit/comments/ask1/",
                "is_self": True,
                "over_18": False,
            },
        ],
    )
    _write_zst_jsonl(
        archive_root / "comments" / "RC_2024-01.zst",
        [
            {
                "id": "dream_comment",
                "subreddit": "DreamJournal",
                "subreddit_id": "t5_dreamjournal",
                "author": "replyguy",
                "body": "I had a similar dream last week.",
                "created_utc": 1704072200,
                "score": 2,
                "link_id": "t3_dj1",
                "parent_id": "t3_dj1",
            }
        ],
    )

    return archive_root


def test_extracts_canonical_corpus_and_resumes(tmp_path):
    archive_root = _archive_fixture(tmp_path)
    output_root = tmp_path / "extracted"

    extractor = SubredditExtractor(
        archive_path=archive_root,
        output_path=output_root,
        subreddits=["science"],
    )
    first = extractor.run()
    second = extractor.run()

    dataset_path = output_root / "science"
    dataset = load_corpus(dataset_path)
    metadata = read_json(dataset_path / "dataset.json")

    assert first.months_processed == 2
    assert first.total_submissions == 2
    assert first.total_comments == 4
    assert second.months_processed == 0
    assert metadata["months"] == ["2020-01", "2020-02"]
    assert dataset.submission_count() == 2
    assert dataset.comment_count() == 4
    assert dataset.metadata.total_submissions == 2
    assert dataset.metadata.total_comments == 4


def test_catalogue_builds_monthly_and_index_outputs(tmp_path):
    archive_root = _archive_fixture(tmp_path)
    catalogue_root = tmp_path / "catalogue"

    catalogue = ArchiveCatalogue(
        archive_path=archive_root,
        output_path=catalogue_root,
        show_progress=False,
        progress_interval=1,
    )
    first = catalogue.run()
    second = catalogue.run()

    combined_rows = read_parquet_records(catalogue_root / "catalogue.parquet")
    index_rows = read_parquet_records(catalogue_root / "subreddit_index.parquet")
    metadata = read_json(catalogue_root / "catalogue.json")

    assert first["months_processed"] == 2
    assert second["months_processed"] == 0
    assert metadata["rows_written"] == len(combined_rows)
    assert any(
        row["month"] == "2020-01"
        and row["subreddit"] == "science"
        and row["n_submissions"] == 1
        and row["n_comments"] == 2
        and row["n_unique_authors"] == 3
        for row in combined_rows
    )
    assert any(
        row["subreddit"] == "science"
        and row["total_records"] == 6
        and row["months_active"] == 2
        for row in index_rows
    )

    rebuilt = SubredditIndex(catalogue_root).build()
    assert rebuilt["subreddits"] == len(index_rows)


def test_extract_logs_progress(tmp_path, caplog):
    archive_root = _archive_fixture(tmp_path)
    output_root = tmp_path / "extracted"

    caplog.set_level("INFO")
    extractor = SubredditExtractor(
        archive_path=archive_root,
        output_path=output_root,
        subreddits=["science"],
        progress_interval=1,
        show_progress=True,
    )
    extractor.run(start_month="2020-01", end_month="2020-01")

    assert any("scanned" in record.getMessage() for record in caplog.records)
    completed_logs = [
        record.getMessage()
        for record in caplog.records
        if "100.0% scanned" in record.getMessage()
    ]
    assert len(completed_logs) == 2


def test_tracks_keyword_sets_and_writes_aggregates(tmp_path):
    archive_root = _archive_fixture(tmp_path)
    output_root = tmp_path / "extracted"
    SubredditExtractor(
        archive_path=archive_root,
        output_path=output_root,
        subreddits=["science"],
    ).run()

    dataset_path = output_root / "science"
    tracking_root = tmp_path / "tracking"
    tracker = KeywordTracker(
        dataset_path=dataset_path,
        output_path=tracking_root,
        keyword_sets=[
            KeywordSet(name="climate", terms=["climate", "warming"], regexes=[r"carbon\s+tax"]),
            KeywordSet(name="policy", terms=["policy"]),
        ],
    )
    result = tracker.run()

    layout = TrackingLayout(tracking_root)
    monthly_counts = read_parquet_records(layout.combined_monthly_counts_path)
    term_counts = read_parquet_records(layout.combined_term_counts_path)
    matched_comments = read_parquet_records(layout.comments_match_path("2020-02"))

    assert result.months_processed == 2
    assert result.total_comments == 5
    assert result.total_submissions == 4
    assert any(
        row["month"] == "2020-01"
        and row["keyword_set"] == "climate"
        and row["record_type"] == "comment"
        and row["matched_records"] == 2
        for row in monthly_counts
    )
    assert any(
        row["keyword_set"] == "policy"
        and row["term"] == "policy"
        and row["matched_records"] >= 1
        for row in term_counts
    )
    assert any(row["keyword_set"] == "climate" for row in matched_comments)
    assert any(row["keyword_set"] == "policy" for row in matched_comments)


def test_builds_thread_tables_and_loader_uses_them(tmp_path):
    archive_root = _archive_fixture(tmp_path)
    output_root = tmp_path / "extracted"
    SubredditExtractor(
        archive_path=archive_root,
        output_path=output_root,
        subreddits=["science"],
    ).run()

    dataset_path = output_root / "science"
    builder = TreeBuilder(dataset_path)
    results = builder.build_all_months()

    dataset = load_corpus(dataset_path)
    comments_df = dataset.comments_dataframe(include_threads=True)
    thread = dataset.get_thread("s1")
    month_two_rows = dataset.comments_dataframe(month="2020-02", include_threads=True)

    assert results == {"2020-01": 1, "2020-02": 1}
    assert {"depth", "thread_size", "missing_parent", "submission_title"} <= set(comments_df.columns)
    assert thread is not None
    assert thread.comment_count == 2
    assert int(comments_df.loc[comments_df["id"] == "c1", "depth"].iloc[0]) == 0
    assert int(comments_df.loc[comments_df["id"] == "c2", "depth"].iloc[0]) == 1
    assert bool(month_two_rows.loc[month_two_rows["id"] == "c5", "missing_parent"].iloc[0]) is True


def test_detects_signals_and_loader_joins_them(tmp_path):
    archive_root = _archive_fixture(tmp_path)
    output_root = tmp_path / "extracted"
    SubredditExtractor(
        archive_path=archive_root,
        output_path=output_root,
        subreddits=["science"],
    ).run()

    dataset_path = output_root / "science"
    TreeBuilder(dataset_path).build_all_months()
    detector = SignalDetector(
        dataset_path,
        detectors=[
            RegexDetector("mentions_climate", r"climate", record_type="both"),
            AuthorIsOPDetector("op_comment"),
        ],
    )
    results = detector.run_all_months()

    dataset = load_corpus(dataset_path)
    comments_df = dataset.comments_dataframe(include_threads=True)
    submissions_df = dataset.submissions_dataframe()
    signal_rows = read_parquet_records(dataset_path / "signals" / "2020-01.parquet")
    signals_metadata = read_json(dataset_path / "signals.json")

    assert results["2020-01"] >= 2
    assert signals_metadata["months_processed"] == 2
    assert any(
        row["record_type"] == "submission"
        and row["record_id"] == "s1"
        and row["mentions_climate"] is True
        for row in signal_rows
    )
    assert "mentions_climate" in comments_df.columns
    assert "mentions_climate" in submissions_df.columns
    assert bool(submissions_df.loc[submissions_df["id"] == "s1", "mentions_climate"].iloc[0]) is True


def test_cli_keyword_config_and_smoke_analysis(tmp_path, monkeypatch, capsys):
    archive_root = _archive_fixture(tmp_path)
    output_root = tmp_path / "extracted"
    SubredditExtractor(
        archive_path=archive_root,
        output_path=output_root,
        subreddits=["science"],
    ).run()

    dataset_path = output_root / "science"
    tracking_root = tmp_path / "tracking"
    config_path = tmp_path / "keyword_sets.json"
    config_path.write_text(
        json.dumps(
            {
                "keyword_sets": [
                    {"name": "climate", "terms": ["climate", "warming"], "regexes": [r"carbon\s+tax"]},
                    {"name": "policy", "terms": ["policy"]},
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "sys.argv",
        [
            "pushshiftreader",
            "track",
            str(dataset_path),
            "--output",
            str(tracking_root),
            "--keyword-config",
            str(config_path),
        ],
    )
    cli_main()

    report = build_smoke_report(dataset_path, tracking_root, top_n=5)
    assert not report["monthly_summary"].empty
    assert not report["tracking_monthly_counts"].empty
    assert not report["tracking_top_terms"].empty

    monkeypatch.setattr(
        "sys.argv",
        [
            "pushshiftreader",
            "analyze-smoke",
            str(dataset_path),
            "--tracking",
            str(tracking_root),
            "--top-n",
            "5",
        ],
    )
    cli_main()
    output = capsys.readouterr().out

    assert "Monthly summary:" in output
    assert "Tracking monthly counts:" in output
    assert "Top 5 tracked terms:" in output


def test_cli_catalogue_command(tmp_path, monkeypatch, capsys):
    archive_root = _archive_fixture(tmp_path)
    catalogue_root = tmp_path / "catalogue"

    monkeypatch.setattr(
        "sys.argv",
        [
            "pushshiftreader",
            "--quiet",
            "catalogue",
            "--archive",
            str(archive_root),
            "--output",
            str(catalogue_root),
            "--progress-interval",
            "1",
        ],
    )
    cli_main()
    output = capsys.readouterr().out

    assert "Catalogue complete" in output
    assert (catalogue_root / "catalogue.parquet").exists()
    assert (catalogue_root / "subreddit_index.parquet").exists()


def test_cli_detect_signals_command(tmp_path, monkeypatch, capsys):
    archive_root = _archive_fixture(tmp_path)
    output_root = tmp_path / "extracted"
    SubredditExtractor(
        archive_path=archive_root,
        output_path=output_root,
        subreddits=["science"],
    ).run()
    TreeBuilder(output_root / "science").build_all_months()

    monkeypatch.setattr(
        "sys.argv",
        [
            "pushshiftreader",
            "detect-signals",
            str(output_root / "science"),
            "--preset",
            "general",
            "--regex-signal",
            "mentions_climate=both:climate",
        ],
    )
    cli_main()
    output = capsys.readouterr().out

    assert "Signal detection complete" in output
    assert (output_root / "science" / "signals" / "2020-01.parquet").exists()


def test_crosssub_builds_overlap_from_author_summaries(tmp_path):
    archive_root = _archive_fixture(tmp_path)
    output_root = tmp_path / "extracted"
    SubredditExtractor(
        archive_path=archive_root,
        output_path=output_root,
        subreddits=["science", "AskReddit"],
    ).run()

    crosssub_root = tmp_path / "crosssub"
    index = CrossSubIndex.from_directory(output_root)
    index.build(min_subreddits=2)
    result = index.save(crosssub_root)

    activity_rows = read_parquet_records(crosssub_root / "author_activity.parquet")
    summary_rows = read_parquet_records(crosssub_root / "author_summary.parquet")
    metadata = read_json(crosssub_root / "crosssub.json")

    assert result["authors"] == 1
    assert result["pairs"] == 2
    assert metadata["authors"] == 1
    assert any(row["author"] == "alice" and row["subreddit"] == "AskReddit" for row in activity_rows)
    assert any(
        row["author"] == "alice"
        and row["n_subreddits"] == 2
        and row["subreddits"] == "AskReddit|science"
        for row in summary_rows
    )


def test_cli_crosssub_command(tmp_path, monkeypatch, capsys):
    archive_root = _archive_fixture(tmp_path)
    output_root = tmp_path / "extracted"
    SubredditExtractor(
        archive_path=archive_root,
        output_path=output_root,
        subreddits=["science", "AskReddit"],
    ).run()

    crosssub_root = tmp_path / "crosssub"
    monkeypatch.setattr(
        "sys.argv",
        [
            "pushshiftreader",
            "cross-sub-index",
            "--extracted",
            str(output_root),
            "--output",
            str(crosssub_root),
            "--min-subreddits",
            "2",
        ],
    )
    cli_main()
    output = capsys.readouterr().out

    assert "Cross-subreddit index complete" in output
    assert (crosssub_root / "author_activity.parquet").exists()
    assert (crosssub_root / "author_summary.parquet").exists()


def test_discovers_dream_subreddits_and_exports_flat_rows(tmp_path):
    archive_root = _dream_archive_fixture(tmp_path)
    catalogue_root = tmp_path / "catalogue"
    ArchiveCatalogue(
        archive_path=archive_root,
        output_path=catalogue_root,
        show_progress=False,
        progress_interval=1,
    ).run()

    discovery_root = tmp_path / "discovery"
    result = DreamSubredditDiscoverer(
        archive_path=archive_root,
        output_path=discovery_root,
        catalogue_path=catalogue_root,
        show_progress=False,
        progress_interval=1,
        min_submissions=1,
        min_matches=1,
    ).run()

    assert result["candidates_written"] >= 1

    with open(discovery_root / "candidates.csv", "r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["subreddit"] == "DreamJournal"
    assert rows[0]["matched_submissions"] == "2"
    assert rows[0]["meta_filtered_submissions"] == "1"

    extracted_root = tmp_path / "extracted"
    SubredditExtractor(
        archive_path=archive_root,
        output_path=extracted_root,
        subreddits=["DreamJournal"],
    ).run()

    export_path = tmp_path / "reddit_dreams.jsonl"
    export_result = DreamCorpusExporter(
        source_path=extracted_root,
        output_path=export_path,
        min_words=20,
    ).run()

    with open(export_path, "r", encoding="utf-8") as handle:
        exported = [json.loads(line) for line in handle if line.strip()]

    assert export_result["rows_written"] == 2
    assert len(exported) == 2
    assert exported[0]["source_platform"] == "reddit"
    assert exported[0]["source_type"] == "reddit_submission"
    assert exported[0]["source_subreddit"] == "DreamJournal"
    assert exported[0]["source_permalink"].startswith("https://www.reddit.com/r/DreamJournal/comments/")
    assert exported[0]["answer_text"]


def test_cli_dream_discovery_and_export_commands(tmp_path, monkeypatch, capsys):
    archive_root = _dream_archive_fixture(tmp_path)
    catalogue_root = tmp_path / "catalogue"
    ArchiveCatalogue(
        archive_path=archive_root,
        output_path=catalogue_root,
        show_progress=False,
        progress_interval=1,
    ).run()

    discovery_root = tmp_path / "discovery"
    monkeypatch.setattr(
        "sys.argv",
        [
            "pushshiftreader",
            "discover-dream-subreddits",
            "--archive",
            str(archive_root),
            "--output",
            str(discovery_root),
            "--catalogue",
            str(catalogue_root),
            "--min-submissions",
            "1",
            "--min-matches",
            "1",
        ],
    )
    cli_main()
    output = capsys.readouterr().out

    assert "Dream subreddit discovery complete" in output
    assert (discovery_root / "candidates.csv").exists()

    extracted_root = tmp_path / "extracted"
    SubredditExtractor(
        archive_path=archive_root,
        output_path=extracted_root,
        subreddits=["DreamJournal"],
    ).run()

    export_path = tmp_path / "reddit_dreams.jsonl"
    monkeypatch.setattr(
        "sys.argv",
        [
            "pushshiftreader",
            "export-dreams",
            "--source",
            str(extracted_root),
            "--output",
            str(export_path),
            "--min-words",
            "20",
        ],
    )
    cli_main()
    output = capsys.readouterr().out

    assert "Dream export complete" in output
    assert export_path.exists()


class _FakeRedditClient:
    def __init__(self):
        self.calls = []

    def get(self, path, params=None):
        self.calls.append((path, dict(params or {})))
        if path == "/r/wales/new":
            return (
                {
                    "data": {
                        "after": None,
                        "children": [
                            {
                                "kind": "t3",
                                "data": {
                                    "id": "abc123",
                                    "name": "t3_abc123",
                                    "subreddit": "wales",
                                    "author": "poster",
                                    "created_utc": 1710000000,
                                    "title": "A Wales post",
                                    "selftext": "Post body",
                                    "url": "https://example.com",
                                    "permalink": "/r/wales/comments/abc123/a_wales_post/",
                                    "score": 12,
                                    "upvote_ratio": 0.94,
                                    "num_comments": 2,
                                    "over_18": False,
                                    "spoiler": False,
                                    "stickied": False,
                                    "locked": False,
                                    "is_self": True,
                                    "link_flair_text": "Discussion",
                                },
                            }
                        ],
                    }
                },
                {},
            )
        if path == "/r/wales/comments/abc123":
            return (
                [
                    {"data": {"children": []}},
                    {
                        "data": {
                            "children": [
                                {
                                    "kind": "t1",
                                    "data": {
                                        "id": "c1",
                                        "name": "t1_c1",
                                        "author": "commenter",
                                        "created_utc": 1710000100,
                                        "body": "First comment",
                                        "link_id": "t3_abc123",
                                        "parent_id": "t3_abc123",
                                        "score": 3,
                                        "is_submitter": False,
                                        "stickied": False,
                                        "distinguished": None,
                                        "permalink": "/r/wales/comments/abc123/_/c1/",
                                        "replies": {
                                            "data": {
                                                "children": [
                                                    {
                                                        "kind": "t1",
                                                        "data": {
                                                            "id": "c2",
                                                            "name": "t1_c2",
                                                            "author": "poster",
                                                            "created_utc": 1710000200,
                                                            "body": "Nested reply",
                                                            "link_id": "t3_abc123",
                                                            "parent_id": "t1_c1",
                                                            "score": 1,
                                                            "is_submitter": True,
                                                            "stickied": False,
                                                            "distinguished": None,
                                                            "permalink": "/r/wales/comments/abc123/_/c2/",
                                                            "replies": "",
                                                        },
                                                    }
                                                ]
                                            }
                                        },
                                    },
                                },
                                {"kind": "more", "data": {"count": 4}},
                            ]
                        }
                    },
                ],
                {},
            )
        raise AssertionError(f"Unexpected fake Reddit path: {path}")


def test_reddit_api_scraper_writes_submissions_comments_and_metadata(tmp_path):
    output_root = tmp_path / "reddit-api"
    scraper = RedditSubmissionScraper(
        client=_FakeRedditClient(),
        output_path=output_root,
        subreddits=["r/wales"],
        max_submissions=1,
        include_raw=True,
    )

    result = scraper.run()

    assert result["total_submissions"] == 1
    assert result["total_comments"] == 2

    with open(output_root / "wales" / "submissions.jsonl", "r", encoding="utf-8") as handle:
        submissions = [json.loads(line) for line in handle]
    with open(output_root / "wales" / "comments.jsonl", "r", encoding="utf-8") as handle:
        comments = [json.loads(line) for line in handle]

    assert submissions[0]["record_type"] == "submission"
    assert submissions[0]["title"] == "A Wales post"
    assert submissions[0]["full_link"] == "https://www.reddit.com/r/wales/comments/abc123/a_wales_post/"
    assert submissions[0]["raw"]["id"] == "abc123"

    assert [comment["id"] for comment in comments] == ["c1", "c2"]
    assert [comment["depth"] for comment in comments] == [0, 1]
    assert comments[1]["parent_id"] == "t1_c1"
    assert comments[1]["raw"]["id"] == "c2"

    metadata = json.loads((output_root / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["subreddits"] == ["wales"]
    assert metadata["total_submissions"] == 1
    assert metadata["total_comments"] == 2


class _FakeRedditDateClient:
    def get(self, path, params=None):
        if path == "/r/wales/new":
            return (
                {
                    "data": {
                        "after": None,
                        "children": [
                            {
                                "kind": "t3",
                                "data": {
                                    "id": "new_post",
                                    "name": "t3_new_post",
                                    "subreddit": "wales",
                                    "author": "poster",
                                    "created_utc": 1710000000,
                                    "title": "Inside the range",
                                    "permalink": "/r/wales/comments/new_post/",
                                },
                            },
                            {
                                "kind": "t3",
                                "data": {
                                    "id": "old_post",
                                    "name": "t3_old_post",
                                    "subreddit": "wales",
                                    "author": "poster",
                                    "created_utc": 1600000000,
                                    "title": "Too old",
                                    "permalink": "/r/wales/comments/old_post/",
                                },
                            },
                        ],
                    }
                },
                {},
            )
        if path == "/r/wales/comments/new_post":
            return ([{}, {"data": {"children": []}}], {})
        raise AssertionError(f"Unexpected fake Reddit path: {path}")


def test_reddit_api_scraper_filters_submissions_by_date_window(tmp_path):
    scraper = RedditSubmissionScraper(
        client=_FakeRedditDateClient(),
        output_path=tmp_path / "reddit-api",
        subreddits=["wales"],
        max_submissions=10,
        since_utc=1700000000,
        until_utc=1720000000,
    )

    result = scraper.run()

    with open(tmp_path / "reddit-api" / "wales" / "submissions.jsonl", "r", encoding="utf-8") as handle:
        submissions = [json.loads(line) for line in handle]

    assert result["total_submissions"] == 1
    assert submissions[0]["id"] == "new_post"
    assert result["since_utc"] == 1700000000
    assert result["until_utc"] == 1720000000


def test_reddit_api_scraper_can_skip_comments(tmp_path):
    scraper = RedditSubmissionScraper(
        client=_FakeRedditClient(),
        output_path=tmp_path / "reddit-api",
        subreddits=["wales"],
        max_submissions=1,
        fetch_comments=False,
    )

    result = scraper.run()

    comments_path = tmp_path / "reddit-api" / "wales" / "comments.jsonl"
    assert result["total_submissions"] == 1
    assert result["total_comments"] == 0
    assert comments_path.exists()
    assert comments_path.read_text(encoding="utf-8") == ""


def test_reddit_comment_fetcher_reads_existing_submission_scrape(tmp_path):
    source = tmp_path / "reddit-api"
    submissions_path = source / "wales" / "submissions.jsonl"
    submissions_path.parent.mkdir(parents=True)
    submissions_path.write_text(
        json.dumps(
            {
                "record_type": "submission",
                "subreddit": "wales",
                "id": "abc123",
                "name": "t3_abc123",
                "title": "A Wales post",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (source / "metadata.json").write_text(json.dumps({"subreddits": ["wales"]}) + "\n", encoding="utf-8")

    result = RedditCommentFetcher(
        client=_FakeRedditClient(),
        source_path=source,
        comments_limit=20,
        force=True,
    ).run()

    with open(source / "wales" / "comments.jsonl", "r", encoding="utf-8") as handle:
        comments = [json.loads(line) for line in handle]

    assert result["total_submissions_processed"] == 1
    assert result["total_comments"] == 2
    assert [comment["id"] for comment in comments] == ["c1", "c2"]
    assert (source / "comments_metadata.json").exists()
