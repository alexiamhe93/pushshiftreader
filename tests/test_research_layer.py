"""
Tests for the research-integration layer: provenance manifests, time-windowed
slicing, scale-tiered selection, sampling, and emergence detection.
"""

import json
from pathlib import Path

import zstandard

from pushshiftreader import (
    KeywordSet,
    KeywordTracker,
    Selector,
    SubredditExtractor,
    SubredditSliceSpec,
    ThreadSpec,
    TimeWindow,
    TurnSpec,
    WordSearcher,
    epoch_of,
    month_to_epoch,
    slice_corpus,
)
from pushshiftreader.cli import main as cli_main
from pushshiftreader.provenance import fingerprint_file
from pushshiftreader.storage import read_json, read_parquet_records


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
                "author": "alice",
                "title": "Climate policy and science",
                "selftext": "Climate change and warming are central topics.",
                "created_utc": 1577836800,
                "score": 10,
                "num_comments": 2,
                "permalink": "/r/science/comments/s1/",
            },
        ],
    )
    _write_zst_jsonl(
        archive_root / "comments" / "RC_2020-01.zst",
        [
            {
                "id": "c1",
                "subreddit": "science",
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
                "author": "carol",
                "body": "Yes, warming is accelerating.",
                "created_utc": 1577837000,
                "score": 3,
                "link_id": "t3_s1",
                "parent_id": "t1_c1",
            },
        ],
    )
    _write_zst_jsonl(
        archive_root / "submissions" / "RS_2020-02.zst",
        [
            {
                "id": "s2",
                "subreddit": "science",
                "author": "dora",
                "title": "Energy policy",
                "selftext": "A carbon tax can change incentives.",
                "created_utc": 1580515200,
                "score": 9,
                "num_comments": 1,
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
                "author": "erin",
                "body": "A carbon tax now.",
                "created_utc": 1580515300,
                "score": 5,
                "link_id": "t3_s2",
                "parent_id": "t3_s2",
            },
        ],
    )
    return archive_root


def _extracted_corpus(tmp_path: Path) -> Path:
    archive_root = _archive_fixture(tmp_path)
    output_root = tmp_path / "extracted"
    SubredditExtractor(
        archive_path=archive_root,
        output_path=output_root,
        subreddits=["science"],
    ).run()
    return output_root / "science"


# ---- provenance ---------------------------------------------------------------


def test_fingerprint_file_hashes_content(tmp_path):
    path = tmp_path / "data.bin"
    path.write_bytes(b"hello world")
    entry = fingerprint_file(path)
    assert entry["size_bytes"] == 11
    assert entry["hash_kind"] == "sha256"
    assert len(entry["hash"]) == 64

    missing = fingerprint_file(tmp_path / "nope.bin")
    assert missing["exists"] is False


def test_extract_track_and_search_emit_manifests(tmp_path):
    archive_root = _archive_fixture(tmp_path)
    output_root = tmp_path / "extracted"
    SubredditExtractor(
        archive_path=archive_root,
        output_path=output_root,
        subreddits=["science"],
    ).run()

    extract_manifest = read_json(output_root / "science" / "manifest.json")
    assert extract_manifest["operation"] == "extract"
    assert extract_manifest["counts"]["submissions"] == 2
    assert extract_manifest["counts"]["comments"] == 3
    assert all("hash" in item for item in extract_manifest["inputs"])
    assert extract_manifest["manifest_version"]
    assert extract_manifest["run_utc"]

    tracking_root = tmp_path / "tracking"
    KeywordTracker(
        dataset_path=output_root / "science",
        output_path=tracking_root,
        keyword_sets=[KeywordSet(name="climate", terms=["climate"])],
    ).run()
    track_manifest = read_json(tracking_root / "manifest.json")
    assert track_manifest["operation"] == "track"
    assert track_manifest["params"]["keyword_sets"][0]["name"] == "climate"

    search_root = tmp_path / "search"
    WordSearcher(
        archive_path=archive_root,
        output_path=search_root,
        pattern=r"climate",
        show_progress=False,
    ).run()
    search_manifest = read_json(search_root / "manifest.json")
    assert search_manifest["operation"] == "search"
    assert search_manifest["params"]["backend"] == "keyword"
    assert search_manifest["counts"]["comments_matched"] == 1


# ---- windows ---------------------------------------------------------------


def test_epoch_bucketing_and_time_windows():
    assert epoch_of(1577836800, "month") == "2020-01"
    assert epoch_of(1577836800, "quarter") == "2020-Q1"
    assert epoch_of(1577836800, "year") == "2020"
    assert month_to_epoch("2020-05", "quarter") == "2020-Q2"

    window = TimeWindow.from_months("2020-01", "2020-01")
    assert window.contains(1577836800)
    assert not window.contains(1580515200)  # 2020-02-01
    assert window.overlaps_month("2020-01")
    assert not window.overlaps_month("2020-02")


def test_slice_corpus_writes_epochs_and_manifest(tmp_path):
    corpus = _extracted_corpus(tmp_path)
    result = slice_corpus(corpus, granularity="quarter")

    assert result.epochs == ["2020-Q1"]
    epoch_dir = result.output_root / "2020-Q1"
    submissions = read_parquet_records(epoch_dir / "submissions.parquet")
    comments = read_parquet_records(epoch_dir / "comments.parquet")
    assert len(submissions) == 2
    assert len(comments) == 3

    manifest = read_json(result.output_root / "manifest.json")
    assert manifest["operation"] == "slice"
    assert manifest["params"]["granularity"] == "quarter"
    assert manifest["counts"]["rows_written"] == 5

    # Resumable: second run skips the already-written epoch.
    second = slice_corpus(corpus, granularity="quarter")
    assert second.rows_written == {}


def test_slice_corpus_respects_window(tmp_path):
    corpus = _extracted_corpus(tmp_path)
    window = TimeWindow.from_months("2020-01", "2020-01")
    result = slice_corpus(corpus, granularity="month", window=window, output_root=tmp_path / "sl")
    assert result.epochs == ["2020-01"]
    rows = read_parquet_records(result.output_root / "2020-01" / "comments.parquet")
    assert {row["id"] for row in rows} == {"c1", "c2"}


# ---- selection ---------------------------------------------------------------


def test_selector_resolves_turn_from_corpus(tmp_path):
    corpus = _extracted_corpus(tmp_path)
    selection = Selector(corpus_root=corpus).resolve(TurnSpec(record_id="c2"))

    assert len(selection.records) == 1
    record = selection.records[0]
    assert record["record_type"] == "comment"
    assert record["author"] == "carol"
    assert selection.manifest.operation == "extract-turn"
    assert selection.manifest.counts["records"] == 1
    assert selection.manifest.inputs  # parquet files fingerprinted


def test_selector_resolves_thread_and_builds_tree(tmp_path):
    corpus = _extracted_corpus(tmp_path)
    selection = Selector(corpus_root=corpus).resolve(ThreadSpec(submission_id="s1"))

    assert selection.manifest.counts == {"records": 3, "comments": 2}
    thread = selection.to_thread()
    assert thread is not None
    assert thread.submission.id == "s1"
    assert thread.comment_count == 2
    # c2 is nested under c1
    assert thread.comments[0].comment.id == "c1"
    assert thread.comments[0].replies[0].comment.id == "c2"


def test_selector_resolves_from_raw_archives(tmp_path):
    archive_root = _archive_fixture(tmp_path)
    selector = Selector(archive_path=archive_root)

    turn = selector.resolve(TurnSpec(record_id="c1", record_type="comment"))
    assert len(turn.records) == 1
    assert turn.records[0]["body"] == "Climate change is real."

    thread = selector.resolve(ThreadSpec(submission_id="s1"))
    assert thread.manifest.counts == {"records": 3, "comments": 2}


def test_selector_subreddit_slice_with_window(tmp_path):
    corpus = _extracted_corpus(tmp_path)
    window = TimeWindow.from_months("2020-02", "2020-02")
    selection = Selector(corpus_root=corpus).resolve(SubredditSliceSpec(window=window))

    assert selection.manifest.counts == {"records": 2, "submissions": 1, "comments": 1}
    assert {record["id"] for record in selection.records} == {"s2", "c4"}


def test_cli_extract_turn_thread_and_slice(tmp_path, monkeypatch, capsys):
    corpus = _extracted_corpus(tmp_path)

    turn_out = tmp_path / "turn"
    monkeypatch.setattr(
        "sys.argv",
        ["pushshiftreader", "extract-turn", "c1", "--corpus", str(corpus), "--output", str(turn_out)],
    )
    cli_main()
    assert "Turn extraction complete" in capsys.readouterr().out
    assert (turn_out / "records.jsonl").exists()
    assert read_json(turn_out / "manifest.json")["operation"] == "extract-turn"

    thread_out = tmp_path / "thread"
    monkeypatch.setattr(
        "sys.argv",
        ["pushshiftreader", "extract-thread", "s1", "--corpus", str(corpus), "--output", str(thread_out)],
    )
    cli_main()
    assert "Thread extraction complete" in capsys.readouterr().out
    with open(thread_out / "records.jsonl", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle]
    assert len(rows) == 3

    slice_out = tmp_path / "slices"
    monkeypatch.setattr(
        "sys.argv",
        ["pushshiftreader", "slice", str(corpus), "--granularity", "year", "--output", str(slice_out)],
    )
    cli_main()
    assert "Slicing complete" in capsys.readouterr().out
    assert (slice_out / "2020" / "comments.parquet").exists()
