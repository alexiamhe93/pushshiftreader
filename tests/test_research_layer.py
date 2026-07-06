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


# ---- sampling ---------------------------------------------------------------


def _synthetic_records():
    """60 records over 3 months, 2 subreddits, with matched-term tags."""
    records = []
    base = 1577836800  # 2020-01-01
    month_offsets = [0, 31 * 86400, 60 * 86400]  # Jan, Feb, Mar 2020
    for index in range(60):
        month = index % 3
        records.append(
            {
                "id": f"r{index:03d}",
                "created_utc": base + month_offsets[month] + index,
                "subreddit": "science" if index % 2 == 0 else "askreddit",
                # term "rare" appears 6 times, "common" 54 times
                "matched_terms": "rare" if index % 10 == 0 else "common",
                "body": f"record {index}",
            }
        )
    return records


def test_random_sampling_is_deterministic_under_seed():
    from pushshiftreader import Sampler

    records = _synthetic_records()
    first = Sampler("random", n=10, seed=42).sample(records)
    second = Sampler("random", n=10, seed=42).sample(records)
    different = Sampler("random", n=10, seed=43).sample(records)

    assert [r["id"] for r in first] == [r["id"] for r in second]
    assert [r["id"] for r in first] != [r["id"] for r in different]
    assert len(first) == 10


def test_stratified_time_draws_evenly_per_epoch():
    from pushshiftreader import Sampler, epoch_of

    records = _synthetic_records()
    sampled = Sampler("stratified_time", n=9, seed=1).sample(records)
    by_epoch = {}
    for record in sampled:
        by_epoch.setdefault(epoch_of(record["created_utc"]), []).append(record)
    assert set(by_epoch) == {"2020-01", "2020-02", "2020-03"}
    assert all(len(group) == 3 for group in by_epoch.values())


def test_stratified_subreddit_draws_evenly():
    from pushshiftreader import Sampler

    sampled = Sampler("stratified_subreddit", n=10, seed=1).sample(_synthetic_records())
    by_sub = {}
    for record in sampled:
        by_sub.setdefault(record["subreddit"], []).append(record)
    assert all(len(group) == 5 for group in by_sub.values())


def test_inverse_frequency_oversamples_rare_terms():
    from pushshiftreader import Sampler

    records = _synthetic_records()
    sampled = Sampler("inverse_frequency", n=10, seed=7).sample(records)
    rare_share = sum(1 for r in sampled if r["matched_terms"] == "rare") / len(sampled)
    population_share = 6 / 60
    assert rare_share > population_share  # rare term oversampled

    again = Sampler("inverse_frequency", n=10, seed=7).sample(records)
    assert [r["id"] for r in sampled] == [r["id"] for r in again]


def test_first_appearance_returns_earliest_for_term():
    from pushshiftreader import Sampler

    sampled = Sampler("first_appearance", n=3, term="rare").sample(_synthetic_records())
    assert [r["id"] for r in sampled] == ["r000", "r030", "r010"] or [
        r["created_utc"] for r in sampled
    ] == sorted(r["created_utc"] for r in sampled)
    assert all(r["matched_terms"] == "rare" for r in sampled)
    assert len(sampled) == 3


def test_sampler_run_emits_manifest(tmp_path):
    from pushshiftreader import Sampler
    from pushshiftreader.sampling import load_records

    records = _synthetic_records()
    input_path = tmp_path / "records.jsonl"
    with open(input_path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")

    result = Sampler("random", n=5, seed=3).run(input_paths=[input_path])
    out_path = result.write(tmp_path / "sample_out")

    assert len(result.records) == 5
    manifest = read_json(tmp_path / "sample_out" / "manifest.json")
    assert manifest["operation"] == "sample"
    assert manifest["seed"] == 3
    assert manifest["counts"] == {"input_records": 60, "sampled": 5}
    assert manifest["inputs"][0]["hash_kind"] == "sha256"
    assert load_records(out_path)


# ---- emergence ---------------------------------------------------------------


def test_detect_emergence_flags_burst_and_zero_fills():
    from pushshiftreader import build_term_series, detect_emergence

    rows = []
    # "steady" hums along at 5/month for 12 months of 2020
    for month in range(1, 13):
        rows.append(
            {"month": f"2020-{month:02d}", "term": "steady", "matched_records": 5}
        )
    # "burst" is absent until a spike in 2020-10 (rows for missing months omitted)
    rows.append({"month": "2020-10", "term": "burst", "matched_records": 40})

    series = build_term_series(rows)
    assert len(series["burst"]) == 12  # zero-filled across the observed span
    assert dict(series["burst"])["2020-03"] == 0

    points = detect_emergence(series, window=6, threshold=3.0, min_count=5)
    assert [point.term for point in points] == ["burst"]
    point = points[0]
    assert point.month == "2020-10"
    assert point.first_month == "2020-10"
    assert point.zscore >= 3.0


def test_run_emergence_over_tracking_run(tmp_path):
    from pushshiftreader import run_emergence

    corpus = _extracted_corpus(tmp_path)
    tracking_root = tmp_path / "tracking"
    KeywordTracker(
        dataset_path=corpus,
        output_path=tracking_root,
        keyword_sets=[KeywordSet(name="climate", terms=["climate", "carbon"])],
    ).run()

    result = run_emergence(tracking_root, window=1, threshold=0.5, min_count=1)
    assert result.manifest.operation == "emergence"
    assert result.manifest.counts["terms"] >= 1
    out = result.write(tmp_path / "emergence_out")
    assert read_json(out)["terms"]


def test_cli_sample_and_emergence(tmp_path, monkeypatch, capsys):
    corpus = _extracted_corpus(tmp_path)
    tracking_root = tmp_path / "tracking"
    KeywordTracker(
        dataset_path=corpus,
        output_path=tracking_root,
        keyword_sets=[KeywordSet(name="climate", terms=["climate"])],
    ).run()

    sample_out = tmp_path / "sample_cli"
    monkeypatch.setattr(
        "sys.argv",
        [
            "pushshiftreader", "sample",
            str(tracking_root / "matches"),
            "--output", str(sample_out),
            "--strategy", "first_appearance",
            "--n", "2",
        ],
    )
    cli_main()
    assert "Sampling complete" in capsys.readouterr().out
    assert (sample_out / "sample.jsonl").exists()
    assert read_json(sample_out / "manifest.json")["params"]["strategy"] == "first_appearance"

    emergence_out = tmp_path / "emergence_cli"
    monkeypatch.setattr(
        "sys.argv",
        [
            "pushshiftreader", "emergence",
            str(tracking_root),
            "--output", str(emergence_out),
            "--window", "1",
            "--threshold", "0.5",
            "--min-count", "1",
        ],
    )
    cli_main()
    assert "Emergence detection complete" in capsys.readouterr().out
    assert (emergence_out / "emergence.json").exists()


# ---- search backends ---------------------------------------------------------


def test_searcher_shim_and_search_package_expose_same_objects():
    from pushshiftreader.searcher import WordSearcher as shim_ws
    from pushshiftreader.search.keyword import WordSearcher as pkg_ws
    from pushshiftreader.search import SearchBackend, SemanticSearcher  # noqa: F401

    assert shim_ws is pkg_ws


class _FakeVectorModel:
    """Deterministic per-word vectors seeded by the word itself."""

    name = "fake-vectors"
    version = "test-1"

    def __init__(self, vocab):
        import numpy as np
        import random as _random

        self._vectors = {}
        for word in vocab:
            rng = _random.Random(word)
            self._vectors[word] = np.array([rng.uniform(-1, 1) for _ in range(8)])

    def vector(self, word):
        return self._vectors.get(word)


def _semantic_fixture():
    """Pool where medical-seed docs and lay-seed docs share exact vocab with seeds."""
    pool = [
        {"id": "m1", "created_utc": 100, "body": "asperger diagnosis clinical"},
        {"id": "m2", "created_utc": 200, "body": "clinical diagnosis asperger disorder"},
        {"id": "l1", "created_utc": 300, "body": "neurodivergent identity community"},
        {"id": "l2", "created_utc": 400, "body": "community identity neurodivergent pride"},
        {"id": "x1", "created_utc": 500, "body": "zzz qqq www"},  # OOV -> dropped
    ]
    vocab = {
        "asperger", "diagnosis", "clinical", "disorder",
        "neurodivergent", "identity", "community", "pride",
    }
    seeds = {
        "medical": ["asperger", "clinical", "diagnosis"],
        "lay": ["neurodivergent", "identity", "community"],
    }
    return pool, _FakeVectorModel(vocab), seeds


def test_semantic_search_tags_nearest_seed_and_scores():
    from pushshiftreader.search.semantic import SemanticSearcher

    pool, model, seeds = _semantic_fixture()
    hits = SemanticSearcher(model, seeds, threshold=-1.0).search(pool)

    by_id = {hit["id"]: hit for hit in hits}
    assert "x1" not in by_id  # fully OOV record cannot be scored
    assert by_id["m1"]["nearest_seed"] == "medical"
    assert by_id["m2"]["nearest_seed"] == "medical"
    assert by_id["l1"]["nearest_seed"] == "lay"
    assert by_id["l2"]["nearest_seed"] == "lay"
    assert set(by_id["m1"]["seed_scores"]) == {"medical", "lay"}
    # scores sorted descending
    scores = [hit["semantic_score"] for hit in hits]
    assert scores == sorted(scores, reverse=True)


def test_semantic_search_run_emits_model_versioned_manifest(tmp_path):
    from pushshiftreader.search.semantic import SemanticSearcher

    pool, model, seeds = _semantic_fixture()
    result = SemanticSearcher(model, seeds, top_k=2).run(pool, epoch="2020-Q1")
    hits_path = result.write(tmp_path / "semantic")

    assert len(result.hits) == 2
    manifest = read_json(tmp_path / "semantic" / "manifest.json")
    assert manifest["operation"] == "search"
    assert manifest["params"]["backend"] == "semantic"
    assert manifest["params"]["epoch"] == "2020-Q1"
    assert manifest["params"]["seeds"]["medical"] == ["asperger", "clinical", "diagnosis"]
    assert manifest["model"] == "fake-vectors"
    assert manifest["model_version"] == "test-1"
    assert manifest["counts"] == {"pool_records": 5, "hits": 2}
    assert hits_path.exists()


def test_search_epochs_uses_period_native_models_and_seeds():
    from pushshiftreader.search.semantic import search_epochs

    pool, model, seeds = _semantic_fixture()
    pools = {"2020-01": pool[:2], "2020-02": pool[2:4]}
    results = search_epochs(
        pools_by_epoch=pools,
        models_by_epoch={"2020-01": model, "2020-02": model},
        seeds_by_epoch={
            "2020-01": {"medical": ["asperger", "clinical"]},
            "2020-02": {"lay": ["neurodivergent", "community"]},
        },
        threshold=-1.0,
    )

    assert set(results) == {"2020-01", "2020-02"}
    jan = results["2020-01"]
    feb = results["2020-02"]
    assert jan.manifest.params["seeds"] == {"medical": ["asperger", "clinical"]}
    assert feb.manifest.params["seeds"] == {"lay": ["neurodivergent", "community"]}
    assert all(hit["nearest_seed"] == "medical" for hit in jan.hits)
    assert all(hit["nearest_seed"] == "lay" for hit in feb.hits)


def test_semantic_searcher_requires_gate():
    import pytest
    from pushshiftreader.search.semantic import SemanticSearcher

    _, model, seeds = _semantic_fixture()
    with pytest.raises(ValueError):
        SemanticSearcher(model, seeds)  # no threshold, no top_k


def test_cli_search_keyword_backend(tmp_path, monkeypatch, capsys):
    archive_root = _archive_fixture(tmp_path)
    out_root = tmp_path / "search_cli"
    monkeypatch.setattr(
        "sys.argv",
        [
            "pushshiftreader", "--quiet", "search",
            "--archive", str(archive_root),
            "--pattern", "climate",
            "--output", str(out_root),
        ],
    )
    cli_main()
    output = capsys.readouterr().out
    assert "Keyword search complete" in output
    assert read_json(out_root / "manifest.json")["params"]["backend"] == "keyword"
