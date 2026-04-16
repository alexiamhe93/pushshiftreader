"""
CLI for canonical subreddit extraction, keyword tracking, and thread building.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List

from .analysis import build_smoke_report
from .catalogue import ArchiveCatalogue, SubredditIndex
from .crosssub import CrossSubIndex
from .extractor import SubredditExtractor
from .loader import load_corpus
from .presets import get_detectors
from .signals import RegexDetector, SignalDetector
from .tracking import KeywordSet, KeywordTracker, load_keyword_sets, merge_keyword_sets
from .trees import TreeBuilder
from .utils import discover_archives, format_size, setup_logging


def _parse_named_values(items: List[str]) -> Dict[str, List[str]]:
    parsed: Dict[str, List[str]] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Expected NAME=value1,value2 format, got: {item}")
        name, values = item.split("=", 1)
        if not name.strip():
            raise ValueError(f"Missing keyword-set name in: {item}")
        parsed.setdefault(name.strip(), [])
        parsed[name.strip()].extend([value.strip() for value in values.split(",") if value.strip()])
    return parsed


def _build_keyword_sets(term_sets: List[str], regex_sets: List[str]) -> List[KeywordSet]:
    terms = _parse_named_values(term_sets or [])
    regexes = _parse_named_values(regex_sets or [])
    names = sorted(set(terms) | set(regexes))
    if not names:
        raise ValueError("At least one --term-set or --regex-set must be provided")
    return [
        KeywordSet(
            name=name,
            terms=terms.get(name, []),
            regexes=regexes.get(name, []),
        )
        for name in names
    ]


def _keyword_sets_from_args(args) -> List[KeywordSet]:
    cli_sets = _build_keyword_sets(args.term_set, args.regex_set) if (args.term_set or args.regex_set) else []
    config_sets = load_keyword_sets(args.keyword_config) if args.keyword_config else []
    keyword_sets = merge_keyword_sets(config_sets, cli_sets)
    if not keyword_sets:
        raise ValueError("Provide --keyword-config and/or at least one --term-set/--regex-set")
    return keyword_sets


def _parse_signal_detectors(args):
    detectors = []
    if args.preset:
        detectors.extend(get_detectors(args.preset))

    for item in args.regex_signal or []:
        if "=" not in item:
            raise ValueError(f"Expected NAME=pattern or NAME=record_type:pattern, got: {item}")
        name, payload = item.split("=", 1)
        record_type = "comment"
        pattern = payload
        if ":" in payload:
            maybe_type, maybe_pattern = payload.split(":", 1)
            if maybe_type in {"comment", "submission", "both"}:
                record_type = maybe_type
                pattern = maybe_pattern
        detectors.append(RegexDetector(name=name.strip(), pattern=pattern, record_type=record_type))

    if not detectors:
        raise ValueError("Provide --preset and/or at least one --regex-signal")
    return detectors


def cmd_extract(args) -> None:
    extractor = SubredditExtractor(
        archive_path=args.archive,
        output_path=args.output,
        subreddits=args.subreddits,
        show_progress=not args.quiet,
        include_patterns=args.include,
        exclude_patterns=args.exclude,
        force=args.force,
        workers=args.workers,
        progress_interval=args.progress_interval,
    )
    result = extractor.run(
        start_month=args.start_month,
        end_month=args.end_month,
    )
    print("\nExtraction complete")
    print(f"  Months processed:   {result.months_processed}")
    print(f"  Total submissions:  {result.total_submissions:,}")
    print(f"  Total comments:     {result.total_comments:,}")
    print(f"  Output root:        {args.output}")


def cmd_track(args) -> None:
    keyword_sets = _keyword_sets_from_args(args)
    tracker = KeywordTracker(
        dataset_path=args.dataset,
        output_path=args.output,
        keyword_sets=keyword_sets,
        force=args.force,
    )
    result = tracker.run(
        start_month=args.start_month,
        end_month=args.end_month,
    )
    print("\nKeyword tracking complete")
    print(f"  Months processed:   {result.months_processed}")
    print(f"  Matched comments:   {result.total_comments:,}")
    print(f"  Matched submissions:{result.total_submissions:,}")
    print(f"  Output root:        {args.output}")


def cmd_catalogue(args) -> None:
    builder = ArchiveCatalogue(
        archive_path=args.archive,
        output_path=args.output,
        show_progress=not args.quiet,
        progress_interval=args.progress_interval,
    )
    result = builder.run(
        start_month=args.start_month,
        end_month=args.end_month,
        min_activity=args.min_activity,
    )
    print("\nCatalogue complete")
    print(f"  Months processed:   {result['months_processed']}")
    print(f"  Subreddits seen:    {result['subreddits_seen']:,}")
    print(f"  Rows written:       {result['rows_written']:,}")
    print(f"  Output root:        {result['output_path']}")


def cmd_subreddit_index(args) -> None:
    index = SubredditIndex(args.catalogue)
    result = index.build(min_records=args.min_records)
    print("\nSubreddit index complete")
    print(f"  Subreddits written: {result['subreddits']:,}")
    print(f"  Output path:        {result['output_path']}")


def cmd_cross_sub_index(args) -> None:
    if args.subreddits:
        index = CrossSubIndex.from_directory(args.extracted, subreddits=args.subreddits)
    else:
        index = CrossSubIndex.from_directory(args.extracted)
    index.build(min_subreddits=args.min_subreddits)
    result = index.save(args.output)
    print("\nCross-subreddit index complete")
    print(f"  Authors found:      {result['authors']:,}")
    print(f"  Activity rows:      {result['pairs']:,}")
    print(f"  Output root:        {args.output}")


def cmd_detect_signals(args) -> None:
    detector = SignalDetector(
        extracted_path=args.dataset,
        detectors=_parse_signal_detectors(args),
    )
    if args.month:
        results = {args.month: detector.run_month(args.month, force=args.force)}
    else:
        results = detector.run_all_months(force=args.force)

    print("\nSignal detection complete")
    print(f"  Months processed:   {len(results)}")
    print(f"  Rows written:       {sum(results.values()):,}")
    print(f"  Dataset:            {args.dataset}")


def cmd_analyze_smoke(args) -> None:
    report = build_smoke_report(
        dataset_path=args.dataset,
        tracking_path=args.tracking,
        top_n=args.top_n,
    )

    print(f"\nSubreddit:            r/{report['subreddit']}")
    if report["months"]:
        print(f"Months:               {report['months'][0]} to {report['months'][-1]}")
    print(f"Comment rows:         {report['comments_rows']:,}")
    print(f"Submission rows:      {report['submissions_rows']:,}")
    print(f"Comment columns:      {len(report['comment_columns'])}")
    print(f"Submission columns:   {len(report['submission_columns'])}")

    monthly_summary = report["monthly_summary"]
    if not monthly_summary.empty:
        print("\nMonthly summary:")
        print(monthly_summary.to_string(index=False))

    top_authors = report["top_comment_authors"]
    if not top_authors.empty:
        print(f"\nTop {len(top_authors)} comment authors:")
        print(top_authors.to_string(index=False))

    tracking_monthly = report.get("tracking_monthly_counts")
    if tracking_monthly is not None and not tracking_monthly.empty:
        print("\nTracking monthly counts:")
        print(tracking_monthly.to_string(index=False))

    tracking_terms = report.get("tracking_top_terms")
    if tracking_terms is not None and not tracking_terms.empty:
        print(f"\nTop {len(tracking_terms)} tracked terms:")
        print(tracking_terms.to_string(index=False))


def cmd_build_threads(args) -> None:
    builder = TreeBuilder(args.dataset)
    if args.month:
        results = {args.month: builder.build_month(args.month)}
    else:
        results = builder.build_all_months()
    print("\nThread building complete")
    print(f"  Months processed:   {len(results)}")
    print(f"  Threads built:      {sum(results.values()):,}")


def cmd_info(args) -> None:
    data = load_corpus(args.dataset)
    print(f"\nSubreddit:            r/{data.subreddit}")
    print(f"Dataset version:      {data.metadata.dataset_version or 'unknown'}")
    print(f"Months available:     {len(data.months)}")
    if data.months:
        print(f"First month:          {data.months[0]}")
        print(f"Last month:           {data.months[-1]}")
    print(f"Total submissions:    {data.metadata.total_submissions:,}")
    print(f"Total comments:       {data.metadata.total_comments:,}")
    print(f"Author-month records: {data.metadata.total_authors_across_months:,}")

    if args.verbose and data.months:
        print("\nMonthly breakdown:")
        for month in data.months:
            stats = data.month_stats(month)
            print(
                f"  {month}: "
                f"{stats.get('submissions_count', 0):,} submissions, "
                f"{stats.get('comments_count', 0):,} comments, "
                f"{stats.get('authors_count', 0):,} authors"
            )


def cmd_list_archives(args) -> None:
    archives = discover_archives(
        args.path,
        comments_subdir=args.comments_dir,
        submissions_subdir=args.submissions_dir,
    )
    print(f"\nFound {len(archives)} archive files in {args.path}")

    comments = [item for item in archives if item.file_type == "comments"]
    submissions = [item for item in archives if item.file_type == "submissions"]

    print(f"Comments archives:    {len(comments)}")
    print(f"Submissions archives: {len(submissions)}")
    if archives:
        months = sorted({item.month_str for item in archives})
        print(f"Date range:           {months[0]} to {months[-1]}")

    if args.verbose:
        for archive in archives[:10]:
            print(f"  {archive.path.name} ({format_size(archive.path.stat().st_size)})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract and analyze Reddit data from Pushshift archives",
        prog="pushshiftreader",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("-q", "--quiet", action="store_true", help="Reduce log verbosity")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    extract_parser = subparsers.add_parser("extract", help="Extract full subreddit corpora to Parquet")
    extract_parser.add_argument("--archive", "-a", type=Path, required=True)
    extract_parser.add_argument("--output", "-o", type=Path, required=True)
    extract_parser.add_argument("--subreddits", "-s", nargs="+", required=True)
    extract_parser.add_argument("--start-month")
    extract_parser.add_argument("--end-month")
    extract_parser.add_argument("--include", nargs="+", metavar="PATTERN")
    extract_parser.add_argument("--exclude", nargs="+", metavar="PATTERN")
    extract_parser.add_argument("--force", action="store_true")
    extract_parser.add_argument("--workers", "-w", type=int, default=1)
    extract_parser.add_argument("--progress-interval", type=int, default=250000)

    catalogue_parser = subparsers.add_parser("catalogue", help="Build archive-wide subreddit/month summary tables")
    catalogue_parser.add_argument("--archive", "-a", type=Path, required=True)
    catalogue_parser.add_argument("--output", "-o", type=Path, required=True)
    catalogue_parser.add_argument("--start-month")
    catalogue_parser.add_argument("--end-month")
    catalogue_parser.add_argument("--min-activity", type=int, default=1)
    catalogue_parser.add_argument("--progress-interval", type=int, default=250000)

    index_parser = subparsers.add_parser("subreddit-index", help="Rebuild subreddit index from a catalogue root")
    index_parser.add_argument("catalogue", type=Path)
    index_parser.add_argument("--min-records", type=int, default=1)

    crosssub_parser = subparsers.add_parser("cross-sub-index", help="Find authors active across multiple corpora")
    crosssub_parser.add_argument("--extracted", "-e", type=Path, required=True)
    crosssub_parser.add_argument("--output", "-o", type=Path, required=True)
    crosssub_parser.add_argument("--subreddits", "-s", nargs="+")
    crosssub_parser.add_argument("--min-subreddits", type=int, default=2)

    signals_parser = subparsers.add_parser("detect-signals", help="Run signal detectors over thread-aware corpora")
    signals_parser.add_argument("dataset", type=Path)
    signals_parser.add_argument("--month", "-m")
    signals_parser.add_argument("--preset", choices=["general", "cmv", "changemyview", "aita", "amitheasshole"])
    signals_parser.add_argument(
        "--regex-signal",
        action="append",
        default=[],
        metavar="NAME=pattern",
        help="Add a regex detector. Optionally use NAME=record_type:pattern where record_type is comment, submission, or both.",
    )
    signals_parser.add_argument("--force", action="store_true")

    track_parser = subparsers.add_parser("track", help="Track named keyword sets across a corpus")
    track_parser.add_argument("dataset", type=Path, help="Path to one extracted subreddit corpus")
    track_parser.add_argument("--output", "-o", type=Path, required=True)
    track_parser.add_argument("--keyword-config", type=Path, help="JSON file containing a keyword_sets list")
    track_parser.add_argument("--term-set", action="append", default=[], metavar="NAME=term1,term2")
    track_parser.add_argument("--regex-set", action="append", default=[], metavar="NAME=pattern1,pattern2")
    track_parser.add_argument("--start-month")
    track_parser.add_argument("--end-month")
    track_parser.add_argument("--force", action="store_true")

    threads_parser = subparsers.add_parser("build-threads", help="Build thread-aware comment tables")
    threads_parser.add_argument("dataset", type=Path)
    threads_parser.add_argument("--month", "-m")

    info_parser = subparsers.add_parser("info", help="Show corpus metadata")
    info_parser.add_argument("dataset", type=Path)

    smoke_parser = subparsers.add_parser("analyze-smoke", help="Print a quick analysis summary for a corpus")
    smoke_parser.add_argument("dataset", type=Path)
    smoke_parser.add_argument("--tracking", type=Path, help="Optional tracking run root to summarize")
    smoke_parser.add_argument("--top-n", type=int, default=10)

    list_parser = subparsers.add_parser("list-archives", help="List raw archive files")
    list_parser.add_argument("path", type=Path)
    list_parser.add_argument("--comments-dir", default="comments")
    list_parser.add_argument("--submissions-dir", default="submissions")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    if args.quiet:
        log_level = logging.WARNING
    setup_logging(level=log_level)

    try:
        if args.command == "extract":
            cmd_extract(args)
        elif args.command == "catalogue":
            cmd_catalogue(args)
        elif args.command == "subreddit-index":
            cmd_subreddit_index(args)
        elif args.command == "cross-sub-index":
            cmd_cross_sub_index(args)
        elif args.command == "detect-signals":
            cmd_detect_signals(args)
        elif args.command == "track":
            cmd_track(args)
        elif args.command == "build-threads":
            cmd_build_threads(args)
        elif args.command == "info":
            cmd_info(args)
        elif args.command == "analyze-smoke":
            cmd_analyze_smoke(args)
        elif args.command == "list-archives":
            cmd_list_archives(args)
        else:
            parser.print_help()
            sys.exit(1)
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
