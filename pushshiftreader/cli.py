"""
Command-line interface for pushshiftreader.

Usage:
    python -m pushshiftreader extract --archive /path/to/dumps --output ./extracted --subreddits AskHistorians science
    python -m pushshiftreader build-trees ./extracted/AskHistorians
    python -m pushshiftreader info ./extracted/AskHistorians
"""

import argparse
import sys
import logging
from pathlib import Path

from .extractor import SubredditExtractor
from .trees import TreeBuilder
from .loader import load_subreddit
from .utils import setup_logging, discover_archives, format_size


def cmd_extract(args):
    """Run subreddit extraction."""
    extractor = SubredditExtractor(
        archive_path=args.archive,
        output_path=args.output,
        subreddits=args.subreddits,
        output_format=args.format,
        show_progress=not args.quiet,
        include_patterns=args.include or [],
        exclude_patterns=args.exclude or [],
        force=args.force,
    )

    result = extractor.run(
        start_month=args.start_month,
        end_month=args.end_month
    )
    
    print(f"\nExtraction complete!")
    print(f"  Subreddits: {', '.join(result.subreddits)}")
    print(f"  Months processed: {result.months_processed}")
    print(f"  Total submissions: {result.total_submissions:,}")
    print(f"  Total comments: {result.total_comments:,}")


def cmd_build_trees(args):
    """Build comment trees for extracted data."""
    builder = TreeBuilder(
        extracted_path=args.path,
        db_path=args.db if args.db else None
    )
    
    if args.month:
        results = {args.month: builder.build_month(args.month)}
    else:
        results = builder.build_all_months()
    
    total = sum(results.values())
    print(f"\nBuilt {total:,} threads across {len(results)} months")


def cmd_info(args):
    """Display information about extracted data."""
    data = load_subreddit(args.path)
    
    print(f"\nSubreddit: r/{data.subreddit}")
    print(f"Months available: {len(data.months)}")
    
    if data.months:
        print(f"  First: {data.months[0]}")
        print(f"  Last: {data.months[-1]}")
    
    if data.metadata.total_submissions > 0:
        print(f"Total submissions: {data.metadata.total_submissions:,}")
        print(f"Total comments: {data.metadata.total_comments:,}")
    
    if args.verbose:
        print("\nMonthly breakdown:")
        for month in data.months:
            stats = data.month_stats(month)
            subs = stats.get('submissions_count', '?')
            comments = stats.get('comments_count', '?')
            print(f"  {month}: {subs:,} submissions, {comments:,} comments")


def cmd_list_archives(args):
    """List available archive files."""
    archives = discover_archives(
        args.path,
        comments_subdir=args.comments_dir,
        submissions_subdir=args.submissions_dir
    )
    
    print(f"\nFound {len(archives)} archive files in {args.path}")
    
    # Group by type
    comments = [a for a in archives if a.file_type == 'comments']
    submissions = [a for a in archives if a.file_type == 'submissions']
    
    print(f"\nComments: {len(comments)} files")
    if comments and args.verbose:
        for a in comments[:5]:
            print(f"  {a.path.name} ({format_size(a.path.stat().st_size)})")
        if len(comments) > 5:
            print(f"  ... and {len(comments) - 5} more")
    
    print(f"\nSubmissions: {len(submissions)} files")
    if submissions and args.verbose:
        for a in submissions[:5]:
            print(f"  {a.path.name} ({format_size(a.path.stat().st_size)})")
        if len(submissions) > 5:
            print(f"  ... and {len(submissions) - 5} more")
    
    # Show date range
    if archives:
        months = sorted(set(a.month_str for a in archives))
        print(f"\nDate range: {months[0]} to {months[-1]}")


def main():
    parser = argparse.ArgumentParser(
        description="Extract and process Reddit data from Pushshift dumps",
        prog="pushshiftreader"
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Suppress progress output'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Extract command
    extract_parser = subparsers.add_parser(
        'extract',
        help='Extract subreddit data from archives'
    )
    extract_parser.add_argument(
        '--archive', '-a',
        type=Path,
        required=True,
        help='Path to archive directory (containing comments/ and submissions/)'
    )
    extract_parser.add_argument(
        '--output', '-o',
        type=Path,
        required=True,
        help='Output directory for extracted data'
    )
    extract_parser.add_argument(
        '--subreddits', '-s',
        nargs='+',
        required=True,
        help='Subreddit names to extract'
    )
    extract_parser.add_argument(
        '--format', '-f',
        choices=['csv', 'jsonl', 'both'],
        default='both',
        help='Output format (default: both)'
    )
    extract_parser.add_argument(
        '--start-month',
        help='Start month (YYYY-MM), inclusive'
    )
    extract_parser.add_argument(
        '--end-month',
        help='End month (YYYY-MM), inclusive'
    )
    extract_parser.add_argument(
        '--include',
        nargs='+',
        metavar='PATTERN',
        help=(
            'Keep records whose text matches at least one of these regex patterns '
            '(case-insensitive). Searches title+selftext for submissions, body for comments.'
        )
    )
    extract_parser.add_argument(
        '--exclude',
        nargs='+',
        metavar='PATTERN',
        help=(
            'Drop records whose text matches any of these regex patterns '
            '(case-insensitive). Applied after --include.'
        )
    )
    extract_parser.add_argument(
        '--force',
        action='store_true',
        help='Re-extract months even if they were already extracted (overrides resume behaviour)'
    )
    
    # Build trees command
    trees_parser = subparsers.add_parser(
        'build-trees',
        help='Build comment trees for extracted data'
    )
    trees_parser.add_argument(
        'path',
        type=Path,
        help='Path to extracted subreddit directory'
    )
    trees_parser.add_argument(
        '--month', '-m',
        help='Specific month to process (YYYY-MM)'
    )
    trees_parser.add_argument(
        '--db',
        type=Path,
        help='Path for SQLite index database (default: in-memory)'
    )
    
    # Info command
    info_parser = subparsers.add_parser(
        'info',
        help='Display information about extracted data'
    )
    info_parser.add_argument(
        'path',
        type=Path,
        help='Path to extracted subreddit directory'
    )
    
    # List archives command
    list_parser = subparsers.add_parser(
        'list-archives',
        help='List available archive files'
    )
    list_parser.add_argument(
        'path',
        type=Path,
        help='Path to archive directory'
    )
    list_parser.add_argument(
        '--comments-dir',
        default='comments',
        help='Comments subdirectory name (default: comments)'
    )
    list_parser.add_argument(
        '--submissions-dir',
        default='submissions',
        help='Submissions subdirectory name (default: submissions)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    if args.quiet:
        log_level = logging.WARNING
    setup_logging(level=log_level)
    
    # Dispatch to command handler
    if args.command == 'extract':
        cmd_extract(args)
    elif args.command == 'build-trees':
        cmd_build_trees(args)
    elif args.command == 'info':
        cmd_info(args)
    elif args.command == 'list-archives':
        cmd_list_archives(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
