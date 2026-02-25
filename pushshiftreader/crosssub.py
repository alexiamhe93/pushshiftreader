"""
Cross-subreddit author index builder.

Reads the subreddit-level ``authors.csv`` from each extracted subreddit
directory and finds authors who appear across multiple subreddits.
No re-streaming of raw archives is required.
"""

import csv
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional

from .utils import ensure_directory, format_duration

logger = logging.getLogger(__name__)

# Fields written to author_activity.csv (one row per author×subreddit pair)
ACTIVITY_FIELDS = [
    'author',
    'subreddit',
    'comment_count',
    'submission_count',
    'comment_score_total',
    'avg_comment_score',
    'submission_score_total',
    'avg_submission_score',
    'months_active',
    'first_seen_utc',
    'last_seen_utc',
]

# Fields written to author_summary.csv (one row per author)
SUMMARY_FIELDS = [
    'author',
    'n_subreddits',
    'subreddits',
    'total_comments',
    'total_submissions',
    'total_months_active',
    'first_seen_utc',
    'last_seen_utc',
]


class CrossSubIndex:
    """
    Find authors who appear across multiple subreddits.

    Reads the already-aggregated ``authors.csv`` from each extracted subreddit
    directory — no re-streaming of archives required.

    Writes two output files:

    * ``author_activity.csv`` — one row per author × subreddit pair, with
      the stats from that subreddit's ``authors.csv``.
    * ``author_summary.csv`` — one row per author, with totals and a
      pipe-separated list of subreddits.

    Example::

        idx = CrossSubIndex.from_directory("./extracted")
        idx.build(min_subreddits=2)
        result = idx.save("./crosssub/")
        print(f"Found {result['authors']} cross-subreddit authors "
              f"across {result['pairs']} subreddit pairs")
    """

    def __init__(self, extracted_paths: List[Path]):
        self.extracted_paths = [Path(p) for p in extracted_paths]
        # Populated by build(): {author: {subreddit: stats_dict}}
        self._index: Dict[str, Dict[str, dict]] = {}

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_directory(
        cls,
        extracted_dir,
        subreddits: Optional[List[str]] = None,
    ) -> "CrossSubIndex":
        """
        Discover subreddit directories automatically.

        A directory is treated as a subreddit extraction if it contains an
        ``authors.csv`` file at its top level.

        Args:
            extracted_dir: Root directory containing one subdirectory per
                           extracted subreddit.
            subreddits:    If given, only include these subreddit names
                           (case-insensitive match against directory names).

        Returns:
            New :class:`CrossSubIndex` instance ready to call :meth:`build` on.
        """
        extracted_dir = Path(extracted_dir)
        filter_set = {s.lower() for s in subreddits} if subreddits else None

        paths = []
        for candidate in sorted(extracted_dir.iterdir()):
            if not candidate.is_dir():
                continue
            if not (candidate / 'authors.csv').exists():
                continue
            if filter_set is None or candidate.name.lower() in filter_set:
                paths.append(candidate)

        if not paths:
            logger.warning(
                f"No subreddit directories with authors.csv found in {extracted_dir}"
            )

        return cls(paths)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self, min_subreddits: int = 2) -> "CrossSubIndex":
        """
        Read ``authors.csv`` from each subreddit and merge into the index.

        Args:
            min_subreddits: Minimum number of subreddits an author must appear
                            in to be included in output.  Default 2.

        Returns:
            ``self`` (for method chaining).
        """
        start_time = time.time()
        combined: Dict[str, Dict[str, dict]] = {}

        for path in self.extracted_paths:
            subreddit = path.name
            authors_csv = path / 'authors.csv'

            if not authors_csv.exists():
                logger.warning(f"No authors.csv in {path}, skipping")
                continue

            n_authors = 0
            with open(authors_csv, newline='', encoding='utf-8') as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    author = row.get('author', '').strip()
                    if not author or author == '[deleted]':
                        continue
                    if author not in combined:
                        combined[author] = {}
                    combined[author][subreddit] = dict(row)
                    n_authors += 1

            logger.info(f"  Read {n_authors:,} authors from r/{subreddit}")

        # Filter to authors present in at least min_subreddits subreddits
        self._index = {
            author: subs
            for author, subs in combined.items()
            if len(subs) >= min_subreddits
        }

        duration = time.time() - start_time
        logger.info(
            f"Cross-sub index built: {len(self._index):,} authors in "
            f">= {min_subreddits} subreddits ({format_duration(duration)})"
        )
        return self

    def save(self, output_dir) -> dict:
        """
        Write ``author_activity.csv`` and ``author_summary.csv`` to *output_dir*.

        Returns:
            Dict with keys ``authors`` (number of cross-subreddit authors) and
            ``pairs`` (total author × subreddit rows written).
        """
        output_dir = ensure_directory(Path(output_dir))
        activity_path = output_dir / 'author_activity.csv'
        summary_path = output_dir / 'author_summary.csv'

        n_pairs = 0

        with open(activity_path, 'w', newline='', encoding='utf-8') as act_fh, \
             open(summary_path, 'w', newline='', encoding='utf-8') as sum_fh:

            act_writer = csv.DictWriter(act_fh, fieldnames=ACTIVITY_FIELDS)
            sum_writer = csv.DictWriter(sum_fh, fieldnames=SUMMARY_FIELDS)
            act_writer.writeheader()
            sum_writer.writeheader()

            for author in sorted(self._index.keys()):
                subs = self._index[author]

                # Aggregates for summary row
                total_comments = 0
                total_submissions = 0
                total_months_active = 0
                first_seen: Optional[str] = None
                last_seen: Optional[str] = None

                for subreddit, stats in sorted(subs.items()):
                    act_writer.writerow({
                        'author': author,
                        'subreddit': subreddit,
                        'comment_count': stats.get('comment_count', 0),
                        'submission_count': stats.get('submission_count', 0),
                        'comment_score_total': stats.get('comment_score_total', 0),
                        'avg_comment_score': stats.get('avg_comment_score', ''),
                        'submission_score_total': stats.get('submission_score_total', 0),
                        'avg_submission_score': stats.get('avg_submission_score', ''),
                        'months_active': stats.get('months_active', ''),
                        'first_seen_utc': stats.get('first_seen_utc', ''),
                        'last_seen_utc': stats.get('last_seen_utc', ''),
                    })
                    n_pairs += 1

                    # Accumulate summary values
                    try:
                        total_comments += int(stats.get('comment_count', 0) or 0)
                    except (ValueError, TypeError):
                        pass
                    try:
                        total_submissions += int(stats.get('submission_count', 0) or 0)
                    except (ValueError, TypeError):
                        pass
                    try:
                        total_months_active += int(stats.get('months_active', 0) or 0)
                    except (ValueError, TypeError):
                        pass

                    fs = stats.get('first_seen_utc', '')
                    ls = stats.get('last_seen_utc', '')
                    if fs:
                        first_seen = min(first_seen, fs) if first_seen else fs
                    if ls:
                        last_seen = max(last_seen, ls) if last_seen else ls

                sum_writer.writerow({
                    'author': author,
                    'n_subreddits': len(subs),
                    'subreddits': '|'.join(sorted(subs.keys())),
                    'total_comments': total_comments,
                    'total_submissions': total_submissions,
                    'total_months_active': total_months_active,
                    'first_seen_utc': first_seen or '',
                    'last_seen_utc': last_seen or '',
                })

        logger.info(
            f"Saved {len(self._index):,} authors ({n_pairs:,} pairs) to {output_dir}"
        )
        return {'authors': len(self._index), 'pairs': n_pairs}
