"""
Back-compat shim: the searcher moved to :mod:`pushshiftreader.search.keyword`.

Existing imports (``from pushshiftreader.searcher import WordSearcher``)
keep working unchanged.
"""

from .search.keyword import (  # noqa: F401
    SearchResult,
    SearchStats,
    WordSearcher,
    assemble_search_results,
)
