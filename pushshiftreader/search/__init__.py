"""
Backend-pluggable search.

- ``search.keyword`` — the concept-neutral floor: regex/keyword scan over
  raw archives, defines candidate-pool membership. This is the original
  ``WordSearcher``, unchanged.
- ``search.semantic`` — DDR retrieval that sub-classifies *within* a pool;
  requires the ``semantic`` extra.
- ``search.base`` — the backend contract and the composition rule.
"""

from .base import SearchBackend
from .keyword import (
    SearchResult,
    SearchStats,
    WordSearcher,
    assemble_search_results,
)
from .semantic import (
    DDRScorer,
    KeyedVectorsModel,
    SemanticResult,
    SemanticSearcher,
    VectorModel,
    search_epochs,
    tokenize,
    train_epoch_model,
)

__all__ = [
    "DDRScorer",
    "KeyedVectorsModel",
    "SearchBackend",
    "SearchResult",
    "SearchStats",
    "SemanticResult",
    "SemanticSearcher",
    "VectorModel",
    "WordSearcher",
    "assemble_search_results",
    "search_epochs",
    "tokenize",
    "train_epoch_model",
]
