"""
Search backend contract.

Two backends, one composition rule: **keyword is the concept-neutral floor**.
The keyword backend defines candidate-pool membership (the string doesn't
care about the concept); the semantic backend only sub-classifies *within* a
pool — it never defines membership, so a present-trained representation
cannot smuggle today's concept into who is even in the sample.

Every backend records provenance for every run.
"""

from typing import Any, Dict, Iterable, List, Protocol, runtime_checkable


@runtime_checkable
class SearchBackend(Protocol):
    """A search backend scores/filters records and emits a manifest."""

    name: str

    def search(self, records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Return matching/scored records (tagged with backend metadata)."""
        ...
