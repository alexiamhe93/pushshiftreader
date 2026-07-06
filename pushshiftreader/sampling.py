"""
Reproducible sampling primitives over extracted corpora and search results.

Novelty is a *sampling* problem, not a search problem: the emergent sense of
a concept is rarest exactly when it is newest, so frequency-ranked retrieval
buries it. These strategies are the designed entry points instead:

- ``random``               — seeded uniform draw.
- ``stratified_time``      — even draw per epoch (month/quarter/year).
- ``stratified_subreddit`` — even draw per subreddit.
- ``inverse_frequency``    — oversample records whose matched term is rare.
- ``first_appearance``     — the earliest N occurrences (optionally per term).

Every sample is deterministic under a fixed seed and emits a manifest
(strategy, seed, params, counts, input fingerprints).
"""

import csv
import gzip
import json
import logging
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Union

from .provenance import Manifest, manifest_run
from .storage import read_parquet_records
from .windows import epoch_of

logger = logging.getLogger(__name__)

STRATEGIES = (
    "random",
    "stratified_time",
    "stratified_subreddit",
    "inverse_frequency",
    "first_appearance",
)


@dataclass
class SampleResult:
    records: List[Dict[str, Any]]
    manifest: Manifest

    def write(self, output_dir: Union[str, Path]) -> Path:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        records_path = output_dir / "sample.jsonl"
        with open(records_path, "w", encoding="utf-8") as handle:
            for record in self.records:
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        self.manifest.add_output(records_path)
        self.manifest.write(output_dir)
        return records_path


def _record_terms(record: Dict[str, Any], term_field: str) -> List[str]:
    """
    Matched terms for a record. Tracking outputs store them as a JSON-encoded
    list (e.g. ``'["autism", "stimming"]'``); plain lists and pipe/comma-joined
    strings are accepted too.
    """
    raw = record.get(term_field)
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(term) for term in raw]
    text = str(raw).strip()
    if text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(term) for term in parsed]
        except json.JSONDecodeError:
            pass
    return [term.strip() for term in text.replace("|", ",").split(",") if term.strip()]


class Sampler:
    """
    Deterministic, manifested sampling over a list of records.

    Args:
        strategy: One of :data:`STRATEGIES`.
        n: Total sample size (for stratified strategies, split evenly across
           strata unless ``per_stratum`` is given).
        seed: RNG seed; identical seed + identical input order = identical sample.
        granularity: Epoch granularity for ``stratified_time``.
        per_stratum: Per-stratum draw size for stratified strategies.
        term_field: Column holding matched terms (tracking outputs use
            ``matched_terms``).
        term: For ``first_appearance``/``inverse_frequency``: restrict to
            records matching this term.
    """

    def __init__(
        self,
        strategy: str,
        n: int = 100,
        seed: Optional[int] = None,
        granularity: str = "month",
        per_stratum: Optional[int] = None,
        term_field: str = "matched_terms",
        term: Optional[str] = None,
    ):
        if strategy not in STRATEGIES:
            raise ValueError(f"Unknown strategy {strategy!r}; expected one of {STRATEGIES}")
        if n < 1:
            raise ValueError("n must be at least 1")
        self.strategy = strategy
        self.n = n
        self.seed = seed
        self.granularity = granularity
        self.per_stratum = per_stratum
        self.term_field = term_field
        self.term = term

    # ---- strategy cores (pure, deterministic) --------------------------------

    def _rng(self) -> random.Random:
        return random.Random(self.seed)

    def _random(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if len(records) <= self.n:
            return list(records)
        return self._rng().sample(records, self.n)

    def _stratified(
        self,
        records: List[Dict[str, Any]],
        key_fn: Callable[[Dict[str, Any]], str],
    ) -> List[Dict[str, Any]]:
        strata: Dict[str, List[Dict[str, Any]]] = {}
        for record in records:
            strata.setdefault(key_fn(record), []).append(record)

        keys = sorted(strata)
        if self.per_stratum is not None:
            quota = {key: self.per_stratum for key in keys}
        else:
            base = self.n // len(keys)
            remainder = self.n % len(keys)
            quota = {key: base + (1 if i < remainder else 0) for i, key in enumerate(keys)}

        rng = self._rng()
        sampled: List[Dict[str, Any]] = []
        for key in keys:
            pool = strata[key]
            take = min(quota[key], len(pool))
            if take <= 0:
                continue
            sampled.extend(pool if take == len(pool) else rng.sample(pool, take))
        return sampled

    def _inverse_frequency(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Weighted draw without replacement, weight = 1 / frequency of the
        record's matched term across the input. Rare (novel) usage is
        oversampled; ties broken by the seeded RNG (Efraimidis–Spirakis keys).
        """
        pool = records
        if self.term:
            pool = [r for r in records if self.term in _record_terms(r, self.term_field)]

        freq: Dict[str, int] = {}
        for record in pool:
            for term in _record_terms(record, self.term_field) or ["<none>"]:
                freq[term] = freq.get(term, 0) + 1

        def weight(record: Dict[str, Any]) -> float:
            terms = _record_terms(record, self.term_field) or ["<none>"]
            # A record's rarity is its rarest matched term.
            return max(1.0 / freq[term] for term in terms)

        rng = self._rng()
        keyed = [
            (math.pow(rng.random(), 1.0 / weight(record)), index, record)
            for index, record in enumerate(pool)
        ]
        keyed.sort(key=lambda item: (-item[0], item[1]))
        return [record for _, _, record in keyed[: self.n]]

    def _first_appearance(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pool = records
        if self.term:
            pool = [r for r in records if self.term in _record_terms(r, self.term_field)]
        ordered = sorted(pool, key=lambda r: (int(r.get("created_utc") or 0), str(r.get("id") or "")))
        return ordered[: self.n]

    # ---- public API ------------------------------------------------------------

    def sample(self, records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        records = list(records)
        if self.strategy == "random":
            return self._random(records)
        if self.strategy == "stratified_time":
            return self._stratified(
                records,
                lambda r: epoch_of(int(r.get("created_utc") or 0), self.granularity),
            )
        if self.strategy == "stratified_subreddit":
            return self._stratified(records, lambda r: str(r.get("subreddit") or ""))
        if self.strategy == "inverse_frequency":
            return self._inverse_frequency(records)
        if self.strategy == "first_appearance":
            return self._first_appearance(records)
        raise AssertionError(f"unreachable strategy {self.strategy}")

    def run(
        self,
        input_paths: Sequence[Union[str, Path]],
        records: Optional[Iterable[Dict[str, Any]]] = None,
    ) -> SampleResult:
        """
        Load records from ``input_paths`` (unless ``records`` is passed
        directly), sample, and return records + manifest.
        """
        with manifest_run(
            "sample",
            params={
                "strategy": self.strategy,
                "n": self.n,
                "granularity": self.granularity,
                "per_stratum": self.per_stratum,
                "term_field": self.term_field,
                "term": self.term,
                "inputs": [str(path) for path in input_paths],
            },
            seed=self.seed,
        ) as manifest:
            if records is None:
                loaded: List[Dict[str, Any]] = []
                for path in input_paths:
                    loaded.extend(load_records(path))
                records = loaded
            else:
                records = list(records)
            manifest.add_inputs(input_paths)
            sampled = self.sample(records)
            manifest.counts = {"input_records": len(records), "sampled": len(sampled)}
        return SampleResult(records=sampled, manifest=manifest)


def load_records(path: Union[str, Path]) -> List[Dict[str, Any]]:
    """
    Load records from any output format the package produces:
    Parquet, JSONL (optionally gzipped), or CSV. Directories load every
    ``*.parquet`` / ``*.jsonl[.gz]`` file beneath them, sorted for determinism.
    """
    path = Path(path)
    if path.is_dir():
        files = sorted(
            list(path.rglob("*.parquet"))
            + list(path.rglob("*.jsonl"))
            + list(path.rglob("*.jsonl.gz"))
        )
        records: List[Dict[str, Any]] = []
        for file_path in files:
            records.extend(load_records(file_path))
        return records

    name = path.name
    if name.endswith(".parquet"):
        return read_parquet_records(path)
    if name.endswith(".jsonl.gz"):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]
    if name.endswith(".jsonl"):
        with open(path, "r", encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]
    if name.endswith(".csv"):
        with open(path, "r", newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    raise ValueError(f"Unsupported record format: {path}")
