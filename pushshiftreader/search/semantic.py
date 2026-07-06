"""
DDR-style semantic retrieval within a keyword pool.

The backend implements Distributed Dictionary Representations (Garten et al.
2018): a seed set of words is averaged into one vector; each document is the
mean of its in-vocabulary word vectors; relevance is cosine similarity.
Cheap, interpretable, and — with per-period vectors — historically
appropriate by construction.

Three commitments from the study design are enforced here rather than left
to discipline:

- **Pool composition.** A semantic search runs over *records* (a keyword
  pool or corpus slice), never over raw archives. Membership in the sample
  is decided by the concept-neutral keyword floor; this backend only
  sub-classifies within it.
- **Period-native seeds.** Indexes are built over corpus slices; seeds are
  supplied *per epoch* (built from the same window being searched and rolled
  forward), not one fixed present-day exemplar.
- **Multi-seed provenance tagging.** Retrieval can use several seed sets
  (e.g. early-medical vs later-lay); every hit is tagged with its nearest
  seed and per-seed scores, so anachronism becomes the measurement instead
  of a bias.

Requires the ``semantic`` extra (``pip install -e ".[semantic]"``) for
numpy; gensim only if you train/load word vectors through the helpers.
"""

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple, Union, runtime_checkable

from ..provenance import Manifest, manifest_run

logger = logging.getLogger(__name__)

_TOKEN_RE = re.compile(r"[a-z0-9']+")

TOKENIZER_NAME = "lowercase_alnum_v1"


def tokenize(text: str) -> List[str]:
    """The package's canonical tokenizer; its name goes into every manifest."""
    return _TOKEN_RE.findall(text.lower())


def _load_numpy():
    try:
        import numpy as np
    except ImportError as exc:
        raise ImportError(
            "numpy is required for semantic search. "
            'Install the extra with: pip install -e ".[semantic]"'
        ) from exc
    return np


@runtime_checkable
class VectorModel(Protocol):
    """Word-vector lookup. ``name`` + ``version`` feed the manifest."""

    name: str
    version: str

    def vector(self, word: str) -> Optional[Sequence[float]]:
        """Vector for a word, or None if out of vocabulary."""
        ...


class KeyedVectorsModel:
    """Adapter for gensim ``KeyedVectors`` (or a trained ``Word2Vec.wv``)."""

    def __init__(self, keyed_vectors, name: str, version: str = ""):
        self._kv = keyed_vectors
        self.name = name
        if not version:
            import gensim

            version = f"gensim-{gensim.__version__}"
        self.version = version

    def vector(self, word: str) -> Optional[Sequence[float]]:
        if word in self._kv:
            return self._kv[word]
        return None

    @classmethod
    def load(cls, path: Union[str, Path], name: Optional[str] = None) -> "KeyedVectorsModel":
        try:
            from gensim.models import KeyedVectors
        except ImportError as exc:
            raise ImportError(
                "gensim is required to load word vectors. "
                'Install the extra with: pip install -e ".[semantic]"'
            ) from exc
        path = Path(path)
        return cls(KeyedVectors.load(str(path)), name=name or path.name)


def train_epoch_model(
    texts: Iterable[str],
    epoch: str,
    vector_size: int = 100,
    window: int = 5,
    min_count: int = 2,
    seed: int = 1,
    workers: int = 1,
    epochs: int = 5,
) -> KeyedVectorsModel:
    """
    Train per-epoch static word vectors over one corpus slice.

    ``workers=1`` + fixed ``seed`` keeps training deterministic (gensim is
    only reproducible single-threaded) — in a drift study the embedding
    itself must be reconstructable.
    """
    try:
        from gensim.models import Word2Vec
    except ImportError as exc:
        raise ImportError(
            "gensim is required to train epoch vectors. "
            'Install the extra with: pip install -e ".[semantic]"'
        ) from exc

    sentences = [tokenize(text) for text in texts]
    model = Word2Vec(
        sentences=sentences,
        vector_size=vector_size,
        window=window,
        min_count=min_count,
        seed=seed,
        workers=workers,
        epochs=epochs,
    )
    return KeyedVectorsModel(model.wv, name=f"word2vec-{epoch}")


def _text_of(record: Dict[str, Any], text_fields: Sequence[str]) -> str:
    return " ".join(str(record.get(field) or "") for field in text_fields)


class DDRScorer:
    """Score documents against one or more seed sets by mean-vector cosine."""

    def __init__(self, model: VectorModel, seeds: Dict[str, List[str]]):
        np = _load_numpy()
        if not seeds:
            raise ValueError("At least one seed set is required")
        self.model = model
        self.seeds = {name: list(terms) for name, terms in seeds.items()}
        self._seed_vectors = {}
        for name, terms in self.seeds.items():
            vec = self._mean_vector([token for term in terms for token in tokenize(term)])
            if vec is None:
                raise ValueError(
                    f"Seed set {name!r} has no in-vocabulary terms in model {model.name!r}"
                )
            self._seed_vectors[name] = vec / np.linalg.norm(vec)

    def _mean_vector(self, tokens: List[str]):
        np = _load_numpy()
        vectors = [self.model.vector(token) for token in tokens]
        vectors = [np.asarray(v, dtype=float) for v in vectors if v is not None]
        if not vectors:
            return None
        return np.mean(vectors, axis=0)

    def score(self, text: str) -> Optional[Dict[str, float]]:
        """Per-seed cosine scores for a document, or None if OOV."""
        np = _load_numpy()
        doc = self._mean_vector(tokenize(text))
        if doc is None:
            return None
        norm = np.linalg.norm(doc)
        if norm == 0:
            return None
        doc = doc / norm
        return {name: float(np.dot(doc, vec)) for name, vec in self._seed_vectors.items()}


class SemanticSearcher:
    """
    Rank/filter records within a pool by DDR similarity to seed sets.

    Args:
        model: word-vector model (name+version recorded in the manifest).
        seeds: mapping of seed-set name -> seed terms. Use several seed sets
            (period-native ones) to make anachronism measurable.
        threshold: keep records whose best seed score >= threshold.
        top_k: alternatively/additionally keep the top-k by best score.
        text_fields: record fields concatenated as the document text.
    """

    name = "semantic"

    def __init__(
        self,
        model: VectorModel,
        seeds: Dict[str, List[str]],
        threshold: Optional[float] = None,
        top_k: Optional[int] = None,
        text_fields: Sequence[str] = ("body", "title", "selftext"),
    ):
        if threshold is None and top_k is None:
            raise ValueError("Provide threshold and/or top_k")
        self.model = model
        self.scorer = DDRScorer(model, seeds)
        self.threshold = threshold
        self.top_k = top_k
        self.text_fields = tuple(text_fields)

    def search(self, records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        hits: List[Dict[str, Any]] = []
        for record in records:
            scores = self.scorer.score(_text_of(record, self.text_fields))
            if scores is None:
                continue
            nearest_seed = max(scores, key=lambda name: scores[name])
            best = scores[nearest_seed]
            if self.threshold is not None and best < self.threshold:
                continue
            tagged = dict(record)
            tagged["semantic_score"] = round(best, 6)
            tagged["nearest_seed"] = nearest_seed
            tagged["seed_scores"] = {name: round(value, 6) for name, value in scores.items()}
            hits.append(tagged)

        hits.sort(key=lambda r: (-r["semantic_score"], str(r.get("id") or "")))
        if self.top_k is not None:
            hits = hits[: self.top_k]
        return hits

    def run(
        self,
        pool_records: Iterable[Dict[str, Any]],
        pool_paths: Optional[Sequence[Union[str, Path]]] = None,
        epoch: Optional[str] = None,
    ) -> "SemanticResult":
        """Search a pool and return hits + manifest (model, seeds, params)."""
        with manifest_run(
            "search",
            params={
                "backend": self.name,
                "seeds": self.scorer.seeds,
                "threshold": self.threshold,
                "top_k": self.top_k,
                "text_fields": list(self.text_fields),
                "tokenizer": TOKENIZER_NAME,
                "epoch": epoch,
                "pool_paths": [str(path) for path in pool_paths or []],
            },
            model=self.model.name,
            model_version=self.model.version,
        ) as manifest:
            manifest.add_inputs(pool_paths or [])
            records = list(pool_records)
            hits = self.search(records)
            manifest.counts = {"pool_records": len(records), "hits": len(hits)}
        return SemanticResult(hits=hits, manifest=manifest)


class SemanticResult:
    def __init__(self, hits: List[Dict[str, Any]], manifest: Manifest):
        self.hits = hits
        self.manifest = manifest

    def write(self, output_dir: Union[str, Path]) -> Path:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        hits_path = output_dir / "hits.jsonl"
        with open(hits_path, "w", encoding="utf-8") as handle:
            for hit in self.hits:
                handle.write(json.dumps(hit, ensure_ascii=False, sort_keys=True) + "\n")
        self.manifest.add_output(hits_path)
        self.manifest.write(output_dir)
        return hits_path


def search_epochs(
    pools_by_epoch: Dict[str, Iterable[Dict[str, Any]]],
    models_by_epoch: Dict[str, VectorModel],
    seeds_by_epoch: Dict[str, Dict[str, List[str]]],
    threshold: Optional[float] = None,
    top_k: Optional[int] = None,
    text_fields: Sequence[str] = ("body", "title", "selftext"),
) -> Dict[str, SemanticResult]:
    """
    Period-native semantic search: each epoch is searched with its own model
    and its own seeds (roll seeds forward window-by-window upstream; the
    per-epoch manifests record exactly which seeds each window saw, so seed
    drift is inspectable afterwards).
    """
    results: Dict[str, SemanticResult] = {}
    for epoch in sorted(pools_by_epoch):
        if epoch not in models_by_epoch:
            raise ValueError(f"No model for epoch {epoch!r}")
        if epoch not in seeds_by_epoch:
            raise ValueError(f"No seeds for epoch {epoch!r}")
        searcher = SemanticSearcher(
            model=models_by_epoch[epoch],
            seeds=seeds_by_epoch[epoch],
            threshold=threshold,
            top_k=top_k,
            text_fields=text_fields,
        )
        results[epoch] = searcher.run(pools_by_epoch[epoch], epoch=epoch)
    return results
