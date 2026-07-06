"""
Diachronic glue: per-epoch indexes and alignment-ready vector exports.

The package's job ends at emitting per-epoch artefacts a drift analysis can
consume; diachronic embedding *alignment* (Procrustes etc.) is analysis and
stays downstream. What ships from here:

- :func:`build_epoch_index` — token frequency tables per epoch slice
  (vocab, counts, doc counts) with the package tokenizer, manifested.
- :func:`export_alignment_bundle` — per-epoch vector matrices restricted to
  the vocabulary shared by *all* epochs (exactly what orthogonal-Procrustes
  alignment consumes), plus the common vocab and per-epoch model versions.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

from .provenance import Manifest, manifest_run
from .search.semantic import TOKENIZER_NAME, VectorModel, tokenize
from .storage import read_parquet_records, write_json
from .utils import ensure_directory

logger = logging.getLogger(__name__)


def _epoch_dirs(slices_root: Path) -> List[Path]:
    return sorted(
        path for path in slices_root.iterdir()
        if path.is_dir() and (path / "epoch.json").exists()
    )


def _epoch_texts(epoch_dir: Path, manifest: Manifest) -> List[str]:
    texts: List[str] = []
    submissions_path = epoch_dir / "submissions.parquet"
    if submissions_path.exists():
        manifest.add_input(submissions_path)
        for row in read_parquet_records(submissions_path):
            texts.append(f"{row.get('title') or ''} {row.get('selftext') or ''}")
    comments_path = epoch_dir / "comments.parquet"
    if comments_path.exists():
        manifest.add_input(comments_path)
        for row in read_parquet_records(comments_path):
            texts.append(row.get("body") or "")
    return texts


def build_epoch_index(
    slices_root: Union[str, Path],
    output_root: Optional[Union[str, Path]] = None,
    min_count: int = 1,
) -> Dict[str, Path]:
    """
    Build per-epoch token frequency indexes over a sliced corpus.

    For each epoch under ``slices_root`` writes
    ``<output_root>/<epoch>/vocab.parquet`` with columns
    ``token / count / doc_count``, plus one manifest for the run.
    Returns ``{epoch: vocab_path}``.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    slices_root = Path(slices_root)
    epoch_dirs = _epoch_dirs(slices_root)
    if not epoch_dirs:
        raise ValueError(f"No epoch slices found under {slices_root} (run slice first)")

    output_root = Path(output_root) if output_root else slices_root / "index"
    vocab_paths: Dict[str, Path] = {}

    with manifest_run(
        "epoch-index",
        params={
            "slices_root": str(slices_root),
            "min_count": min_count,
            "tokenizer": TOKENIZER_NAME,
        },
    ) as manifest:
        total_tokens = 0
        for epoch_dir in epoch_dirs:
            epoch = epoch_dir.name
            counts: Dict[str, int] = {}
            doc_counts: Dict[str, int] = {}
            for text in _epoch_texts(epoch_dir, manifest):
                tokens = tokenize(text)
                for token in tokens:
                    counts[token] = counts.get(token, 0) + 1
                for token in set(tokens):
                    doc_counts[token] = doc_counts.get(token, 0) + 1

            rows = sorted(
                (token, count, doc_counts[token])
                for token, count in counts.items()
                if count >= min_count
            )
            table = pa.table(
                {
                    "token": [row[0] for row in rows],
                    "count": [row[1] for row in rows],
                    "doc_count": [row[2] for row in rows],
                }
            )
            vocab_path = ensure_directory(output_root / epoch) / "vocab.parquet"
            pq.write_table(table, vocab_path, compression="zstd")
            manifest.add_output(vocab_path)
            vocab_paths[epoch] = vocab_path
            total_tokens += len(rows)

        manifest.counts = {"epochs": len(epoch_dirs), "vocab_tokens": total_tokens}
    manifest.write(output_root)
    logger.info("Indexed %s epochs (%s vocab rows)", len(epoch_dirs), f"{total_tokens:,}")
    return vocab_paths


def export_alignment_bundle(
    models_by_epoch: Dict[str, VectorModel],
    vocab_by_epoch: Dict[str, List[str]],
    output_root: Union[str, Path],
) -> Path:
    """
    Emit alignment-ready per-epoch vector matrices.

    Restricts to the vocabulary present in *every* epoch's word list **and**
    in-vocabulary for that epoch's model, writes one
    ``<output_root>/<epoch>/vectors.parquet`` (token + list<float> vector)
    per epoch in identical token order, plus ``common_vocab.json`` and a
    manifest recording each epoch's model name+version. Orthogonal
    Procrustes downstream loads token-aligned matrices directly.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    if set(models_by_epoch) != set(vocab_by_epoch):
        raise ValueError("models_by_epoch and vocab_by_epoch must cover the same epochs")
    if not models_by_epoch:
        raise ValueError("At least one epoch is required")

    epochs = sorted(models_by_epoch)
    common = set(vocab_by_epoch[epochs[0]])
    for epoch in epochs[1:]:
        common &= set(vocab_by_epoch[epoch])
    common_tokens = sorted(
        token
        for token in common
        if all(models_by_epoch[epoch].vector(token) is not None for epoch in epochs)
    )
    if not common_tokens:
        raise ValueError("No shared in-vocabulary tokens across epochs; nothing to align")

    output_root = Path(output_root)

    with manifest_run(
        "alignment-bundle",
        params={
            "epochs": epochs,
            "models": {
                epoch: {"name": model.name, "version": model.version}
                for epoch, model in models_by_epoch.items()
            },
            "tokenizer": TOKENIZER_NAME,
        },
    ) as manifest:
        for epoch in epochs:
            model = models_by_epoch[epoch]
            vectors = [
                [float(value) for value in model.vector(token)]  # type: ignore[union-attr]
                for token in common_tokens
            ]
            table = pa.table(
                {
                    "token": common_tokens,
                    "vector": pa.array(vectors, type=pa.list_(pa.float64())),
                }
            )
            vectors_path = ensure_directory(output_root / epoch) / "vectors.parquet"
            pq.write_table(table, vectors_path, compression="zstd")
            manifest.add_output(vectors_path)

        vocab_path = output_root / "common_vocab.json"
        write_json(vocab_path, {"tokens": common_tokens, "epochs": epochs})
        manifest.add_output(vocab_path)
        manifest.counts = {"epochs": len(epochs), "common_vocab": len(common_tokens)}
    manifest.write(output_root)
    logger.info(
        "Exported alignment bundle: %s epochs, %s shared tokens",
        len(epochs),
        f"{len(common_tokens):,}",
    )
    return output_root
