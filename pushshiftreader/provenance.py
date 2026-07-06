"""
Provenance manifests for research-facing operations.

In a study *about* concept drift, the tool's own configuration is a confound:
which archives were read, which filters applied, which seed drew the sample,
which model (and version) scored the records. Every extraction, search,
tracking, slicing, and sampling operation therefore emits a ``Manifest``
recording inputs (with content fingerprints), the operation and its params,
the random seed, any model + version, run timestamp, outputs, and counts.

Manifests are additive: they sit alongside existing metadata files and never
change the behaviour of the operation that emits them.
"""

import hashlib
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

from .storage import write_json

MANIFEST_VERSION = "1.0"
MANIFEST_FILENAME = "manifest.json"

# Files larger than this get a head+tail fingerprint instead of a full sha256,
# so manifest emission stays cheap even over multi-GB .zst archives.
FULL_HASH_MAX_BYTES = 256 * 1024 * 1024
_CHUNK_BYTES = 8 * 1024 * 1024


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def fingerprint_file(
    path: Union[str, Path],
    full_hash_max_bytes: int = FULL_HASH_MAX_BYTES,
) -> Dict[str, Any]:
    """
    Content fingerprint for one input file.

    Small files get a full ``sha256``. Large files get ``sha256_head_tail``
    (hash of the first and last 8 MB plus the size), which detects
    truncation/replacement without re-reading tens of gigabytes per run.
    Missing files are recorded rather than raising, so a manifest can still
    be written for a partially-moved dataset.
    """
    path = Path(path)
    entry: Dict[str, Any] = {"path": str(path)}

    if not path.exists():
        entry["exists"] = False
        return entry

    if path.is_dir():
        entry["is_dir"] = True
        return entry

    size = path.stat().st_size
    entry["size_bytes"] = size

    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        if size <= full_hash_max_bytes:
            for chunk in iter(lambda: handle.read(_CHUNK_BYTES), b""):
                digest.update(chunk)
            entry["hash_kind"] = "sha256"
        else:
            digest.update(handle.read(_CHUNK_BYTES))
            handle.seek(max(size - _CHUNK_BYTES, 0))
            digest.update(handle.read(_CHUNK_BYTES))
            digest.update(str(size).encode("ascii"))
            entry["hash_kind"] = "sha256_head_tail"
    entry["hash"] = digest.hexdigest()
    return entry


@dataclass
class Manifest:
    """One reproducibility record for one operation run."""

    operation: str
    params: Dict[str, Any] = field(default_factory=dict)
    inputs: List[Dict[str, Any]] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)
    counts: Dict[str, int] = field(default_factory=dict)
    seed: Optional[int] = None
    model: Optional[str] = None
    model_version: Optional[str] = None
    run_utc: str = field(default_factory=_utc_now)
    duration_seconds: Optional[float] = None
    manifest_version: str = MANIFEST_VERSION
    package_version: Optional[str] = None

    def __post_init__(self):
        if self.package_version is None:
            from . import __version__

            self.package_version = __version__

    def add_input(self, path: Union[str, Path], fingerprint: bool = True) -> None:
        if fingerprint:
            self.inputs.append(fingerprint_file(path))
        else:
            self.inputs.append({"path": str(path)})

    def add_inputs(self, paths: Iterable[Union[str, Path]], fingerprint: bool = True) -> None:
        for path in paths:
            self.add_input(path, fingerprint=fingerprint)

    def add_output(self, path: Union[str, Path]) -> None:
        self.outputs.append(str(path))

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def write(self, path: Union[str, Path]) -> Path:
        path = Path(path)
        if path.is_dir() or path.suffix != ".json":
            path = path / MANIFEST_FILENAME
        write_json(path, self.to_dict())
        return path


class manifest_run:
    """
    Context manager that times an operation and yields its manifest.

    The shared mechanism that lets extractor, searcher, tracker, slicer, and
    samplers all emit manifests without bespoke code::

        with manifest_run("sample", params={...}, seed=7) as manifest:
            ...
            manifest.counts["sampled"] = len(rows)
            manifest.add_output(out_path)
        manifest.write(out_dir)
    """

    def __init__(self, operation: str, **kwargs):
        self.manifest = Manifest(operation=operation, **kwargs)
        self._started: Optional[float] = None

    def __enter__(self) -> Manifest:
        self._started = time.time()
        return self.manifest

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.manifest.duration_seconds = round(time.time() - (self._started or time.time()), 3)
        return False


def write_manifest(
    directory: Union[str, Path],
    operation: str,
    params: Optional[Dict[str, Any]] = None,
    input_paths: Optional[Iterable[Union[str, Path]]] = None,
    outputs: Optional[Iterable[Union[str, Path]]] = None,
    counts: Optional[Dict[str, int]] = None,
    seed: Optional[int] = None,
    model: Optional[str] = None,
    model_version: Optional[str] = None,
    duration_seconds: Optional[float] = None,
    fingerprint_inputs: bool = True,
) -> Path:
    """One-call manifest emission for retrofitted operations."""
    manifest = Manifest(
        operation=operation,
        params=dict(params or {}),
        counts=dict(counts or {}),
        seed=seed,
        model=model,
        model_version=model_version,
        duration_seconds=duration_seconds,
    )
    manifest.add_inputs(input_paths or [], fingerprint=fingerprint_inputs)
    for path in outputs or []:
        manifest.add_output(path)
    return manifest.write(Path(directory))
