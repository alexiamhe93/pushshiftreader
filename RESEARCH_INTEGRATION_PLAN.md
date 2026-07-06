# pushshiftreader — Research Integration Plan

> **STATUS: EXECUTED 2026-07-06** — all four phases shipped (commits
> ca295f7 → 0e5f342), 46 tests green, live e2e CLI run verified the
> emergence → first_appearance loop. Phase-3 embedding decision resolved as
> planned: DDR-first over per-epoch vectors (`search/semantic.py`), gensim
> behind the optional `[semantic]` extra. This document is now the design
> record, not a to-do list.

Scope of this plan: make `pushshiftreader` a **data-sampling substrate** for social-science
studies of *looping effects* (first study: how the concept of "autism" changes over time,
Reddit corpus + parallel bibliometrics; Gillespie & Wagoner 2025).

The package does **not** try to solve the measurement problems (concept drift, novelty
detection, embedding anachronism). Its job is to make every sampling decision
**explicit, reproducible, reversible, and multi-scale**, so a downstream study can defend
exactly which data it looked at.

Derived from the 2026-07-06 student-mode interview. Read that framing before building.

---

## Design commitments (the through-line)

1. **Keyword is the concept-neutral floor.** The string "autism" doesn't care about the
   concept. Keyword/BoW match defines the candidate *pool*; semantic search only
   sub-classifies *within* the pool — never defines membership. This stops a present-trained
   embedding smuggling today's concept into who is even in the sample.
2. **Provenance is non-negotiable.** In a study *about* drift, the tool's own model version is
   a confound. Every extraction/search/sample emits a manifest: inputs (source hashes),
   operation, params, seed, model+version, run timestamp, outputs, counts.
3. **Multi-scale by design.** One API resolves a *single turn*, a *full post+comment thread*,
   or a *full subreddit* — same provenance, same manifest, different unit.
4. **Novelty is a sampling problem, not a search problem.** The emergent sense is rarest when
   newest; frequency-ranked search buries it. Provide inverse-frequency, first-appearance, and
   change-point-driven sampling as the entry points instead.
5. **Time is a first-class axis.** Windowed slices are a core object so per-epoch (diachronic)
   analysis downstream falls out for free.

---

## Current architecture (what exists)

| Module | Role |
|--------|------|
| `reader.py` | low-level `.zst` record iteration (`read_zst_records`, `ZstReader`) |
| `extractor.py` | `SubredditExtractor`: raw `.zst` → canonical Parquet per subreddit/month; subreddit+keyword filter |
| `searcher.py` | `WordSearcher`: keyword search across archives (parallel/sequential) |
| `tracking.py` | `KeywordTracker`/`KeywordSet`: monthly keyword tracking → matched corpora + term-count tables |
| `trees.py` | `TreeBuilder`: reconstruct thread-aware comment tables; `load_threads` |
| `signals.py` | `Detector` ABC + `SignalDetector` over threads |
| `catalogue.py` | `ArchiveCatalogue` + `SubredditIndex`: archive-wide summary |
| `crosssub.py` | cross-subreddit author overlap |
| `storage.py` | Parquet writers + `*Layout` path objects |
| `models.py` | `Submission`, `Comment` dataclasses |
| `cli.py` | command surface |

Extraction today is **subreddit-scale only**. Search today is **keyword-only**. There is no
sampling layer, no windowing object, no manifest contract.

---

## New / changed modules

### A. `provenance.py` (new) — the manifest contract  ★ build first
- `Manifest` dataclass + `write_manifest(path, ...)`: inputs (source archive paths + sha256),
  operation, params, `seed`, `model` + `model_version`, `run_utc`, output paths, record counts.
- A small mixin/decorator so `extractor`, `searcher`, `tracker`, and the new samplers all emit
  one without bespoke code each.
- Retrofit onto existing extract/search/track outputs (additive, no behaviour change).

### B. `windows.py` (new) — time-windowed slicing
- `TimeWindow` / `Epoch`: month / quarter / year bucketing over `created_utc`.
- `slice_corpus(corpus_root, granularity) -> per-epoch Parquet` under a `slices/<epoch>/` layout.
- Substrate for diachronic embeddings and temporal search. Pure stdlib + pyarrow.

### C. `selection.py` (new) — scale-tiered extraction units
Unify the three scales behind one resolver, over raw archives **or** an extracted corpus:
- `Turn` — a single comment/submission (by id, or first match of a query).
- `Thread` — a submission + full comment tree (reuse `models.Thread` + `trees.TreeBuilder`).
- `SubredditSlice` — a subreddit over an optional `TimeWindow` (wraps `SubredditExtractor`).
- `Selector.resolve(spec) -> Iterator[record] + Manifest`.

### D. `sampling.py` (new) — reproducible sampling primitives
`Sampler(strategy, seed=...)` over an extracted corpus or a search result. Strategies:
- `random`
- `stratified_time` (even draw per epoch)
- `stratified_subreddit`
- `inverse_frequency` (oversample rare/novel usage) — the one this study needs
- `first_appearance` (earliest N occurrences of a term)

Every sample writes a manifest (strategy, seed, params, counts, source hash). Deterministic
under a fixed seed.

### E. `emergence.py` (new) — change-point / novelty detection
- Consume `tracking.py` term-count time series → detect inflection points
  (start simple: rolling z-score; optional PELT/ruptures if a dep is acceptable).
- Output: terms whose usage inflects in window W → feed `first_appearance` sampler to pull
  their early instances. This is the entry point for the rare-emergent-sense problem.

### F. `search/` (refactor `searcher.py` into a backend-pluggable package)
- `search/base.py` — `SearchBackend` protocol; every backend records provenance.
- `search/keyword.py` — current `WordSearcher` behaviour, unchanged (the floor). **No regression.**
- `search/semantic.py` — embedding/DDR backend (optional extra):
  - build an index over a **corpus slice** (per-epoch), not the whole corpus;
  - **period-native seeds**: query built from the same window being searched, rolled forward
    window-by-window (watch the seed drift) rather than a fixed present-day exemplar;
  - **multi-seed provenance tagging**: retrieve with several seeds (early-medical, later-lay),
    tag each hit with its nearest seed — anachronism becomes the measurement, not a bias.
- Composition rule enforced in the CLI: semantic runs **within** a keyword pool by default.

---

## CLI additions

| Command | Does |
|---------|------|
| `extract-turn` | pull a single comment/submission by id (+ manifest) |
| `extract-thread` | pull one submission + full comment tree (+ manifest) |
| `slice` | window a corpus into epochs |
| `sample` | run a sampling strategy over a corpus, emit sample + manifest |
| `emergence` | detect inflection terms from a tracking run |
| `search --backend {keyword,semantic}` | extend existing search; keyword is default |

(`extract` stays subreddit-scale as-is.)

---

## Build order (incremental, each shippable)

**Phase 1 — Foundations (no ML deps)**
1. `provenance.py` manifest contract; retrofit extractor/searcher/tracker.
2. `windows.py` slicing + `slice` CLI.
3. `selection.py` scale tiers + `extract-turn` / `extract-thread` CLI.

**Phase 2 — Sampling**
4. `sampling.py` strategies + `sample` CLI (all seedable + manifested).
5. `emergence.py` change-point + `emergence` CLI (reads tracking term series).

**Phase 3 — Semantic retrieval**
6. Refactor `searcher.py` → `search/` (keyword backend = current behaviour, tests green).
7. `search/semantic.py` DDR/embedding backend, period-native + multi-seed. Optional extra
   `pip install -e ".[semantic]"`.

**Phase 4 — Diachronic glue**
8. Per-epoch index helper; emit alignment-ready slices (Procrustes alignment itself left to
   downstream).

---

## Open decision to resolve at build time (Phase 3)

**Embedding backend.** Options, in recommended order:
1. **DDR (Garten et al. 2018)** over per-period static vectors — cheap, interpretable,
   and historically appropriate (period vectors avoid the anachronism by construction).
2. **Local sentence-transformer / Ollama `nomic-embed`** — reproducible, offline, no API cost;
   fits Alex's existing local-LLM setup. Heavier; single present-day model unless per-epoch
   fine-tuned.
3. API embeddings — rejected for a reproducibility-critical, drift-sensitive study.

Recommendation: ship **DDR first** (it directly answers the anachronism objection), keep the
sentence-transformer backend as an optional second backend behind the same protocol.

---

## Non-goals (explicitly out of scope for the package)
- Sense induction / word-sense disambiguation, topic modelling, manual-coding UIs — downstream.
- Diachronic embedding *alignment* (Procrustes) — package emits the slices; alignment is analysis.
- Bibliometric corpus handling — parallel track, not Reddit-side.
