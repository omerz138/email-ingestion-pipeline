# Email File Ingestion Pipeline

The ingestion head of a data platform: it discovers email files in
date-partitioned directories, recursively unpacks containers (ZIP / MBOX / PST),
deduplicates by content hash, and stages clean individual emails with full
lineage. SQLite is the single source of truth for CDC state, lineage, skipped
files, and the dedup index.

The full design (architecture, unique-id choice, CDC strategy, edge-case
decisions, and production scaling) is in [docs/DESIGN.md](docs/DESIGN.md)
(also available as a [Notion document](https://app.notion.com/p/372f6b6e9b588019ae4ed5e63a8727b6?source=copy_link)).
AI-process notes are in [docs/AI_PROCESS.md](docs/AI_PROCESS.md).

## Setup

The pipeline itself needs only the Python standard library (Python 3.9+).
Tests use `pytest`.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

PST support is optional. Install `libpff-python` to enable it; otherwise PST
files are skipped and recorded with a reason.

## Generate sample data

```bash
python scripts/generate_test_data.py test_data
```

This writes the provided fixtures plus the edge-case fixtures (nested ZIPs,
colliding inner names, password-protected ZIP, corrupt ZIP, non-email files,
empty containers, duplicates across partitions) under `test_data/`.

## Run

```bash
# Backfill: process everything (run once at onboarding)
python -m pipeline run --namespace namespace_a --mode backfill \
    --bucket ./test_data --out ./output

# Incremental: process only new/unprocessed sources (the 15-minute cron)
python -m pipeline run --namespace namespace_a --mode incremental \
    --bucket ./test_data --out ./output
```

Outputs land under `--out`:

- `staged/<partition>/<id>.eml` - the deduplicated individual emails.
- `state.db` - SQLite source of truth (sources, emails, lineage, skipped).
- `manifest.jsonl` - optional export for downstream consumers, one JSON object
  per staged email: `email_id`, `partition`, `staged_path`, and `lineage`.

## Test

```bash
pytest
```

## Layout

```
pipeline/
  classify.py    # format/container classification
  unpack.py      # recursive worklist unpacker + guards
  stage.py       # content-hash dedup + staging
  store.py       # SQLite source of truth
  pipeline.py    # discovery + CDC orchestration
  manifest.py    # optional manifest export
  __main__.py    # CLI
tests/           # unit + end-to-end + edge-case + crash-restart tests
scripts/         # test-data generator
docs/            # DESIGN.md, AI_PROCESS.md
```
