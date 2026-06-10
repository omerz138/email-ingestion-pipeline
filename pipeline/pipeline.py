"""Discovery + CDC orchestration."""

from __future__ import annotations

import hashlib
import os
from typing import List, Tuple

from .models import Blob, Lineage, RunSummary, Source
from .stage import stage
from .unpack import expand

_PARTITION_PREFIX = "timestamp="


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def discover_partitions(bucket: str, namespace: str) -> List[Tuple[str, str]]:
    """Return ``(partition_date, partition_dir)`` pairs, sorted by date."""
    ns_dir = os.path.join(bucket, namespace)
    if not os.path.isdir(ns_dir):
        return []
    partitions = []
    for name in sorted(os.listdir(ns_dir)):
        full = os.path.join(ns_dir, name)
        if name.startswith(_PARTITION_PREFIX) and os.path.isdir(full):
            partitions.append((name[len(_PARTITION_PREFIX):], full))
    return partitions


def list_sources(bucket: str, namespace: str, partition: str, partition_dir: str) -> List[Source]:
    sources = []
    for name in sorted(os.listdir(partition_dir)):
        abs_path = os.path.join(partition_dir, name)
        if not os.path.isfile(abs_path):
            continue
        rel = os.path.relpath(abs_path, bucket)
        sources.append(Source(namespace, partition, rel, abs_path, _sha256_file(abs_path)))
    return sources


def run(bucket: str, namespace: str, out_dir: str, mode: str, store) -> RunSummary:
    """Process a namespace.

    ``mode`` is "backfill" (process everything) or "incremental" (skip sources
    already recorded as processed). All writes for one source are committed
    together; an unexpected error rolls the source back and re-raises so the
    next run retries it (crash-safe by construction).
    """
    summary = RunSummary()

    for partition, partition_dir in discover_partitions(bucket, namespace):
        for source in list_sources(bucket, namespace, partition, partition_dir):
            if mode == "incremental" and store.is_processed(source.source_path, source.source_hash):
                continue
            try:
                with open(source.abs_path, "rb") as fh:
                    data = fh.read()
                root = Blob(
                    name=os.path.basename(source.source_path),
                    data=data,
                    lineage=Lineage(source.source_path),
                    partition=partition,
                    source_path=source.source_path,
                )
                for leaf in expand(root, store):
                    result = stage(leaf, store, out_dir)
                    if result == "staged":
                        summary.emails_staged += 1
                    else:
                        summary.duplicates += 1
                store.mark_processed(source.source_path, source.source_hash, namespace, partition)
                store.commit()
                summary.sources_processed += 1
            except Exception:
                store.rollback()
                raise

    summary.files_skipped = store.skip_count()
    return summary
