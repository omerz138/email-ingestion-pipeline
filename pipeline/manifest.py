"""Optional manifest export for downstream consumers.

Generated from SQLite only for downstream consumption; SQLite remains the
source of truth.
"""

from __future__ import annotations

import json
import os


def export_manifest(out_dir: str, store) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "manifest.jsonl")
    with open(path, "w", encoding="utf-8") as fh:
        for content_hash, partition, staged_path in store.iter_emails():
            record = {
                "email_id": content_hash,
                "partition": partition,
                "staged_path": staged_path,
                "lineage": store.lineages_for(content_hash),
            }
            fh.write(json.dumps(record) + "\n")
    return path
