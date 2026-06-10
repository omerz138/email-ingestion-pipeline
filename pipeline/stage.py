"""Stage individual emails into the output tree.

The unique id is the SHA-256 of the leaf email's raw bytes. The staged path is
deterministic (derived from that hash), which is what makes re-processing after
a crash idempotent.
"""

from __future__ import annotations

import hashlib
import os

from .models import LeafEmail

_EXT_BY_KIND = {"eml": ".eml", "html": ".html", "msg": ".msg"}


def stage(leaf: LeafEmail, store, out_dir: str) -> str:
    """Stage a leaf email. Returns "staged" or "duplicate"."""
    content_hash = hashlib.sha256(leaf.data).hexdigest()

    if store.email_exists(content_hash):
        # Dedup: keep provenance, skip the write.
        store.add_lineage(content_hash, str(leaf.lineage))
        return "duplicate"

    ext = _EXT_BY_KIND.get(leaf.kind, ".eml")
    rel_path = os.path.join("staged", leaf.partition, content_hash + ext)
    abs_path = os.path.join(out_dir, rel_path)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)

    # Deterministic path -> rewriting after a crash produces identical bytes.
    with open(abs_path, "wb") as fh:
        fh.write(leaf.data)

    store.record_email(content_hash, rel_path, leaf.partition, str(leaf.lineage))
    return "staged"
