"""Classify a blob into an email format or container kind."""

from __future__ import annotations

import os

from .models import Blob

LEAF_KINDS = {"eml", "html", "msg"}
CONTAINER_KINDS = {"zip", "mbox", "pst"}

_EXT_KIND = {
    ".eml": "eml",
    ".html": "html",
    ".htm": "html",
    ".msg": "msg",
    ".zip": "zip",
    ".mbox": "mbox",
    ".pst": "pst",
}


def classify(blob: Blob) -> str:
    """Return one of LEAF_KINDS, CONTAINER_KINDS, or ``"unknown"``."""
    if blob.kind:
        return blob.kind

    _, ext = os.path.splitext(blob.name.lower())
    kind = _EXT_KIND.get(ext)
    if kind:
        return kind

    # Magic-byte fallback for nested files with missing/odd extensions.
    head = blob.data[:8]
    if head[:4] == b"PK\x03\x04":
        return "zip"
    if head[:5] == b"From ":
        return "mbox"
    if head[:4] == b"!BDN":  # PST/OST signature
        return "pst"
    return "unknown"
