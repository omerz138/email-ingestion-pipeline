"""Focused unit tests for classification, lineage, and unpacker guards."""

from __future__ import annotations

from pipeline.classify import classify
from pipeline.models import Blob, Lineage
from pipeline.unpack import expand


def _blob(name, data=b"", kind=None):
    return Blob(name=name, data=data, lineage=Lineage(name), partition="p", source_path=name, kind=kind)


def test_classify_by_extension():
    assert classify(_blob("a.eml")) == "eml"
    assert classify(_blob("a.html")) == "html"
    assert classify(_blob("a.msg")) == "msg"
    assert classify(_blob("a.zip")) == "zip"
    assert classify(_blob("a.mbox")) == "mbox"
    assert classify(_blob("a.pst")) == "pst"
    assert classify(_blob("a.png")) == "unknown"


def test_classify_kind_override_wins():
    assert classify(_blob("conversations.mbox#0", kind="eml")) == "eml"


def test_classify_magic_bytes_fallback():
    assert classify(_blob("noext", data=b"PK\x03\x04rest")) == "zip"
    assert classify(_blob("noext", data=b"From foo@bar\n")) == "mbox"


def test_lineage_extends():
    chain = Lineage("root").extend("!a.zip").extend("#2")
    assert str(chain) == "root!a.zip#2"


class _FakeStore:
    def __init__(self):
        self.skips = []

    def record_skip(self, source_path, chain, reason):
        self.skips.append((source_path, chain, reason))


def test_depth_guard_records_skip():
    store = _FakeStore()
    blob = _blob("deep.zip", data=b"PK\x03\x04")  # treated as container
    leaves = list(expand(blob, store, max_depth=0))
    assert leaves == []
    assert store.skips and store.skips[0][2] == "depth_exceeded"
