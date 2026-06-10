"""End-to-end and edge-case tests for the ingestion pipeline."""

from __future__ import annotations

import hashlib
import json
import os

import pytest

from pipeline.manifest import export_manifest
from pipeline.pipeline import run
from pipeline.store import Store
from tests.fixtures_builder import (
    EXPECTED_UNIQUE_EMAILS,
    NAMESPACE,
    SAME_BODY,
    build_bucket,
    make_eml,
)


def _run(bucket, out, mode="backfill", db=None):
    store = Store(db or os.path.join(out, "state.db"))
    try:
        summary = run(bucket, NAMESPACE, out, mode, store)
        manifest_path = export_manifest(out, store)
    finally:
        store.close()
    return summary, manifest_path


def _read_manifest(path):
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


@pytest.fixture()
def bucket(tmp_path):
    return build_bucket(str(tmp_path / "bucket"))


# --- happy path + containers ----------------------------------------------

def test_backfill_stages_expected_unique_emails(bucket, tmp_path):
    out = str(tmp_path / "out")
    summary, manifest_path = _run(bucket, out, mode="backfill")

    records = _read_manifest(manifest_path)
    assert summary.emails_staged == EXPECTED_UNIQUE_EMAILS
    assert len(records) == EXPECTED_UNIQUE_EMAILS

    # Every staged file exists and its bytes hash to its id.
    for rec in records:
        staged = os.path.join(out, rec["staged_path"])
        assert os.path.isfile(staged)
        with open(staged, "rb") as fh:
            assert hashlib.sha256(fh.read()).hexdigest() == rec["email_id"]


def test_zip_lineage(bucket, tmp_path):
    out = str(tmp_path / "out")
    _, manifest_path = _run(bucket, out, mode="backfill")
    chains = {c for rec in _read_manifest(manifest_path) for c in rec["lineage"]}
    assert any(c.endswith("batch.zip!invoice.eml") for c in chains)


def test_mbox_lineage_uses_hash_index(bucket, tmp_path):
    out = str(tmp_path / "out")
    _, manifest_path = _run(bucket, out, mode="backfill")
    chains = {c for rec in _read_manifest(manifest_path) for c in rec["lineage"]}
    assert any(c.endswith("conversations.mbox#0") for c in chains)


def test_deeply_nested_chain(bucket, tmp_path):
    out = str(tmp_path / "out")
    _, manifest_path = _run(bucket, out, mode="backfill")
    chains = {c for rec in _read_manifest(manifest_path) for c in rec["lineage"]}
    # ZIP -> ZIP -> MBOX -> message
    assert any("deep_chain.zip!mid.zip!chain.mbox#" in c for c in chains)
    # ZIP -> ZIP -> email
    assert any(c.endswith("nested.zip!inner.zip!deep1.eml") for c in chains)


# --- dedup -----------------------------------------------------------------

def test_identical_inner_names_kept_distinct(bucket, tmp_path):
    out = str(tmp_path / "out")
    _, manifest_path = _run(bucket, out, mode="backfill")
    records = _read_manifest(manifest_path)
    collide_ids = {
        rec["email_id"]
        for rec in records
        if any("collide_" in c for c in rec["lineage"])
    }
    assert len(collide_ids) == 2  # same filename, different bytes -> two emails


def test_two_mboxes_same_index_kept_distinct(bucket, tmp_path):
    out = str(tmp_path / "out")
    _, manifest_path = _run(bucket, out, mode="backfill")
    records = _read_manifest(manifest_path)
    x_ids = {
        rec["email_id"]
        for rec in records
        if any("mailbox_x.mbox#" in c for c in rec["lineage"])
    }
    y_ids = {
        rec["email_id"]
        for rec in records
        if any("mailbox_y.mbox#" in c for c in rec["lineage"])
    }
    # Two MBOXes both produce messages at index #0/#1; identical position but
    # different bytes must yield four distinct emails, not collapse by name.
    assert len(x_ids) == 2
    assert len(y_ids) == 2
    assert x_ids.isdisjoint(y_ids)


def test_identical_content_collapses_with_multiple_lineages(bucket, tmp_path):
    out = str(tmp_path / "out")
    _, manifest_path = _run(bucket, out, mode="backfill")
    same_id = hashlib.sha256(make_eml("Same", SAME_BODY)).hexdigest()
    records = {rec["email_id"]: rec for rec in _read_manifest(manifest_path)}
    assert same_id in records
    lineages = records[same_id]["lineage"]
    assert len(lineages) == 2
    assert any("dup_a.zip" in c for c in lineages)
    assert any("dup_b.zip" in c for c in lineages)


def test_same_filename_different_partitions(bucket, tmp_path):
    out = str(tmp_path / "out")
    _, manifest_path = _run(bucket, out, mode="backfill")
    partitions = {
        rec["partition"]
        for rec in _read_manifest(manifest_path)
        if any(c.endswith("dup_name.eml") for c in rec["lineage"])
    }
    assert partitions == {"2024-07-15", "2024-07-16"}


# --- skips -----------------------------------------------------------------

def test_skip_reasons_recorded(bucket, tmp_path):
    out = str(tmp_path / "out")
    store = Store(os.path.join(out, "state.db"))
    try:
        run(bucket, NAMESPACE, out, "backfill", store)
        reasons = {reason for _, _, reason in store.skip_reasons()}
    finally:
        store.close()
    assert {"encrypted_zip", "corrupt_zip", "non_email", "empty_container"} <= reasons


# --- CDC: incremental + idempotency ---------------------------------------

def test_incremental_rerun_does_no_work(bucket, tmp_path):
    out = str(tmp_path / "out")
    db = os.path.join(out, "state.db")
    _run(bucket, out, mode="backfill", db=db)
    summary2, _ = _run(bucket, out, mode="incremental", db=db)
    assert summary2.emails_staged == 0
    assert summary2.duplicates == 0


def test_reupload_same_partition_across_runs_is_deduped(bucket, tmp_path):
    out = str(tmp_path / "out")
    db = os.path.join(out, "state.db")
    summary1, _ = _run(bucket, out, mode="backfill", db=db)

    # Simulate a fresh upload of an already-seen file by clearing CDC state
    # (same content) and rerunning: dedup must prevent a second staged copy.
    store = Store(db)
    try:
        store.conn.execute("DELETE FROM sources")
        store.commit()
        before = store.email_count()
        summary2 = run(bucket, NAMESPACE, out, "incremental", store)
        after = store.email_count()
    finally:
        store.close()

    assert after == before
    assert summary2.emails_staged == 0
    assert summary2.duplicates > 0


# --- crash safety ----------------------------------------------------------

def test_crash_midrun_then_restart_no_duplicates(bucket, tmp_path, monkeypatch):
    out = str(tmp_path / "out")
    db = os.path.join(out, "state.db")

    # Baseline: a clean full run in a separate output.
    clean_out = str(tmp_path / "clean")
    baseline_summary, _ = _run(bucket, clean_out, mode="backfill")
    expected = baseline_summary.emails_staged

    # Crash: make the 3rd commit raise, mid-run, after some files are written.
    import pipeline.pipeline as pp

    store = Store(db)
    real_commit = store.commit
    state = {"n": 0}

    def flaky_commit():
        state["n"] += 1
        if state["n"] == 3:
            raise RuntimeError("simulated crash before commit")
        return real_commit()

    monkeypatch.setattr(store, "commit", flaky_commit)
    with pytest.raises(RuntimeError):
        pp.run(bucket, NAMESPACE, out, "backfill", store)
    store.close()

    # Restart with a fresh store on the same DB + output dir.
    store2 = Store(db)
    try:
        run(bucket, NAMESPACE, out, "incremental", store2)
        final_count = store2.email_count()
    finally:
        store2.close()

    assert final_count == expected

    # No duplicate files and every file matches its content hash.
    staged_root = os.path.join(out, "staged")
    seen = set()
    for dirpath, _, filenames in os.walk(staged_root):
        for name in filenames:
            full = os.path.join(dirpath, name)
            with open(full, "rb") as fh:
                digest = hashlib.sha256(fh.read()).hexdigest()
            assert digest not in seen
            seen.add(digest)
            assert name.startswith(digest)
    assert len(seen) == expected
