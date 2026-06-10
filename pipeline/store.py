"""SQLite store - the single source of truth for CDC state, lineage,
skipped files, and the dedup index.

Writes for a single source are committed together so the pipeline is
crash-safe and idempotent: a crash before ``commit()`` simply leaves the
source unprocessed, and it is retried on the next run.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import List, Tuple

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
  source_path  TEXT NOT NULL,
  source_hash  TEXT NOT NULL,   -- sha256 of source bytes (CDC identity)
  namespace    TEXT NOT NULL,
  partition    TEXT NOT NULL,
  status       TEXT NOT NULL,   -- pending | processed | skipped
  updated_at   TEXT NOT NULL,
  PRIMARY KEY (source_path, source_hash)
);

CREATE TABLE IF NOT EXISTS emails (
  content_hash TEXT PRIMARY KEY, -- sha256 of leaf email bytes (the unique id)
  partition    TEXT NOT NULL,
  staged_path  TEXT NOT NULL,
  created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lineage (
  content_hash TEXT NOT NULL REFERENCES emails(content_hash),
  chain        TEXT NOT NULL,
  PRIMARY KEY (content_hash, chain)
);

CREATE TABLE IF NOT EXISTS skipped (
  source_path  TEXT NOT NULL,
  chain        TEXT,
  reason       TEXT NOT NULL,
  created_at   TEXT NOT NULL
);
"""


class Store:
    def __init__(self, db_path: str):
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.utcnow().isoformat()

    # --- CDC / source identity ---------------------------------------------

    def is_processed(self, source_path: str, source_hash: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM sources WHERE source_path=? AND source_hash=? AND status='processed'",
            (source_path, source_hash),
        )
        return cur.fetchone() is not None

    def mark_processed(self, source_path: str, source_hash: str, namespace: str, partition: str) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO sources(source_path, source_hash, namespace, partition, status, updated_at) "
            "VALUES (?, ?, ?, ?, 'processed', ?)",
            (source_path, source_hash, namespace, partition, self._now()),
        )

    # --- emails / dedup / lineage ------------------------------------------

    def email_exists(self, content_hash: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM emails WHERE content_hash=?", (content_hash,))
        return cur.fetchone() is not None

    def record_email(self, content_hash: str, staged_path: str, partition: str, chain: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO emails(content_hash, partition, staged_path, created_at) VALUES (?, ?, ?, ?)",
            (content_hash, partition, staged_path, self._now()),
        )
        self.add_lineage(content_hash, chain)

    def add_lineage(self, content_hash: str, chain: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO lineage(content_hash, chain) VALUES (?, ?)",
            (content_hash, chain),
        )

    def record_skip(self, source_path: str, chain: str, reason: str) -> None:
        self.conn.execute(
            "INSERT INTO skipped(source_path, chain, reason, created_at) VALUES (?, ?, ?, ?)",
            (source_path, chain, reason, self._now()),
        )

    # --- transaction control -----------------------------------------------

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def close(self) -> None:
        self.conn.close()

    # --- read helpers (manifest export / summaries) ------------------------

    def iter_emails(self) -> List[Tuple[str, str, str]]:
        cur = self.conn.execute(
            "SELECT content_hash, partition, staged_path FROM emails ORDER BY partition, content_hash"
        )
        return cur.fetchall()

    def lineages_for(self, content_hash: str) -> List[str]:
        cur = self.conn.execute(
            "SELECT chain FROM lineage WHERE content_hash=? ORDER BY chain", (content_hash,)
        )
        return [row[0] for row in cur.fetchall()]

    def email_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]

    def skip_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM skipped").fetchone()[0]

    def skip_reasons(self) -> List[Tuple[str, str, str]]:
        cur = self.conn.execute("SELECT source_path, chain, reason FROM skipped ORDER BY reason, source_path")
        return cur.fetchall()
