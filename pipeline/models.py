"""Core data structures shared across the pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Lineage:
    """Ordered provenance chain rendered as a single string.

    Container hops are separated by ``!`` and a message index inside an
    MBOX/PST by ``#``, for example::

        namespace_a/timestamp=2024-07-15/archive.zip!nested.zip!mailbox.mbox#3
    """

    value: str

    def extend(self, segment: str) -> "Lineage":
        return Lineage(self.value + segment)

    def __str__(self) -> str:
        return self.value


@dataclass
class Source:
    """A top-level file discovered directly inside a date partition."""

    namespace: str
    partition: str          # e.g. "2024-07-15"
    source_path: str        # relative to the bucket root
    abs_path: str
    source_hash: str        # sha256 of the source bytes (CDC identity)


@dataclass
class Blob:
    """A unit of data flowing through the recursive unpacker."""

    name: str
    data: bytes
    lineage: Lineage
    partition: str
    source_path: str                 # originating top-level source (for skip rows)
    kind: Optional[str] = None       # explicit kind override (e.g. mbox/pst messages)


@dataclass
class LeafEmail:
    """An individual email ready to be staged."""

    data: bytes
    lineage: Lineage
    partition: str
    kind: str
    source_path: str


@dataclass
class RunSummary:
    sources_processed: int = 0
    emails_staged: int = 0
    duplicates: int = 0
    files_skipped: int = 0
