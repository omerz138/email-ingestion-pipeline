"""Email file ingestion pipeline.

Discovers email files in date-partitioned directories, recursively unpacks
containers (ZIP / MBOX / PST), deduplicates by content hash, and stages the
resulting individual emails with full lineage. SQLite is the source of truth.

See docs/DESIGN.md for the design.
"""

from .pipeline import run
from .store import Store

__all__ = ["run", "Store"]
