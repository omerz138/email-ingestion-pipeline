"""Command-line entry point.

Examples:
    python -m pipeline run --namespace namespace_a --mode backfill   --bucket ./test_data --out ./output
    python -m pipeline run --namespace namespace_a --mode incremental --bucket ./test_data --out ./output
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Optional, Sequence

from .manifest import export_manifest
from .pipeline import run
from .store import Store


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="run the ingestion pipeline")
    run_parser.add_argument("--namespace", required=True)
    run_parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    run_parser.add_argument("--bucket", required=True, help="path to the bucket root")
    run_parser.add_argument("--out", required=True, help="output directory for staged emails")

    args = parser.parse_args(argv)

    if args.command == "run":
        db_path = os.path.join(args.out, "state.db")
        store = Store(db_path)
        try:
            summary = run(args.bucket, args.namespace, args.out, args.mode, store)
            manifest_path = export_manifest(args.out, store)
        finally:
            store.close()

        print(
            "sources_processed=%d emails_staged=%d duplicates=%d files_skipped=%d"
            % (
                summary.sources_processed,
                summary.emails_staged,
                summary.duplicates,
                summary.files_skipped,
            )
        )
        print("manifest=%s" % manifest_path)
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
