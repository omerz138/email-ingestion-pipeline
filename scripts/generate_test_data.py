"""Populate ./test_data with the fixture bucket for manual pipeline runs.

Usage:
    python scripts/generate_test_data.py [target_dir]
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.fixtures_builder import build_bucket


def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else "test_data"
    build_bucket(target)
    print("Wrote fixtures to %s/" % target)


if __name__ == "__main__":
    main()
