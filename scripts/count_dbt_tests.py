#!/usr/bin/env python
"""count the dbt tests in the parsed manifest

the readme quotes a test count and a number nobody can reproduce is a number
nobody should believe so this prints it straight off target/manifest.json
make dbt-count-tests runs it
"""

from __future__ import annotations

import collections
import json
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    manifest_path = Path(argv[0]) if argv else Path("target/manifest.json")
    if not manifest_path.exists():
        print(f"{manifest_path} not found; run dbt parse first", file=sys.stderr)
        return 1

    manifest = json.loads(manifest_path.read_text())
    nodes = manifest["nodes"]
    tests = [n for n in nodes.values() if n["resource_type"] == "test"]
    generic = [t for t in tests if t.get("test_metadata")]
    singular = [t for t in tests if not t.get("test_metadata")]

    by_type = collections.Counter(t["test_metadata"]["name"] for t in generic)

    print(f"dbt tests: {len(tests)}")
    print(f"  generic:  {len(generic)}")
    print(f"  singular: {len(singular)}")
    print("\ngeneric tests by type:")
    for name, count in by_type.most_common():
        print(f"  {name:<34} {count}")
    print("\nsingular tests:")
    for test in sorted(singular, key=lambda t: t["name"]):
        print(f"  {test['name']}")

    print("\nother nodes:")
    for kind in ("model", "snapshot", "seed"):
        print(f"  {kind + 's':<34} {sum(1 for n in nodes.values() if n['resource_type'] == kind)}")
    print(f"  {'sources':<34} {len(manifest['sources'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
