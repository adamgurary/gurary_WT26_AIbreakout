"""Write a sanitized, read-only inventory for a configured deployment target."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from deploy.inventory import inventory_target


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=("fevm", "field_eng"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    output = args.output or ROOT / "deploy" / "inventory" / f"live-{args.target}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    inventory = inventory_target(args.target)
    output.write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
