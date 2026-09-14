#!/usr/bin/env python3
"""CLI: validate a Bobabricks demo stack and write a go/no-go JSON report.

Usage
-----
  python -m scripts.validate_stack \\
      --target {fevm,field_eng} \\
      --expected-state {baseline,upgraded} \\
      --output <path>

Exit codes
----------
  0 – GO (all required checks PASS)
  1 – NO-GO (at least one required check FAIL)
  2 – Usage or configuration error
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from deploy.validation import validate_stack  # noqa: E402


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a Bobabricks demo stack."
    )
    parser.add_argument(
        "--target",
        required=True,
        choices=("fevm", "field_eng"),
        help="Deployment target key.",
    )
    parser.add_argument(
        "--expected-state",
        required=True,
        choices=("baseline", "upgraded"),
        dest="expected_state",
        help="Expected demo state.",
    )
    parser.add_argument(
        "--output",
        required=False,
        default=None,
        help="Path to write the go/no-go JSON report.",
    )
    parser.add_argument(
        "--fevm-baseline-hash",
        required=False,
        default="",
        dest="fevm_baseline_hash",
        help=(
            "Expected sha256 of the canonical FEVM shared ops_tasks snapshot."
            " Required for the fevm_shared_ops_unchanged check to be meaningful."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    try:
        report = validate_stack(
            args.target,
            args.expected_state,
            fevm_baseline_hash=args.fevm_baseline_hash,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: validation aborted — {exc}", file=sys.stderr)
        return 2

    result = report.as_dict()
    verdict = "GO" if report.go else "NO-GO"

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(result, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"{verdict}: report written to {out_path}")
    else:
        print(f"{verdict}")

    _print_summary(report.checks)

    return 0 if report.go else 1


def _print_summary(checks: list) -> None:
    width = max((len(c.name) for c in checks), default=30)
    for check in checks:
        mark = {"PASS": "✓", "FAIL": "✗", "N/A": "–"}.get(check.status, "?")
        line = f"  {mark} {check.name:<{width}}"
        if check.status == "FAIL" and check.error:
            line += f"  {check.error[:100]}"
        print(line)


if __name__ == "__main__":
    sys.exit(main())
