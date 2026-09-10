#!/usr/bin/env python3
"""Render an isolated target/state deployment tree."""

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from deploy.render import render_deployment


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", choices=("fevm", "field_eng"))
    parser.add_argument("state", choices=("baseline", "upgraded"))
    arguments = parser.parse_args()
    print(render_deployment(arguments.target, arguments.state))


if __name__ == "__main__":
    main()
