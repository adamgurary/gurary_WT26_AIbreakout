"""Make deployment tests discoverable without shadowing production code."""

from pathlib import Path


_PRODUCTION_DEPLOY = Path(__file__).resolve().parents[2] / "deploy"
__path__.insert(0, str(_PRODUCTION_DEPLOY))
