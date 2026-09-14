"""
Create a disposable rehearsal worktree from the demo-baseline-v1 tag.

Usage:
    python scripts/new_demo_worktree.py [--rehearsals-root <path>]

Default rehearsals root: /Users/adam.gurary/Desktop/bobabricks-rehearsals/
Each run creates a uniquely timestamped directory under that root so runs
never collide with each other or with the working fork.

The caller's working tree is never touched. No reset, restore, or checkout
is run on any existing branch.
"""
from __future__ import annotations

import argparse
import datetime
import os
import subprocess
import sys

_REHEARSALS_ROOT = "/Users/adam.gurary/Desktop/bobabricks-rehearsals"
_TAG = "demo-baseline-v1"
_REQUIRED_REMOTES = {"origin", "upstream"}


def _run(cmd: list[str], cwd: str | None = None) -> str:
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command {cmd!r} failed (exit {result.returncode}):\n{result.stderr.strip()}"
        )
    return result.stdout.strip()


def _verify_prerequisites(repo_root: str) -> None:
    """Confirm origin, upstream, and the tag all exist before doing anything."""
    # Check remotes
    raw = _run(["git", "remote"], cwd=repo_root)
    present = set(raw.splitlines())
    missing = _REQUIRED_REMOTES - present
    if missing:
        raise RuntimeError(
            f"Required remotes missing from {repo_root!r}: {sorted(missing)}"
        )

    # Check tag exists (locally or after fetch)
    try:
        _run(["git", "rev-parse", f"refs/tags/{_TAG}"], cwd=repo_root)
    except RuntimeError:
        # Try fetching the tag from origin
        try:
            _run(["git", "fetch", "origin", f"refs/tags/{_TAG}:refs/tags/{_TAG}"], cwd=repo_root)
        except RuntimeError as fetch_err:
            raise RuntimeError(
                f"Tag {_TAG!r} not found locally and could not be fetched: {fetch_err}"
            ) from fetch_err


def _make_dest(rehearsals_root: str) -> str:
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return os.path.join(rehearsals_root, f"rehearsal-{ts}")


def create_rehearsal_worktree(
    repo_root: str | None = None,
    rehearsals_root: str = _REHEARSALS_ROOT,
) -> str:
    """
    Create a disposable worktree from demo-baseline-v1.

    Parameters
    ----------
    repo_root:
        The git repository to create the worktree from. Defaults to the
        directory containing this script's parent (i.e. the fork root).
    rehearsals_root:
        Parent directory for rehearsal worktrees.

    Returns
    -------
    str
        Absolute path to the new worktree (the Omnigent working directory).
    """
    if repo_root is None:
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    _verify_prerequisites(repo_root)

    dest = _make_dest(rehearsals_root)

    if os.path.exists(dest):
        raise FileExistsError(
            f"Destination already exists: {dest!r}. "
            "This should not happen with UTC-second timestamps unless two "
            "calls are made within the same second."
        )

    os.makedirs(rehearsals_root, exist_ok=True)

    _run(
        ["git", "worktree", "add", "--detach", dest, _TAG],
        cwd=repo_root,
    )

    print(f"Rehearsal worktree created.")
    print(f"Omnigent working directory: {dest}")
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rehearsals-root",
        default=_REHEARSALS_ROOT,
        help=f"Parent directory for rehearsal worktrees (default: {_REHEARSALS_ROOT})",
    )
    args = parser.parse_args()

    try:
        path = create_rehearsal_worktree(rehearsals_root=args.rehearsals_root)
        print(f"\nReady. Open this directory in Omnigent to start the rehearsal:\n  {path}")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
