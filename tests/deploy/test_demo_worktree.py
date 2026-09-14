"""
Unit tests for scripts/new_demo_worktree.py.

All git and filesystem operations are mocked. No real worktrees are created,
no real repos are mutated, and the Desktop is never touched.
"""
from __future__ import annotations

import os
import sys
import unittest
from io import StringIO
from unittest.mock import MagicMock, call, patch

# Make the scripts directory importable without installing anything.
_SCRIPTS_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "scripts"
)
sys.path.insert(0, os.path.abspath(_SCRIPTS_DIR))

from new_demo_worktree import (  # noqa: E402
    _REHEARSALS_ROOT,
    _TAG,
    _verify_prerequisites,
    create_rehearsal_worktree,
)


class TestVerifyPrerequisites(unittest.TestCase):
    """_verify_prerequisites raises when remotes or the tag are absent."""

    def _run_side_effect(self, cmd, cwd=None):
        """Return plausible stdout for known safe commands; raise for others."""
        if cmd == ["git", "remote"]:
            return "origin\nupstream"
        if cmd == ["git", "rev-parse", f"refs/tags/{_TAG}"]:
            return "abc123def456abc123def456abc123def456abc1"
        raise AssertionError(f"Unexpected command in test: {cmd!r}")

    @patch("new_demo_worktree._run")
    def test_passes_when_remotes_and_tag_exist(self, mock_run):
        mock_run.side_effect = self._run_side_effect
        _verify_prerequisites("/fake/repo")  # must not raise

    @patch("new_demo_worktree._run")
    def test_raises_when_origin_missing(self, mock_run):
        def side_effect(cmd, cwd=None):
            if cmd == ["git", "remote"]:
                return "upstream"  # origin absent
            return "abc"
        mock_run.side_effect = side_effect
        with self.assertRaises(RuntimeError) as ctx:
            _verify_prerequisites("/fake/repo")
        self.assertIn("origin", str(ctx.exception))

    @patch("new_demo_worktree._run")
    def test_raises_when_upstream_missing(self, mock_run):
        def side_effect(cmd, cwd=None):
            if cmd == ["git", "remote"]:
                return "origin"  # upstream absent
            return "abc"
        mock_run.side_effect = side_effect
        with self.assertRaises(RuntimeError) as ctx:
            _verify_prerequisites("/fake/repo")
        self.assertIn("upstream", str(ctx.exception))

    @patch("new_demo_worktree._run")
    def test_raises_when_tag_missing_locally_and_fetch_fails(self, mock_run):
        def side_effect(cmd, cwd=None):
            if cmd == ["git", "remote"]:
                return "origin\nupstream"
            if cmd[0:2] == ["git", "rev-parse"]:
                raise RuntimeError("unknown revision")
            if cmd[0:2] == ["git", "fetch"]:
                raise RuntimeError("fetch failed")
            return ""
        mock_run.side_effect = side_effect
        with self.assertRaises(RuntimeError) as ctx:
            _verify_prerequisites("/fake/repo")
        self.assertIn(_TAG, str(ctx.exception))


class TestCreateRehearsalWorktree(unittest.TestCase):
    """create_rehearsal_worktree behaves correctly without touching real git."""

    def _make_mock_run(self, extra: dict | None = None):
        """Return a _run mock that handles standard queries."""
        defaults = {
            ("git", "remote"): "origin\nupstream",
            ("git", "rev-parse"): "abc123def456abc123def456abc123def456abc1",
        }
        if extra:
            defaults.update(extra)

        def side_effect(cmd, cwd=None):
            key = tuple(cmd[:2])
            if key in defaults:
                return defaults[key]
            # Allow git worktree add to succeed silently
            if cmd[:3] == ["git", "worktree", "add"]:
                return ""
            raise AssertionError(f"Unexpected _run call: {cmd!r}")

        return side_effect

    @patch("new_demo_worktree.os.makedirs")
    @patch("new_demo_worktree.os.path.exists", return_value=False)
    @patch("new_demo_worktree._run")
    def test_uses_detach_flag_and_tag(self, mock_run, mock_exists, mock_makedirs):
        mock_run.side_effect = self._make_mock_run()
        result = create_rehearsal_worktree(
            repo_root="/fake/repo",
            rehearsals_root="/fake/rehearsals",
        )
        # Find the worktree add call
        worktree_calls = [
            c for c in mock_run.call_args_list
            if c.args[0][:3] == ["git", "worktree", "add"]
        ]
        self.assertEqual(len(worktree_calls), 1)
        cmd = worktree_calls[0].args[0]
        self.assertIn("--detach", cmd)
        self.assertIn(_TAG, cmd)

    @patch("new_demo_worktree.os.makedirs")
    @patch("new_demo_worktree.os.path.exists", return_value=False)
    @patch("new_demo_worktree._run")
    def test_dest_is_under_rehearsals_root(self, mock_run, mock_exists, mock_makedirs):
        mock_run.side_effect = self._make_mock_run()
        result = create_rehearsal_worktree(
            repo_root="/fake/repo",
            rehearsals_root="/fake/rehearsals",
        )
        self.assertTrue(result.startswith("/fake/rehearsals/"))

    @patch("new_demo_worktree.os.makedirs")
    @patch("new_demo_worktree.os.path.exists", return_value=False)
    @patch("new_demo_worktree._run")
    def test_prints_omnigent_working_directory(self, mock_run, mock_exists, mock_makedirs):
        mock_run.side_effect = self._make_mock_run()
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            result = create_rehearsal_worktree(
                repo_root="/fake/repo",
                rehearsals_root="/fake/rehearsals",
            )
            output = mock_stdout.getvalue()
        self.assertIn("Omnigent working directory", output)
        self.assertIn(result, output)

    @patch("new_demo_worktree.os.makedirs")
    @patch("new_demo_worktree.os.path.exists", return_value=True)  # dest already exists
    @patch("new_demo_worktree._run")
    def test_refuses_existing_destination(self, mock_run, mock_exists, mock_makedirs):
        mock_run.side_effect = self._make_mock_run()
        with self.assertRaises(FileExistsError):
            create_rehearsal_worktree(
                repo_root="/fake/repo",
                rehearsals_root="/fake/rehearsals",
            )

    @patch("new_demo_worktree.os.makedirs")
    @patch("new_demo_worktree.os.path.exists", return_value=False)
    @patch("new_demo_worktree._run")
    def test_never_runs_reset_restore_checkout(self, mock_run, mock_exists, mock_makedirs):
        mock_run.side_effect = self._make_mock_run()
        create_rehearsal_worktree(
            repo_root="/fake/repo",
            rehearsals_root="/fake/rehearsals",
        )
        forbidden = {"reset", "restore", "checkout"}
        for c in mock_run.call_args_list:
            cmd = c.args[0]
            if len(cmd) >= 2 and cmd[0] == "git":
                self.assertNotIn(
                    cmd[1],
                    forbidden,
                    msg=f"Forbidden git subcommand {cmd[1]!r} was called: {cmd!r}",
                )

    @patch("new_demo_worktree.os.makedirs")
    @patch("new_demo_worktree.os.path.exists", return_value=False)
    @patch("new_demo_worktree._run")
    def test_dest_includes_utc_timestamp(self, mock_run, mock_exists, mock_makedirs):
        """Destination path contains a UTC timestamp pattern."""
        import re
        mock_run.side_effect = self._make_mock_run()
        result = create_rehearsal_worktree(
            repo_root="/fake/repo",
            rehearsals_root="/fake/rehearsals",
        )
        # e.g. rehearsal-20261214T120000Z
        self.assertRegex(os.path.basename(result), r"rehearsal-\d{8}T\d{6}Z")


if __name__ == "__main__":
    unittest.main()
