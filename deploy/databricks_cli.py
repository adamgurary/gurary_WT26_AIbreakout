"""Small JSON-only wrapper around the Databricks CLI."""

from __future__ import annotations

import json
import subprocess
from typing import Any
from urllib.parse import urlsplit, urlunsplit


REQUIRED_USER = "adam.gurary@databricks.com"


def normalize_host(host: str) -> str:
    """Return the canonical workspace URL used for identity comparisons."""
    if not isinstance(host, str) or not host:
        raise RuntimeError("Databricks profile did not provide a host")
    parsed = urlsplit(host.strip())
    if parsed.scheme.lower() != "https" or not parsed.netloc or parsed.path.rstrip("/"):
        raise RuntimeError(f"Invalid Databricks workspace host: {host!r}")
    return urlunsplit(("https", parsed.netloc.lower(), "", "", ""))


def run_json(profile: str, args: list[str], payload: dict | None = None) -> dict | list:
    """Run a read or write CLI API command without shell interpolation."""
    command = ["databricks", *args]
    if payload is not None:
        command.extend(["--json", "@/dev/stdin"])
    command.extend(["--profile", profile, "--output", "json"])
    result = subprocess.run(
        command,
        input=json.dumps(payload) if payload is not None else None,
        capture_output=True,
        text=True,
        check=True,
    )
    try:
        decoded = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("Databricks CLI did not return JSON") from error
    if not isinstance(decoded, (dict, list)):
        raise RuntimeError("Databricks CLI JSON response must be an object or list")
    return decoded


def _profile_host(auth_environment: dict | list) -> str:
    if not isinstance(auth_environment, dict):
        raise RuntimeError("Databricks auth environment response must be an object")
    environment = auth_environment.get("env", auth_environment)
    if not isinstance(environment, dict):
        raise RuntimeError("Databricks auth environment is malformed")
    host = environment.get("host") or environment.get("DATABRICKS_HOST")
    if not isinstance(host, str):
        raise RuntimeError("Databricks auth environment did not provide DATABRICKS_HOST")
    return host


def live_workspace_id(profile: str) -> str:
    """Return the workspace ID configured for *profile*.

    Reads ``Config(profile=profile).workspace_id``, which the SDK resolves from the
    Databricks config file.  This avoids any live network call and never hangs on
    SPOG/account-level profiles where ``get_workspace_id()`` times out.
    """
    from databricks.sdk.core import Config  # lazy import — keeps module importable without SDK

    ws = Config(profile=profile).workspace_id
    if not ws:
        raise RuntimeError(f"profile {profile!r} has no workspace_id configured")
    return str(ws)


def assert_profile(profile: str, expected_host: str) -> dict:
    """Verify the CLI profile targets the expected workspace and Adam's user."""
    auth_environment = run_json(profile, ["auth", "env"])
    current_user = run_json(profile, ["current-user", "me"])
    if not isinstance(current_user, dict):
        raise RuntimeError("Databricks current-user response must be an object")

    actual_host = normalize_host(_profile_host(auth_environment))
    expected = normalize_host(expected_host)
    if actual_host != expected:
        raise RuntimeError(f"Databricks profile host mismatch: expected {expected}, got {actual_host}")

    # The generated CLI currently emits SCIM's camelCase ``userName`` while
    # older releases emitted the SDK-style ``user_name``.  Treat only those
    # two exact spellings as identity fields; never fall back to display name
    # or email metadata.
    user_name = current_user.get("user_name", current_user.get("userName"))
    if user_name != REQUIRED_USER:
        raise RuntimeError(f"Databricks current user mismatch: expected {REQUIRED_USER}, got {user_name!r}")
    return {"host": actual_host, "current_user": current_user}
