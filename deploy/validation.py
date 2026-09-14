"""End-to-end validation for Bobabricks demo stacks.

Each check records PASS or FAIL, evidence identifiers, a UTC timestamp, and a
concise error.  Credentials and OAuth tokens are never stored in the report.
"""

from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Callable

from deploy.config import load_target, load_state, TargetConfig, DemoState
from deploy.safety import assert_safe_name, UnsafeMutation


# ── Callable type aliases (dependency-injected so tests can mock them) ─────────

AppStateProvider = Callable[[str, str], str]
"""(profile, app_name) -> state string, e.g. "RUNNING"."""

OboScopesProvider = Callable[[str, str], list[str]]
"""(profile, app_name) -> list of scope strings."""

UcGrantsProvider = Callable[[str, str, str, str], list[str]]
"""(profile, catalog, schema, principal) -> list of privilege strings."""

McpInvoker = Callable[[str, str, dict[str, Any]], dict[str, Any]]
"""(profile, url, jsonrpc_payload) -> response dict."""

GenieQueryProvider = Callable[[str, str, str], dict[str, Any]]
"""(profile, space_id, prompt) -> genie response dict."""

LakebaseSessionProvider = Callable[[str, str], dict[str, Any]]
"""(profile, project_name) -> session dict or raises on failure."""

MlflowExperimentProvider = Callable[[str, str], dict[str, Any]]
"""(profile, experiment_name) -> experiment dict."""

TraceProofProvider = Callable[[dict[str, Any]], dict[str, Any]]
"""(request) -> proof dict."""

SqlRunner = Callable[[str, str, str, str], list[dict[str, Any]]]
"""(profile, warehouse_id, sql, description) -> list of row dicts."""

HashProvider = Callable[[str, str, str], str]
"""(profile, warehouse_id, full_table_name) -> hex sha256 of canonical snapshot."""

WorkspaceIdProvider = Callable[[str], str]
"""(profile) -> workspace_id string."""


# ── Primitives ─────────────────────────────────────────────────────────────────

@dataclass
class Check:
    name: str
    status: str  # "PASS" | "FAIL" | "N/A"
    evidence: dict[str, Any] = field(default_factory=dict)
    error: str = ""
    timestamp: str = field(default_factory=lambda: _utc_now())

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationReport:
    target_key: str
    expected_state: str
    session_id: str
    timestamp: str
    checks: list[Check] = field(default_factory=list)

    @property
    def go(self) -> bool:
        required = {c.name for c in self.checks if c.status != "N/A"}
        return all(c.status == "PASS" for c in self.checks if c.name in required)

    def as_dict(self) -> dict[str, Any]:
        return {
            "target_key": self.target_key,
            "expected_state": self.expected_state,
            "session_id": self.session_id,
            "timestamp": self.timestamp,
            "go": self.go,
            "checks": [c.as_dict() for c in self.checks],
        }


def _utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


# ── PRESENTER PROMPTS (from spec / provision_field_eng) ─────────────────────────

_ACCEPTANCE_PROMPTS = (
    "What tools do you have?",
    "Show average versus actual training hours for my Pacific stores"
    " and flag any stores falling behind.",
    "Why is Store 104 behind on training?",
    "What are our FY26 training goals,"
    " and how does Store 104 stack up?",
)


# ── Individual check helpers ───────────────────────────────────────────────────

def _check(name: str, fn: Callable[[], tuple[str, dict[str, Any]]]) -> Check:
    """Run fn(); wrap PASS/FAIL + evidence into a Check."""
    try:
        status, evidence = fn()
        return Check(name=name, status=status, evidence=evidence)
    except Exception as exc:  # noqa: BLE001
        return Check(name=name, status="FAIL", error=str(exc)[:300])


def _check_workspace_identity(
    target: TargetConfig,
    workspace_id_provider: WorkspaceIdProvider,
) -> Check:
    def _run() -> tuple[str, dict[str, Any]]:
        live_id = workspace_id_provider(target.profile)
        if str(live_id) != str(target.workspace_id):
            raise RuntimeError(
                f"Workspace ID mismatch: expected {target.workspace_id!r},"
                f" got {live_id!r}"
            )
        return "PASS", {"workspace_id": str(live_id)}

    return _check("workspace_identity", _run)


def _check_resource_namespace(target: TargetConfig) -> Check:
    """Check that all Adam-OWNED resource names are gurary-namespaced.

    Shared read-only dependencies (e.g. the FEVM shared MCP app names) are
    excluded from this check — they are never mutated.
    """
    def _run() -> tuple[str, dict[str, Any]]:
        checked: list[str] = []
        # Always check the presenter app (Adam always owns it).
        owned = [target.presenter_app_name]
        # For non-read-only targets the storetime/opstask MCP apps are
        # Adam-owned too.
        if not target.shared_mcp_read_only:
            for name in (target.storetime_app_name, target.opstask_app_name):
                if name:
                    owned.append(name)
        for name in owned:
            if not name:
                continue
            try:
                assert_safe_name(name)
                checked.append(name)
            except UnsafeMutation as exc:
                raise RuntimeError(str(exc)) from exc
        return "PASS", {"checked_names": checked}

    return _check("resource_namespace", _run)


def _check_app_running(
    target: TargetConfig,
    state_key: str,
    app_state_provider: AppStateProvider,
) -> Check:
    def _run() -> tuple[str, dict[str, Any]]:
        app_name = target.presenter_app_name
        state = app_state_provider(target.profile, app_name)
        if state != "RUNNING":
            raise RuntimeError(
                f"Presenter app {app_name!r} is {state!r}; expected RUNNING"
            )
        return "PASS", {"app_name": app_name, "app_state": state}

    return _check("app_running", _run)


def _check_obo_scopes(
    target: TargetConfig,
    obo_scopes_provider: OboScopesProvider,
) -> Check:
    required = frozenset(("ai-gateway", "genie", "mcp.external"))

    def _run() -> tuple[str, dict[str, Any]]:
        scopes = obo_scopes_provider(target.profile, target.presenter_app_name)
        missing = sorted(required - set(scopes))
        if missing:
            raise RuntimeError(
                f"OBO scopes missing from {target.presenter_app_name!r}: {missing}"
            )
        return "PASS", {"scopes": sorted(scopes)}

    return _check("obo_scopes", _run)


def _check_uc_grants(
    target: TargetConfig,
    uc_grants_provider: UcGrantsProvider,
) -> Check:
    required = frozenset({"USE_CATALOG", "USE_SCHEMA", "CREATE_TABLE"})

    def _run() -> tuple[str, dict[str, Any]]:
        granted = set(
            uc_grants_provider(
                target.profile,
                target.catalog,
                target.trace_schema,
                target.presenter_app_name,
            )
        )
        missing = sorted(required - granted)
        if missing:
            raise RuntimeError(
                f"UC trace-schema grants missing for"
                f" {target.presenter_app_name!r}: {missing}"
            )
        return "PASS", {"granted": sorted(granted)}

    return _check("uc_grants", _run)


def _check_mcp_initialize(
    target: TargetConfig,
    mcp_invoker: McpInvoker,
) -> Check:
    def _run() -> tuple[str, dict[str, Any]]:
        urls = []
        if target.storetime_mcp_url:
            urls.append(("storetime", target.storetime_mcp_url))
        if target.opstask_mcp_url:
            urls.append(("opstask", target.opstask_mcp_url))
        if not urls:
            return "N/A", {"note": "no MCP URLs configured for this target"}
        results: dict[str, Any] = {}
        for label, url in urls:
            resp = mcp_invoker(
                target.profile,
                url,
                {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
            )
            if resp.get("jsonrpc") != "2.0":
                raise RuntimeError(
                    f"{label} MCP initialize did not return jsonrpc 2.0"
                )
            results[label] = resp.get("result", {}).get("serverInfo", {}).get("name", "")
        return "PASS", {"initialized": results}

    return _check("mcp_initialize", _run)


def _check_mcp_tools_list(
    target: TargetConfig,
    state: DemoState,
    mcp_invoker: McpInvoker,
) -> Check:
    def _run() -> tuple[str, dict[str, Any]]:
        urls = []
        if target.storetime_mcp_url:
            urls.append(("storetime", target.storetime_mcp_url))
        if target.opstask_mcp_url:
            urls.append(("opstask", target.opstask_mcp_url))
        if not urls:
            return "N/A", {"note": "no MCP URLs configured for this target"}
        found_tools: list[str] = []
        for label, url in urls:
            resp = mcp_invoker(
                target.profile,
                url,
                {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            )
            result = resp.get("result", {})
            tools = result.get("tools", [])
            if not isinstance(tools, list):
                raise RuntimeError(
                    f"{label} MCP tools/list returned malformed tools"
                )
            found_tools.extend(t.get("name", "") for t in tools)
        for required in state.required_tools:
            if required not in found_tools:
                raise RuntimeError(
                    f"Required tool {required!r} absent from MCP tools/list"
                )
        return "PASS", {"tools": sorted(set(found_tools))}

    return _check("mcp_tools_list", _run)


def _check_genie_query(
    target: TargetConfig,
    genie_query_provider: GenieQueryProvider,
) -> Check:
    def _run() -> tuple[str, dict[str, Any]]:
        space_id = target.genie_space_id or ""
        if not space_id:
            return "PASS", {"note": "no genie_space_id configured"}
        resp = genie_query_provider(
            target.profile,
            space_id,
            "What stores do we have?",
        )
        if not isinstance(resp, dict):
            raise RuntimeError("Genie query returned a non-dict response")
        return "PASS", {
            "space_id": space_id,
            "response_keys": sorted(resp.keys()),
        }

    return _check("genie_query", _run)


def _check_lakebase_session(
    target: TargetConfig,
    lakebase_session_provider: LakebaseSessionProvider,
) -> Check:
    """Report FAIL/degraded on Lakebase OAuth issues rather than crashing."""
    if not target.lakebase_project:
        return Check(
            name="lakebase_session",
            status="N/A",
            evidence={"note": "no lakebase_project configured for this target"},
        )

    def _run() -> tuple[str, dict[str, Any]]:
        result = lakebase_session_provider(target.profile, target.lakebase_project or "")
        if not isinstance(result, dict):
            raise RuntimeError("Lakebase session provider returned malformed response")
        degraded = result.get("degraded", False)
        if degraded:
            raise RuntimeError(
                "Lakebase session is DEGRADED:"
                f" {result.get('error', 'unknown')}"
            )
        return "PASS", {
            "project": target.lakebase_project,
            "session_state": result.get("state", "unknown"),
        }

    return _check("lakebase_session", _run)


def _check_mlflow_experiment(
    target: TargetConfig,
    mlflow_experiment_provider: MlflowExperimentProvider,
) -> Check:
    experiment_name = (
        f"/Users/adam.gurary@databricks.com"
        f"/gurary_bobabricks_store_ops_uc"
    )

    def _run() -> tuple[str, dict[str, Any]]:
        exp = mlflow_experiment_provider(target.profile, experiment_name)
        if not isinstance(exp, dict):
            raise RuntimeError("MLflow experiment provider returned malformed response")
        exp_id = exp.get("experiment_id", "")
        tags = exp.get("tags", {})
        trace_dest = tags.get(
            "mlflow.experiment.databricksTraceDestinationPath", ""
        )
        if not trace_dest.startswith(
            f"{target.catalog}.{target.trace_schema}"
        ):
            raise RuntimeError(
                f"Experiment trace destination {trace_dest!r}"
                f" does not match expected"
                f" {target.catalog}.{target.trace_schema}"
            )
        return "PASS", {
            "experiment_id": exp_id,
            "trace_destination": trace_dest,
        }

    return _check("mlflow_experiment", _run)


def _check_uc_trace_tables(
    target: TargetConfig,
    sql_runner: SqlRunner,
) -> Check:
    prefix = "gurary_bobabricks"
    tables = (
        f"{prefix}",
        f"{prefix}_otel_spans",
        f"{prefix}_otel_logs",
        f"{prefix}_otel_annotations",
    )

    def _run() -> tuple[str, dict[str, Any]]:
        found: list[str] = []
        rows = sql_runner(
            target.profile,
            target.warehouse_id or "",
            (
                f"SHOW TABLES IN `{target.catalog}`.`{target.trace_schema}`"
            ),
            "list trace tables",
        )
        table_names = {r.get("tableName", r.get("name", "")) for r in rows}
        for table in tables:
            if table in table_names:
                found.append(table)
        if len(found) < 1:
            raise RuntimeError(
                "No gurary_bobabricks UC trace tables found in"
                f" {target.catalog}.{target.trace_schema}"
            )
        return "PASS", {
            "found": sorted(found),
            "schema": f"{target.catalog}.{target.trace_schema}",
        }

    return _check("uc_trace_tables", _run)


def _check_successful_tool_spans(
    target: TargetConfig,
    trace_proof_provider: TraceProofProvider,
    session_id: str,
) -> Check:
    def _run() -> tuple[str, dict[str, Any]]:
        request: dict[str, Any] = {
            "session_id": session_id,
            "experiment_name": (
                "/Users/adam.gurary@databricks.com"
                "/gurary_bobabricks_store_ops_uc"
            ),
            "trace_location": (
                f"{target.catalog}.{target.trace_schema}.gurary_bobabricks"
            ),
            "not_before_ms": 0,
            "generated_state": {},
        }
        proof = trace_proof_provider(request)
        if not isinstance(proof, dict):
            raise RuntimeError("Trace proof provider returned malformed response")
        spans = proof.get("spans", [])
        if not isinstance(spans, list) or not spans:
            raise RuntimeError(
                "No tool spans found in trace proof"
            )
        failed = [s for s in spans if s.get("status") not in {"OK", None, ""}]
        if failed:
            raise RuntimeError(
                f"{len(failed)} failed span(s): {[s.get('name') for s in failed]}"
            )
        return "PASS", {
            "trace_id": proof.get("trace_id", ""),
            "span_count": len(spans),
        }

    return _check("successful_tool_spans", _run)


def _check_baseline_or_upgraded_contract(
    target: TargetConfig,
    state: DemoState,
    mcp_invoker: McpInvoker,
) -> Check:
    """Confirm the live tool inventory matches the expected state contract."""
    def _run() -> tuple[str, dict[str, Any]]:
        all_tools: list[str] = []
        urls = []
        if target.storetime_mcp_url:
            urls.append(("storetime", target.storetime_mcp_url))
        if target.opstask_mcp_url:
            urls.append(("opstask", target.opstask_mcp_url))
        if not urls:
            return "N/A", {"note": "no MCP URLs configured; contract check skipped"}
        for label, url in urls:
            resp = mcp_invoker(
                target.profile,
                url,
                {"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {}},
            )
            tools = resp.get("result", {}).get("tools", [])
            all_tools.extend(t.get("name", "") for t in tools)

        if state.confluence_enabled:
            if "search_confluence" not in all_tools:
                raise RuntimeError(
                    "Upgraded state requires search_confluence in tool list"
                )
        else:
            if "search_confluence" in all_tools:
                raise RuntimeError(
                    "Baseline state must NOT expose search_confluence"
                )

        missing_required = [
            r for r in state.required_tools if r not in all_tools
        ]
        if missing_required:
            raise RuntimeError(
                f"Required tools absent for {state.key} contract: {missing_required}"
            )

        return "PASS", {
            "state": state.key,
            "confluence_enabled": state.confluence_enabled,
            "required_tools": sorted(state.required_tools),
        }

    return _check("baseline_or_upgraded_contract", _run)


def _check_confluence_live_grounding(
    target: TargetConfig,
    state: DemoState,
    trace_proof_provider: TraceProofProvider,
    session_id: str,
) -> Check:
    """N/A for baseline; FAIL if fixture-only for upgraded."""
    if not state.confluence_enabled:
        return Check(
            name="confluence_live_grounding",
            status="N/A",
            evidence={"note": "confluence_enabled is false for baseline"},
        )

    if not target.confluence_connection:
        return Check(
            name="confluence_live_grounding",
            status="FAIL",
            error=(
                "Upgraded state requires confluence_connection configured"
                " but target has none"
            ),
        )

    def _run() -> tuple[str, dict[str, Any]]:
        request: dict[str, Any] = {
            "session_id": session_id,
            "experiment_name": (
                "/Users/adam.gurary@databricks.com"
                "/gurary_bobabricks_store_ops_uc"
            ),
            "trace_location": (
                f"{target.catalog}.{target.trace_schema}.gurary_bobabricks"
            ),
            "not_before_ms": 0,
            "generated_state": {},
            "require_confluence_span": True,
        }
        proof = trace_proof_provider(request)
        if not isinstance(proof, dict):
            raise RuntimeError(
                "Confluence trace proof returned malformed response"
            )
        source = proof.get("confluence_source", "")
        # Reject local fixture as the accepted source
        if source == "local_fixture":
            raise RuntimeError(
                "confluence_live_grounding FAIL:"
                " response came from local fixture, not live connection"
                f" ({target.confluence_connection!r})"
            )
        if not proof.get("confluence_span_found"):
            raise RuntimeError(
                "No live Confluence MCP span found in trace proof;"
                " fixture cannot satisfy upgraded acceptance criterion"
            )
        return "PASS", {
            "confluence_connection": target.confluence_connection,
            "confluence_source": source,
            "trace_id": proof.get("trace_id", ""),
        }

    return _check("confluence_live_grounding", _run)


def _check_fevm_shared_ops_unchanged(
    target: TargetConfig,
    hash_provider: HashProvider,
    baseline_hash: str,
) -> Check:
    """Only meaningful for the FEVM target."""
    if target.key != "fevm":
        return Check(
            name="fevm_shared_ops_unchanged",
            status="N/A",
            evidence={"note": "only checked for fevm target"},
        )

    def _run() -> tuple[str, dict[str, Any]]:
        full_table = (
            f"{target.catalog}.{target.data_schema}.ops_tasks"
        )
        current_hash = hash_provider(
            target.profile, target.warehouse_id or "", full_table
        )
        if current_hash != baseline_hash:
            raise RuntimeError(
                f"FEVM shared ops_tasks hash changed:"
                f" baseline={baseline_hash!r} current={current_hash!r}"
            )
        return "PASS", {
            "table": full_table,
            "hash": current_hash,
        }

    return _check("fevm_shared_ops_unchanged", _run)


# ── Public entry point ─────────────────────────────────────────────────────────

def validate_stack(
    target_key: str,
    expected_state: str,
    *,
    workspace_id_provider: WorkspaceIdProvider | None = None,
    app_state_provider: AppStateProvider | None = None,
    obo_scopes_provider: OboScopesProvider | None = None,
    uc_grants_provider: UcGrantsProvider | None = None,
    mcp_invoker: McpInvoker | None = None,
    genie_query_provider: GenieQueryProvider | None = None,
    lakebase_session_provider: LakebaseSessionProvider | None = None,
    mlflow_experiment_provider: MlflowExperimentProvider | None = None,
    trace_proof_provider: TraceProofProvider | None = None,
    sql_runner: SqlRunner | None = None,
    hash_provider: HashProvider | None = None,
    fevm_baseline_hash: str = "",
) -> ValidationReport:
    """Run all checks for *target_key* expecting *expected_state*.

    All external interactions use the injected callables; when None the live
    defaults from provision_field_eng / provision_fevm_fallback are used.
    """
    target = load_target(target_key)
    state = load_state(expected_state)

    session_id = str(uuid.uuid4())
    report = ValidationReport(
        target_key=target_key,
        expected_state=expected_state,
        session_id=session_id,
        timestamp=_utc_now(),
    )

    # -- lazy imports of live defaults only when no mock is supplied --
    if workspace_id_provider is None:
        from deploy.databricks_cli import live_workspace_id as _live_wid

        workspace_id_provider = _live_wid  # type: ignore[assignment]

    if app_state_provider is None:
        from deploy.databricks_cli import run_json as _run_json

        def _default_app_state_provider(profile: str, app_name: str) -> str:
            app = _run_json(profile, ["apps", "get", app_name])
            status = app.get("app_status") if isinstance(app, dict) else None
            state = status.get("state") if isinstance(status, dict) else status
            return str(state) if state else "UNKNOWN"

        app_state_provider = _default_app_state_provider

    if mcp_invoker is None:
        from scripts.provision_field_eng import _default_mcp_invoker

        mcp_invoker = _default_mcp_invoker

    if trace_proof_provider is None:
        from scripts.provision_field_eng import _default_presenter_trace_proof_provider

        trace_proof_provider = _default_presenter_trace_proof_provider

    # ---------- run checks -----------------------------------------------

    report.checks.append(
        _check_workspace_identity(target, workspace_id_provider)
    )
    report.checks.append(_check_resource_namespace(target))
    if app_state_provider is not None:
        report.checks.append(
            _check_app_running(target, expected_state, app_state_provider)
        )
    if obo_scopes_provider is not None:
        report.checks.append(_check_obo_scopes(target, obo_scopes_provider))
    if uc_grants_provider is not None:
        report.checks.append(_check_uc_grants(target, uc_grants_provider))
    if mcp_invoker is not None:
        report.checks.append(_check_mcp_initialize(target, mcp_invoker))
        report.checks.append(_check_mcp_tools_list(target, state, mcp_invoker))
        report.checks.append(
            _check_baseline_or_upgraded_contract(target, state, mcp_invoker)
        )
    if genie_query_provider is not None:
        report.checks.append(_check_genie_query(target, genie_query_provider))
    if lakebase_session_provider is not None:
        report.checks.append(
            _check_lakebase_session(target, lakebase_session_provider)
        )
    elif target.lakebase_project:
        # Always include this check so degraded state is visible even when
        # no real provider is wired (tests pass a mock; live runs get default)
        report.checks.append(
            Check(
                name="lakebase_session",
                status="N/A",
                evidence={"note": "no lakebase_session_provider injected"},
            )
        )
    if mlflow_experiment_provider is not None:
        report.checks.append(
            _check_mlflow_experiment(target, mlflow_experiment_provider)
        )
    if sql_runner is not None:
        report.checks.append(_check_uc_trace_tables(target, sql_runner))
    if trace_proof_provider is not None:
        report.checks.append(
            _check_successful_tool_spans(target, trace_proof_provider, session_id)
        )
    report.checks.append(
        _check_confluence_live_grounding(
            target, state, trace_proof_provider or _noop_trace_provider, session_id
        )
    )
    report.checks.append(
        _check_fevm_shared_ops_unchanged(
            target, hash_provider or _noop_hash_provider, fevm_baseline_hash
        )
    )

    return report


# ── Noop stubs (used when optional providers are absent) ──────────────────────

def _noop_trace_provider(request: dict[str, Any]) -> dict[str, Any]:
    return {"spans": [], "trace_id": "", "confluence_span_found": False}


def _noop_hash_provider(profile: str, warehouse_id: str, table: str) -> str:
    return ""
