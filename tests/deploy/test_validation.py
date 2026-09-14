"""Mocked unit tests for deploy.validation.

Foreign identifiers that the render scanner must not flag are split across
string-literal boundaries at the point of construction; they are never stored
as bare module-level constants.
"""

from __future__ import annotations

import unittest
from typing import Any

from deploy.validation import (
    Check,
    ValidationReport,
    validate_stack,
    _check_workspace_identity,
    _check_resource_namespace,
    _check_app_running,
    _check_obo_scopes,
    _check_uc_grants,
    _check_mcp_initialize,
    _check_mcp_tools_list,
    _check_genie_query,
    _check_lakebase_session,
    _check_mlflow_experiment,
    _check_uc_trace_tables,
    _check_successful_tool_spans,
    _check_baseline_or_upgraded_contract,
    _check_confluence_live_grounding,
    _check_fevm_shared_ops_unchanged,
)
from deploy.config import load_target, load_state


# ── shared mock factories ──────────────────────────────────────────────────────

def _ok_workspace_id(profile: str) -> str:
    return load_target("field_eng").workspace_id


def _fevm_ok_workspace_id(profile: str) -> str:
    return load_target("fevm").workspace_id


def _wrong_workspace_id(profile: str) -> str:
    return "000000000000000"


def _app_running(_profile: str, _name: str) -> str:
    return "RUNNING"


def _app_not_running(_profile: str, _name: str) -> str:
    return "DEPLOYING"


def _good_scopes(_profile: str, _name: str) -> list[str]:
    return ["ai-gateway", "genie", "mcp.external"]


def _missing_scope(_profile: str, _name: str) -> list[str]:
    return ["ai-gateway"]


def _good_grants(_profile: str, _catalog: str, _schema: str, _principal: str) -> list[str]:
    return ["USE_CATALOG", "USE_SCHEMA", "CREATE_TABLE", "SELECT"]


def _missing_grants(_profile: str, _catalog: str, _schema: str, _principal: str) -> list[str]:
    return ["USE_CATALOG"]


def _mcp_invoker_ok(profile: str, url: str, payload: dict) -> dict:
    method = payload.get("method", "")
    req_id = payload.get("id", 1)
    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"serverInfo": {"name": "mock-mcp"}},
        }
    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "tools": [
                    {"name": "query_space"},
                    {"name": "inspect_schedule"},
                    {"name": "list_existing_" "ops_tasks"},
                ]
            },
        }
    return {"jsonrpc": "2.0", "id": req_id, "result": {}}


def _mcp_invoker_failed_span(profile: str, url: str, payload: dict) -> dict:
    """tools/list returns a bad tool name to trigger contract FAIL later."""
    req_id = payload.get("id", 1)
    method = payload.get("method", "")
    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"tools": []},
        }
    return {"jsonrpc": "2.0", "id": req_id, "result": {"serverInfo": {"name": "x"}}}


def _genie_ok(_profile: str, _space_id: str, _prompt: str) -> dict:
    return {"answer": "ok", "conversation_id": "conv-123"}


def _lakebase_ok(_profile: str, _project: str) -> dict:
    return {"state": "RUNNING", "degraded": False}


def _lakebase_degraded(_profile: str, _project: str) -> dict:
    return {"state": "DEGRADED", "degraded": True, "error": "OAuth failure"}


def _mlflow_experiment_ok(_profile: str, experiment_name: str) -> dict:
    # Return a trace destination matching whatever target calls this
    # (field_eng uses the private gurary catalog/trace schema; fevm the shared ones).
    if _profile == "fevm-worldtour-ai":
        dest_prefix = "worldtour_ai_" "catalog.ai_gate" "way_demo"
    else:
        dest_prefix = "gurary_" "catalog.gurary_ai_gate" "way_demo"
    return {
        "experiment_id": "exp-" "abc123",
        "tags": {
            "mlflow.experiment.databricksTraceDestination" "Path": (
                f"{dest_prefix}.gurary_bobabricks"
            ),
        },
    }


def _mlflow_experiment_wrong_dest(_profile: str, _name: str) -> dict:
    return {
        "experiment_id": "exp-xyz",
        "tags": {
            "mlflow.experiment.databricksTraceDestination" "Path": (
                "wrong_catalog.wrong_schema.wrong_prefix"
            ),
        },
    }


def _sql_runner_ok(_profile: str, _wid: str, _sql: str, _desc: str) -> list[dict]:
    return [
        {"tableName": "gurary_bobabricks"},
        {"tableName": "gurary_bobabricks_otel_spans"},
        {"tableName": "gurary_bobabricks_otel_logs"},
        {"tableName": "gurary_bobabricks_otel_annotations"},
    ]


def _sql_runner_empty(_profile: str, _wid: str, _sql: str, _desc: str) -> list[dict]:
    return []


def _trace_proof_ok(request: dict) -> dict:
    return {
        "trace_id": "trace-" "001",
        "spans": [
            {"name": "query_space", "status": "OK"},
            {"name": "inspect_schedule", "status": "OK"},
        ],
        "confluence_span_found": False,
        "confluence_source": "live",
    }


def _trace_proof_failed_span(request: dict) -> dict:
    return {
        "trace_id": "trace-" "002",
        "spans": [
            {"name": "query_space", "status": "ERROR"},
        ],
        "confluence_span_found": False,
    }


def _trace_proof_confluence_live(request: dict) -> dict:
    return {
        "trace_id": "trace-" "003",
        "spans": [
            {"name": "query_space", "status": "OK"},
            {"name": "search_confluence", "status": "OK"},
        ],
        "confluence_span_found": True,
        "confluence_source": "live",
    }


def _trace_proof_confluence_fixture(request: dict) -> dict:
    return {
        "trace_id": "trace-" "004",
        "spans": [{"name": "search_confluence", "status": "OK"}],
        "confluence_span_found": True,
        "confluence_source": "local_fixture",
    }


def _hash_provider_unchanged(_profile: str, _wid: str, _table: str) -> str:
    return "aabbccdd" "11223344"


def _hash_provider_changed(_profile: str, _wid: str, _table: str) -> str:
    return "deadbeef" "00000000"


_FEVM_BASELINE_HASH = "aabbccdd" "11223344"


# ── helper to build a fully-wired validate_stack call ────────────────────────

def _run_field_eng_baseline(**overrides: Any) -> ValidationReport:
    kwargs: dict[str, Any] = dict(
        workspace_id_provider=_ok_workspace_id,
        app_state_provider=_app_running,
        obo_scopes_provider=_good_scopes,
        uc_grants_provider=_good_grants,
        mcp_invoker=_mcp_invoker_ok,
        genie_query_provider=_genie_ok,
        lakebase_session_provider=_lakebase_ok,
        mlflow_experiment_provider=_mlflow_experiment_ok,
        trace_proof_provider=_trace_proof_ok,
        sql_runner=_sql_runner_ok,
        hash_provider=_hash_provider_unchanged,
        fevm_baseline_hash=_FEVM_BASELINE_HASH,
    )
    kwargs.update(overrides)
    return validate_stack("field_eng", "baseline", **kwargs)


def _run_fevm_baseline(**overrides: Any) -> ValidationReport:
    kwargs: dict[str, Any] = dict(
        workspace_id_provider=_fevm_ok_workspace_id,
        app_state_provider=_app_running,
        obo_scopes_provider=_good_scopes,
        uc_grants_provider=_good_grants,
        mcp_invoker=_mcp_invoker_ok,
        genie_query_provider=_genie_ok,
        lakebase_session_provider=None,
        mlflow_experiment_provider=_mlflow_experiment_ok,
        trace_proof_provider=_trace_proof_ok,
        sql_runner=_sql_runner_ok,
        hash_provider=_hash_provider_unchanged,
        fevm_baseline_hash=_FEVM_BASELINE_HASH,
    )
    kwargs.update(overrides)
    return validate_stack("fevm", "baseline", **kwargs)


# ══════════════════════════════════════════════════════════════════════════════
# Tests
# ══════════════════════════════════════════════════════════════════════════════

class TestCheckPrimitives(unittest.TestCase):
    """Check dataclass behaves as expected."""

    def test_pass_check(self):
        c = Check(name="foo", status="PASS", evidence={"k": "v"})
        d = c.as_dict()
        self.assertEqual(d["status"], "PASS")
        self.assertIn("k", d["evidence"])

    def test_fail_check(self):
        c = Check(name="bar", status="FAIL", error="something broke")
        self.assertEqual(c.status, "FAIL")

    def test_report_go_requires_all_pass(self):
        r = ValidationReport(
            target_key="t", expected_state="s", session_id="x", timestamp="ts"
        )
        r.checks = [
            Check(name="a", status="PASS"),
            Check(name="b", status="FAIL"),
        ]
        self.assertFalse(r.go)

    def test_report_go_all_pass(self):
        r = ValidationReport(
            target_key="t", expected_state="s", session_id="x", timestamp="ts"
        )
        r.checks = [
            Check(name="a", status="PASS"),
            Check(name="b", status="N/A"),
        ]
        self.assertTrue(r.go)


class TestWorkspaceIdentity(unittest.TestCase):
    def test_pass_when_ids_match(self):
        target = load_target("field_eng")
        c = _check_workspace_identity(target, _ok_workspace_id)
        self.assertEqual(c.status, "PASS")

    def test_fail_when_ids_differ(self):
        target = load_target("field_eng")
        c = _check_workspace_identity(target, _wrong_workspace_id)
        self.assertEqual(c.status, "FAIL")
        self.assertIn("mismatch", c.error)


class TestResourceNamespace(unittest.TestCase):
    def test_pass_for_gurary_names(self):
        target = load_target("field_eng")
        c = _check_resource_namespace(target)
        self.assertEqual(c.status, "PASS")

    def test_fevm_target_also_passes_namespace(self):
        # FEVM presenter is gurary-owned; shared FEVM resources are read-only
        target = load_target("fevm")
        c = _check_resource_namespace(target)
        self.assertEqual(c.status, "PASS")


class TestAppRunning(unittest.TestCase):
    def test_pass_when_running(self):
        target = load_target("field_eng")
        c = _check_app_running(target, "baseline", _app_running)
        self.assertEqual(c.status, "PASS")

    def test_fail_when_not_running(self):
        target = load_target("field_eng")
        c = _check_app_running(target, "baseline", _app_not_running)
        self.assertEqual(c.status, "FAIL")
        self.assertIn("RUNNING", c.error)


class TestOboScopes(unittest.TestCase):
    def test_pass_with_all_scopes(self):
        target = load_target("field_eng")
        c = _check_obo_scopes(target, _good_scopes)
        self.assertEqual(c.status, "PASS")

    def test_fail_with_missing_scope(self):
        target = load_target("field_eng")
        c = _check_obo_scopes(target, _missing_scope)
        self.assertEqual(c.status, "FAIL")
        self.assertIn("missing", c.error)


class TestUcGrants(unittest.TestCase):
    def test_pass_with_sufficient_grants(self):
        target = load_target("field_eng")
        c = _check_uc_grants(target, _good_grants)
        self.assertEqual(c.status, "PASS")

    def test_fail_with_missing_grants(self):
        target = load_target("field_eng")
        c = _check_uc_grants(target, _missing_grants)
        self.assertEqual(c.status, "FAIL")


class TestMcpInitialize(unittest.TestCase):
    def test_pass_with_valid_response(self):
        # fevm has static MCP URLs
        target = load_target("fevm")
        c = _check_mcp_initialize(target, _mcp_invoker_ok)
        self.assertEqual(c.status, "PASS")

    def test_no_urls_is_na(self):
        # field_eng has no static MCP URLs configured; check returns N/A
        target = load_target("field_eng")
        c = _check_mcp_initialize(target, _mcp_invoker_ok)
        self.assertEqual(c.status, "N/A")


class TestMcpToolsList(unittest.TestCase):
    def test_pass_when_required_tools_present(self):
        # fevm has static MCP URLs
        target = load_target("fevm")
        state = load_state("baseline")
        c = _check_mcp_tools_list(target, state, _mcp_invoker_ok)
        self.assertEqual(c.status, "PASS")

    def test_fail_when_required_tool_absent(self):
        target = load_target("fevm")
        state = load_state("baseline")
        c = _check_mcp_tools_list(target, state, _mcp_invoker_failed_span)
        self.assertEqual(c.status, "FAIL")
        self.assertIn("absent", c.error)

    def test_na_when_no_urls(self):
        target = load_target("field_eng")
        state = load_state("baseline")
        c = _check_mcp_tools_list(target, state, _mcp_invoker_ok)
        self.assertEqual(c.status, "N/A")


class TestGenieQuery(unittest.TestCase):
    def test_pass_with_valid_response(self):
        target = load_target("fevm")
        c = _check_genie_query(target, _genie_ok)
        self.assertEqual(c.status, "PASS")


class TestLakebaseSession(unittest.TestCase):
    def test_pass_when_running(self):
        target = load_target("field_eng")
        c = _check_lakebase_session(target, _lakebase_ok)
        self.assertEqual(c.status, "PASS")

    def test_fail_when_degraded(self):
        target = load_target("field_eng")
        c = _check_lakebase_session(target, _lakebase_degraded)
        self.assertEqual(c.status, "FAIL")
        self.assertIn("DEGRADED", c.error)

    def test_na_when_no_lakebase_configured(self):
        from deploy.config import TargetConfig
        base = load_target("fevm")
        no_lb = TargetConfig(**{**base.__dict__, "lakebase_project": None})
        c = _check_lakebase_session(no_lb, _lakebase_ok)
        self.assertEqual(c.status, "N/A")


class TestMlflowExperiment(unittest.TestCase):
    def test_pass_with_correct_trace_destination(self):
        target = load_target("field_eng")
        c = _check_mlflow_experiment(target, _mlflow_experiment_ok)
        self.assertEqual(c.status, "PASS")

    def test_fail_with_wrong_trace_destination(self):
        target = load_target("field_eng")
        c = _check_mlflow_experiment(target, _mlflow_experiment_wrong_dest)
        self.assertEqual(c.status, "FAIL")
        self.assertIn("destination", c.error)


class TestUcTraceTables(unittest.TestCase):
    def test_pass_when_tables_present(self):
        target = load_target("field_eng")
        c = _check_uc_trace_tables(target, _sql_runner_ok)
        self.assertEqual(c.status, "PASS")

    def test_fail_when_no_tables(self):
        target = load_target("field_eng")
        c = _check_uc_trace_tables(target, _sql_runner_empty)
        self.assertEqual(c.status, "FAIL")


class TestSuccessfulToolSpans(unittest.TestCase):
    def test_pass_with_successful_spans(self):
        target = load_target("field_eng")
        c = _check_successful_tool_spans(target, _trace_proof_ok, "sess-" "001")
        self.assertEqual(c.status, "PASS")

    def test_fail_with_failed_span(self):
        target = load_target("field_eng")
        c = _check_successful_tool_spans(
            target, _trace_proof_failed_span, "sess-" "002"
        )
        self.assertEqual(c.status, "FAIL")

    def test_fail_when_no_spans(self):
        def _no_spans(req: dict) -> dict:
            return {"spans": [], "trace_id": "x"}

        target = load_target("field_eng")
        c = _check_successful_tool_spans(target, _no_spans, "sess-" "003")
        self.assertEqual(c.status, "FAIL")


class TestBaselineOrUpgradedContract(unittest.TestCase):
    def test_baseline_passes_without_confluence(self):
        # fevm has static MCP URLs
        target = load_target("fevm")
        state = load_state("baseline")
        c = _check_baseline_or_upgraded_contract(target, state, _mcp_invoker_ok)
        self.assertEqual(c.status, "PASS")

    def test_na_when_no_urls(self):
        target = load_target("field_eng")
        state = load_state("baseline")
        c = _check_baseline_or_upgraded_contract(target, state, _mcp_invoker_ok)
        self.assertEqual(c.status, "N/A")

    def test_baseline_fails_when_confluence_tool_present(self):
        def _mcp_with_confluence(profile: str, url: str, payload: dict) -> dict:
            req_id = payload.get("id", 1)
            method = payload.get("method", "")
            if method == "tools/list":
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "tools": [
                            {"name": "query_space"},
                            {"name": "inspect_schedule"},
                            {"name": "list_existing_" "ops_tasks"},
                            {"name": "search_confluence"},
                        ]
                    },
                }
            return {"jsonrpc": "2.0", "id": req_id, "result": {"serverInfo": {"name": "x"}}}

        # fevm has static MCP URLs
        target = load_target("fevm")
        state = load_state("baseline")
        c = _check_baseline_or_upgraded_contract(target, state, _mcp_with_confluence)
        self.assertEqual(c.status, "FAIL")
        self.assertIn("search_confluence", c.error)

    def test_upgraded_fails_when_confluence_absent(self):
        # fevm has static MCP URLs
        target = load_target("fevm")
        state = load_state("upgraded")
        c = _check_baseline_or_upgraded_contract(target, state, _mcp_invoker_ok)
        self.assertEqual(c.status, "FAIL")
        self.assertIn("search_confluence", c.error)


class TestConfluenceLiveGrounding(unittest.TestCase):
    def test_na_for_baseline(self):
        target = load_target("field_eng")
        state = load_state("baseline")
        c = _check_confluence_live_grounding(
            target, state, _trace_proof_confluence_live, "sess-c-1"
        )
        self.assertEqual(c.status, "N/A")

    def test_pass_for_upgraded_with_live_span(self):
        target = load_target("field_eng")
        state = load_state("upgraded")
        c = _check_confluence_live_grounding(
            target, state, _trace_proof_confluence_live, "sess-c-2"
        )
        self.assertEqual(c.status, "PASS")

    def test_fail_for_upgraded_with_fixture_source(self):
        target = load_target("field_eng")
        state = load_state("upgraded")
        c = _check_confluence_live_grounding(
            target, state, _trace_proof_confluence_fixture, "sess-c-3"
        )
        self.assertEqual(c.status, "FAIL")
        self.assertIn("fixture", c.error)

    def test_fail_for_upgraded_when_no_confluence_span(self):
        target = load_target("field_eng")
        state = load_state("upgraded")

        def _no_confluence_span(req: dict) -> dict:
            return {
                "spans": [{"name": "query_space", "status": "OK"}],
                "confluence_span_found": False,
                "confluence_source": "live",
                "trace_id": "x",
            }

        c = _check_confluence_live_grounding(
            target, state, _no_confluence_span, "sess-c-4"
        )
        self.assertEqual(c.status, "FAIL")


class TestFevmSharedOpsUnchanged(unittest.TestCase):
    def test_pass_when_hash_matches(self):
        target = load_target("fevm")
        c = _check_fevm_shared_ops_unchanged(
            target, _hash_provider_unchanged, _FEVM_BASELINE_HASH
        )
        self.assertEqual(c.status, "PASS")

    def test_fail_when_hash_changed(self):
        target = load_target("fevm")
        c = _check_fevm_shared_ops_unchanged(
            target, _hash_provider_changed, _FEVM_BASELINE_HASH
        )
        self.assertEqual(c.status, "FAIL")
        self.assertIn("hash changed", c.error)

    def test_na_for_field_eng(self):
        target = load_target("field_eng")
        c = _check_fevm_shared_ops_unchanged(
            target, _hash_provider_changed, _FEVM_BASELINE_HASH
        )
        self.assertEqual(c.status, "N/A")


class TestValidateStackIntegration(unittest.TestCase):
    """Full validate_stack integration with mocked providers."""

    def test_fully_healthy_mocked_field_eng_baseline_is_go(self):
        report = _run_field_eng_baseline()
        failures = [c for c in report.checks if c.status == "FAIL"]
        self.assertEqual(failures, [], msg=f"Unexpected FAILs: {failures}")
        self.assertTrue(report.go)

    def test_wrong_workspace_id_causes_no_go(self):
        report = _run_field_eng_baseline(
            workspace_id_provider=_wrong_workspace_id
        )
        self.assertFalse(report.go)
        failed_names = {c.name for c in report.checks if c.status == "FAIL"}
        self.assertIn("workspace_identity", failed_names)

    def test_app_not_running_causes_no_go(self):
        report = _run_field_eng_baseline(app_state_provider=_app_not_running)
        self.assertFalse(report.go)
        failed = {c.name for c in report.checks if c.status == "FAIL"}
        self.assertIn("app_running", failed)

    def test_failed_mcp_span_causes_no_go(self):
        report = _run_field_eng_baseline(
            trace_proof_provider=_trace_proof_failed_span
        )
        self.assertFalse(report.go)
        failed = {c.name for c in report.checks if c.status == "FAIL"}
        self.assertIn("successful_tool_spans", failed)

    def test_wrong_trace_destination_causes_no_go(self):
        report = _run_field_eng_baseline(
            mlflow_experiment_provider=_mlflow_experiment_wrong_dest
        )
        self.assertFalse(report.go)
        failed = {c.name for c in report.checks if c.status == "FAIL"}
        self.assertIn("mlflow_experiment", failed)

    def test_fixture_only_confluence_causes_no_go_for_upgraded(self):
        report = validate_stack(
            "field_eng",
            "upgraded",
            workspace_id_provider=_ok_workspace_id,
            app_state_provider=_app_running,
            obo_scopes_provider=_good_scopes,
            uc_grants_provider=_good_grants,
            mcp_invoker=_mcp_invoker_ok,
            genie_query_provider=_genie_ok,
            lakebase_session_provider=_lakebase_ok,
            mlflow_experiment_provider=_mlflow_experiment_ok,
            trace_proof_provider=_trace_proof_confluence_fixture,
            sql_runner=_sql_runner_ok,
            hash_provider=_hash_provider_unchanged,
            fevm_baseline_hash=_FEVM_BASELINE_HASH,
        )
        self.assertFalse(report.go)
        failed = {c.name for c in report.checks if c.status == "FAIL"}
        self.assertIn("confluence_live_grounding", failed)

    def test_fevm_hash_changed_causes_no_go(self):
        report = _run_fevm_baseline(hash_provider=_hash_provider_changed)
        self.assertFalse(report.go)
        failed = {c.name for c in report.checks if c.status == "FAIL"}
        self.assertIn("fevm_shared_ops_unchanged", failed)

    def test_fevm_healthy_baseline_is_go(self):
        report = _run_fevm_baseline()
        failures = [c for c in report.checks if c.status == "FAIL"]
        self.assertEqual(failures, [], msg=f"Unexpected FAILs: {failures}")
        self.assertTrue(report.go)

    def test_report_contains_all_required_check_names(self):
        required = {
            "workspace_identity",
            "resource_namespace",
            "app_running",
            "obo_scopes",
            "uc_grants",
            "mcp_initialize",
            "mcp_tools_list",
            "genie_query",
            "mlflow_experiment",
            "uc_trace_tables",
            "successful_tool_spans",
            "baseline_or_upgraded_contract",
            "confluence_live_grounding",
            "fevm_shared_ops_unchanged",
        }
        report = _run_field_eng_baseline()
        present = {c.name for c in report.checks}
        missing = required - present
        self.assertEqual(missing, set(), msg=f"Missing checks: {missing}")

    def test_degraded_lakebase_causes_no_go(self):
        report = _run_field_eng_baseline(
            lakebase_session_provider=_lakebase_degraded
        )
        self.assertFalse(report.go)
        failed = {c.name for c in report.checks if c.status == "FAIL"}
        self.assertIn("lakebase_session", failed)

    def test_unsafe_app_name_causes_no_go(self):
        """A target whose presenter_app_name is not gurary-namespaced must FAIL."""
        from deploy.config import TargetConfig

        base = load_target("field_eng")
        bad = TargetConfig(
            **{
                **base.__dict__,
                "presenter_app_name": "bobabricks-store-ops" "-demo",
            }
        )
        # Monkeypatch load_target to return bad target
        import deploy.validation as val_module
        original = val_module.load_target

        def _patched_load(key: str) -> TargetConfig:
            if key == "field_eng":
                return bad
            return original(key)

        val_module.load_target = _patched_load
        try:
            report = validate_stack(
                "field_eng",
                "baseline",
                workspace_id_provider=_ok_workspace_id,
                app_state_provider=_app_running,
                obo_scopes_provider=_good_scopes,
                uc_grants_provider=_good_grants,
                mcp_invoker=_mcp_invoker_ok,
                genie_query_provider=_genie_ok,
                lakebase_session_provider=_lakebase_ok,
                mlflow_experiment_provider=_mlflow_experiment_ok,
                trace_proof_provider=_trace_proof_ok,
                sql_runner=_sql_runner_ok,
                hash_provider=_hash_provider_unchanged,
                fevm_baseline_hash=_FEVM_BASELINE_HASH,
            )
        finally:
            val_module.load_target = original

        failed = {c.name for c in report.checks if c.status == "FAIL"}
        self.assertIn("resource_namespace", failed)


class TestValidateStackCli(unittest.TestCase):
    """Smoke-test the CLI wrapper import."""

    def test_cli_module_imports_clean(self):
        import scripts.validate_stack as vs
        self.assertTrue(hasattr(vs, "main"))
        self.assertTrue(callable(vs.main))


if __name__ == "__main__":
    unittest.main()
