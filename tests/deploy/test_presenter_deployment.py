from __future__ import annotations

import importlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml


PROFILE = "dbc-f7444b38"
HOST = "https://dbc-f7444b38-7453.staging.cloud.databricks.com"
WORKSPACE_ID = "715783009495722"
APP_NAME = "gurary-bobabricks-store-ops"
APP_ID = "11111111-1111-4111-8111-111111111111"
PRINCIPAL = "22222222-2222-4222-8222-222222222222"
WAREHOUSE_ID = "a1b2c3d4e5f60718"
GENIE_ID = "01f123456789abcdef0123456789abcd"
CATALOG = "gurary_" "catalog"
DATA_SCHEMA = "gurary_bobabricks_store_" "ops"
TRACE_SCHEMA = "gurary_ai_gate" "way_demo"
STORETIME_APP = "gurary-bobabricks-store" "time"
OPSTASK_APP = "gurary-bobabricks-ops" "task-mcp"
WORKSPACE_PATH = "/Workspace/Users/adam.gurary@databricks.com/gurary_WT26_AIbreakout"
EXPERIMENT = f"/Users/{PRINCIPAL}/gurary_bobabricks_store_ops_uc"
TRACE_PREFIX = f"{CATALOG}.{TRACE_SCHEMA}.gurary_bobabricks"
PROMPTS = (
    "What tools do you have?",
    "Show average versus actual training hours for my Pacific stores and flag any stores falling behind.",
    "Why is Store 104 behind on training?",
    "What are our FY26 training goals, and how does Store 104 stack up?",
)
BASELINE_IDS = {"OPS-2194", "OPS-2201", "OPS-2208", "OPS-2210"}


def original_state() -> dict:
    return {
        "target": "field_eng",
        "warehouse_id": WAREHOUSE_ID,
        "genie_space_id": GENIE_ID,
        "lakebase": {
            "validated": True,
            "enabled": True,
            "project": "gurary-bobabricks",
            "branch": "production",
            "endpoint": "primary",
            "database": "databricks_postgres",
            "host": "private.database.databricks.com",
            "schema": "gurary_bobabricks_app",
        },
        "storetime_app_id": "33333333-3333-4333-8333-333333333333",
        "storetime_service_principal_client_id": "44444444-4444-4444-8444-444444444444",
        "storetime_mcp_url": (
            f"https://{STORETIME_APP}-{WORKSPACE_ID}.staging.aws.databricksapps.com/mcp"
        ),
        "opstask_app_id": "55555555-5555-4555-8555-555555555555",
        "opstask_service_principal_client_id": "66666666-6666-4666-8666-666666666666",
        "opstask_mcp_url": (
            f"https://{OPSTASK_APP}-{WORKSPACE_ID}.staging.aws.databricksapps.com/mcp"
        ),
    }


def app_record() -> dict:
    return {
        "id": APP_ID,
        "name": APP_NAME,
        "url": f"https://{APP_NAME}-{WORKSPACE_ID}.staging.aws.databricksapps.com",
        "creator": "adam.gurary@databricks.com",
        "service_principal_client_id": PRINCIPAL,
        "service_principal_name": PRINCIPAL,
        "user_api_scopes": ["ai-gateway", "genie", "mcp.external"],
        "effective_user_api_scopes": ["ai-gateway", "genie", "mcp.external"],
        "app_status": {"state": "RUNNING"},
        "compute_status": {"state": "ACTIVE"},
    }


def verified_identity(profile: str, host: str) -> dict:
    if (profile, host) != (PROFILE, HOST):
        raise AssertionError("wrong field-eng binding")
    return {
        "host": host,
        "current_user": {"userName": "adam.gurary@databricks.com"},
    }


class PlanBoundary:
    def __init__(self, app: dict | None = None):
        self.app = app

    def __call__(self, profile: str, args: list[str], payload: dict | None = None):
        if profile != PROFILE or payload is not None:
            raise AssertionError("presenter planning crossed the read-only boundary")
        if args == ["apps", "list"]:
            return {"apps": [{"name": APP_NAME}] if self.app else []}
        if args == ["apps", "get", APP_NAME] and self.app:
            return dict(self.app)
        raise AssertionError(f"unexpected presenter plan request: {args}")


class MutablePresenterBoundary:
    def __init__(self):
        self.events: list[tuple] = []
        self.app: dict | None = None
        self.grants: dict[str, set[str]] = {}
        self.workspace_exists = False
        self.workspace_can_read = False
        self.deployment: dict | None = None

    def verify_profile(self, profile: str, host: str) -> dict:
        self.events.append(("binding", profile, host))
        return verified_identity(profile, host)

    def workspace_id(self, profile: str) -> str:
        self.events.append(("workspace", profile))
        return WORKSPACE_ID

    def __call__(self, profile: str, args: list[str], payload: dict | None = None):
        if profile != PROFILE:
            raise AssertionError(f"wrong profile: {profile}")
        key = tuple(args)
        if key == ("apps", "list"):
            self.events.append(("inspection", "apps"))
            return {"apps": [{"name": APP_NAME}] if self.app else []}
        if key == ("apps", "get", APP_NAME):
            self.events.append(("inspection", "app"))
            if self.app is None:
                raise AssertionError("presenter app read before create")
            return dict(self.app)
        if key == ("apps", "create", APP_NAME):
            self.events.append(("mutation", "create_app", APP_NAME))
            if payload is not None:
                raise AssertionError("installed CLI requires positional app create")
            self.app = app_record()
            self.app["user_api_scopes"] = []
            self.app["effective_user_api_scopes"] = []
            return dict(self.app)
        if key == ("apps", "update", APP_NAME):
            self.events.append(("mutation", "scopes", APP_NAME))
            if payload != {"user_api_scopes": ["ai-gateway", "genie", "mcp.external"]}:
                raise AssertionError("unsafe OBO scope payload")
            self.app["user_api_scopes"] = list(payload["user_api_scopes"])
            self.app["effective_user_api_scopes"] = list(payload["user_api_scopes"])
            return dict(self.app)
        if key == ("catalogs", "get", CATALOG):
            self.events.append(("inspection", "catalog"))
            return {"name": CATALOG, "owner": "adam.gurary@databricks.com"}
        if key == ("schemas", "get", f"{CATALOG}.{TRACE_SCHEMA}"):
            self.events.append(("inspection", "trace_schema"))
            return {
                "full_name": f"{CATALOG}.{TRACE_SCHEMA}",
                "owner": "adam.gurary@databricks.com",
            }
        if key[:2] == ("grants", "get"):
            self.events.append(("inspection", "grants", key[2], key[3]))
            privileges = sorted(self.grants.get(key[3], set()))
            return {
                "privilege_assignments": (
                    [{"principal": PRINCIPAL, "privileges": privileges}]
                    if privileges
                    else []
                )
            }
        if key[:2] == ("grants", "update"):
            change = payload["changes"][0]
            self.events.append(("mutation", "grant", key[2], key[3]))
            if change.get("principal") != PRINCIPAL or "remove" in change:
                raise AssertionError("unsafe presenter grant")
            self.grants.setdefault(key[3], set()).update(change["add"])
            return {}
        if key == ("workspace", "list", "/Workspace/Users/adam.gurary@databricks.com"):
            self.events.append(("inspection", "workspace_parent"))
            return {
                "objects": (
                    [{"path": WORKSPACE_PATH, "object_type": "DIRECTORY", "object_id": "42"}]
                    if self.workspace_exists
                    else []
                )
            }
        if key == ("workspace", "list", WORKSPACE_PATH):
            self.events.append(("inspection", "workspace_source"))
            return {
                "objects": (
                    [{"path": f"{WORKSPACE_PATH}/app.yaml", "object_type": "FILE"}]
                    if self.workspace_exists
                    else []
                )
            }
        if key == ("workspace", "get-status", WORKSPACE_PATH):
            self.events.append(("inspection", "workspace_status"))
            return {"path": WORKSPACE_PATH, "object_type": "DIRECTORY", "object_id": "42"}
        if key == ("workspace", "get-permissions", "directories", "42"):
            self.events.append(("inspection", "workspace_permissions"))
            return {
                "access_control_list": (
                    [{
                        "service_principal_name": PRINCIPAL,
                        "all_permissions": [{"permission_level": "CAN_READ"}],
                    }]
                    if self.workspace_can_read
                    else []
                )
            }
        if key == ("workspace", "update-permissions", "directories", "42"):
            self.events.append(("mutation", "workspace_acl", WORKSPACE_PATH))
            if payload != {
                "access_control_list": [{
                    "service_principal_name": PRINCIPAL,
                    "permission_level": "CAN_READ",
                }]
            }:
                raise AssertionError("unsafe workspace ACL")
            self.workspace_can_read = True
            return {}
        if key == (
            "apps",
            "deploy",
            APP_NAME,
            "--source-code-path",
            WORKSPACE_PATH,
        ):
            self.events.append(("mutation", "deploy", APP_NAME))
            self.deployment = {
                "deployment_id": "deployment-123",
                "source_code_path": WORKSPACE_PATH,
                "status": {"state": "SUCCEEDED"},
            }
            return dict(self.deployment)
        if key == ("apps", "get-deployment", APP_NAME, "deployment-123"):
            self.events.append(("inspection", "deployment"))
            return dict(self.deployment)
        if key[:3] == ("api", "post", "/api/2.0/sql/statements"):
            self.events.append(("inspection", "ops_baseline"))
            statement = payload.get("statement", "")
            expected = f"`{CATALOG}`.`{DATA_SCHEMA}`.`gurary_ops_tasks`"
            if expected not in statement or not statement.lstrip().startswith("SELECT"):
                raise AssertionError("presenter acceptance escaped read-only OpsTask SQL")
            return {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "schema": {"columns": [{"name": "task_id"}]},
                    "total_row_count": 4,
                },
                "result": {"data_array": [[value] for value in sorted(BASELINE_IDS)]},
            }
        raise AssertionError(f"unexpected presenter boundary: {args}, {payload}")

    def sync(self, command: list[str]) -> None:
        self.events.append(("mutation", "sync", WORKSPACE_PATH))
        if command[-3:] != [WORKSPACE_PATH, "--profile", PROFILE]:
            raise AssertionError(f"unsafe sync destination: {command}")
        self.workspace_exists = True


def lakebase_proof(request: dict) -> dict:
    if request["app"]["service_principal_client_id"] != PRINCIPAL:
        raise AssertionError("Lakebase role was not derived from the authoritative app")
    return {
        "role": PRINCIPAL,
        "schema": "gurary_bobabricks_app",
        "schema_privileges": ["USAGE"],
        "table_privileges": {
            "agent_sessions": ["DELETE", "INSERT", "SELECT", "UPDATE"],
            "agent_messages": ["DELETE", "INSERT", "SELECT", "UPDATE"],
        },
        "sequence_privileges": {"agent_messages_id_seq": ["USAGE"]},
        "outside_schema_privileges": [],
    }


def response_text(index: int) -> str:
    if index == 0:
        return "Available tools: Genie, StoreTime, and read-only OpsTask."
    if index == 3:
        return (
            "The official company FY26 training goal is not connected. "
            "Live Store 104 evidence: 81.8% completion; StoreTime shows 42 scheduled, "
            "28 completed, and 14 converted hours. Sources: Genie, StoreTime."
        )
    return "Live Pacific and Store 104 evidence. Sources: Genie, StoreTime."


def app_invoker_factory(observed: list[dict]):
    def invoke(profile: str, app_url: str, payload: dict) -> dict:
        observed.append(payload)
        index = len(observed) - 1
        return {
            "output": [{"role": "assistant", "content": response_text(index)}],
            "custom_outputs": {"session_id": "fresh-session"},
        }

    return invoke


def trace_proof(request: dict) -> dict:
    index = request["turn_index"]
    spans = [{"name": "ResponsesAgent", "status": "OK"}]
    if index > 0:
        spans.extend(
            [
                {"name": "genie_query", "status": "OK"},
                {"name": "inspect_schedule_storetime", "status": "OK"},
            ]
        )
    return {
        "experiment_name": EXPERIMENT,
        "experiment_tags": {
            "mlflow.experiment.databricksTraceDestinationPath": TRACE_PREFIX,
            "mlflow.experiment.databricksTraceSpanStorageTable": f"{TRACE_PREFIX}_otel_spans",
            "mlflow.experiment.databricksTraceLogStorageTable": f"{TRACE_PREFIX}_otel_logs",
            "mlflow.experiment.databricksTraceAnnotationsTable": f"{TRACE_PREFIX}_otel_annotations",
        },
        "trace_id": f"trace-{index}",
        "trace_request_time_ms": request["not_before_ms"] + 1,
        "trace_session_id": "fresh-session",
        "trace_state": "OK",
        "spans": spans,
        "uc_tables": [
            TRACE_PREFIX,
            f"{TRACE_PREFIX}_otel_spans",
            f"{TRACE_PREFIX}_otel_logs",
            f"{TRACE_PREFIX}_otel_annotations",
        ],
        "private_resources": {
            "catalog": CATALOG,
            "genie_space_id": GENIE_ID,
            "storetime_app": STORETIME_APP,
            "opstask_app": OPSTASK_APP,
        },
    }


class PresenterPlanTest(unittest.TestCase):
    def test_plan_pins_exact_presenter_scopes_grants_and_generated_state(self):
        field_eng = importlib.import_module("scripts.provision_field_eng")
        build = getattr(field_eng, "build_presenter_deployment_plan", None)
        self.assertTrue(callable(build), "presenter deployment planning is missing")
        if not callable(build):
            return

        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "field_eng.json"
            state_path.write_text(json.dumps(original_state()), encoding="utf-8")
            plan = build(
                run_cli=PlanBoundary(),
                verify_profile=verified_identity,
                workspace_id_provider=lambda _profile: WORKSPACE_ID,
                state_path=state_path,
            )

        self.assertEqual(plan.app_name, APP_NAME)
        self.assertEqual(plan.workspace_path, WORKSPACE_PATH)
        self.assertEqual(plan.scopes, ("ai-gateway", "genie", "mcp.external"))
        self.assertEqual(
            {(grant.securable_type, grant.full_name): grant.privileges for grant in plan.grants},
            {
                ("catalog", CATALOG): frozenset({"USE_CATALOG"}),
                ("schema", f"{CATALOG}.{TRACE_SCHEMA}"): frozenset(
                    {"USE_SCHEMA", "CREATE_TABLE"}
                ),
            },
        )
        self.assertEqual(plan.generated_state, original_state())


class PresenterApplyTest(unittest.TestCase):
    def test_apply_preserves_state_renders_private_baseline_and_accepts_four_turns(self):
        field_eng = importlib.import_module("scripts.provision_field_eng")
        apply_plan = getattr(field_eng, "apply_presenter_deployment_plan", None)
        self.assertTrue(callable(apply_plan), "presenter deployment reconciliation is missing")
        if not callable(apply_plan):
            return

        boundary = MutablePresenterBoundary()
        observed: list[dict] = []
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = Path(temporary_directory)
            state_path = temporary / "state" / "field_eng.json"
            initial = original_state()
            state_path.parent.mkdir()
            state_path.write_text(json.dumps(initial), encoding="utf-8")
            plan = field_eng.build_presenter_deployment_plan(
                run_cli=boundary,
                verify_profile=boundary.verify_profile,
                workspace_id_provider=boundary.workspace_id,
                state_path=state_path,
            )
            with patch("deploy.render.STATE_ROOT", state_path.parent), patch(
                "deploy.render.BUILD_ROOT", temporary / "build"
            ):
                evidence = apply_plan(
                    plan,
                    run_cli=boundary,
                    verify_profile=boundary.verify_profile,
                    workspace_id_provider=boundary.workspace_id,
                    state_path=state_path,
                    sync_runner=boundary.sync,
                    lakebase_access_reconciler=lakebase_proof,
                    app_invoker=app_invoker_factory(observed),
                    trace_proof_provider=trace_proof,
                    session_id_provider=lambda: "fresh-session",
                    now_ms=iter(range(1000, 1010)).__next__,
                    waiter=lambda _seconds: None,
                )

            saved = json.loads(state_path.read_text(encoding="utf-8"))
            for key, value in initial.items():
                self.assertEqual(saved[key], value)
            self.assertEqual(
                set(saved) - set(initial),
                {
                    "presenter_app_id",
                    "presenter_app_url",
                    "presenter_service_principal_client_id",
                    "mlflow_experiment_name",
                },
            )
            manifest = yaml.safe_load(
                (temporary / "build" / "field_eng" / "baseline" / "app.yaml").read_text(
                    encoding="utf-8"
                )
            )
            environment = {entry["name"]: str(entry["value"]) for entry in manifest["env"]}
            self.assertEqual(environment["DATABRICKS_WAREHOUSE_ID"], WAREHOUSE_ID)
            self.assertEqual(environment["GENIE_SPACE_ID"], GENIE_ID)
            self.assertEqual(environment["STORETIME_MCP_URL"], initial["storetime_mcp_url"])
            self.assertEqual(environment["OPSTASK_MCP_URL"], initial["opstask_mcp_url"])
            self.assertEqual(environment["MLFLOW_EXPERIMENT_NAME"], EXPERIMENT)
            self.assertEqual(environment["MLFLOW_TRACE_UC_CATALOG"], CATALOG)
            self.assertEqual(environment["MLFLOW_TRACE_UC_SCHEMA"], TRACE_SCHEMA)
            self.assertEqual(environment["MLFLOW_TRACE_UC_TABLE_PREFIX"], "gurary_bobabricks")
            self.assertEqual(environment["LAKEBASE_MEMORY_SCHEMA"], "gurary_bobabricks_app")
            self.assertEqual(environment["SHARED_MCP_READ_ONLY"], "false")
            self.assertNotIn("CONFLUENCE_MCP_ENABLED", environment)
            self.assertNotIn("ATLASSIAN_MCP_URL", environment)

        self.assertEqual([item["input"][0]["content"] for item in observed], list(PROMPTS))
        self.assertTrue(all(item["custom_inputs"]["session_id"] == "fresh-session" for item in observed))
        self.assertEqual(evidence.deployment_state, "SUCCEEDED")
        self.assertEqual(evidence.app_state, "RUNNING")
        self.assertEqual(evidence.compute_state, "ACTIVE")
        self.assertEqual(evidence.prompt_count, 4)
        self.assertEqual(evidence.ops_baseline_count, 4)
        self.assertEqual(evidence.trace_table_count, 4)
        self.assertEqual(
            boundary.grants,
            {
                CATALOG: {"USE_CATALOG"},
                f"{CATALOG}.{TRACE_SCHEMA}": {"USE_SCHEMA", "CREATE_TABLE"},
            },
        )

        previous_mutation = -1
        for mutation_index in [
            index for index, event in enumerate(boundary.events) if event[0] == "mutation"
        ]:
            preflight = boundary.events[previous_mutation + 1 : mutation_index]
            self.assertTrue(any(event[0] == "binding" for event in preflight), boundary.events[mutation_index])
            self.assertTrue(any(event[0] == "workspace" for event in preflight), boundary.events[mutation_index])
            self.assertTrue(any(event[0] == "inspection" for event in preflight), boundary.events[mutation_index])
            previous_mutation = mutation_index

    def test_trace_validation_rejects_missing_log_or_annotation_tables(self):
        field_eng = importlib.import_module("scripts.provision_field_eng")
        validate = getattr(field_eng, "_validate_presenter_trace_proof", None)
        self.assertTrue(callable(validate), "four-table trace validation is missing")
        if not callable(validate):
            return
        request = {
            "experiment_name": EXPERIMENT,
            "trace_location": TRACE_PREFIX,
            "session_id": "fresh-session",
            "not_before_ms": 1000,
            "turn_index": 1,
            "generated_state": original_state(),
        }
        incomplete = trace_proof(request)
        incomplete["experiment_tags"].pop(
            "mlflow.experiment.databricksTraceLogStorageTable"
        )
        with self.assertRaisesRegex(RuntimeError, "four exact UC trace tags"):
            validate(request, incomplete)

    def test_lakebase_validation_rejects_access_outside_isolated_schema(self):
        field_eng = importlib.import_module("scripts.provision_field_eng")
        validate = getattr(field_eng, "_validate_lakebase_access_proof", None)
        self.assertTrue(callable(validate), "isolated Lakebase proof validation is missing")
        if not callable(validate):
            return
        proof = lakebase_proof({"app": app_record()})
        proof["outside_schema_privileges"] = ["public.some_table:SELECT"]
        with self.assertRaisesRegex(RuntimeError, "outside"):
            validate(PRINCIPAL, proof)


if __name__ == "__main__":
    unittest.main()


class DefaultProvidersUnitTest(unittest.TestCase):
    """Unit tests for the three new default providers (all mocked -- no live calls)."""

    def setUp(self):
        self.field_eng = importlib.import_module("scripts.provision_field_eng")

    # ── app invoker ──────────────────────────────────────────────────────────

    def test_default_app_invoker_returns_json_on_200(self):
        invoker = getattr(self.field_eng, "_default_presenter_app_invoker", None)
        self.assertIsNotNone(invoker, "_default_presenter_app_invoker is missing")
        import unittest.mock as mock
        fake_response = mock.MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = {"output": [{"role": "assistant", "content": "hi"}]}
        fake_client = mock.MagicMock()
        fake_client.__enter__ = mock.MagicMock(return_value=fake_client)
        fake_client.__exit__ = mock.MagicMock(return_value=False)
        fake_client.post.return_value = fake_response
        with mock.patch("httpx.Client", return_value=fake_client), \
             mock.patch("databricks.sdk.WorkspaceClient") as mock_wc:
            mock_wc.return_value.config.authenticate.return_value = {"Authorization": "Bearer t"}
            result = invoker("dbc-f7444b38", "https://app.example.com", {"input": []})
        self.assertEqual(result["output"][0]["content"], "hi")

    def test_default_app_invoker_raises_transient_on_502(self):
        invoker = getattr(self.field_eng, "_default_presenter_app_invoker", None)
        self.assertIsNotNone(invoker)
        TransientError = getattr(self.field_eng, "TransientPresenterInvocationError", None)
        self.assertIsNotNone(TransientError, "TransientPresenterInvocationError missing")
        import unittest.mock as mock
        fake_response = mock.MagicMock()
        fake_response.status_code = 502
        fake_client = mock.MagicMock()
        fake_client.__enter__ = mock.MagicMock(return_value=fake_client)
        fake_client.__exit__ = mock.MagicMock(return_value=False)
        fake_client.post.return_value = fake_response
        with mock.patch("httpx.Client", return_value=fake_client), \
             mock.patch("databricks.sdk.WorkspaceClient") as mock_wc:
            mock_wc.return_value.config.authenticate.return_value = {}
            with self.assertRaises(TransientError):
                invoker("dbc-f7444b38", "https://app.example.com", {})

    # ── trace proof provider ─────────────────────────────────────────────────

    def test_default_trace_proof_provider_returns_four_tags(self):
        provider = getattr(self.field_eng, "_default_presenter_trace_proof_provider", None)
        self.assertIsNotNone(provider, "_default_presenter_trace_proof_provider is missing")
        import unittest.mock as mock
        not_before = 1000
        exp_tags = {
            "mlflow.experiment.databricksTraceDestinationPath": TRACE_PREFIX,
            "mlflow.experiment.databricksTraceSpanStorageTable": f"{TRACE_PREFIX}_otel_spans",
            "mlflow.experiment.databricksTraceLogStorageTable": f"{TRACE_PREFIX}_otel_logs",
            "mlflow.experiment.databricksTraceAnnotationsTable": f"{TRACE_PREFIX}_otel_annotations",
        }
        fake_experiment = mock.MagicMock()
        fake_experiment.name = EXPERIMENT
        fake_experiment.tags = exp_tags
        fake_trace_info = mock.MagicMock()
        fake_trace_info.trace_id = "t-001"
        fake_trace_info.request_time = not_before + 5
        fake_trace_info.trace_metadata = {"mlflow.trace.session": "ses-123"}
        fake_trace_info.state = "OK"
        fake_candidate = mock.MagicMock()
        fake_candidate.info = fake_trace_info
        fake_span = mock.MagicMock()
        fake_span.name = "RootSpan"
        fake_span.status = "OK"
        fake_trace_obj = mock.MagicMock()
        fake_trace_obj.data.spans = [fake_span]
        mock_client = mock.MagicMock()
        mock_client.get_experiment_by_name.return_value = fake_experiment
        mock_client.search_traces.return_value = [fake_candidate]
        mock_client.get_trace.return_value = fake_trace_obj
        request = {
            "experiment_name": EXPERIMENT,
            "trace_location": TRACE_PREFIX,
            "session_id": "ses-123",
            "not_before_ms": not_before,
            "turn_index": 0,
            "generated_state": {"warehouse_id": WAREHOUSE_ID, "target": "field_eng"},
        }
        with mock.patch("mlflow.MlflowClient", return_value=mock_client):
            proof = provider(request)
        self.assertEqual(proof["experiment_name"], EXPERIMENT)
        self.assertIn("mlflow.experiment.databricksTraceLogStorageTable", proof["experiment_tags"])
        self.assertIn("mlflow.experiment.databricksTraceAnnotationsTable", proof["experiment_tags"])
        self.assertEqual(len(proof["uc_tables"]), 4)
        self.assertEqual(proof["trace_id"], "t-001")

    def test_default_trace_proof_provider_raises_when_no_matching_trace(self):
        provider = getattr(self.field_eng, "_default_presenter_trace_proof_provider", None)
        self.assertIsNotNone(provider)
        import unittest.mock as mock
        request = {
            "experiment_name": EXPERIMENT,
            "trace_location": TRACE_PREFIX,
            "session_id": "ses-999",
            "not_before_ms": 1000,
            "turn_index": 0,
            "generated_state": {},
        }
        mock_client = mock.MagicMock()
        mock_client.get_experiment_by_name.return_value = None  # never found
        with mock.patch("mlflow.MlflowClient", return_value=mock_client), \
             mock.patch("time.monotonic", side_effect=[0.0, 200.0]):
            with mock.patch("time.sleep"):
                with self.assertRaisesRegex(RuntimeError, "deadline"):
                    provider(request)

    # ── lakebase access reconciler ───────────────────────────────────────────

    def test_default_lakebase_reconciler_returns_correct_proof_shape(self):
        reconciler = getattr(self.field_eng, "_default_lakebase_access_reconciler", None)
        self.assertIsNotNone(reconciler, "_default_lakebase_access_reconciler is missing")
        import unittest.mock as mock
        mock_cursor = mock.MagicMock()
        mock_cursor.__enter__ = mock.MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = mock.MagicMock(return_value=False)
        # pg_roles check returns a row (role exists)
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.fetchall.return_value = []  # no outside privileges
        mock_conn = mock.MagicMock()
        mock_conn.__enter__ = mock.MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = mock.MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_endpoint = mock.MagicMock()
        mock_endpoint.status.hosts.host = "primary.database.databricks.com"
        mock_cred = mock.MagicMock()
        mock_cred.token = "short-lived-token"
        mock_ws = mock.MagicMock()
        mock_ws.postgres.generate_database_credential.return_value = mock_cred
        with mock.patch("scripts.provision_field_eng._find_endpoint", return_value=mock_endpoint), \
             mock.patch("databricks.sdk.WorkspaceClient", return_value=mock_ws), \
             mock.patch("scripts.provision_field_eng.assert_profile", return_value={
                 "host": "https://dbc-f7444b38-7453.staging.cloud.databricks.com",
                 "current_user": {"userName": "adam.gurary@databricks.com"},
             }), \
             mock.patch("psycopg.connect", return_value=mock_conn):
            proof = reconciler({"app": app_record()})
        self.assertEqual(proof["schema"], "gurary_bobabricks_app")
        self.assertEqual(proof["principal"], PRINCIPAL)
        self.assertEqual(proof["outside_schema_privileges"], [])

    def test_default_lakebase_reconciler_raises_when_endpoint_missing(self):
        reconciler = getattr(self.field_eng, "_default_lakebase_access_reconciler", None)
        self.assertIsNotNone(reconciler)
        import unittest.mock as mock
        with mock.patch("scripts.provision_field_eng._find_endpoint", return_value=None), \
             mock.patch("databricks.sdk.WorkspaceClient"):
            with self.assertRaisesRegex(RuntimeError, "endpoint not found"):
                reconciler({"app": app_record()})

    def test_apply_presenter_defaults_are_real_functions_not_none(self):
        """Confirm the three providers have non-None defaults on the live function."""
        import inspect
        apply_fn = getattr(self.field_eng, "apply_presenter_deployment_plan", None)
        self.assertIsNotNone(apply_fn)
        sig = inspect.signature(apply_fn)
        for param_name in ("app_invoker", "trace_proof_provider", "lakebase_access_reconciler"):
            default = sig.parameters[param_name].default
            self.assertIsNotNone(default, f"{param_name} default must not be None")
            self.assertTrue(callable(default), f"{param_name} default must be callable")
