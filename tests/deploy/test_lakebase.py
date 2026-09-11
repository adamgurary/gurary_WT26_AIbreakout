from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from databricks.sdk.service.postgres import (
    Branch,
    BranchSpec,
    BranchStatus,
    Database,
    DatabaseCredential,
    DatabaseDatabaseStatus,
    Endpoint,
    EndpointHosts,
    EndpointSpec,
    EndpointStatus,
    EndpointType,
    FieldMask,
    Project,
    ProjectSpec,
    ProjectStatus,
    Timestamp,
)

from deploy.config import load_target
from deploy.lakebase import LakebaseState, ensure_lakebase
from scripts.provision_field_eng import reconcile_lakebase_state


PROJECT_ID = "gurary-bobabricks"
PROJECT_NAME = f"projects/{PROJECT_ID}"
BRANCH_ID = "production"
BRANCH_NAME = f"{PROJECT_NAME}/branches/{BRANCH_ID}"
ENDPOINT_ID = "primary"
ENDPOINT_NAME = f"{BRANCH_NAME}/endpoints/{ENDPOINT_ID}"
DATABASE_ID = "databricks_postgres"
DATABASE_RESOURCE_ID = "databricks-postgres"
DATABASE_NAME = f"{BRANCH_NAME}/databases/{DATABASE_RESOURCE_ID}"
SCHEMA = "gurary_bobabricks_app"
HOST = "owned-lakebase.database.databricks.com"


def project() -> Project:
    return Project(
        name=PROJECT_NAME,
        project_id=PROJECT_ID,
        spec=ProjectSpec(display_name=PROJECT_ID, pg_version=16),
        status=ProjectStatus(
            display_name=PROJECT_ID,
            pg_version=16,
            project_id=PROJECT_ID,
            owner="adam.gurary@databricks.com",
        ),
    )


def branch() -> Branch:
    return Branch(
        name=BRANCH_NAME,
        branch_id=BRANCH_ID,
        parent=PROJECT_NAME,
        spec=BranchSpec(no_expiry=True, is_protected=True),
        status=BranchStatus(branch_id=BRANCH_ID, is_protected=True),
    )


def endpoint() -> Endpoint:
    return Endpoint(
        name=ENDPOINT_NAME,
        endpoint_id=ENDPOINT_ID,
        parent=BRANCH_NAME,
        spec=EndpointSpec(
            endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
            autoscaling_limit_min_cu=0.5,
            autoscaling_limit_max_cu=2.0,
        ),
        status=EndpointStatus(
            endpoint_id=ENDPOINT_ID,
            endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
            autoscaling_limit_min_cu=0.5,
            autoscaling_limit_max_cu=2.0,
            hosts=EndpointHosts(host=HOST),
        ),
    )


def database() -> Database:
    return Database(
        name=DATABASE_NAME,
        database_id=DATABASE_RESOURCE_ID,
        parent=BRANCH_NAME,
        status=DatabaseDatabaseStatus(
            database_id=DATABASE_RESOURCE_ID,
            postgres_database=DATABASE_ID,
        ),
    )


class CompletedOperation:
    def __init__(self, result):
        self.result = result
        self.wait_count = 0

    def wait(self):
        self.wait_count += 1
        return self.result


class FakePostgres:
    def __init__(self, *, populated: bool = True, include_database: bool = True):
        self.projects = [project()] if populated else []
        self.branches = [branch()] if populated else []
        self.endpoints = [endpoint()] if populated else []
        self.databases = [database()] if populated and include_database else []
        self.create_calls: list[tuple] = []
        self.operations: list[CompletedOperation] = []
        self.credential_calls: list[str] = []

    def list_projects(self):
        return iter(self.projects)

    def create_project(self, project: Project, project_id: str):
        self.create_calls.append(("project", project, project_id))
        created = globals()["project"]()
        self.projects.append(created)
        operation = CompletedOperation(created)
        self.operations.append(operation)
        return operation

    def list_branches(self, parent: str):
        self.assert_parent(parent, PROJECT_NAME)
        return iter(self.branches)

    def create_branch(self, parent: str, branch: Branch, branch_id: str):
        self.assert_parent(parent, PROJECT_NAME)
        self.create_calls.append(("branch", branch, branch_id))
        created = globals()["branch"]()
        self.branches.append(created)
        self.databases.append(database())
        operation = CompletedOperation(created)
        self.operations.append(operation)
        return operation

    def update_branch(self, name: str, branch: Branch, update_mask: FieldMask):
        self.assert_parent(name, BRANCH_NAME)
        self.create_calls.append(("branch_update", branch, update_mask))
        current = self.branches[0]
        updated = Branch(
            name=BRANCH_NAME,
            branch_id=BRANCH_ID,
            parent=PROJECT_NAME,
            spec=BranchSpec(
                expire_time=current.spec.expire_time,
                is_protected=branch.spec.is_protected,
                no_expiry=current.spec.no_expiry,
            ),
            status=BranchStatus(
                branch_id=BRANCH_ID,
                expire_time=current.status.expire_time,
                is_protected=branch.spec.is_protected,
            ),
        )
        self.branches = [updated]
        operation = CompletedOperation(updated)
        self.operations.append(operation)
        return operation

    def list_endpoints(self, parent: str):
        self.assert_parent(parent, BRANCH_NAME)
        return iter(self.endpoints)

    def create_endpoint(self, parent: str, endpoint: Endpoint, endpoint_id: str):
        self.assert_parent(parent, BRANCH_NAME)
        self.create_calls.append(("endpoint", endpoint, endpoint_id))
        created = globals()["endpoint"]()
        self.endpoints.append(created)
        operation = CompletedOperation(created)
        self.operations.append(operation)
        return operation

    def update_endpoint(self, name: str, endpoint: Endpoint, update_mask: FieldMask):
        self.assert_parent(name, ENDPOINT_NAME)
        self.create_calls.append(("endpoint_update", endpoint, update_mask))
        updated = globals()["endpoint"]()
        self.endpoints = [updated]
        operation = CompletedOperation(updated)
        self.operations.append(operation)
        return operation

    def list_databases(self, parent: str):
        self.assert_parent(parent, BRANCH_NAME)
        return iter(self.databases)

    def generate_database_credential(self, endpoint: str):
        self.assert_parent(endpoint, ENDPOINT_NAME)
        self.credential_calls.append(endpoint)
        return DatabaseCredential(token="short-lived-test-token")

    @staticmethod
    def assert_parent(actual: str, expected: str):
        if actual != expected:
            raise AssertionError(f"unexpected resource parent: {actual}")


class FakeWorkspace:
    def __init__(self, postgres: FakePostgres):
        config = load_target("field_eng")
        self.postgres = postgres
        self.config = SimpleNamespace(host=config.host, profile=config.profile)
        self.current_user = SimpleNamespace(
            me=lambda: SimpleNamespace(user_name="adam.gurary@databricks.com")
        )

    def get_workspace_id(self):
        return int(load_target("field_eng").workspace_id)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.last_statement = ""

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement: str, parameters=None):
        self.last_statement = statement
        self.connection.statements.append((statement, parameters))
        if statement == f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};":
            self.connection.schema_exists = True

    def fetchone(self):
        return (self.connection.schema_exists,)


class FakeConnection:
    def __init__(self, database_boundary):
        self.database_boundary = database_boundary
        self.schema_exists = database_boundary.schema_exists
        self.statements: list[tuple[str, object]] = []
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.database_boundary.schema_exists = self.schema_exists
        self.database_boundary.statements.extend(self.statements)
        return False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1


class FakeDatabaseBoundary:
    def __init__(self):
        self.schema_exists = False
        self.connect_kwargs: list[dict] = []
        self.statements: list[tuple[str, object]] = []

    def connect(self, **kwargs):
        self.connect_kwargs.append(dict(kwargs))
        return FakeConnection(self)


def identity(_profile: str, host: str) -> dict:
    return {
        "host": host,
        "current_user": {"userName": "adam.gurary@databricks.com"},
    }


class LakebaseReconciliationTest(unittest.TestCase):
    def test_existing_exact_resources_are_reused_across_runs(self):
        postgres = FakePostgres()
        database_boundary = FakeDatabaseBoundary()
        workspace = FakeWorkspace(postgres)

        with patch("deploy.lakebase.assert_profile", side_effect=identity), patch(
            "deploy.lakebase.psycopg.connect", side_effect=database_boundary.connect
        ):
            first = ensure_lakebase(workspace, load_target("field_eng"))
            second = ensure_lakebase(workspace, load_target("field_eng"))

        self.assertEqual(first, second)
        self.assertEqual(
            first.as_dict(),
            {
                "validated": True,
                "enabled": True,
                "project": PROJECT_ID,
                "branch": BRANCH_ID,
                "endpoint": ENDPOINT_ID,
                "host": HOST,
                "database": DATABASE_ID,
                "schema": SCHEMA,
            },
        )
        self.assertEqual(postgres.create_calls, [])
        self.assertEqual(
            [statement for statement, _parameters in database_boundary.statements].count(
                f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"
            ),
            2,
        )

    def test_missing_resources_use_exact_models_and_wait_for_every_create(self):
        postgres = FakePostgres(populated=False)
        database_boundary = FakeDatabaseBoundary()

        with patch("deploy.lakebase.assert_profile", side_effect=identity), patch(
            "deploy.lakebase.psycopg.connect", side_effect=database_boundary.connect
        ):
            state = ensure_lakebase(FakeWorkspace(postgres), load_target("field_eng"))

        self.assertEqual(state.project, PROJECT_ID)
        self.assertEqual([call[0] for call in postgres.create_calls], ["project", "branch", "endpoint"])
        project_call, branch_call, endpoint_call = postgres.create_calls
        self.assertEqual(project_call[2], PROJECT_ID)
        self.assertEqual(project_call[1].spec, ProjectSpec(display_name=PROJECT_ID, pg_version=16))
        self.assertEqual(branch_call[2], BRANCH_ID)
        self.assertEqual(branch_call[1].spec, BranchSpec(no_expiry=True, is_protected=True))
        self.assertEqual(endpoint_call[2], ENDPOINT_ID)
        self.assertEqual(
            endpoint_call[1].spec,
            EndpointSpec(
                endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=0.5,
                autoscaling_limit_max_cu=2.0,
            ),
        )
        self.assertEqual([operation.wait_count for operation in postgres.operations], [1, 1, 1])

    def test_missing_default_database_is_rejected(self):
        postgres = FakePostgres(include_database=False)
        database_boundary = FakeDatabaseBoundary()

        with patch("deploy.lakebase.assert_profile", side_effect=identity), patch(
            "deploy.lakebase.psycopg.connect", side_effect=database_boundary.connect
        ):
            with self.assertRaisesRegex(RuntimeError, "databricks_postgres"):
                ensure_lakebase(FakeWorkspace(postgres), load_target("field_eng"))

        self.assertEqual(database_boundary.connect_kwargs, [])

    def test_auto_created_unprotected_production_branch_is_safely_updated(self):
        postgres = FakePostgres()
        postgres.branches = [
            Branch(
                name=BRANCH_NAME,
                branch_id=BRANCH_ID,
                parent=PROJECT_NAME,
                spec=BranchSpec(),
                status=BranchStatus(branch_id=BRANCH_ID, is_protected=False),
            )
        ]
        postgres.endpoints = []
        database_boundary = FakeDatabaseBoundary()

        with patch("deploy.lakebase.assert_profile", side_effect=identity), patch(
            "deploy.lakebase.psycopg.connect", side_effect=database_boundary.connect
        ):
            ensure_lakebase(FakeWorkspace(postgres), load_target("field_eng"))

        update = postgres.create_calls[0]
        self.assertEqual(update[0], "branch_update")
        self.assertEqual(update[1].name, BRANCH_NAME)
        self.assertEqual(update[1].spec, BranchSpec(no_expiry=True, is_protected=True))
        self.assertEqual(
            update[2],
            FieldMask(["spec.is_protected"]),
        )
        self.assertEqual(postgres.operations[0].wait_count, 1)

    def test_status_without_expiration_is_accepted_when_sdk_omits_no_expiry_spec(self):
        postgres = FakePostgres()
        postgres.branches = [
            Branch(
                name=BRANCH_NAME,
                branch_id=BRANCH_ID,
                parent=PROJECT_NAME,
                spec=BranchSpec(),
                status=BranchStatus(branch_id=BRANCH_ID, is_protected=True, expire_time=None),
            )
        ]
        database_boundary = FakeDatabaseBoundary()

        with patch("deploy.lakebase.assert_profile", side_effect=identity), patch(
            "deploy.lakebase.psycopg.connect", side_effect=database_boundary.connect
        ):
            ensure_lakebase(FakeWorkspace(postgres), load_target("field_eng"))

        self.assertEqual(postgres.create_calls, [])

    def test_expiring_branch_fails_before_unsupported_update(self):
        expires = Timestamp(seconds=1893456000)
        postgres = FakePostgres()
        postgres.branches = [
            Branch(
                name=BRANCH_NAME,
                branch_id=BRANCH_ID,
                parent=PROJECT_NAME,
                spec=BranchSpec(
                    expire_time=expires,
                    is_protected=True,
                    no_expiry=False,
                ),
                status=BranchStatus(
                    branch_id=BRANCH_ID,
                    expire_time=expires,
                    is_protected=True,
                ),
            )
        ]

        with patch("deploy.lakebase.assert_profile", side_effect=identity):
            with self.assertRaisesRegex(
                RuntimeError,
                "cannot clear expiration in place",
            ):
                ensure_lakebase(FakeWorkspace(postgres), load_target("field_eng"))

        self.assertEqual(postgres.create_calls, [])

    def test_auto_created_primary_endpoint_is_updated_in_place_to_exact_limits(self):
        postgres = FakePostgres()
        postgres.endpoints = [
            Endpoint(
                name=ENDPOINT_NAME,
                endpoint_id=ENDPOINT_ID,
                parent=BRANCH_NAME,
                spec=EndpointSpec(endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE),
                status=EndpointStatus(
                    endpoint_id=ENDPOINT_ID,
                    endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                    autoscaling_limit_min_cu=1.0,
                    autoscaling_limit_max_cu=1.0,
                    hosts=EndpointHosts(host=HOST),
                ),
            )
        ]
        database_boundary = FakeDatabaseBoundary()

        with patch("deploy.lakebase.assert_profile", side_effect=identity), patch(
            "deploy.lakebase.psycopg.connect", side_effect=database_boundary.connect
        ):
            ensure_lakebase(FakeWorkspace(postgres), load_target("field_eng"))

        update = postgres.create_calls[0]
        self.assertEqual(update[0], "endpoint_update")
        self.assertEqual(update[1].name, ENDPOINT_NAME)
        self.assertEqual(
            update[1].spec,
            EndpointSpec(
                endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=0.5,
                autoscaling_limit_max_cu=2.0,
            ),
        )
        self.assertEqual(
            update[2],
            FieldMask(
                [
                    "spec.autoscaling_limit_min_cu",
                    "spec.autoscaling_limit_max_cu",
                ]
            ),
        )
        self.assertEqual(postgres.operations[0].wait_count, 1)

    def test_non_gurary_project_is_rejected_before_inventory_or_mutation(self):
        postgres = FakePostgres(populated=False)
        unsafe = replace(load_target("field_eng"), lakebase_project="bobabricks")

        with self.assertRaisesRegex(RuntimeError, "gurary-"):
            ensure_lakebase(FakeWorkspace(postgres), unsafe)

        self.assertEqual(postgres.create_calls, [])

    def test_exact_project_owned_by_another_user_is_rejected(self):
        postgres = FakePostgres()
        postgres.projects[0].status.owner = "someone.else@databricks.com"

        with patch("deploy.lakebase.assert_profile", side_effect=identity):
            with self.assertRaisesRegex(RuntimeError, "owner"):
                ensure_lakebase(FakeWorkspace(postgres), load_target("field_eng"))

        self.assertEqual(postgres.create_calls, [])

    def test_database_connection_is_tls_and_credential_stays_in_memory(self):
        postgres = FakePostgres()
        database_boundary = FakeDatabaseBoundary()

        with patch("deploy.lakebase.assert_profile", side_effect=identity), patch(
            "deploy.lakebase.psycopg.connect", side_effect=database_boundary.connect
        ):
            ensure_lakebase(FakeWorkspace(postgres), load_target("field_eng"))

        self.assertEqual(len(database_boundary.connect_kwargs), 1)
        kwargs = database_boundary.connect_kwargs[0]
        self.assertEqual(kwargs["host"], HOST)
        self.assertEqual(kwargs["dbname"], DATABASE_ID)
        self.assertEqual(kwargs["user"], "adam.gurary@databricks.com")
        self.assertEqual(kwargs["sslmode"], "require")
        self.assertEqual(kwargs["password"], "short-lived-test-token")
        self.assertNotIn("short-lived-test-token", json.dumps(LakebaseState(
            project=PROJECT_ID,
            branch=BRANCH_ID,
            endpoint=ENDPOINT_ID,
            host=HOST,
            database=DATABASE_ID,
            schema=SCHEMA,
        ).as_dict()))


class LakebaseGeneratedStateTest(unittest.TestCase):
    def test_reconciliation_preserves_existing_ids_and_adds_validated_lakebase(self):
        expected_lakebase = LakebaseState(
            project=PROJECT_ID,
            branch=BRANCH_ID,
            endpoint=ENDPOINT_ID,
            host=HOST,
            database=DATABASE_ID,
            schema=SCHEMA,
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "field_eng.json"
            original = {
                "target": "field_eng",
                "warehouse_id": "02e73edf357eb637",
                "genie_space_id": "01f1ae26c9461d158d6bc0105fc4d353",
            }
            state_path.write_text(json.dumps(original), encoding="utf-8")

            state = reconcile_lakebase_state(
                state_path=state_path,
                workspace_factory=lambda profile: FakeWorkspace(FakePostgres()),
                lakebase_ensurer=lambda workspace, config: expected_lakebase,
            )

            persisted = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertEqual(state, expected_lakebase)
        self.assertEqual(
            persisted,
            {**original, "lakebase": expected_lakebase.as_dict()},
        )


if __name__ == "__main__":
    unittest.main()
