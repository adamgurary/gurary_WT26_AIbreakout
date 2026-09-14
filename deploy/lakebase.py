"""Fail-closed reconciliation for the private field-eng Lakebase project."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Iterable, TypeVar

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Branch,
    BranchSpec,
    Database,
    Endpoint,
    EndpointSpec,
    EndpointType,
    FieldMask,
    Project,
    ProjectSpec,
)

from .config import TargetConfig
from .databricks_cli import REQUIRED_USER, assert_profile, live_workspace_id, normalize_host
from .safety import assert_safe_mutation


PROJECT_ID = "gurary-bobabricks"
BRANCH_ID = "production"
ENDPOINT_ID = "primary"
DATABASE_ID = "databricks_postgres"
DATABASE_RESOURCE_ID = "databricks-postgres"
SCHEMA_ID = "gurary_bobabricks_app"
FIELD_PROFILE = "dbc-f7444b38"
FIELD_HOST = "https://dbc-f7444b38-7453.staging.cloud.databricks.com"
FIELD_WORKSPACE_ID = "715783009495722"
PROJECT_NAME = f"projects/{PROJECT_ID}"
BRANCH_NAME = f"{PROJECT_NAME}/branches/{BRANCH_ID}"
ENDPOINT_NAME = f"{BRANCH_NAME}/endpoints/{ENDPOINT_ID}"
DATABASE_NAME = f"{BRANCH_NAME}/databases/{DATABASE_RESOURCE_ID}"
CREATE_SCHEMA_SQL = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_ID};"
_HOST = re.compile(
    r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.database(?:\.[a-z0-9-]+)*\.databricks\.com"
)


@dataclass(frozen=True)
class LakebaseState:
    project: str
    branch: str
    endpoint: str
    host: str
    database: str
    schema: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "validated": True,
            "enabled": True,
            "project": self.project,
            "branch": self.branch,
            "endpoint": self.endpoint,
            "host": self.host,
            "database": self.database,
            "schema": self.schema,
        }


def _require_target(target: TargetConfig) -> None:
    if not isinstance(target.lakebase_project, str) or not target.lakebase_project.startswith(
        "gurary-"
    ):
        raise RuntimeError("Lakebase project must start with gurary-")
    expected = {
        "key": "field_eng",
        "profile": FIELD_PROFILE,
        "host": FIELD_HOST,
        "workspace_id": FIELD_WORKSPACE_ID,
        "lakebase_project": PROJECT_ID,
        "lakebase_branch": BRANCH_ID,
        "lakebase_endpoint": ENDPOINT_ID,
        "lakebase_database": DATABASE_ID,
        "lakebase_schema": SCHEMA_ID,
    }
    if any(getattr(target, field) != value for field, value in expected.items()):
        raise RuntimeError("Field-eng Lakebase configuration does not match the safety contract")
    for resource_type, name in (
        ("Lakebase project", PROJECT_ID),
        ("Lakebase schema", SCHEMA_ID),
    ):
        assert_safe_mutation(target, resource_type, name)


def _authenticated_user(identity: dict[str, Any]) -> str:
    current_user = identity.get("current_user") if isinstance(identity, dict) else None
    if not isinstance(current_user, dict):
        raise RuntimeError("Databricks current-user identity is malformed")
    user_name = current_user.get("user_name", current_user.get("userName"))
    if user_name != REQUIRED_USER:
        raise RuntimeError("Databricks current user does not match the Lakebase owner")
    return user_name


def _assert_binding(workspace: WorkspaceClient, target: TargetConfig) -> str:
    _require_target(target)
    identity = assert_profile(target.profile, target.host)
    user_name = _authenticated_user(identity)
    actual_host = getattr(getattr(workspace, "config", None), "host", None)
    if normalize_host(actual_host) != normalize_host(target.host):
        raise RuntimeError("Lakebase SDK client host does not match the field-eng target")
    if live_workspace_id(target.profile) != target.workspace_id:
        raise RuntimeError("Lakebase SDK workspace ID does not match the field-eng target")
    return user_name


T = TypeVar("T")


def _exact_one(records: Iterable[T], expected_name: str, label: str) -> T | None:
    matches = [record for record in records if getattr(record, "name", None) == expected_name]
    if len(matches) > 1:
        raise RuntimeError(f"Databricks returned duplicate owned {label} records")
    return matches[0] if matches else None


def _setting(record: Any, field: str) -> Any:
    status = getattr(record, "status", None)
    status_value = getattr(status, field, None)
    if status_value is not None:
        return status_value
    return getattr(getattr(record, "spec", None), field, None)


def _validate_project(project: Project) -> Project:
    if (
        project.name != PROJECT_NAME
        or getattr(project, "project_id", None) not in (None, PROJECT_ID)
        or _setting(project, "display_name") != PROJECT_ID
        or _setting(project, "pg_version") != 16
        or getattr(getattr(project, "status", None), "owner", None) != REQUIRED_USER
    ):
        raise RuntimeError("Owned Lakebase project readback does not match the exact owner contract")
    return project


def _find_project(workspace: WorkspaceClient) -> Project | None:
    project = _exact_one(workspace.postgres.list_projects(), PROJECT_NAME, "Lakebase project")
    return _validate_project(project) if project is not None else None


def _ensure_project(workspace: WorkspaceClient, target: TargetConfig) -> Project:
    _assert_binding(workspace, target)
    project = _find_project(workspace)
    if project is None:
        operation = workspace.postgres.create_project(
            project=Project(spec=ProjectSpec(display_name=PROJECT_ID, pg_version=16)),
            project_id=PROJECT_ID,
        )
        _validate_project(operation.wait())
        project = _find_project(workspace)
        if project is None:
            raise RuntimeError("Created Lakebase project was missing from readback")
    return project


def _validate_branch(branch: Branch) -> Branch:
    if (
        branch.name != BRANCH_NAME
        or getattr(branch, "branch_id", None) not in (None, BRANCH_ID)
        or getattr(branch, "parent", None) not in (None, PROJECT_NAME)
        or _setting(branch, "is_protected") is not True
        or not _branch_has_no_expiry(branch)
    ):
        raise RuntimeError("Owned Lakebase branch readback does not match the protected contract")
    return branch


def _branch_has_no_expiry(branch: Branch) -> bool:
    no_expiry = getattr(getattr(branch, "spec", None), "no_expiry", None)
    if no_expiry is not None:
        return no_expiry is True
    status = getattr(branch, "status", None)
    return status is not None and getattr(status, "expire_time", None) is None


def _validate_branch_identity(branch: Branch) -> Branch:
    if (
        branch.name != BRANCH_NAME
        or getattr(branch, "branch_id", None) not in (None, BRANCH_ID)
        or getattr(branch, "parent", None) not in (None, PROJECT_NAME)
    ):
        raise RuntimeError("Owned Lakebase branch readback does not match the exact identity")
    return branch


def _find_branch(workspace: WorkspaceClient) -> Branch | None:
    branch = _exact_one(
        workspace.postgres.list_branches(parent=PROJECT_NAME), BRANCH_NAME, "Lakebase branch"
    )
    return _validate_branch_identity(branch) if branch is not None else None


def _ensure_branch(workspace: WorkspaceClient, target: TargetConfig) -> Branch:
    _assert_binding(workspace, target)
    if _find_project(workspace) is None:
        raise RuntimeError("Owned Lakebase project disappeared before branch reconciliation")
    branch = _find_branch(workspace)
    if branch is None:
        operation = workspace.postgres.create_branch(
            parent=PROJECT_NAME,
            branch=Branch(spec=BranchSpec(no_expiry=True, is_protected=True)),
            branch_id=BRANCH_ID,
        )
        _validate_branch(operation.wait())
        branch = _find_branch(workspace)
        if branch is None:
            raise RuntimeError("Created Lakebase branch was missing from readback")
    elif not _branch_has_no_expiry(branch):
        raise RuntimeError(
            "Owned Lakebase branch is expiring and cannot clear expiration in place "
            "with the supported branch update API"
        )
    elif _setting(branch, "is_protected") is not True:
        operation = workspace.postgres.update_branch(
            name=BRANCH_NAME,
            branch=Branch(
                name=BRANCH_NAME,
                spec=BranchSpec(no_expiry=True, is_protected=True),
            ),
            update_mask=FieldMask(["spec.is_protected"]),
        )
        _validate_branch(operation.wait())
        branch = _find_branch(workspace)
        if branch is None:
            raise RuntimeError("Updated Lakebase branch was missing from readback")
    return _validate_branch(branch)


def _validate_endpoint(endpoint: Endpoint) -> Endpoint:
    status = getattr(endpoint, "status", None)
    hosts = getattr(status, "hosts", None)
    host = getattr(hosts, "host", None)
    if (
        endpoint.name != ENDPOINT_NAME
        or getattr(endpoint, "endpoint_id", None) not in (None, ENDPOINT_ID)
        or getattr(endpoint, "parent", None) not in (None, BRANCH_NAME)
        or _setting(endpoint, "endpoint_type")
        != EndpointType.ENDPOINT_TYPE_READ_WRITE
        or _setting(endpoint, "autoscaling_limit_min_cu") != 0.5
        or _setting(endpoint, "autoscaling_limit_max_cu") != 2.0
        or not isinstance(host, str)
        or not _HOST.fullmatch(host)
    ):
        raise RuntimeError("Owned Lakebase endpoint readback does not match the autoscaling contract")
    return endpoint


def _validate_endpoint_identity(endpoint: Endpoint) -> Endpoint:
    status = getattr(endpoint, "status", None)
    hosts = getattr(status, "hosts", None)
    host = getattr(hosts, "host", None)
    if (
        endpoint.name != ENDPOINT_NAME
        or getattr(endpoint, "endpoint_id", None) not in (None, ENDPOINT_ID)
        or getattr(endpoint, "parent", None) not in (None, BRANCH_NAME)
        or _setting(endpoint, "endpoint_type")
        != EndpointType.ENDPOINT_TYPE_READ_WRITE
        or not isinstance(host, str)
        or not _HOST.fullmatch(host)
    ):
        raise RuntimeError("Owned Lakebase endpoint readback does not match the exact identity")
    return endpoint


def _find_endpoint(workspace: WorkspaceClient) -> Endpoint | None:
    endpoint = _exact_one(
        workspace.postgres.list_endpoints(parent=BRANCH_NAME),
        ENDPOINT_NAME,
        "Lakebase endpoint",
    )
    return _validate_endpoint_identity(endpoint) if endpoint is not None else None


def _ensure_endpoint(workspace: WorkspaceClient, target: TargetConfig) -> Endpoint:
    _assert_binding(workspace, target)
    branch = _find_branch(workspace)
    if _find_project(workspace) is None or branch is None:
        raise RuntimeError("Owned Lakebase hierarchy disappeared before endpoint reconciliation")
    _validate_branch(branch)
    endpoint = _find_endpoint(workspace)
    if endpoint is None:
        operation = workspace.postgres.create_endpoint(
            parent=BRANCH_NAME,
            endpoint=Endpoint(
                spec=EndpointSpec(
                    endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                    autoscaling_limit_min_cu=0.5,
                    autoscaling_limit_max_cu=2.0,
                )
            ),
            endpoint_id=ENDPOINT_ID,
        )
        _validate_endpoint(operation.wait())
        endpoint = _find_endpoint(workspace)
        if endpoint is None:
            raise RuntimeError("Created Lakebase endpoint was missing from readback")
    elif (
        _setting(endpoint, "autoscaling_limit_min_cu") != 0.5
        or _setting(endpoint, "autoscaling_limit_max_cu") != 2.0
    ):
        operation = workspace.postgres.update_endpoint(
            name=ENDPOINT_NAME,
            endpoint=Endpoint(
                name=ENDPOINT_NAME,
                spec=EndpointSpec(
                    endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                    autoscaling_limit_min_cu=0.5,
                    autoscaling_limit_max_cu=2.0,
                ),
            ),
            update_mask=FieldMask(
                [
                    "spec.autoscaling_limit_min_cu",
                    "spec.autoscaling_limit_max_cu",
                ]
            ),
        )
        _validate_endpoint(operation.wait())
        endpoint = _find_endpoint(workspace)
        if endpoint is None:
            raise RuntimeError("Updated Lakebase endpoint was missing from readback")
    return _validate_endpoint(endpoint)


def _validate_database(database: Database) -> Database:
    postgres_database = getattr(getattr(database, "status", None), "postgres_database", None)
    if (
        database.name != DATABASE_NAME
        or getattr(database, "database_id", None) not in (None, DATABASE_RESOURCE_ID)
        or getattr(database, "parent", None) not in (None, BRANCH_NAME)
        or postgres_database != DATABASE_ID
    ):
        raise RuntimeError("Owned Lakebase database readback does not match databricks_postgres")
    return database


def _find_database(workspace: WorkspaceClient) -> Database | None:
    database = _exact_one(
        workspace.postgres.list_databases(parent=BRANCH_NAME),
        DATABASE_NAME,
        "Lakebase database",
    )
    return _validate_database(database) if database is not None else None


def _endpoint_host(endpoint: Endpoint) -> str:
    host = getattr(getattr(getattr(endpoint, "status", None), "hosts", None), "host", None)
    if not isinstance(host, str) or not _HOST.fullmatch(host):
        raise RuntimeError("Owned Lakebase endpoint has no valid read-write host")
    return host


def _ensure_schema(
    workspace: WorkspaceClient, target: TargetConfig, endpoint: Endpoint
) -> None:
    user_name = _assert_binding(workspace, target)
    branch = _find_branch(workspace)
    if _find_project(workspace) is None or branch is None:
        raise RuntimeError("Owned Lakebase hierarchy disappeared before schema reconciliation")
    _validate_branch(branch)
    current_endpoint = _find_endpoint(workspace)
    if current_endpoint is None or current_endpoint.name != endpoint.name:
        raise RuntimeError("Owned Lakebase endpoint disappeared before schema reconciliation")
    _validate_endpoint(current_endpoint)
    if _find_database(workspace) is None:
        raise RuntimeError("Required Lakebase database databricks_postgres does not exist")

    credential = workspace.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
    token = getattr(credential, "token", None)
    if not isinstance(token, str) or not token:
        raise RuntimeError("Lakebase generated credential did not contain an in-memory token")

    with psycopg.connect(
        host=_endpoint_host(current_endpoint),
        dbname=DATABASE_ID,
        user=user_name,
        password=token,
        sslmode="require",
        connect_timeout=15,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s);",
                (SCHEMA_ID,),
            )
            before = cursor.fetchone()
            if not isinstance(before, tuple) or len(before) != 1:
                raise RuntimeError("Lakebase schema precheck returned malformed evidence")
            cursor.execute(CREATE_SCHEMA_SQL)
            connection.commit()
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s);",
                (SCHEMA_ID,),
            )
            after = cursor.fetchone()
            if after != (True,):
                raise RuntimeError("Lakebase schema readback did not confirm the isolated schema")


def ensure_lakebase(workspace: WorkspaceClient, config: TargetConfig) -> LakebaseState:
    """Create or reuse the one exact private Lakebase hierarchy and isolated schema."""
    _require_target(config)
    _ensure_project(workspace, config)
    _ensure_branch(workspace, config)
    endpoint = _ensure_endpoint(workspace, config)
    if _find_database(workspace) is None:
        raise RuntimeError("Required Lakebase database databricks_postgres does not exist")
    _ensure_schema(workspace, config, endpoint)
    return LakebaseState(
        project=PROJECT_ID,
        branch=BRANCH_ID,
        endpoint=ENDPOINT_ID,
        host=_endpoint_host(endpoint),
        database=DATABASE_ID,
        schema=SCHEMA_ID,
    )
