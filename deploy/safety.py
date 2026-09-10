"""Fail-closed guards for deployment mutations."""

from .config import TargetConfig


FORBIDDEN_NAMES = {
    "bobabricks-store-ops-demo",
    "bobabricks-storetime-mcp",
    "bobabricks-opstask-mcp",
    "worldtour_ai_catalog.bobabricks_store_ops",
    "system_ai_agent_atlassian_mcp",
}


class UnsafeMutation(RuntimeError):
    pass


def assert_safe_name(name: str) -> None:
    leaf = name.rsplit(".", 1)[-1]
    if not (leaf.startswith("gurary_") or leaf.startswith("gurary-")):
        raise UnsafeMutation(f"Refusing non-gurary resource: {name}")


def assert_safe_mutation(target: TargetConfig, resource_type: str, name: str) -> None:
    if name in FORBIDDEN_NAMES or any(name.startswith(value + ".") for value in FORBIDDEN_NAMES):
        raise UnsafeMutation(f"Refusing shared resource mutation: {resource_type} {name}")
    assert_safe_name(name)


def assert_safe_fevm_grant(app_name: str, securable: str, privileges: set[str]) -> None:
    allowed = {
        "worldtour_ai_catalog": {"USE_CATALOG"},
        "worldtour_ai_catalog.ai_gateway_demo": {"USE_SCHEMA", "CREATE_TABLE"},
    }
    if app_name != "gurary-bobabricks-store-ops":
        raise UnsafeMutation(f"Refusing grant for non-owned app: {app_name}")
    if securable not in allowed or not privileges.issubset(allowed[securable]):
        raise UnsafeMutation(f"Refusing FEVM grant on {securable}: {sorted(privileges)}")
