"""Render isolated target deployments without mutating tracked source files."""

from __future__ import annotations

import ctypes
from collections import Counter
from hashlib import sha256
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

import yaml

from .config import DemoState, TargetConfig, load_state, load_target


ROOT = Path(__file__).resolve().parents[1]
BUILD_ROOT = ROOT / ".build"
STATE_ROOT = ROOT / "deploy" / "state"
ROOT_MANIFESTS = (Path("app.yaml"), Path("app.yml"))
MCP_MANIFESTS = (
    Path("mcp-apps/storetime/app.yaml"), Path("mcp-apps/storetime/app.yml"),
    Path("mcp-apps/opstask/app.yaml"), Path("mcp-apps/opstask/app.yml"),
)
_AMBER_IDENTIFIERS = {
    "ad341da9-d12e-4688-ad1c-3c049cf70486",
    "bobabricks-store-ops-demo",
    "/Users/ad341da9-d12e-4688-ad1c-3c049cf70486/bobabricks-store-ops-demo-uc",
}
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
_OWNED_METADATA = {
    "presenter_app_id", "presenter_app_url", "presenter_service_principal_client_id",
    "storetime_app_id", "storetime_app_url", "storetime_service_principal_client_id",
    "opstask_app_id", "opstask_app_url", "opstask_service_principal_client_id",
}
_FIELD_REQUIRED = {
    "target", "warehouse_id", "genie_space_id", "storetime_mcp_url", "opstask_mcp_url",
    "mlflow_experiment_name", "lakebase",
}
# Each safe-reference entry binds one exact relative path, one exact forbidden
# identifier, the SHA-256 fingerprint of the complete UTF-8 line, and the
# maximum number of occurrences for that exact policy key. The identifier
# aliases are split so the policy source does not create new literal matches.
_REF_AMBER_EXPERIMENT = ("/Users/ad341da9-d12e-4688-ad1c-" "3c049cf70486/bobabricks-store-" "ops-demo-uc")
_REF_FEVM_GENIE_ID = ("01f1968067e81bd0" "9eacb0d88bcc57de")
_REF_FEVM_WAREHOUSE_ID = ("88fd32ee" "6d9438ac")
_REF_AMBER_PRINCIPAL = ("ad341da9-d12e-4688-ad1c-" "3c049cf70486")
_REF_FEVM_TRACE_SCHEMA = ("ai_gate" "way_demo")
_REF_FEVM_OPSTASK_APP = ("bobabricks-ops" "task-mcp")
_REF_AMBER_APP = ("bobabricks-store-" "ops-demo")
_REF_FEVM_STORETIME_APP = ("bobabricks-store" "time-mcp")
_REF_FEVM_DATA_SCHEMA = ("bobabricks_store_" "ops")
_REF_FIELD_OPSTASK_APP = ("gurary-bobabricks-ops" "task-mcp")
_REF_FIELD_STORETIME_APP = ("gurary-bobabricks-store" "time")
_REF_FIELD_TRACE_SCHEMA = ("gurary_ai_gate" "way_demo")
_REF_FIELD_GENIE_NAME = ("gurary_bobabricks_store_" "operations")
_REF_FIELD_DATA_SCHEMA = ("gurary_bobabricks_store_" "ops")
_REF_FIELD_WAREHOUSE_NAME = ("gurary_bobabricks_" "warehouse")
_REF_FIELD_CATALOG = ("gurary_" "catalog")
_REF_FEVM_OPSTASK_URL = ("https://bobabricks-opstask-mcp-7474657163903557.aws." "databricksapps.com/mcp")
_REF_FEVM_STORETIME_URL = ("https://bobabricks-storetime-mcp-7474657163903557.aws." "databricksapps.com/mcp")
_REF_FEVM_CATALOG = ("worldtour_ai_" "catalog")
_SAFE_REFERENCE_LINES: dict[tuple[Path, str, str], int] = {
    (Path('.env.databricks.example'), _REF_AMBER_APP, '3fbe29b84b7e16e3c6750f5cc6df1c40eb06dbccf68b258124af57701a0a8913'): 1,
    (Path('AGENTS.md'), _REF_AMBER_APP, '1e5e26da7ce79d2132ddf987177d8e8c8d5dca3ec629374d08d28f96cdaaaea2'): 1,
    (Path('AGENTS.md'), _REF_AMBER_APP, '429ce26608b92ae7d6ac308ced16f2adb4fbf5a0a2d88b70748ffff62350a470'): 1,
    (Path('AGENTS.md'), _REF_AMBER_APP, 'f003b6e229a046fffbfd7ac6b69c2ebbe84d8e12cd81d53a6fefd06931b2e671'): 1,
    (Path('CODEX_UPGRADE_PROMPT.txt'), _REF_AMBER_APP, '0b65f1de8fdac08cb43b7820808c10bbaa4b115f4eb0a2d766710f1489bca4d0'): 1,
    (Path('CODEX_UPGRADE_PROMPT.txt'), _REF_AMBER_APP, '1cc8395140ff9bd01749d061e2856a86f682e807a62935322eed4982ba3029a5'): 1,
    (Path('CODEX_UPGRADE_PROMPT.txt'), _REF_AMBER_APP, '26a2cbd74f8f358ea36deb6a21a338a9c608fbe0b3245c62d717429e63c56668'): 1,
    (Path('DATABRICKS_BUILD_PLAN.md'), _REF_AMBER_APP, '5610d73dd47418bb142ec859d87a070476ff0aa5d513a578c99e3ae14c95da4f'): 2,
    (Path('DATABRICKS_BUILD_PLAN.md'), _REF_FEVM_OPSTASK_APP, '9877223430f49d02dbbd4f7275c2ddb96a55f1a59471621ebb28db0de8cf5a6f'): 1,
    (Path('DATABRICKS_BUILD_PLAN.md'), _REF_FEVM_STORETIME_APP, 'dc3a19b0263cc2c24d6c6843110a2f78dd2945c6ba646b57f3029d79efb3de9b'): 1,
    (Path('DATABRICKS_BUILD_PLAN.md'), _REF_AMBER_APP, 'f003b6e229a046fffbfd7ac6b69c2ebbe84d8e12cd81d53a6fefd06931b2e671'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_FEVM_CATALOG, '27081d79b933f7b8b6e2b4e0b0a36a268cfe4097aa4c579987378b371a1f284a'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_AMBER_APP, '4d384433bd922862534c6ca5b793e8fb68d48d3de1ab6f03161acd0afc7e2d8b'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_AMBER_APP, '5f2624d827142cc911310fe6bd298233ce9439f9ed64fb6ae1c5096f8ff7d903'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_FEVM_CATALOG, '62c3f0c01abb86cce872876b88d9ab4a2831927ab0ff8ea28e3a12c7e177d56d'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_FEVM_CATALOG, '68ce02aa0e3f367c4e00dacc50e176220e38d7555c76d6ca31ee3078a96bc4d4'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_FEVM_DATA_SCHEMA, '8cb47daa7e894286a2eaa26f333aa535c2a6a579f553934a8cef2ab7a14b86ae'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_FEVM_CATALOG, '8cb47daa7e894286a2eaa26f333aa535c2a6a579f553934a8cef2ab7a14b86ae'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_FEVM_TRACE_SCHEMA, 'a713e6a5bfdce265897a0c8a9ecc97a2431fcc28074014543e649652066881c8'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_AMBER_APP, 'a713e6a5bfdce265897a0c8a9ecc97a2431fcc28074014543e649652066881c8'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_AMBER_APP, 'e6bccb0b38c2610c60a0501eec1c3e509cc8be2066635028e53af984a3ad127d'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_AMBER_APP, 'f003b6e229a046fffbfd7ac6b69c2ebbe84d8e12cd81d53a6fefd06931b2e671'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_FEVM_OPSTASK_APP, 'f781048da943a94f0610c2f5cc186c92f50d1d7e19342976980964c9c2cf4c6c'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_FEVM_DATA_SCHEMA, 'f781048da943a94f0610c2f5cc186c92f50d1d7e19342976980964c9c2cf4c6c'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_FEVM_STORETIME_APP, 'f96d26458277c76d4fbf2ed1ea6702eb37e0923075d9f8af1b3bc372b5fbfaf9'): 1,
    (Path('PRESENTER_SETUP.md'), _REF_FEVM_DATA_SCHEMA, 'f96d26458277c76d4fbf2ed1ea6702eb37e0923075d9f8af1b3bc372b5fbfaf9'): 1,
    (Path('README.md'), _REF_FEVM_OPSTASK_APP, '142890e194dec9ac5c5dfbbcc61b654c493b58fd2c0dc09f49893ea2033093b1'): 1,
    (Path('README.md'), _REF_FEVM_STORETIME_APP, '142890e194dec9ac5c5dfbbcc61b654c493b58fd2c0dc09f49893ea2033093b1'): 1,
    (Path('agent_server/agent.py'), _REF_FEVM_STORETIME_URL, '0cf7d6c9e3dfc3d5c789665f1599d197bd736d14f1de76e06fc6ea3ec08dcf6d'): 1,
    (Path('agent_server/agent.py'), _REF_FEVM_STORETIME_APP, '3aa614cf5907773fba9daa705601778d05cabec8e8ec5c689af26530cac77d65'): 1,
    (Path('agent_server/agent.py'), _REF_FEVM_STORETIME_APP, '7273fe91c95a1529fc70b1b63425445af1fe1e0667c5ed0951ee9713bf415696'): 2,
    (Path('agent_server/agent.py'), _REF_FEVM_GENIE_ID, '731a170f689563e91d91ef54746788ac1ccad5e6678bb1254e8dc13f6e45d59b'): 1,
    (Path('agent_server/agent.py'), _REF_FEVM_OPSTASK_APP, 'd5828f88fb946cccf1e523ae9282a064e9145592c6a38fe81b619e88da5af99f'): 1,
    (Path('agent_server/agent.py'), _REF_FEVM_OPSTASK_APP, 'de547daf8838dd5cc90b2594212509fb4cda3ec4f3f4ecf32ce1cf4325e9171c'): 2,
    (Path('agent_server/agent.py'), _REF_FEVM_OPSTASK_URL, 'edf9256a4a23a2c29f01a2dcb111003ff49a98f4a7e474d8d85c761b5022db0b'): 1,
    (Path('app/app.py'), _REF_FEVM_STORETIME_APP, '058f97f4158c4bad773c931753b431d78f3f8c5ef803f1f9c33dc5e660c13d9d'): 1,
    (Path('app/app.py'), _REF_FEVM_OPSTASK_APP, '382791c74cf8b4a2db1cafddaf26db58eac60d3d22af3fb81fb093f0602c3024'): 1,
    (Path('app/app.py'), _REF_FEVM_GENIE_ID, '6bc84bb8db0ba3de66ca8d096d40ddbf84b3d5a97604f3843d0890b7a600924b'): 1,
    (Path('app/app.py'), _REF_FEVM_WAREHOUSE_ID, '6ee9ecb34c6c5151fbfa9ab834999a2d7f7ed41cd0bd4f01d1193ebb09e6ee38'): 1,
    (Path('app/app.py'), _REF_FEVM_STORETIME_URL, '84078efe0e2f8895e3063c2819e2a6aa115fa6860530eebf6073d63d30386361'): 1,
    (Path('app/app.py'), _REF_FEVM_OPSTASK_URL, 'a7beb1c878e9ba1f9ae0880ad806a72a2d729a426c5e4648a613c411ad5607d3'): 1,
    (Path('app/app.yaml'), _REF_FEVM_DATA_SCHEMA, '1ef191f67500ba4ea8f741dc9336eb8114e8b191c38afa2ad53e60d3bdc1c70e'): 1,
    (Path('app/app.yaml'), _REF_FEVM_STORETIME_APP, '2eec1301191cdc8737b218fea0235b604933d143833f9dea7bc5bf62adbb840e'): 1,
    (Path('app/app.yaml'), _REF_FEVM_CATALOG, '3ffe8c01b71a675cffece0b2594710c34bd48b6382e37a63df1a96858a7f7281'): 1,
    (Path('app/app.yaml'), _REF_FEVM_STORETIME_URL, '581e1c6ff453ce54118002f99310c19f5cc1869264df49eff689bc5f4d3e1209'): 1,
    (Path('app/app.yaml'), _REF_FEVM_OPSTASK_URL, '7654de256188627c883be9a67977578d45500f9d2e47bd71ab27f6de82374617'): 1,
    (Path('app/app.yaml'), _REF_FEVM_OPSTASK_APP, '97964d0160d3a21886ef88371bf14960de0bef2e45ff2f9d1be540758efea53a'): 1,
    (Path('app/app.yaml'), _REF_FEVM_GENIE_ID, 'fd1ae8b60d911853a4cf1c71093c7a350b1d407da12a7fdc94f7b458769ad783'): 1,
    (Path('app/bobabricks_agent.py'), _REF_FEVM_GENIE_ID, '6bc84bb8db0ba3de66ca8d096d40ddbf84b3d5a97604f3843d0890b7a600924b'): 1,
    (Path('archive/long-version/BOBABRICKS_DEMO_RUNBOOK.md'), _REF_AMBER_APP, '650acc9e95f3fcf6140825ca565b7b9385756e217b371b3d83d069193ef08889'): 1,
    (Path('databricks.yml'), _REF_AMBER_APP, '935d2cbd629dbc2255e8070b203c1da738d0b458bf36d4e5ee2f18dc9d75dbbd'): 1,
    (Path('deploy/render.py'), _REF_AMBER_EXPERIMENT, '262c49a46d78269c2d25388b755ecf57f77e8cf7725b41c5d18cf8d5a65b3a2d'): 1,
    (Path('deploy/render.py'), _REF_AMBER_PRINCIPAL, '262c49a46d78269c2d25388b755ecf57f77e8cf7725b41c5d18cf8d5a65b3a2d'): 1,
    (Path('deploy/render.py'), _REF_AMBER_APP, '262c49a46d78269c2d25388b755ecf57f77e8cf7725b41c5d18cf8d5a65b3a2d'): 1,
    (Path('deploy/render.py'), _REF_AMBER_PRINCIPAL, '3d05b9dab87b83b8a1ff255afacc15015c9f76b5f22ceadbe77688904ef34100'): 1,
    (Path('deploy/render.py'), _REF_AMBER_APP, '834287b6aea6ec92b72034cc83038d5b60e77c5620e6be8dcbd55142b81be917'): 1,
    (Path('deploy/safety.py'), _REF_FEVM_CATALOG, '4c8403faff9d6fbb785c09fba5c924541a59ccd6388dd8d6fb4c3b725f6f9755'): 1,
    (Path('deploy/safety.py'), _REF_FEVM_TRACE_SCHEMA, '5ac4d2d3aa92a69d1e0d2ce50287a6829e498ed6db8aad38cf495e8e174f5a20'): 1,
    (Path('deploy/safety.py'), _REF_FEVM_CATALOG, '5ac4d2d3aa92a69d1e0d2ce50287a6829e498ed6db8aad38cf495e8e174f5a20'): 1,
    (Path('deploy/safety.py'), _REF_FEVM_STORETIME_APP, '662cde5dff893f28fb8cab51c1aebdff10cf96a478100d627a231b26c42c8cdf'): 1,
    (Path('deploy/safety.py'), _REF_AMBER_APP, '834287b6aea6ec92b72034cc83038d5b60e77c5620e6be8dcbd55142b81be917'): 1,
    (Path('deploy/safety.py'), _REF_FEVM_OPSTASK_APP, 'a40ed4dacd9f930480b4d40edb29f4113e018242b605f1e0aa56c74ef0b7a816'): 1,
    (Path('deploy/safety.py'), _REF_FEVM_DATA_SCHEMA, 'caf0e6f1da7f86e39dc7951d2e063e4ef50a4d75cf66e9e3b203138ff78ebdbb'): 1,
    (Path('deploy/safety.py'), _REF_FEVM_CATALOG, 'caf0e6f1da7f86e39dc7951d2e063e4ef50a4d75cf66e9e3b203138ff78ebdbb'): 1,
    (Path('deploy/targets/fevm.yaml'), _REF_FEVM_TRACE_SCHEMA, '07bf28753150061f5e23da743400f073ab8182df55c91c58d9fecd9cf8fd0b68'): 1,
    (Path('deploy/targets/fevm.yaml'), _REF_FEVM_OPSTASK_URL, '11ccd441f83f2f39330e2d50d26e296125360418bd10f3e882c768011454e1df'): 1,
    (Path('deploy/targets/fevm.yaml'), _REF_FEVM_STORETIME_URL, '26220a44b36a490efe9d1b1d290a7107e717464292bf0eff3faaba06ed542b27'): 1,
    (Path('deploy/targets/fevm.yaml'), _REF_FEVM_STORETIME_APP, '588f2b92570a9418ebeaabb7e2cc7eab41aa9d09b8c5700875ae529da8522795'): 1,
    (Path('deploy/targets/fevm.yaml'), _REF_FEVM_OPSTASK_APP, '59aa5c0f6a2fdd5606838da9d7b349f541b48db45675f9faa872b215142a3881'): 1,
    (Path('deploy/targets/fevm.yaml'), _REF_FEVM_WAREHOUSE_ID, '5a25136aad584a74bfd0798326c970edf3e0512d8ab93ccff0d627362fa8455d'): 1,
    (Path('deploy/targets/fevm.yaml'), _REF_FEVM_GENIE_ID, '73c6015bc1ba6c31dd01406045604bf7a40ccd41a89c34381ced39e09a298b0c'): 1,
    (Path('deploy/targets/fevm.yaml'), _REF_FEVM_DATA_SCHEMA, 'e4558ad624d942157535b2cc936c801b0968682dfba9a7d5555bd8c5d7fa86f2'): 1,
    (Path('deploy/targets/fevm.yaml'), _REF_FEVM_CATALOG, 'ff629253bdfd816858d90cd133946ac73013392109aca229f062839a3ae09093'): 1,
    (Path('deploy/targets/field_eng.yaml'), _REF_FIELD_GENIE_NAME, '0bb7c89dd760554eabcf4831420a6fb6fcd6ba404aec83118559da40fc842f28'): 1,
    (Path('deploy/targets/field_eng.yaml'), _REF_FIELD_CATALOG, '3af2fd4186881544c1af9bf8288f46e94ca115dbe4ef1a2d832fe1b74f4affe3'): 1,
    (Path('deploy/targets/field_eng.yaml'), _REF_FIELD_TRACE_SCHEMA, '7b733b2867bcbe56f12fea81c46ceb02695437d102793dd6201a453f0407f7e6'): 1,
    (Path('deploy/targets/field_eng.yaml'), _REF_FIELD_OPSTASK_APP, '91db8426a7be5733c9be04ec475d880b1ee8de51c7b1193952844a7c4f935f0d'): 1,
    (Path('deploy/targets/field_eng.yaml'), _REF_FIELD_DATA_SCHEMA, '9cabfa5fb5f1585b7ef066180fb1034cb75a902952ce19a24c7c5f6aedb80d6b'): 1,
    (Path('deploy/targets/field_eng.yaml'), _REF_FIELD_STORETIME_APP, 'db4ebadbf4296d5db632bf8d14768b606cdcab4e9ec308902fe72229cbf0c8bd'): 1,
    (Path('deploy/targets/field_eng.yaml'), _REF_FIELD_WAREHOUSE_NAME, 'f617c1632690c1185feb25eedf0bdc5421ab037f3440493b52a3fe3b9889d30a'): 1,
    (Path('mcp-apps/README.md'), _REF_FEVM_OPSTASK_APP, '0da57af4cba8879fa286724ce57355cac48a34a7911ef9ca1dce216bdb3eaf3f'): 1,
    (Path('mcp-apps/README.md'), _REF_FEVM_STORETIME_APP, '1fbd67aa5a37523e8ad36b27713c9163afa70cf0c45dc4b2e1e70ec37ced6e62'): 1,
    (Path('mcp-apps/README.md'), _REF_FEVM_OPSTASK_APP, '50d8406bd1d1fb6cd77d034f5b7a459339c78bd9decdcd2fe906de572b6f871e'): 1,
    (Path('mcp-apps/README.md'), _REF_FEVM_WAREHOUSE_ID, '6126fc826eb3e98a8ac917506640150d5a9eb84b4215c276cebf6caf3a145066'): 1,
    (Path('mcp-apps/README.md'), _REF_FEVM_STORETIME_APP, '9ddd5b34576848f973d9bc788255e3a6bd744373704d6de9383d52d765858a47'): 1,
    (Path('mcp-apps/README.md'), _REF_FEVM_STORETIME_APP, 'db2b2bb9be0f81cbdf5e9b33725028cc562b4d4fe6465d692461bcfe54f48c0e'): 1,
    (Path('mcp-apps/README.md'), _REF_FEVM_OPSTASK_APP, 'df34a64e1f01ff3640f6501040f2e3656bc9dae4256882cb481650a754b94664'): 1,
    (Path('mcp-apps/inventory/app.yaml'), _REF_FEVM_DATA_SCHEMA, '1ef191f67500ba4ea8f741dc9336eb8114e8b191c38afa2ad53e60d3bdc1c70e'): 1,
    (Path('mcp-apps/inventory/app.yaml'), _REF_FEVM_WAREHOUSE_ID, '31d4dbaa43ffdb7454588d3dd1ed9f09797c7f20096f279446215136a8f4b282'): 1,
    (Path('mcp-apps/inventory/app.yaml'), _REF_FEVM_CATALOG, '3ffe8c01b71a675cffece0b2594710c34bd48b6382e37a63df1a96858a7f7281'): 1,
    (Path('mcp-apps/inventory/app.yml'), _REF_FEVM_DATA_SCHEMA, '1ef191f67500ba4ea8f741dc9336eb8114e8b191c38afa2ad53e60d3bdc1c70e'): 1,
    (Path('mcp-apps/inventory/app.yml'), _REF_FEVM_WAREHOUSE_ID, '31d4dbaa43ffdb7454588d3dd1ed9f09797c7f20096f279446215136a8f4b282'): 1,
    (Path('mcp-apps/inventory/app.yml'), _REF_FEVM_CATALOG, '3ffe8c01b71a675cffece0b2594710c34bd48b6382e37a63df1a96858a7f7281'): 1,
    (Path('mcp-apps/opstask/app.py'), _REF_FEVM_OPSTASK_APP, 'a40ed4dacd9f930480b4d40edb29f4113e018242b605f1e0aa56c74ef0b7a816'): 1,
    (Path('mcp-apps/opstask/databricks.yml'), _REF_FEVM_OPSTASK_APP, '2489bdd853899cfd8d1c50fbd7cb882023bc385342cb9a698a939f1c9e7be5d9'): 1,
    (Path('mcp-apps/opstask/databricks.yml'), _REF_FEVM_OPSTASK_APP, '412ddd4cf5621bc6ad59d096d306e34355fbee8bb59d142b7028927b39e51767'): 1,
    (Path('mcp-apps/storetime/app.py'), _REF_FEVM_STORETIME_APP, '662cde5dff893f28fb8cab51c1aebdff10cf96a478100d627a231b26c42c8cdf'): 1,
    (Path('mcp-apps/storetime/databricks.yml'), _REF_FEVM_STORETIME_APP, '3ddef1b6401db884aeb2f7bbdb02e2d17a0552a813142c4e8b571fb45f37ecaf'): 1,
    (Path('mcp-apps/storetime/databricks.yml'), _REF_FEVM_STORETIME_APP, 'bcc105dfcf85f3aec116a67f8ceb6d3439449a2de5ba7938c595f253b8cff79c'): 1,
    (Path('script.md'), _REF_AMBER_APP, 'e64d5f25835f2efdb276bb94b4fc28b8f99e7a33083f2be3f20f65687c9a4536'): 1,
    (Path('script.md'), _REF_AMBER_APP, 'f003b6e229a046fffbfd7ac6b69c2ebbe84d8e12cd81d53a6fefd06931b2e671'): 1,
    (Path('scripts/provision_databricks_assets.py'), _REF_FEVM_WAREHOUSE_ID, 'fc476ee21d4d425b90bd02488c27d1d452167297d39b44c40f281d9d6b17e6d4'): 1,
    (Path('scripts/start_app.py'), _REF_FEVM_OPSTASK_APP, '0a58ffdb1a78e4f60c9fe1fc9ea5f17c7b6328952ffa0a6241220fd3f6337bc7'): 2,
    (Path('scripts/start_app.py'), _REF_FEVM_STORETIME_APP, 'a8ccd6ebfce1283147edf867326c8e67a0be171fb64fb6925b3a0a2fed359c53'): 1,
    (Path('scripts/start_app.py'), _REF_FEVM_STORETIME_APP, 'f9283abd20876cc6193bda1daaa05098da2798e89c4b2208506548975b06a618'): 2,
    (Path('tests/deploy/test_config.py'), _REF_FEVM_DATA_SCHEMA, '26b6dbd8a69007aa7a589b62dce41fcba102d2ec5bf2c3fad5ccceb28734a544'): 1,
    (Path('tests/deploy/test_config.py'), _REF_FEVM_CATALOG, '26b6dbd8a69007aa7a589b62dce41fcba102d2ec5bf2c3fad5ccceb28734a544'): 1,
    (Path('tests/deploy/test_config.py'), _REF_AMBER_APP, '4104a707957001395e66993640a606b917a71217295d9d2bffd31465aa9aab19'): 1,
    (Path('tests/deploy/test_config.py'), _REF_FEVM_DATA_SCHEMA, '5255b7ad5f277eab814909bb7c0b7c5a325a05942a90c7249a7da045e54df251'): 1,
    (Path('tests/deploy/test_config.py'), _REF_FEVM_CATALOG, '5255b7ad5f277eab814909bb7c0b7c5a325a05942a90c7249a7da045e54df251'): 1,
    (Path('tests/deploy/test_config.py'), _REF_FEVM_STORETIME_APP, '5b9ebc9833baa8e445ffea12e607d701d44878ced177130c5d93f3d9240791af'): 1,
    (Path('tests/deploy/test_config.py'), _REF_AMBER_APP, '8924156d7555b1781e41a605750f3cd8998efe33f1e7ac8f77386277760d24f6'): 1,
    (Path('tests/deploy/test_config.py'), _REF_FEVM_TRACE_SCHEMA, 'b1964a67f1a621ce64498e26ebccd533ff3d16345f3893ed9c418fe86931ee28'): 1,
    (Path('tests/deploy/test_config.py'), _REF_FEVM_CATALOG, 'b1964a67f1a621ce64498e26ebccd533ff3d16345f3893ed9c418fe86931ee28'): 1,
    (Path('tests/deploy/test_config.py'), _REF_FEVM_OPSTASK_APP, 'dabe6f8030b176793c480e31d5a0013d27085fd1bad51a8dc54e42838f71cd0b'): 1,
    (Path('tests/deploy/test_config.py'), _REF_FEVM_TRACE_SCHEMA, 'db759e5850b0d4a11f6568fd51824fd77a6e38bd8394d8a38ad6a308e9b515e2'): 1,
    (Path('tests/deploy/test_config.py'), _REF_FEVM_CATALOG, 'db759e5850b0d4a11f6568fd51824fd77a6e38bd8394d8a38ad6a308e9b515e2'): 1,
    (Path('tests/deploy/test_config.py'), _REF_AMBER_APP, 'e1659112ccacb5f9eab873e7698583fd81cf9d9bf4303438331ee525df0c8f15'): 1,
    (Path('tests/deploy/test_render.py'), _REF_AMBER_APP, '14024762aa9079bc21d9b1def62ddd4bea44428609d46a111c36974de6953f1f'): 1,
    (Path('tests/deploy/test_render.py'), _REF_AMBER_APP, '329d9639e60fd0f216fe6a1bc193e4bcd2fa1fd0812f4014c33c537782720f72'): 1,
    (Path('tests/deploy/test_render.py'), _REF_AMBER_PRINCIPAL, '7652ca425fd12add57ea30ef383cb3e3dbc74fc906126cce09988792d1a3623e'): 1,
    (Path('tests/deploy/test_render.py'), _REF_AMBER_APP, '9bec9a8bb4a9afd935526bfebed6e013c17334685cb616d011a660f1915469f8'): 1,
    (Path('tests/deploy/test_render.py'), _REF_AMBER_APP, 'b271387ac077e45247f45bed860e5ca22d57cfc7f794fc1029808b73d4283628'): 1,
    (Path('tests/deploy/test_render.py'), _REF_AMBER_APP, 'ffd22bcca09f5266d817692c783d26b5739ddf45ca72ea9f332a702ca664069f'): 1,
}
_LAKEBASE_HOST = re.compile(
    r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.database(?:\.[a-z0-9-]+)*\.databricks\.com"
)


def _require_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"Generated state {field} must be a non-empty string")
    return value


def _require_identifier(value: Any, field: str) -> str:
    value = _require_string(value, field)
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Generated state {field} is not a concrete identifier")
    return value


def _app_url(app_name: str, target: TargetConfig) -> str:
    return f"https://{app_name}-{target.workspace_id}.{target.apps_domain}"


def _mcp_url(app_name: str, target: TargetConfig) -> str:
    return f"{_app_url(app_name, target)}/mcp"


def _require_exact_url(value: Any, field: str, expected: str) -> str:
    value = _require_string(value, field)
    parsed = urlparse(value)
    if (
        value != expected
        or parsed.scheme != "https"
        or parsed.query
        or parsed.fragment
        or parsed.params
    ):
        raise ValueError(f"Generated state {field} is not the exact target URL")
    return value


def _validate_metadata(target: TargetConfig, data: dict[str, Any]) -> None:
    for field in _OWNED_METADATA & set(data):
        if field.endswith("_url"):
            app_name = {
                "presenter_app_url": target.presenter_app_name,
                "storetime_app_url": target.storetime_app_name,
                "opstask_app_url": target.opstask_app_name,
            }[field]
            _require_exact_url(data[field], field, _app_url(app_name, target))
        else:
            _require_identifier(data[field], field)


def _validate_lakebase(target: TargetConfig, lakebase: Any) -> dict[str, Any]:
    if not isinstance(lakebase, dict) or lakebase.get("validated") is not True:
        raise ValueError("Generated Lakebase state must be a validated object")
    disabled = {"validated", "enabled", "schema"}
    enabled = disabled | {"project", "branch", "endpoint", "database", "host"}
    if target.key == "fevm":
        enabled |= {"workspace_id", "workspace_host"}
    if lakebase.get("enabled") is False:
        if set(lakebase) != disabled or lakebase["schema"] != "gurary_bobabricks_app":
            raise ValueError("Disabled Lakebase state must name only the isolated schema")
        return lakebase
    if lakebase.get("enabled") is not True or set(lakebase) != enabled:
        raise ValueError("Enabled Lakebase state must be complete")
    for field in ("project", "branch", "endpoint", "database", "schema"):
        _require_identifier(lakebase[field], f"lakebase.{field}")
    if lakebase["schema"] != "gurary_bobabricks_app":
        raise ValueError("Lakebase state must use the isolated schema")
    host = _require_string(lakebase["host"], "lakebase.host")
    if not _LAKEBASE_HOST.fullmatch(host):
        raise ValueError("Lakebase host is not a valid Lakebase hostname")
    if target.key == "fevm":
        if (
            lakebase["workspace_id"] != target.workspace_id
            or lakebase["workspace_host"] != target.host
        ):
            raise ValueError("FEVM Lakebase state is not target-affine")
    if target.key == "field_eng":
        expected = {
            "project": target.lakebase_project, "branch": target.lakebase_branch,
            "endpoint": target.lakebase_endpoint, "database": target.lakebase_database,
        }
        if any(lakebase[field] != value for field, value in expected.items()):
            raise ValueError("Field Eng Lakebase state is not target-safe")
    return lakebase


def _validate_generated_state(target: TargetConfig, data: dict[str, Any]) -> dict[str, Any]:
    allowed = {"target", "mlflow_experiment_name", "lakebase"} | _OWNED_METADATA
    if target.key == "field_eng":
        allowed |= {"warehouse_id", "genie_space_id", "storetime_mcp_url", "opstask_mcp_url"}
    unknown = set(data) - allowed
    if unknown:
        raise ValueError(f"Unknown generated-state fields: {sorted(unknown)}")
    if data.get("target") != target.key:
        raise ValueError(f"Generated state is not for target {target.key}")
    _validate_metadata(target, data)

    experiment = _require_string(data.get("mlflow_experiment_name"), "mlflow_experiment_name")
    if not experiment.startswith("/") or "gurary" not in experiment:
        raise ValueError("Generated state experiment must be an owned MLflow path")
    _validate_lakebase(target, data.get("lakebase"))

    if target.key == "field_eng":
        missing = _FIELD_REQUIRED - set(data)
        if missing:
            raise ValueError(f"Field Eng state is not render-ready: {sorted(missing)}")
        _require_identifier(data["warehouse_id"], "warehouse_id")
        _require_identifier(data["genie_space_id"], "genie_space_id")
        _require_exact_url(data["storetime_mcp_url"], "storetime_mcp_url", _mcp_url(target.storetime_app_name, target))
        _require_exact_url(data["opstask_mcp_url"], "opstask_mcp_url", _mcp_url(target.opstask_app_name, target))
    return data


def _load_generated_state(target: TargetConfig) -> dict[str, Any]:
    path = STATE_ROOT / f"{target.key}.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid generated state: {path}") from error
    if not isinstance(data, dict):
        raise ValueError(f"Expected generated-state object in {path}")
    return _validate_generated_state(target, data)


def _lakebase_environment(target: TargetConfig, state: dict[str, Any]) -> list[tuple[str, str]]:
    lakebase = state.get("lakebase")
    if lakebase is None:
        return []
    if lakebase["enabled"] is False:
        return [("BOBABRICKS_DISABLE_LAKEBASE", "1")]
    return [
        ("LAKEBASE_ENDPOINT", f"projects/{lakebase['project']}/branches/{lakebase['branch']}/endpoints/{lakebase['endpoint']}"),
        ("LAKEBASE_AUTOSCALING_PROJECT", lakebase["project"]),
        ("LAKEBASE_AUTOSCALING_BRANCH", lakebase["branch"]),
        ("LAKEBASE_HOST", lakebase["host"]),
        ("LAKEBASE_DATABASE_NAME", lakebase["database"]),
        ("LAKEBASE_MEMORY_SCHEMA", lakebase["schema"]),
        ("LAKEBASE_AGENT_MEMORY_SCHEMA", lakebase["schema"]),
    ]


def _presenter_environment(target: TargetConfig, state: DemoState, generated: dict[str, Any]) -> list[tuple[str, str]]:
    warehouse = generated.get("warehouse_id", target.warehouse_id or target.warehouse_name)
    genie = generated.get("genie_space_id", target.genie_space_id or target.genie_space_name)
    if not isinstance(warehouse, str) or not isinstance(genie, str):
        raise ValueError(f"Target {target.key} is missing a deployment identifier")
    storetime_url = generated.get("storetime_mcp_url", target.storetime_mcp_url or _mcp_url(target.storetime_app_name, target))
    opstask_url = generated.get("opstask_mcp_url", target.opstask_mcp_url or _mcp_url(target.opstask_app_name, target))
    experiment = generated.get("mlflow_experiment_name", f"/Shared/{target.presenter_app_name}-uc")
    environment = [
        ("CHAT_APP_PORT", "3000"), ("AGENT_MODEL", "databricks-gpt-5"),
        ("MLFLOW_TRACKING_URI", "databricks"), ("MLFLOW_REGISTRY_URI", "databricks-uc"),
        ("MLFLOW_EXPERIMENT_NAME", experiment), ("MLFLOW_TRACE_UC_CATALOG", target.catalog),
        ("MLFLOW_TRACE_UC_SCHEMA", target.trace_schema), ("MLFLOW_TRACE_UC_TABLE_PREFIX", "gurary_bobabricks"),
        ("GENIE_SPACE_ID", genie), ("ENABLE_GENIE_MCP", "true"), ("DISABLE_MCP_SERVERS", "false"),
        ("DATABRICKS_WAREHOUSE_ID", warehouse), ("DATABRICKS_CATALOG", target.catalog),
        ("DATABRICKS_SCHEMA", target.data_schema), ("DATABRICKS_TABLE_PREFIX", target.table_prefix),
        ("SHARED_MCP_READ_ONLY", str(target.shared_mcp_read_only).lower()),
        ("STORETIME_APP_NAME", target.storetime_app_name), ("STORETIME_MCP_URL", storetime_url),
        ("OPSTASK_APP_NAME", target.opstask_app_name), ("OPSTASK_MCP_URL", opstask_url),
    ]
    environment.extend(_lakebase_environment(target, generated))
    if state.confluence_enabled:
        environment.extend([
            ("CONFLUENCE_MCP_ENABLED", "true"),
            ("ATLASSIAN_MCP_URL", f"{target.host.rstrip('/')}/api/2.0/mcp/external/{target.confluence_connection}"),
        ])
    return environment


def _mcp_environment(target: TargetConfig, generated: dict[str, Any]) -> list[tuple[str, str]]:
    warehouse = generated.get("warehouse_id", target.warehouse_id or target.warehouse_name)
    if not isinstance(warehouse, str):
        raise ValueError(f"Target {target.key} is missing a warehouse identifier")
    return [
        ("DATABRICKS_WAREHOUSE_ID", warehouse), ("DATABRICKS_CATALOG", target.catalog),
        ("DATABRICKS_SCHEMA", target.data_schema), ("DATABRICKS_TABLE_PREFIX", target.table_prefix),
    ]


def _write_manifest(path: Path, environment: list[tuple[str, str]]) -> None:
    manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or "command" not in manifest:
        raise ValueError(f"Invalid renderer-owned manifest: {path}")
    manifest["env"] = [{"name": name, "value": value} for name, value in environment]
    path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")


def _ignored_names(directory: Path, names: list[str]) -> set[str]:
    relative = directory.relative_to(ROOT)
    ignored = {name for name in names if name in {".git", ".build", ".venv", ".superpowers", "__pycache__", ".pytest_cache"}}
    if relative == Path("deploy"):
        ignored.update({"state", "inventory"})
    if relative == Path("."):
        # Operational doc: intentionally references both lanes' identifiers, so
        # it is excluded from the deployed build rather than scanned/exempted.
        ignored.update({"RUNBOOK_GURARY.md"})
    return ignored


def _copy_source(destination: Path) -> None:
    for directory, directories, files in os.walk(ROOT, followlinks=False):
        path = Path(directory)
        ignored = _ignored_names(path, directories + files)
        directories[:] = [name for name in directories if name not in ignored]
        if any(name not in ignored and (path / name).is_symlink() for name in directories + files):
            raise ValueError(f"Refusing source symlink below {path}")
    shutil.copytree(ROOT, destination, ignore=lambda directory, names: _ignored_names(Path(directory), names))


def _all_declared_identifiers() -> set[str]:
    values = set()
    for key in ("fevm", "field_eng"):
        target = load_target(key)
        values.update(
            str(value)
            for value in (
                target.presenter_app_name, target.catalog, target.data_schema, target.trace_schema,
                target.warehouse_id, target.warehouse_name, target.genie_space_id, target.genie_space_name,
                target.storetime_app_name, target.storetime_mcp_url, target.opstask_app_name,
                target.opstask_mcp_url, target.confluence_connection,
            )
            if value
        )
    return values


def _line_contains_identifier(line: str, identifier: str) -> bool:
    if identifier in _AMBER_IDENTIFIERS:
        return identifier in line
    return re.search(rf"(?<![\w-]){re.escape(identifier)}(?![\w-])", line) is not None


def _safe_reference_key(relative: Path, identifier: str, line: str) -> tuple[Path, str, str]:
    return relative, identifier, sha256(line.encode("utf-8")).hexdigest()


def _is_safe_reference(relative: Path, identifier: str, line: str, count: int) -> bool:
    if not _line_contains_identifier(line, identifier):
        return False
    policy_key = _safe_reference_key(relative, identifier, line)
    allowed_count = _SAFE_REFERENCE_LINES.get(policy_key, 0)
    return count <= allowed_count


def _scan_rendered_text(root: Path, target: TargetConfig) -> None:
    target_values = {
        str(value)
        for value in (
            target.presenter_app_name, target.catalog, target.data_schema, target.trace_schema,
            target.warehouse_id, target.warehouse_name, target.genie_space_id, target.genie_space_name,
            target.storetime_app_name, target.storetime_mcp_url, target.opstask_app_name,
            target.opstask_mcp_url, target.confluence_connection,
        )
        if value
    }
    foreign = _all_declared_identifiers() - target_values
    forbidden = _AMBER_IDENTIFIERS | foreign
    reference_counts: Counter[tuple[Path, str, str]] = Counter()
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        content = path.read_bytes()
        if b"\0" in content:
            continue
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            continue
        relative = path.relative_to(root)
        lines = text.splitlines()
        for line in lines:
            for identifier in forbidden:
                if not _line_contains_identifier(line, identifier):
                    continue
                policy_key = _safe_reference_key(relative, identifier, line)
                reference_counts[policy_key] += 1
                if not _is_safe_reference(
                    relative, identifier, line, reference_counts[policy_key]
                ):
                    raise ValueError(f"Unsafe rendered identifier in {relative}: {identifier}")


def _atomic_swap(first: Path, second: Path) -> None:
    if sys.platform != "darwin":
        raise RuntimeError("Atomic legacy-build migration requires macOS renamex_np")
    renamex_np = ctypes.CDLL(None, use_errno=True).renamex_np
    renamex_np.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
    renamex_np.restype = ctypes.c_int
    if renamex_np(os.fsencode(first), os.fsencode(second), 0x00000002) != 0:
        raise OSError(ctypes.get_errno(), f"Could not atomically swap {first} and {second}")


def _install_pointer(destination: Path, target_root: Path, version_path: Path) -> None:
    temporary_link = target_root / f".{destination.name}-{uuid4().hex}"
    temporary_link.symlink_to(Path(".versions") / version_path.name)
    if destination.exists() and not destination.is_symlink():
        _atomic_swap(destination, temporary_link)
        os.replace(temporary_link, version_path.parent / f"legacy-{destination.name}-{uuid4().hex}")
    elif destination.is_symlink():
        _atomic_swap(destination, temporary_link)
    else:
        os.replace(temporary_link, destination)


def render_deployment(target_key: str, state_key: str) -> Path:
    """Render one target/state deployment under ``.build/{target}/{state}``."""
    target = load_target(target_key)
    state = load_state(state_key)
    generated = _load_generated_state(target)
    target_root = BUILD_ROOT / target.key
    destination = target_root / state.key
    versions_root = target_root / ".versions"
    target_root.mkdir(parents=True, exist_ok=True)
    versions_root.mkdir(exist_ok=True)
    version_name = f"{state.key}-{uuid4().hex}"
    with tempfile.TemporaryDirectory(prefix=f".{state.key}-render-", dir=versions_root) as temporary:
        staged_root = Path(temporary) / "deployment"
        _copy_source(staged_root)
        presenter_environment = _presenter_environment(target, state, generated)
        for relative in ROOT_MANIFESTS:
            _write_manifest(staged_root / relative, presenter_environment)
        mcp_environment = _mcp_environment(target, generated)
        for relative in MCP_MANIFESTS:
            _write_manifest(staged_root / relative, mcp_environment)
        _scan_rendered_text(staged_root, target)
        version_path = versions_root / version_name
        os.replace(staged_root, version_path)
        _install_pointer(destination, target_root, version_path)
    return destination
