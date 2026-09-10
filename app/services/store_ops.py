from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
DEFAULT_CREATED_TASKS_PATH = Path(os.getenv("OPSTASK_CREATED_TASKS_PATH", "/tmp/bobabricks_created_ops_tasks.json"))

ALLOWED_CATEGORIES = {"sales", "labor", "inventory", "training", "customer_experience"}
SEVERITY_ORDER = {"high": 0, "medium": 1, "low": 2}


@dataclass(frozen=True)
class RiskIssue:
    store_id: str
    store_name: str
    severity: str
    category: str
    evidence: str
    recommended_action: str

    @property
    def store(self) -> str:
        return f"{self.store_id} - {self.store_name}"


@dataclass(frozen=True)
class OpsTaskDraft:
    store_id: str
    title: str
    description: str
    category: str
    severity: str


class LocalDataStore:
    """CSV/JSON-backed implementation of the app's Databricks/MCP data contracts."""

    def __init__(self, data_dir: Path = DATA):
        self.data_dir = data_dir

    def store_metrics(self, region: str) -> list[dict[str, str]]:
        return [row for row in self._load_csv("store_metrics.csv") if row["region"].lower() == region.lower()]

    def schedules(self) -> dict[str, dict[str, str]]:
        return {row["store_id"]: row for row in self._load_csv("schedules.csv")}

    def inventory(self) -> list[dict[str, str]]:
        return self._load_csv("inventory.csv")

    def playbooks(self) -> dict:
        return json.loads((self.data_dir / "confluence_playbooks.json").read_text())

    def existing_tasks(self) -> list[dict]:
        return json.loads((self.data_dir / "ops_tasks.json").read_text())

    def regions(self) -> list[str]:
        return sorted({row["region"] for row in self._load_csv("store_metrics.csv")})

    def _load_csv(self, name: str) -> list[dict[str, str]]:
        with (self.data_dir / name).open(newline="") as handle:
            return list(csv.DictReader(handle))


class OpsTaskClient:
    """Approval-gated local stand-in for the OpsTask Databricks App MCP."""

    def __init__(self, data_store: LocalDataStore, created_tasks_path: Path = DEFAULT_CREATED_TASKS_PATH):
        self.data_store = data_store
        self.created_tasks_path = created_tasks_path

    def list_tasks(self) -> list[dict]:
        return self.data_store.existing_tasks() + self._created_tasks()

    def create_tasks(self, drafts: Iterable[OpsTaskDraft]) -> list[dict]:
        created = self._created_tasks()
        next_number = self._next_task_number(created)
        new_tasks = []

        for draft in drafts:
            task = {
                "task_id": f"OPS-{next_number}",
                "store_id": int(draft.store_id) if draft.store_id.isdigit() else draft.store_id,
                "title": draft.title,
                "description": draft.description,
                "category": draft.category,
                "severity": draft.severity,
                "status": "open",
            }
            next_number += 1
            created.append(task)
            new_tasks.append(task)

        self.created_tasks_path.parent.mkdir(parents=True, exist_ok=True)
        self.created_tasks_path.write_text(json.dumps(created, indent=2))
        return new_tasks

    def _created_tasks(self) -> list[dict]:
        if not self.created_tasks_path.exists():
            return []
        return json.loads(self.created_tasks_path.read_text())

    def _next_task_number(self, created_tasks: list[dict]) -> int:
        numbers = []
        for task in self.data_store.existing_tasks() + created_tasks:
            task_id = str(task.get("task_id", ""))
            if task_id.startswith("OPS-") and task_id[4:].isdigit():
                numbers.append(int(task_id[4:]))
        return max(numbers, default=2200) + 1


class StoreOpsEngine:
    def __init__(self, data_store: LocalDataStore | None = None):
        self.data_store = data_store or LocalDataStore()

    def build_weekly_briefing(self, region: str) -> list[RiskIssue]:
        metrics = self.data_store.store_metrics(region)
        schedules = self.data_store.schedules()
        inventory = self.data_store.inventory()
        playbooks = self.data_store.playbooks()
        targets = playbooks["FY26 Store Operations Goals"]
        target_training = float(targets["training_completion_target_pct"])
        target_wait = float(targets["customer_wait_target_minutes"])

        issues: list[RiskIssue] = []
        metrics_by_store = {row["store_id"]: row for row in metrics}

        for row in metrics:
            store_id = row["store_id"]
            completion = float(row["training_completion_pct"])
            if completion < target_training:
                schedule = schedules.get(store_id, {})
                issues.append(
                    RiskIssue(
                        store_id=store_id,
                        store_name=row["store_name"],
                        severity=self._training_severity(completion, target_training),
                        category="training",
                        evidence=(
                            f"Training completion is {completion:.0f}% versus the FY26 {target_training:.0f}% "
                            f"target; {schedule.get('converted_training_hours', '0')} hours were converted "
                            f"for {schedule.get('conversion_reason') or 'coverage'}."
                        ),
                        recommended_action="Protect training blocks this week and review rush-window staffing coverage.",
                    )
                )

            wait = float(row["avg_wait_minutes"])
            if wait > target_wait:
                issues.append(
                    RiskIssue(
                        store_id=store_id,
                        store_name=row["store_name"],
                        severity="medium" if wait < 8 else "high",
                        category="customer_experience",
                        evidence=(
                            f"Average wait time is {wait:.1f} minutes versus the FY26 "
                            f"{target_wait:.0f}-minute target; customer satisfaction is {row['customer_satisfaction']}."
                        ),
                        recommended_action="Review queue staffing and peak-window service flow.",
                    )
                )

        for row in inventory:
            metric = metrics_by_store.get(row["store_id"])
            if row["stockout_risk"] == "high" and metric:
                issues.append(
                    RiskIssue(
                        store_id=row["store_id"],
                        store_name=metric["store_name"],
                        severity="high",
                        category="inventory",
                        evidence=(
                            f"{row['ingredient']} has {row['on_hand_units']} units on hand against "
                            f"{row['forecast_7d_units']} forecast units and a {row['reorder_eta_days']}-day reorder ETA."
                        ),
                        recommended_action=(
                            "Expedite an inter-store transfer or vendor escalation before the next delivery cutoff."
                        ),
                    )
                )

        return sorted(issues, key=lambda issue: (SEVERITY_ORDER[issue.severity], issue.store_id, issue.category))

    def draft_high_severity_tasks(self, issues: Iterable[RiskIssue], existing_tasks: Iterable[dict]) -> list[OpsTaskDraft]:
        existing_by_store = {
            str(task.get("store_id")): str(task.get("title", "")).lower()
            for task in existing_tasks
            if str(task.get("status", "")).lower() != "closed"
        }
        drafts = []

        for issue in issues:
            if issue.severity != "high":
                continue
            existing_title = existing_by_store.get(issue.store_id, "")
            if issue.category == "training" and "training" in existing_title:
                continue
            if issue.category == "customer_experience" and ("wait" in existing_title or "queue" in existing_title):
                continue

            drafts.append(
                OpsTaskDraft(
                    store_id=issue.store_id,
                    title=f"{issue.category.replace('_', ' ').title()} risk follow-up for Store {issue.store_id}",
                    description=f"{issue.evidence} Recommended action: {issue.recommended_action}",
                    category=issue.category,
                    severity=issue.severity,
                )
            )

        return drafts

    def render_briefing_markdown(self, region: str, issues: Iterable[RiskIssue], drafts: Iterable[OpsTaskDraft]) -> str:
        issue_list = list(issues)
        draft_list = list(drafts)
        lines = [f"### Weekly {region} Regional Risk Briefing", ""]

        if not issue_list:
            lines.append("No regional risks exceeded the configured thresholds.")
            return "\n".join(lines)

        for issue in issue_list:
            lines.extend(
                [
                    f"- **Store:** {issue.store}",
                    f"  - **Severity:** {issue.severity}",
                    f"  - **Category:** `{issue.category}`",
                    f"  - **Evidence:** {issue.evidence}",
                    f"  - **Recommended action:** {issue.recommended_action}",
                    "",
                ]
            )

        if draft_list:
            stores = ", ".join(draft.store_id for draft in draft_list)
            lines.append(
                f"I found {len(draft_list)} high-severity risks without duplicate open tasks. "
                f"Approve below to create OpsTask tickets for stores {stores}."
            )
        else:
            lines.append("No new high-severity OpsTask tickets are recommended because matching open tasks already exist.")

        return "\n".join(lines)

    def tool_status(self) -> list[dict[str, str]]:
        # Confluence/Atlassian is enabled only by the live upgrade and is not
        # surfaced in the baseline demo tool listing.
        tools = [
            {"tool": "Genie Agent", "status": "configured", "purpose": "governed store metrics"},
            {"tool": "StoreTime", "status": "configured", "purpose": "schedules and training conversion"},
            {
                "tool": "OpsTask",
                "status": "configured",
                "purpose": "list follow-up tickets; create only after approval",
            },
        ]
        if os.getenv("CONFLUENCE_MCP_ENABLED", "false").strip().lower() in {"1", "true", "on", "yes"}:
            tools.append(
                {
                    "tool": "Confluence",
                    "status": "configured",
                    "purpose": "FY26 goals and operating playbooks",
                }
            )
        return tools

    @staticmethod
    def _training_severity(completion_pct: float, target_pct: float) -> str:
        gap = target_pct - completion_pct
        if gap >= 10:
            return "high"
        if gap > 0:
            return "medium"
        return "low"
