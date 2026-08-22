import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from gza_server.app import create_app

from gza.db import SqliteTaskStore


def _client(store: SqliteTaskStore, *, project_dir: Path | None = None) -> TestClient:
    return TestClient(create_app(store_factory=lambda: store, project_dir=project_dir))


def test_implement_task_detail_renders_metadata_prompt_and_full_json(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add(
        "## Ship safely\n\nUse **targeted checks** before release.",
        task_type="implement",
        tags=("backend", "release"),
        branch="feature/task-detail",
    )
    task.status = "completed"
    task.started_at = datetime(2026, 8, 18, 10, 0, tzinfo=UTC)
    task.completed_at = datetime(2026, 8, 18, 10, 2, tzinfo=UTC)
    task.duration_seconds = 120.5
    store.update(task)
    stored = store.get(task.id or "")
    assert stored is not None
    client = _client(store)

    response = client.get(f"/tasks/{task.id}")
    api_response = client.get(f"/api/tasks/{task.id}")

    assert response.status_code == 200
    assert f"<h1>{task.id}</h1>" in response.text
    assert "implement" in response.text
    assert 'class="status status-completed"' in response.text
    assert '<span class="tag">backend</span>' in response.text
    assert "feature/task-detail" in response.text
    assert "2026-08-18 10:00:00 UTC" in response.text
    assert "2026-08-18 10:02:00 UTC" in response.text
    assert "120.5 seconds" in response.text
    assert "<h2>Ship safely</h2>" in response.text
    assert "Use <strong>targeted checks</strong> before release." in response.text
    assert "<h2>Output</h2>" not in response.text

    assert api_response.status_code == 200
    record = api_response.json()
    assert record["id"] == task.id
    assert record["project_id"] == "server-test"
    assert record["detail_url"] == f"/projects/server-test/tasks/{task.id}"
    assert record["type"] == "implement"
    assert record["task_type"] == "implement"
    assert record["prompt"] == task.prompt
    assert record["tags"] == ["backend", "release"]
    assert record["branch"] == "feature/task-detail"
    assert record["duration_seconds"] == 120.5
    assert record["updated_at"] == stored.updated_at.isoformat().replace("+00:00", "Z")
    assert record["plan_content"] is None


def test_plan_task_detail_renders_cli_plan_content_below_prompt(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    plan = store.add("# Planning prompt\n\nDefine the rollout.", task_type="plan")
    plan_content = "## Rollout plan\n\n1. Ship canary\n2. Measure results"
    store.mark_completed(plan, output_content=plan_content)
    client = _client(store, project_dir=tmp_path)

    response = client.get(f"/tasks/{plan.id}")
    api_response = client.get(f"/api/tasks/{plan.id}")

    assert response.status_code == 200
    assert "<h1>Planning prompt</h1>" in response.text
    assert "<h2>Output</h2>" in response.text
    assert "<h2>Rollout plan</h2>" in response.text
    assert "<li>Ship canary</li>" in response.text
    assert response.text.index("Planning prompt") < response.text.index("Rollout plan")
    assert api_response.status_code == 200
    assert api_response.json()["output_content"] == plan_content
    assert api_response.json()["plan_content"] == plan_content


def test_task_detail_links_direct_parents_and_children(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    based_on_parent = store.add("Plan parent", task_type="plan")
    dependency_parent = store.add("Dependency parent", task_type="implement")
    task = store.add(
        "Lineage center",
        task_type="implement",
        based_on=based_on_parent.id,
        depends_on=dependency_parent.id,
    )
    based_on_child = store.add("Review child", task_type="review", based_on=task.id)
    dependency_child = store.add("Dependent child", task_type="implement", depends_on=task.id)
    client = _client(store)

    response = client.get(f"/tasks/{task.id}")
    record = client.get(f"/api/tasks/{task.id}").json()

    assert response.status_code == 200
    for related in (based_on_parent, dependency_parent, based_on_child, dependency_child):
        assert f'href="/projects/server-test/tasks/{related.id}"' in response.text
    assert {parent["id"] for parent in record["parents"]} == {
        based_on_parent.id,
        dependency_parent.id,
    }
    assert {child["id"] for child in record["children"]} == {
        based_on_child.id,
        dependency_child.id,
    }
    assert client.get(f"/tasks/{based_on_child.id}").status_code == 200


def test_unknown_task_detail_returns_clean_html_and_json_404(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    client = _client(store)

    response = client.get("/tasks/srv-999")
    api_response = client.get("/api/tasks/srv-999")

    assert response.status_code == 404
    assert "<h1>Task not found</h1>" in response.text
    assert "srv-999" in response.text
    assert 'href="/tasks"' in response.text
    assert api_response.status_code == 404
    assert api_response.json() == {"detail": "Task srv-999 not found"}


def test_task_detail_resolves_task_from_another_shared_project(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    first_store = SqliteTaskStore(db_path, prefix="alpha", project_id="alpha")
    second_store = SqliteTaskStore(db_path, prefix="beta", project_id="beta")
    first_store.add("Alpha task", task_type="implement")
    beta = second_store.add("## Beta task", task_type="implement")

    response = _client(first_store).get(f"/tasks/{beta.id}")

    assert response.status_code == 200
    assert f"<h1>{beta.id}</h1>" in response.text
    assert "<h2>Beta task</h2>" in response.text


def test_colliding_task_ids_require_project_qualification_and_keep_lineage_scoped(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    alpha_store = SqliteTaskStore(db_path, prefix="same", project_id="alpha")
    beta_store = SqliteTaskStore(db_path, prefix="same", project_id="beta")
    alpha_parent = alpha_store.add("Alpha parent", task_type="plan")
    alpha = alpha_store.add("Alpha center", based_on=alpha_parent.id)
    alpha_child = alpha_store.add("Alpha child", depends_on=alpha.id)
    beta_parent = beta_store.add("Beta parent", task_type="plan")
    beta = beta_store.add("Beta center", based_on=beta_parent.id)
    beta_child = beta_store.add("Beta child", depends_on=beta.id)
    assert alpha.id == beta.id
    client = _client(alpha_store)

    bare_html = client.get(f"/tasks/{alpha.id}")
    bare_json = client.get(f"/api/tasks/{alpha.id}")
    alpha_html = client.get(f"/projects/alpha/tasks/{alpha.id}")
    alpha_json = client.get(f"/api/projects/alpha/tasks/{alpha.id}")
    beta_html = client.get(f"/projects/beta/tasks/{beta.id}")
    beta_json = client.get(f"/api/projects/beta/tasks/{beta.id}")
    task_list = client.get("/tasks").text

    assert bare_html.status_code == 409
    assert "Ambiguous task ID" in bare_html.text
    assert bare_json.status_code == 409
    assert "ambiguous across projects: alpha, beta" in bare_json.json()["detail"]
    assert alpha_html.status_code == beta_html.status_code == 200
    assert "Alpha center" in alpha_html.text
    assert "Beta center" not in alpha_html.text
    assert "Beta center" in beta_html.text
    assert "Alpha center" not in beta_html.text
    assert alpha_json.json()["project_id"] == "alpha"
    assert beta_json.json()["project_id"] == "beta"
    assert {item["id"] for item in alpha_json.json()["parents"]} == {alpha_parent.id}
    assert {item["id"] for item in alpha_json.json()["children"]} == {alpha_child.id}
    assert {item["id"] for item in beta_json.json()["parents"]} == {beta_parent.id}
    assert {item["id"] for item in beta_json.json()["children"]} == {beta_child.id}
    assert all(
        item["project_id"] == "alpha"
        for item in alpha_json.json()["parents"] + alpha_json.json()["children"]
    )
    assert all(
        item["project_id"] == "beta"
        for item in beta_json.json()["parents"] + beta_json.json()["children"]
    )
    assert f'href="/projects/alpha/tasks/{alpha.id}"' in task_list
    assert f'href="/projects/beta/tasks/{beta.id}"' in task_list


def test_cross_project_plan_content_uses_owner_root_for_file_only_and_newer_file(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    server_root = tmp_path / "server-project"
    owner_root = tmp_path / "owner-project"
    server_root.mkdir()
    owner_root.mkdir()
    # Both roots need a real config: the shared database only registers a
    # project root for a configured checkout whose identity matches, and an
    # unregistered root cannot be resolved back to its own plan files.
    (server_root / "gza.yaml").write_text(
        "project_name: Server\nproject_id: server\nproject_prefix: server\ndb_path: ../shared.db\n",
        encoding="utf-8",
    )
    (owner_root / "gza.yaml").write_text(
        "project_name: Owner\nproject_id: owner\nproject_prefix: owner\ndb_path: ../shared.db\n",
        encoding="utf-8",
    )
    server_store = SqliteTaskStore(
        db_path,
        prefix="server",
        project_id="server",
        project_root=server_root,
        project_name="Server",
    )
    owner_store = SqliteTaskStore(
        db_path,
        prefix="owner",
        project_id="owner",
        project_root=owner_root,
        project_name="Owner",
    )
    report_dir = Path("plans")
    (server_root / report_dir).mkdir()
    (owner_root / report_dir).mkdir()

    file_only = owner_store.add("File-only plan", task_type="plan")
    file_only.report_file = "plans/file-only.md"
    file_only.status = "completed"
    file_only.completed_at = datetime.now(UTC)
    owner_store.update(file_only)
    (owner_root / file_only.report_file).write_text("## Owner file-only content\n")
    (server_root / file_only.report_file).write_text("## Conflicting server file-only content\n")

    newer = owner_store.add("Newer-file plan", task_type="plan")
    newer.report_file = "plans/newer.md"
    owner_store.mark_completed(newer, output_content="## Stale persisted content\n")
    newer = owner_store.get(newer.id or "")
    assert newer is not None and newer.completed_at is not None
    owner_newer_path = owner_root / (newer.report_file or "")
    server_newer_path = server_root / (newer.report_file or "")
    owner_newer_path.write_text("## Owner newer file content\n")
    server_newer_path.write_text("## Conflicting server newer content\n")
    newer_timestamp = (newer.completed_at + timedelta(seconds=5)).timestamp()
    os.utime(owner_newer_path, (newer_timestamp, newer_timestamp))
    os.utime(server_newer_path, (newer_timestamp, newer_timestamp))

    client = _client(server_store, project_dir=server_root)
    cases = (
        (file_only, "Owner file-only content", "Conflicting server file-only content"),
        (newer, "Owner newer file content", "Conflicting server newer content"),
    )
    for task, expected, foreign in cases:
        html = client.get(f"/projects/owner/tasks/{task.id}")
        json_response = client.get(f"/api/projects/owner/tasks/{task.id}")

        assert html.status_code == json_response.status_code == 200
        assert expected in html.text
        assert foreign not in html.text
        assert json_response.json()["plan_content"] == f"## {expected}\n"
        assert foreign not in json_response.json()["plan_content"]


@pytest.mark.parametrize(
    "task_type",
    ["implement", "explore", "review", "improve", "fix", "internal"],
)
def test_non_plan_task_detail_renders_output_without_edit_affordance(
    tmp_path: Path, task_type: str
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    task = store.add("Do the thing.", task_type=task_type)
    store.mark_completed(task, output_content="## Findings\n\nOne blocker remains.")
    client = _client(store, project_dir=tmp_path)

    response = client.get(f"/tasks/{task.id}")
    record = client.get(f"/api/tasks/{task.id}").json()

    assert response.status_code == 200
    assert "<h2>Output</h2>" in response.text
    assert "<h2>Findings</h2>" in response.text
    assert "edit=plan" not in response.text
    assert record["task_output"] == "## Findings\n\nOne blocker remains."
    assert record["plan_content"] is None


REVIEW_REPORT = (
    "## Summary\n\n- Mostly good.\n\n"
    "## Blockers\n\n"
    "### B1 Unhandled API error\n"
    "Evidence: missing branch\n"
    "Open-state citation: `src/api.py:12-18`\n"
    "Impact: crashes\n"
    "Required fix: handle error path\n"
    "Required tests: add regression\n\n"
    "## Follow-Ups\n\n"
    "### F1 Tighten input checks\n"
    "Evidence: optional field assumptions\n"
    "Impact: low risk\n"
    "Recommended follow-up: validate optional values\n"
    "Recommended tests: malformed-input case\n\n"
    "## Questions / Assumptions\n\nNone.\n\n"
    "## Verdict\n\nVerdict: CHANGES_REQUESTED\n"
)


def test_implement_task_shows_findings_from_its_child_review(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    implement = store.add("Build it.", task_type="implement")
    review = store.add("Review it.", task_type="review", based_on=implement.id)
    store.mark_completed(review, output_content=REVIEW_REPORT)
    client = _client(store, project_dir=tmp_path)

    response = client.get(f"/tasks/{implement.id}")
    record = client.get(f"/api/tasks/{implement.id}").json()

    assert response.status_code == 200
    assert "<h2>Review findings</h2>" in response.text
    assert "Unhandled API error" in response.text
    assert "CHANGES_REQUESTED" in response.text

    (summary,) = record["reviews"]
    assert summary["task_id"] == review.id
    assert summary["verdict"] == "CHANGES_REQUESTED"
    assert [finding["severity"] for finding in summary["findings"]] == ["BLOCKER", "FOLLOWUP"]


def test_review_task_does_not_recurse_into_its_own_findings_section(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    review = store.add("Review it.", task_type="review")
    store.mark_completed(review, output_content=REVIEW_REPORT)
    client = _client(store, project_dir=tmp_path)

    response = client.get(f"/tasks/{review.id}")
    record = client.get(f"/api/tasks/{review.id}").json()

    # The review's own report is its Output; it is not also a "review of itself".
    assert "<h2>Output</h2>" in response.text
    assert "<h2>Review findings</h2>" not in response.text
    assert record["reviews"] == []


def test_task_without_reviews_has_no_findings_section(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.db", prefix="srv", project_id="server-test")
    implement = store.add("Build it.", task_type="implement")
    client = _client(store, project_dir=tmp_path)

    response = client.get(f"/tasks/{implement.id}")

    assert "<h2>Review findings</h2>" not in response.text
    assert client.get(f"/api/tasks/{implement.id}").json()["reviews"] == []
