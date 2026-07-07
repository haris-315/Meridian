#!/usr/bin/env python3
"""Meridian Hub - a single long-lived FastAPI server that manages orchestrator
runs for any number of projects, VS-Code-style: a landing page listing known
projects (open an existing folder, or scaffold a new one), and a per-project
view with a prompt box, live run state, and inspectable run history.

Usage:
    .venv/bin/python dashboard/app.py [--port 8787]
    # or: .venv/bin/uvicorn app:app --app-dir dashboard --port 8787

Endpoints:
    GET    /                                    the SPA shell
    GET    /api/hub/projects                    list known projects
    POST   /api/hub/projects                    open/register an existing folder
    POST   /api/hub/scaffold                    create a new empty project
    DELETE /api/hub/projects/{id}                forget a project (registry only)
    GET    /api/projects/{id}/state?after=N     live snapshot (polled)
    POST   /api/projects/{id}/run               start an orchestration
    POST   /api/projects/{id}/stop              stop the running orchestration
    GET    /api/projects/{id}/runs              run history for this project
    GET    /api/projects/{id}/runs/{run_id}     full detail of one past run
"""
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

DASHBOARD_DIR = Path(__file__).resolve().parent
ORCHESTRATOR_DIR = DASHBOARD_DIR.parent / "orchestrator"
sys.path.insert(0, str(ORCHESTRATOR_DIR))
sys.path.insert(0, str(DASHBOARD_DIR))

from brain import Brain  # noqa: E402
from state import StateManager  # noqa: E402
from confidence import compute_all_confidences  # noqa: E402
from hub_registry import ProjectRegistry, scaffold_project  # noqa: E402

app = FastAPI(title="Meridian Hub")
registry = ProjectRegistry()


class OpenProjectRequest(BaseModel):
    path: str
    name: Optional[str] = None


class ScaffoldRequest(BaseModel):
    path: str
    name: str


class RunRequest(BaseModel):
    goal: str


class Runner:
    """Owns at most one orchestrator subprocess for one project."""

    def __init__(self, project_dir: Path):
        self.project_dir = project_dir
        self.process: Optional[subprocess.Popen] = None
        self.goal = ""
        self.lock = threading.Lock()

    def status(self) -> dict:
        with self.lock:
            running = self.process is not None and self.process.poll() is None
            return {
                'running': running,
                'pid': self.process.pid if running else None,
                'goal': self.goal if running else "",
                'exit_code': (None if running or self.process is None
                              else self.process.returncode),
            }

    def start(self, goal: str) -> dict:
        with self.lock:
            if self.process is not None and self.process.poll() is None:
                return {'ok': False, 'error': 'A run is already in progress'}

            brain_dir = self.project_dir / ".meridian"
            brain_dir.mkdir(exist_ok=True)
            log_file = open(brain_dir / "run.log", "w")
            self.process = subprocess.Popen(
                [sys.executable, "-u", str(ORCHESTRATOR_DIR / "main.py"), goal, "--non-interactive"],
                cwd=str(self.project_dir),
                stdout=log_file,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
            self.goal = goal
            return {'ok': True, 'pid': self.process.pid}

    def stop(self) -> dict:
        with self.lock:
            if self.process is None or self.process.poll() is not None:
                return {'ok': False, 'error': 'No run in progress'}
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError) as e:
                return {'ok': False, 'error': str(e)}
            return {'ok': True}


_runners: Dict[str, Runner] = {}
_runners_lock = threading.Lock()


def _project_or_404(project_id: str) -> dict:
    entry = registry.get(project_id)
    if not entry:
        raise HTTPException(404, f"Unknown project id: {project_id}")
    return entry


def _runner_for(project_id: str, project_dir: Path) -> Runner:
    with _runners_lock:
        if project_id not in _runners:
            _runners[project_id] = Runner(project_dir)
        return _runners[project_id]


# ------------------------------------------------------------------- hub


@app.get("/api/hub/projects")
def list_projects():
    projects = registry.list()
    for p in projects:
        runner = _runners.get(p['id'])
        p['runner'] = runner.status() if runner else {'running': False, 'pid': None, 'goal': '', 'exit_code': None}
    return {"projects": projects}


@app.post("/api/hub/projects")
def open_project(body: OpenProjectRequest):
    path = Path(body.path).expanduser()
    if not path.is_dir():
        raise HTTPException(400, f"{path} is not a directory")
    entry = registry.add(path, name=body.name)
    return entry


@app.post("/api/hub/scaffold")
def scaffold(body: ScaffoldRequest):
    try:
        path = scaffold_project(Path(body.path), body.name)
    except FileExistsError as e:
        raise HTTPException(400, str(e))
    entry = registry.add(path, name=body.name)
    return entry


@app.delete("/api/hub/projects/{project_id}")
def remove_project(project_id: str):
    registry.remove(project_id)
    return {"ok": True}


# --------------------------------------------------------------- project


@app.get("/api/projects/{project_id}/state")
def project_state(project_id: str, after: int = 0):
    entry = _project_or_404(project_id)
    registry.touch(project_id)
    project_dir = Path(entry['path'])
    brain = Brain(str(project_dir))
    state = StateManager(brain.orchestrator_db)
    payload = state.get_dashboard_state(events_after=after)
    payload['swarm'] = brain.swarm_snapshot()
    payload['runner'] = _runner_for(project_id, project_dir).status()
    payload['project'] = entry
    payload['task_confidence'] = compute_all_confidences(payload['dag'], payload['tasks'])
    return payload


@app.post("/api/projects/{project_id}/run")
def start_run(project_id: str, body: RunRequest):
    entry = _project_or_404(project_id)
    goal = body.goal.strip()
    if not goal:
        raise HTTPException(400, "goal is required")
    runner = _runner_for(project_id, Path(entry['path']))
    result = runner.start(goal)
    if not result['ok']:
        raise HTTPException(409, result['error'])
    return result


@app.post("/api/projects/{project_id}/stop")
def stop_run(project_id: str):
    entry = _project_or_404(project_id)
    runner = _runner_for(project_id, Path(entry['path']))
    result = runner.stop()
    if not result['ok']:
        raise HTTPException(409, result['error'])
    return result


@app.get("/api/projects/{project_id}/runs")
def run_history(project_id: str):
    entry = _project_or_404(project_id)
    brain = Brain(entry['path'])
    state = StateManager(brain.orchestrator_db)
    return {"runs": state.list_runs()}


@app.get("/api/projects/{project_id}/runs/{run_id}")
def run_detail(project_id: str, run_id: int):
    entry = _project_or_404(project_id)
    brain = Brain(entry['path'])
    state = StateManager(brain.orchestrator_db)
    snapshot = state.get_run_snapshot(run_id)
    if not snapshot['run']:
        raise HTTPException(404, f"No run {run_id} in this project")
    snapshot['task_confidence'] = compute_all_confidences(snapshot['dag'], snapshot['tasks'])
    return snapshot


# ---------------------------------------------------------------- static


@app.get("/")
def index():
    return FileResponse(str(DASHBOARD_DIR / "index.html"))


@app.exception_handler(404)
def not_found(request, exc):
    return JSONResponse({"error": str(exc.detail)}, status_code=404)


def main() -> None:
    import uvicorn

    port = 8787
    if "--port" in sys.argv:
        port = int(sys.argv[sys.argv.index("--port") + 1])
    print(f"Meridian Hub -> http://127.0.0.1:{port}")
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")


if __name__ == "__main__":
    main()
