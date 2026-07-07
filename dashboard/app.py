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
import asyncio
import os
import signal
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
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

# Serve static assets from build if built
dist_dir = DASHBOARD_DIR / "dist"
dist_assets = dist_dir / "assets"
dist_assets.mkdir(parents=True, exist_ok=True)
app.mount("/assets", StaticFiles(directory=str(dist_assets)), name="assets")


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
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                    self.process.wait()
            except (ProcessLookupError, PermissionError):
                pass

            # Sync run status to database
            try:
                brain = Brain(str(self.project_dir))
                state = StateManager(brain.orchestrator_db)
                conn = state._connect()
                cursor = conn.cursor()
                cursor.execute("SELECT run_id, status FROM runs ORDER BY run_id DESC LIMIT 1")
                row = cursor.fetchone()
                if row and row[1] == 'running':
                    cursor.execute(
                        "UPDATE runs SET status = ?, finished_at = ? WHERE run_id = ?",
                        ('stopped', datetime.now().isoformat(), row[0])
                    )
                    conn.commit()
                conn.close()
            except Exception as e:
                print(f"Error marking run as stopped: {e}")

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


def _sync_orphan_runs(project_id: str, project_dir: Path) -> None:
    """If the runner says it's not running but the database still says 'running',
    then it was interrupted. Mark it as 'interrupted'."""
    runner = _runner_for(project_id, project_dir)
    status = runner.status()
    if not status['running']:
        try:
            brain = Brain(str(project_dir))
            state = StateManager(brain.orchestrator_db)
            conn = state._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT run_id, status FROM runs ORDER BY run_id DESC LIMIT 1")
            row = cursor.fetchone()
            if row and row[1] == 'running':
                cursor.execute(
                    "UPDATE runs SET status = ?, finished_at = ? WHERE run_id = ?",
                    ('interrupted', datetime.now().isoformat(), row[0])
                )
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error syncing orphan runs: {e}")


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


@app.get("/api/hub/browse")
def browse_directory(path: Optional[str] = None):
    if path:
        curr = Path(path).expanduser().resolve()
    else:
        curr = Path.home().resolve()
        
    if not curr.exists() or not curr.is_dir():
        curr = Path.home().resolve()
        
    directories = []
    try:
        for entry in curr.iterdir():
            if entry.is_dir() and not entry.name.startswith('.'):
                directories.append({
                    'name': entry.name,
                    'path': str(entry.resolve())
                })
    except PermissionError:
        pass
        
    directories.sort(key=lambda d: d['name'].lower())
    
    return {
        'current_path': str(curr),
        'parent_path': str(curr.parent) if curr.parent != curr else None,
        'directories': directories
    }


@app.get("/api/projects/{project_id}/state")
def project_state(project_id: str, after: int = 0):
    entry = _project_or_404(project_id)
    registry.touch(project_id)
    project_dir = Path(entry['path'])
    _sync_orphan_runs(project_id, project_dir)
    brain = Brain(str(project_dir))
    state = StateManager(brain.orchestrator_db)
    payload = state.get_dashboard_state(events_after=after)
    payload['swarm'] = brain.swarm_snapshot()
    payload['runner'] = _runner_for(project_id, project_dir).status()
    payload['project'] = entry
    payload['task_confidence'] = compute_all_confidences(payload['dag'], payload['tasks'])
    return payload


@app.websocket("/api/projects/{project_id}/ws")
async def project_websocket(websocket: WebSocket, project_id: str):
    await websocket.accept()
    entry = _project_or_404(project_id)
    project_dir = Path(entry['path'])
    
    # Sync orphan runs on connection open
    _sync_orphan_runs(project_id, project_dir)
    
    brain = Brain(str(project_dir))
    state = StateManager(brain.orchestrator_db)
    
    last_event_id = 0
    
    try:
        while True:
            runner = _runner_for(project_id, project_dir)
            runner_status = runner.status()
            
            # Periodically sync orphans if not running (in case main.py exited)
            if not runner_status['running']:
                _sync_orphan_runs(project_id, project_dir)
                
            payload = state.get_dashboard_state(events_after=last_event_id)
            
            if payload['events']:
                last_event_id = max(e['event_id'] for e in payload['events'])
                
            payload['swarm'] = brain.swarm_snapshot()
            payload['runner'] = runner_status
            payload['project'] = entry
            payload['task_confidence'] = compute_all_confidences(payload['dag'], payload['tasks'])
            
            await websocket.send_json(payload)
            
            if runner_status['running']:
                await asyncio.sleep(0.2)
            else:
                await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WS error: {e}")
        try:
            await websocket.close()
        except Exception:
            pass


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
    dist_index = DASHBOARD_DIR / "dist" / "index.html"
    if dist_index.exists():
        return FileResponse(str(dist_index))
    return FileResponse(str(DASHBOARD_DIR / "index.html"))


@app.get("/{fallback_path:path}")
def index_fallback(fallback_path: str = ""):
    if fallback_path.startswith("api/") or fallback_path.startswith("assets/"):
        raise HTTPException(status_code=404)
    dist_index = DASHBOARD_DIR / "dist" / "index.html"
    if dist_index.exists():
        return FileResponse(str(dist_index))
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
