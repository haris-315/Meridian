#!/usr/bin/env python3
"""Meridian dashboard server - pure stdlib visual wrapper around the orchestrator.

Usage:
    python3 dashboard/server.py [project_dir] [--port 8787]

Serves a live dashboard for the given project directory (default: cwd):
  GET  /            the dashboard page
  GET  /api/state   full snapshot: run, DAG, tasks, agents, events, swarm, runner
  POST /api/run     {"goal": "..."} - start an orchestrator run in project_dir
  POST /api/stop    terminate the running orchestration (whole process group)

The orchestrator subprocess runs with --non-interactive and logs to
<project>/.meridian/run.log; all dashboard state is read from the project's
.meridian/ brain, so the page works for runs started from a terminal too.
"""
import json
import os
import signal
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

DASHBOARD_DIR = Path(__file__).resolve().parent
ORCHESTRATOR_DIR = DASHBOARD_DIR.parent / "orchestrator"
sys.path.insert(0, str(ORCHESTRATOR_DIR))

from brain import Brain  # noqa: E402
from state import StateManager  # noqa: E402
from confidence import compute_all_confidences  # noqa: E402


class Runner:
    """Owns at most one orchestrator subprocess for the project."""

    def __init__(self, project_dir: Path):
        self.project_dir = project_dir
        self.process: subprocess.Popen = None
        self.goal: str = ""
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
            # New session so /api/stop can kill the whole tree (claude -p children).
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


def make_handler(project_dir: Path, runner: Runner):
    brain = Brain(str(project_dir))
    index_html = (DASHBOARD_DIR / "index.html").read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # silence per-request noise
            pass

        def _send(self, code: int, body: bytes, content_type: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _send_json(self, payload: dict, code: int = 200) -> None:
            self._send(code, json.dumps(payload).encode(), "application/json")

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path in ("/", "/index.html"):
                self._send(200, index_html, "text/html; charset=utf-8")
            elif parsed.path == "/api/state":
                after = int(parse_qs(parsed.query).get("after", ["0"])[0])
                # A fresh StateManager per request keeps SQLite access
                # single-threaded-per-connection (handler threads).
                state = StateManager(brain.orchestrator_db)
                payload = state.get_dashboard_state(events_after=after)
                payload['swarm'] = brain.swarm_snapshot()
                payload['runner'] = runner.status()
                payload['project_dir'] = str(project_dir)
                payload['task_confidence'] = compute_all_confidences(payload['dag'], payload['tasks'])
                self._send_json(payload)
            else:
                self._send(404, b"not found", "text/plain")

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError:
                self._send_json({'ok': False, 'error': 'invalid JSON'}, 400)
                return

            if self.path == "/api/run":
                goal = (body.get("goal") or "").strip()
                if not goal:
                    self._send_json({'ok': False, 'error': 'goal is required'}, 400)
                    return
                result = runner.start(goal)
                self._send_json(result, 200 if result['ok'] else 409)
            elif self.path == "/api/stop":
                result = runner.stop()
                self._send_json(result, 200 if result['ok'] else 409)
            else:
                self._send(404, b"not found", "text/plain")

    return Handler


def main() -> None:
    args = [a for a in sys.argv[1:]]
    port = 8787
    if "--port" in args:
        i = args.index("--port")
        port = int(args[i + 1])
        del args[i:i + 2]
    project_dir = Path(args[0]).resolve() if args else Path.cwd()

    if not project_dir.is_dir():
        print(f"error: {project_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    runner = Runner(project_dir)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(project_dir, runner))
    print(f"Meridian dashboard for {project_dir}")
    print(f"  -> http://127.0.0.1:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")


if __name__ == "__main__":
    main()
