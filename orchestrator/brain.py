"""Per-project persistent brain.

Every project the orchestrator runs against gets a `.meridian/` folder in that
project's directory, holding everything the run needs to remember:

    .meridian/
    ├── orchestrator.db   # Meridian's own state: runs, tasks, agents, events
    ├── memory.db         # ruflo cross-wave memory (CLAUDE_FLOW_DB_PATH)
    ├── ruvector.db       # ruflo vector store (dropped by the ruflo CLI)
    └── .claude-flow/     # ruflo agent/task ledgers

This works because every ruflo subprocess is launched with cwd set to the
`.meridian/` folder itself and CLAUDE_FLOW_DB_PATH=memory.db, so all of the
CLI's cwd-relative artifacts land inside the brain instead of polluting the
project root. All ruflo calls are best-effort: failures are logged to stderr
and swallowed, never raised - a memory/coordination outage must not block the
orchestrator loop.
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

BRAIN_DIR_NAME = ".meridian"
RUFLO_NAMESPACE = "meridian"
RUFLO_TIMEOUT_SECONDS = 30


def is_ruflo_available() -> bool:
    """Fast PATH check for the ruflo CLI. Does not spawn a process."""
    return shutil.which("ruflo") is not None


def parse_trailing_json(stdout: str) -> Optional[dict]:
    """The ruflo CLI's `--format json` still prefixes JSON output with a
    human-readable status message and ASCII table on stdout - the JSON object is
    always the last thing printed, starting at the first '{'. Returns None on
    any parse failure rather than raising, since these calls are all best-effort."""
    start = stdout.find("{")
    if start == -1:
        return None
    try:
        return json.loads(stdout[start:])
    except json.JSONDecodeError:
        return None


class Brain:
    """Project-local persistent storage + the single gateway to the ruflo CLI."""

    def __init__(self, working_dir: str = "."):
        self.working_dir = Path(working_dir).resolve()
        self.dir = self.working_dir / BRAIN_DIR_NAME
        self.dir.mkdir(exist_ok=True)

    @property
    def orchestrator_db(self) -> str:
        return str(self.dir / "orchestrator.db")

    # ------------------------------------------------------------------ ruflo

    def _run_ruflo(self, args: List[str], timeout: int = RUFLO_TIMEOUT_SECONDS
                   ) -> Optional["subprocess.CompletedProcess"]:
        """Run a ruflo CLI command scoped to this project's brain. Returns the
        CompletedProcess, or None if ruflo is missing/timed out (already logged)."""
        if not is_ruflo_available():
            return None
        env = dict(os.environ, CLAUDE_FLOW_DB_PATH="memory.db")
        try:
            return subprocess.run(
                ["ruflo", *args],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(self.dir),
                env=env,
                stdin=subprocess.DEVNULL,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            print(f"[WARN] ruflo {' '.join(args[:2])} failed: {e}", file=sys.stderr)
            return None

    def memory_store(self, key: str, value: Any, namespace: str = RUFLO_NAMESPACE) -> bool:
        """Store a value (dict/list are JSON-encoded) in the project brain."""
        if not isinstance(value, str):
            value = json.dumps(value)
        result = self._run_ruflo([
            "memory", "store", "--key", key, "--value", value,
            "--namespace", namespace, "--upsert",
        ])
        if result is None or result.returncode != 0:
            stderr = (result.stderr.strip() if result else "ruflo unavailable")
            print(f"[WARN] Brain memory store failed for '{key}': {stderr}", file=sys.stderr)
            return False
        return True

    def memory_retrieve(self, key: str, namespace: str = RUFLO_NAMESPACE) -> Optional[str]:
        """Retrieve a stored value, or None if missing/unavailable."""
        result = self._run_ruflo([
            "memory", "retrieve", "--key", key,
            "--namespace", namespace, "--value-only",
        ])
        if result is None or result.returncode != 0 or not result.stdout.strip():
            return None
        return result.stdout.strip()

    def agent_spawn(self, name: str, task_description: str) -> Optional[str]:
        """Register a coordination agent in the brain's ruflo ledger."""
        result = self._run_ruflo([
            "agent", "spawn", "-t", "coder", "-n", name,
            "--task", task_description, "--timeout", "300", "--format", "json",
        ])
        if result is None or result.returncode != 0:
            stderr = (result.stderr.strip() if result else "ruflo unavailable")
            print(f"[WARN] Brain agent spawn failed for '{name}': {stderr}", file=sys.stderr)
            return None
        data = parse_trailing_json(result.stdout)
        return data.get("agentId") if data else None

    def task_create(self, description: str, tags: str) -> Optional[str]:
        """Register a coordination task in the brain's ruflo ledger."""
        result = self._run_ruflo([
            "task", "create", "-t", "implementation", "-d", description,
            "--tags", tags, "--timeout", "300", "--format", "json",
        ])
        if result is None or result.returncode != 0:
            stderr = (result.stderr.strip() if result else "ruflo unavailable")
            print(f"[WARN] Brain task create failed: {stderr}", file=sys.stderr)
            return None
        data = parse_trailing_json(result.stdout)
        return (data.get("taskId") or data.get("id")) if data else None

    def task_assign(self, ruflo_task_id: str, ruflo_agent_id: str) -> None:
        result = self._run_ruflo(["task", "assign", ruflo_task_id, "-a", ruflo_agent_id])
        if result is not None and result.returncode != 0:
            print(f"[WARN] Brain task assign failed for {ruflo_task_id}: "
                  f"{result.stderr.strip()}", file=sys.stderr)

    def swarm_snapshot(self) -> Dict[str, Any]:
        """Read the ruflo agent ledger straight from the brain folder (no CLI
        round-trip) for the dashboard's swarm panel. Returns {} if absent."""
        store = self.dir / ".claude-flow" / "agents" / "store.json"
        try:
            with open(store) as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}
        agents = data.get("agents", data if isinstance(data, list) else [])
        if isinstance(agents, dict):
            agents = list(agents.values())
        return {
            "total_agents": len(agents),
            "agents": [
                {
                    "id": a.get("id"),
                    "name": a.get("name"),
                    "type": a.get("type"),
                    "status": a.get("status"),
                    "created": a.get("createdAt"),
                }
                for a in agents
                if isinstance(a, dict)
            ],
        }


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        print("Test 1 - Brain folder creation:")
        brain = Brain(tmpdir)
        print(f"  Brain dir exists: {brain.dir.exists()}")
        print(f"  orchestrator_db under brain: {brain.orchestrator_db.startswith(str(brain.dir))}")

        print("\nTest 2 - parse_trailing_json:")
        parsed = parse_trailing_json('[INFO] blah\n+--+\n{"agentId": "a-1"}')
        print(f"  Parsed agentId: {parsed and parsed.get('agentId')}")
        print(f"  Garbage returns None: {parse_trailing_json('no json here') is None}")

        print("\nTest 3 - ruflo availability:")
        print(f"  is_ruflo_available(): {is_ruflo_available()}")

        if is_ruflo_available():
            print("\nTest 4 - project-local memory round-trip:")
            stored = brain.memory_store("selftest", {"ok": True})
            value = brain.memory_retrieve("selftest")
            print(f"  Stored: {stored}, retrieved: {value}")
            print(f"  memory.db created inside brain: {(brain.dir / 'memory.db').exists()}")
            print(f"  Missing key returns None: {brain.memory_retrieve('nope_xyz') is None}")
        else:
            print("\nTest 4 skipped - ruflo CLI not installed")
