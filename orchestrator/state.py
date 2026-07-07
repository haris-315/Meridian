import sqlite3
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path

from brain import Brain


def _now() -> str:
    return datetime.now().isoformat()


class StateManager:
    """SQLite persistence for the run, plus mirrored cross-wave summaries into
    the project Brain (ruflo memory) so later waves' prompts can pull real
    context. The DB lives in the project's .meridian/ folder when constructed
    via a Brain; a bare path is still accepted for tests."""

    def __init__(self, db_path: str = "orchestrator.db", brain: Optional[Brain] = None):
        self.db_path = Path(db_path)
        self.brain = brain
        self.run_id: Optional[int] = None
        self.init_db()

    def _connect(self) -> sqlite3.Connection:
        # Executor worker threads report agent states concurrently with the
        # main loop's writes; a busy timeout keeps brief lock contention from
        # surfacing as "database is locked" errors.
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA busy_timeout = 10000")
        return conn

    def init_db(self) -> None:
        """Initialize SQLite database schema."""
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT,
                run_id INTEGER,
                status TEXT NOT NULL,
                description TEXT,
                result TEXT,
                cost_usd REAL,
                verified BOOLEAN,
                executor_output TEXT,
                verifier_output TEXT,
                wave INTEGER,
                timestamp TEXT,
                ruflo_task_id TEXT,
                ruflo_agent_id TEXT,
                retry_count INTEGER DEFAULT 0,
                PRIMARY KEY (task_id, run_id)
            )
        """)

        # Migrate pre-existing DBs: task_ids (task_0, task_1, ...) are reused by
        # every run, so a `tasks` table without run_id silently overwrites one
        # run's results with the next run's - breaking cost accounting and
        # cross-run history for any project run more than once.
        cursor.execute("PRAGMA table_info(tasks)")
        existing_cols = [row[1] for row in cursor.fetchall()]
        if existing_cols and 'run_id' not in existing_cols:
            cursor.execute("ALTER TABLE tasks RENAME TO tasks_pre_run_id")
            cursor.execute("""
                CREATE TABLE tasks (
                    task_id TEXT,
                    run_id INTEGER,
                    status TEXT NOT NULL,
                    description TEXT,
                    result TEXT,
                    cost_usd REAL,
                    verified BOOLEAN,
                    executor_output TEXT,
                    verifier_output TEXT,
                    wave INTEGER,
                    timestamp TEXT,
                    ruflo_task_id TEXT,
                    ruflo_agent_id TEXT,
                    retry_count INTEGER DEFAULT 0,
                    PRIMARY KEY (task_id, run_id)
                )
            """)
            cursor.execute("""
                INSERT INTO tasks (task_id, run_id, status, description, result, cost_usd,
                                    verified, executor_output, verifier_output, wave, timestamp,
                                    ruflo_task_id, ruflo_agent_id, retry_count)
                SELECT task_id, NULL, status, description, result, cost_usd, verified,
                       executor_output, verifier_output, wave, timestamp,
                       ruflo_task_id, ruflo_agent_id, retry_count
                FROM tasks_pre_run_id
            """)
            cursor.execute("DROP TABLE tasks_pre_run_id")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                goal TEXT NOT NULL,
                status TEXT NOT NULL,
                working_dir TEXT,
                started_at TEXT,
                finished_at TEXT,
                total_cost_usd REAL DEFAULT 0
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agents (
                agent_key TEXT PRIMARY KEY,
                run_id INTEGER,
                task_id TEXT,
                wave INTEGER,
                status TEXT,
                detail TEXT,
                ruflo_agent_id TEXT,
                started_at TEXT,
                updated_at TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER,
                ts TEXT,
                level TEXT,
                source TEXT,
                message TEXT,
                wave INTEGER
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dag_snapshot (
                run_id INTEGER PRIMARY KEY,
                dag_json TEXT,
                updated_at TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS task_reasoning (
                reasoning_id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT,
                run_id INTEGER,
                wave INTEGER,
                attempt_number INTEGER,
                failure_type TEXT,
                diagnostic_message TEXT,
                timestamp TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS checkpoints (
                run_id INTEGER PRIMARY KEY,
                wave_number INTEGER,
                dag_json TEXT,
                retry_counts_json TEXT,
                updated_at TEXT
            )
        """)

        conn.commit()
        conn.close()

    # ----------------------------------------------------------------- runs

    def start_run(self, goal: str, working_dir: str) -> int:
        """Record a new run and make it current. Any run left dangling in
        'running' state (e.g. a previous crash) is marked interrupted first."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("UPDATE runs SET status = 'interrupted', finished_at = ? WHERE status = 'running'",
                       (_now(),))
        cursor.execute(
            "INSERT INTO runs (goal, status, working_dir, started_at) VALUES (?, 'running', ?, ?)",
            (goal, working_dir, _now()),
        )
        self.run_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return self.run_id

    def finish_run(self, status: str, total_cost_usd: float) -> None:
        if self.run_id is None:
            return
        conn = self._connect()
        conn.execute(
            "UPDATE runs SET status = ?, finished_at = ?, total_cost_usd = ? WHERE run_id = ?",
            (status, _now(), total_cost_usd, self.run_id),
        )
        conn.commit()
        conn.close()

    # ----------------------------------------------------------- dag snapshot

    def sync_dag(self, dag: Dict[str, Any]) -> None:
        """Persist the full DAG (including tasks not yet executed) so the
        dashboard can render pending/ready nodes and dependency edges - the
        tasks table only ever contains executed attempts. `dag` is the live
        {task_id: TaskNode} dict; statuses are read fresh each call."""
        snapshot = [
            {
                'id': node.id,
                'description': node.description,
                'dependencies': list(node.dependencies),
                'status': node.status.value,
                'verify_commands': list(node.verify_commands),
            }
            for node in dag.values()
        ]
        try:
            conn = self._connect()
            conn.execute(
                "INSERT OR REPLACE INTO dag_snapshot (run_id, dag_json, updated_at) VALUES (?, ?, ?)",
                (self.run_id or 0, json.dumps(snapshot), _now()),
            )
            conn.commit()
            conn.close()
        except sqlite3.Error:
            pass

    # ------------------------------------------------------------ reasoning

    def store_reasoning(self, task_id: str, wave: int, failure_type: str,
                        diagnostic_message: str, attempt_number: int = 0) -> None:
        """Persist why a task failed (structured, not just raw output) for the
        dashboard's reasoning trace and future failure-pattern analysis."""
        try:
            conn = self._connect()
            conn.execute("""
                INSERT INTO task_reasoning
                (task_id, run_id, wave, attempt_number, failure_type, diagnostic_message, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (task_id, self.run_id, wave, attempt_number, failure_type,
                  diagnostic_message[:1000], _now()))
            conn.commit()
            conn.close()
        except sqlite3.Error:
            pass

    def get_reasoning_for_task(self, task_id: str) -> List[Dict[str, Any]]:
        """All recorded failure diagnoses for one task, oldest first - the
        agent's attempt history in one call."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT wave, attempt_number, failure_type, diagnostic_message, timestamp
            FROM task_reasoning WHERE task_id = ? AND run_id IS ?
            ORDER BY reasoning_id ASC
        """, (task_id, self.run_id))
        rows = cursor.fetchall()
        conn.close()
        return [
            {'wave': r[0], 'attempt_number': r[1], 'failure_type': r[2],
             'diagnostic_message': r[3], 'timestamp': r[4]}
            for r in rows
        ]

    # ----------------------------------------------------------- checkpoints

    def save_checkpoint(self, dag: Dict[str, Any], wave_number: int,
                        retry_counts: Dict[str, int]) -> None:
        """Snapshot enough state to resume this run from scratch after a crash:
        full DAG (with statuses), current wave number, and retry budgets used
        so far. Called after every wave - a crash mid-wave loses at most one
        wave of progress, not the whole run."""
        snapshot = [
            {
                'id': node.id,
                'description': node.description,
                'dependencies': list(node.dependencies),
                'status': node.status.value,
                'verify_commands': list(node.verify_commands),
            }
            for node in dag.values()
        ]
        try:
            conn = self._connect()
            conn.execute("""
                INSERT OR REPLACE INTO checkpoints
                (run_id, wave_number, dag_json, retry_counts_json, updated_at)
                VALUES (?, ?, ?, ?, ?)
            """, (self.run_id, wave_number, json.dumps(snapshot), json.dumps(retry_counts), _now()))
            conn.commit()
            conn.close()
        except sqlite3.Error:
            pass

    def find_resumable_run(self) -> Optional[Dict[str, Any]]:
        """Find the most recent run left 'interrupted' (start_run marks any
        dangling 'running' row this way on the next startup - a live process
        never leaves its own run in that state) that has a checkpoint to
        resume from. Returns None if there's nothing to resume."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT run_id, goal, working_dir FROM runs
            WHERE status = 'interrupted' ORDER BY run_id DESC LIMIT 1
        """)
        row = cursor.fetchone()
        if not row:
            conn.close()
            return None
        run_id, goal, working_dir = row

        cursor.execute("""
            SELECT wave_number, dag_json, retry_counts_json FROM checkpoints WHERE run_id = ?
        """, (run_id,))
        cp = cursor.fetchone()
        conn.close()
        if not cp:
            return None

        return {
            'run_id': run_id,
            'goal': goal,
            'working_dir': working_dir,
            'wave_number': cp[0],
            'dag_json': cp[1],
            'retry_counts': json.loads(cp[2] or '{}'),
        }

    # ------------------------------------------------------------ cross-run

    def get_last_completed_run_context(self) -> Optional[Dict[str, Any]]:
        """Summary of the most recent 'complete' run before this one in the
        same project - so a fresh goal like 'refactor the calculator' can be
        decomposed and executed with real knowledge of what already exists,
        instead of starting blind. Returns None for a project's first run."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT run_id, goal, total_cost_usd, finished_at FROM runs
            WHERE status = 'complete' AND run_id != ? ORDER BY run_id DESC LIMIT 1
        """, (self.run_id if self.run_id is not None else -1,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return None
        prior_run_id, goal, cost, finished_at = row

        cursor.execute("""
            SELECT task_id, description, status, result FROM tasks
            WHERE run_id = ? ORDER BY wave ASC, timestamp ASC
        """, (prior_run_id,))
        tasks = [
            {'task_id': r[0], 'description': r[1], 'status': r[2], 'result': (r[3] or '')[:300]}
            for r in cursor.fetchall()
        ]
        conn.close()

        return {
            'run_id': prior_run_id,
            'goal': goal,
            'cost_usd': cost or 0.0,
            'finished_at': finished_at,
            'tasks': tasks,
        }

    # --------------------------------------------------------------- events

    def log_event(self, level: str, source: str, message: str, wave: int = 0) -> None:
        """Append a log line for the dashboard's live stream. Best-effort."""
        try:
            conn = self._connect()
            conn.execute(
                "INSERT INTO events (run_id, ts, level, source, message, wave) VALUES (?, ?, ?, ?, ?, ?)",
                (self.run_id, _now(), level, source, message, wave),
            )
            conn.commit()
            conn.close()
        except sqlite3.Error:
            pass

    # --------------------------------------------------------------- agents

    def set_agent_state(self, task_id: str, wave: int, status: str,
                        detail: str = "", ruflo_agent_id: Optional[str] = None) -> None:
        """Upsert the live state of the agent working one task in one wave.
        Called from executor worker threads - must never raise."""
        agent_key = f"wave_{wave}_{task_id}"
        try:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT started_at, ruflo_agent_id FROM agents WHERE agent_key = ?", (agent_key,))
            row = cursor.fetchone()
            started_at = row[0] if row else _now()
            kept_ruflo_id = ruflo_agent_id or (row[1] if row else None)
            cursor.execute("""
                INSERT OR REPLACE INTO agents
                (agent_key, run_id, task_id, wave, status, detail, ruflo_agent_id, started_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (agent_key, self.run_id, task_id, wave, status, detail[:300],
                  kept_ruflo_id, started_at, _now()))
            conn.commit()
            conn.close()
        except sqlite3.Error:
            pass

    # ---------------------------------------------------------------- tasks

    def write_task_result(
        self,
        task_id: str,
        executor_result: Dict[str, Any],
        verifier_result: Dict[str, Any],
        description: str = "",
        wave: int = 0,
        ruflo_task_id: Optional[str] = None,
        ruflo_agent_id: Optional[str] = None,
        retry_count: int = 0
    ) -> None:
        """Write task execution and verification results."""
        conn = self._connect()
        cursor = conn.cursor()

        success = verifier_result.get('passed', False)
        status = "done" if success else "failed"

        cursor.execute("""
            INSERT OR REPLACE INTO tasks
            (task_id, run_id, status, description, result, cost_usd, verified,
             executor_output, verifier_output, wave, timestamp,
             ruflo_task_id, ruflo_agent_id, retry_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            task_id,
            self.run_id,
            status,
            description,
            executor_result.get('result', ''),
            executor_result.get('cost_usd', 0.0),
            success,
            json.dumps(executor_result),
            json.dumps(verifier_result),
            wave,
            _now(),
            ruflo_task_id,
            ruflo_agent_id,
            retry_count
        ))

        conn.commit()
        conn.close()

        if self.brain:
            self.brain.memory_store(f"wave_{wave}_task_{task_id}", {
                'task_id': task_id,
                'description': description,
                'result': executor_result.get('result', ''),
                'verified': success,
                'verifier_output': (verifier_result.get('output') or '')[:500],
                'cost_usd': executor_result.get('cost_usd', 0.0),
            })

    def get_task_status(self, task_id: str) -> Optional[str]:
        """Get current status of a task in the current run."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM tasks WHERE task_id = ? AND run_id IS ?", (task_id, self.run_id))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None

    def get_wave_summary(self, wave: Optional[int] = None) -> Dict[str, Any]:
        """Get summary of a specific wave's results in the current run
        (defaults to the latest wave)."""
        conn = self._connect()
        cursor = conn.cursor()

        if wave is None:
            cursor.execute("SELECT MAX(wave) FROM tasks WHERE run_id IS ?", (self.run_id,))
            row = cursor.fetchone()
            wave = row[0] if row and row[0] is not None else 0

        cursor.execute("""
            SELECT COUNT(*), SUM(cost_usd), SUM(CASE WHEN verified THEN 1 ELSE 0 END)
            FROM tasks WHERE status = 'done' AND wave = ? AND run_id IS ?
        """, (wave, self.run_id))
        done_count, total_cost, verified_count = cursor.fetchone()
        done_count = done_count or 0
        total_cost = total_cost or 0.0
        verified_count = verified_count or 0

        cursor.execute("""
            SELECT COUNT(*) FROM tasks WHERE status = 'failed' AND wave = ? AND run_id IS ?
        """, (wave, self.run_id))
        failed_count = cursor.fetchone()[0] or 0

        cursor.execute("""
            SELECT task_id, description, status FROM tasks
            WHERE wave = ? AND run_id IS ? ORDER BY timestamp DESC LIMIT 5
        """, (wave, self.run_id))
        recent = cursor.fetchall()

        cursor.execute("""
            SELECT COUNT(*), SUM(cost_usd) FROM tasks WHERE status = 'done' AND run_id IS ?
        """, (self.run_id,))
        total_done_count, total_cost_all = cursor.fetchone()
        conn.close()

        return {
            'wave': wave,
            'tasks_completed': done_count,
            'tasks_failed': failed_count,
            'tests_passed': verified_count,
            'total_cost_usd': round(total_cost, 4),
            'cumulative_tasks_completed': total_done_count or 0,
            'cumulative_cost_usd': round(total_cost_all or 0.0, 4),
            'recent_tasks': [
                {'id': r[0], 'description': r[1], 'status': r[2]} for r in recent
            ]
        }

    def write_wave_summary_memory(self, wave: int) -> None:
        """Store one aggregate summary of an entire wave (all tasks, pass/fail,
        short results) in the Brain under 'wave_{n}_summary', so the next wave's
        executor prompts can pull the full shape of what was attempted, not just
        their direct dependencies. Never raises."""
        if not self.brain:
            return

        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT task_id, description, status, result FROM tasks WHERE wave = ? AND run_id IS ?
        """, (wave, self.run_id))
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return

        self.brain.memory_store(f"wave_{wave}_summary", [
            {
                'task_id': r[0],
                'description': r[1],
                'status': r[2],
                'result': (r[3] or '')[:300],
            }
            for r in rows
        ])

    def get_full_run_summary(self) -> Dict[str, Any]:
        """Cross-wave view of the current run: every task's full detail plus
        aggregates, for the final report (get_wave_summary only scopes to one
        wave and two narrow cumulative numbers - not enough on its own)."""
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT task_id, status, description, result, cost_usd, verified,
                   verifier_output, wave, timestamp, ruflo_task_id, ruflo_agent_id, retry_count
            FROM tasks WHERE run_id IS ? ORDER BY wave ASC, timestamp ASC
        """, (self.run_id,))
        rows = cursor.fetchall()

        cursor.execute("""
            SELECT status, COUNT(*), SUM(cost_usd) FROM tasks WHERE run_id IS ? GROUP BY status
        """, (self.run_id,))
        by_status = {r[0]: {'count': r[1], 'cost_usd': r[2] or 0.0} for r in cursor.fetchall()}
        conn.close()

        tasks = [
            {
                'task_id': r[0],
                'status': r[1],
                'description': r[2],
                'result': r[3],
                'cost_usd': r[4] or 0.0,
                'verified': bool(r[5]),
                'verifier_output': (r[6] or '')[:500],
                'wave': r[7],
                'timestamp': r[8],
                'ruflo_task_id': r[9],
                'ruflo_agent_id': r[10],
                'retry_count': r[11] or 0,
            }
            for r in rows
        ]

        total_cost = sum(t['cost_usd'] for t in tasks)

        return {
            'tasks': tasks,
            'total_tasks': len(tasks),
            'by_status': by_status,
            'total_cost_usd': round(total_cost, 4),
        }

    def get_failed_tasks(self, min_retry_count: Optional[int] = None) -> List[Dict[str, Any]]:
        """Failed tasks, optionally filtered to those that have exhausted a retry
        budget. Used to build the stall-escalation prompt."""
        conn = self._connect()
        cursor = conn.cursor()

        query = """
            SELECT task_id, description, verifier_output, wave, retry_count, cost_usd
            FROM tasks WHERE status = 'failed' AND run_id IS ?
        """
        params: tuple = (self.run_id,)
        if min_retry_count is not None:
            query += " AND retry_count >= ?"
            params = params + (min_retry_count,)
        query += " ORDER BY wave DESC, timestamp DESC"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        return [
            {
                'task_id': r[0],
                'description': r[1],
                'verifier_output': (r[2] or '')[:500],
                'wave': r[3],
                'retry_count': r[4] or 0,
                'cost_usd': r[5] or 0.0,
            }
            for r in rows
        ]

    # ------------------------------------------------------------ dashboard

    def get_dashboard_state(self, events_after: int = 0) -> Dict[str, Any]:
        """One-call snapshot for the frontend: latest run, all tasks, live agent
        states, and events newer than the client's last-seen event_id."""
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT run_id, goal, status, working_dir, started_at, finished_at, total_cost_usd
            FROM runs ORDER BY run_id DESC LIMIT 1
        """)
        run_row = cursor.fetchone()
        run = None
        if run_row:
            run = {
                'run_id': run_row[0], 'goal': run_row[1], 'status': run_row[2],
                'working_dir': run_row[3], 'started_at': run_row[4],
                'finished_at': run_row[5], 'total_cost_usd': run_row[6] or 0.0,
            }

        dashboard_run_id = run['run_id'] if run else None
        cursor.execute("""
            SELECT task_id, status, description, result, cost_usd, verified, wave, retry_count
            FROM tasks WHERE run_id IS ? ORDER BY wave ASC, task_id ASC
        """, (dashboard_run_id,))
        tasks = [
            {
                'task_id': r[0], 'status': r[1], 'description': r[2],
                'result': (r[3] or '')[:400], 'cost_usd': r[4] or 0.0,
                'verified': bool(r[5]), 'wave': r[6], 'retry_count': r[7] or 0,
            }
            for r in cursor.fetchall()
        ]

        run_filter = (run['run_id'],) if run else (None,)
        cursor.execute("""
            SELECT task_id, wave, status, detail, ruflo_agent_id, started_at, updated_at
            FROM agents WHERE run_id IS ? ORDER BY wave ASC, task_id ASC
        """, run_filter)
        agents = [
            {
                'task_id': r[0], 'wave': r[1], 'status': r[2], 'detail': r[3],
                'ruflo_agent_id': r[4], 'started_at': r[5], 'updated_at': r[6],
            }
            for r in cursor.fetchall()
        ]

        cursor.execute("""
            SELECT event_id, ts, level, source, message, wave
            FROM events WHERE event_id > ? ORDER BY event_id ASC LIMIT 500
        """, (events_after,))
        events = [
            {
                'event_id': r[0], 'ts': r[1], 'level': r[2],
                'source': r[3], 'message': r[4], 'wave': r[5],
            }
            for r in cursor.fetchall()
        ]

        cursor.execute("SELECT dag_json FROM dag_snapshot ORDER BY run_id DESC LIMIT 1")
        dag_row = cursor.fetchone()

        cursor.execute("""
            SELECT task_id, wave, attempt_number, failure_type, diagnostic_message, timestamp
            FROM task_reasoning WHERE run_id IS ? ORDER BY reasoning_id DESC LIMIT 100
        """, run_filter)
        reasoning = [
            {'task_id': r[0], 'wave': r[1], 'attempt_number': r[2],
             'failure_type': r[3], 'diagnostic_message': r[4], 'timestamp': r[5]}
            for r in cursor.fetchall()
        ]
        conn.close()

        dag = []
        if dag_row and dag_row[0]:
            try:
                dag = json.loads(dag_row[0])
            except json.JSONDecodeError:
                dag = []

        return {'run': run, 'tasks': tasks, 'agents': agents, 'events': events, 'dag': dag,
                'reasoning': reasoning}

    def clear(self) -> None:
        """Clear all data (for testing)."""
        conn = self._connect()
        cursor = conn.cursor()
        for table in ("tasks", "runs", "agents", "events", "task_reasoning", "checkpoints"):
            cursor.execute(f"DELETE FROM {table}")
        conn.commit()
        conn.close()


if __name__ == "__main__":
    import tempfile
    import os

    with tempfile.TemporaryDirectory() as tmpdir:
        test_db = os.path.join(tmpdir, "test_orchestrator.db")
        manager = StateManager(test_db)

        print("Test 1 - Write and read task result:")
        executor_result = {
            'task_id': 'task_1',
            'result': 'Successfully created function',
            'cost_usd': 0.0042,
            'success': True
        }
        verifier_result = {
            'passed': True,
            'output': 'Tests passed',
            'failed_command': None
        }
        manager.write_task_result('task_1', executor_result, verifier_result, 'Create function')
        print(f"  Task status: {manager.get_task_status('task_1')}")

        print("\nTest 2 - Wave summary:")
        summary = manager.get_wave_summary()
        print(f"  Tasks completed: {summary['tasks_completed']}")
        print(f"  Total cost: ${summary['total_cost_usd']}")

        print("\nTest 3 - Failed task:")
        manager.write_task_result('task_2',
            {'task_id': 'task_2', 'result': 'Error', 'cost_usd': 0.001},
            {'passed': False, 'output': 'Test failed', 'failed_command': 'pytest'}
        )
        print(f"  Task 2 status: {manager.get_task_status('task_2')}")

        print("\nTest 4 - Runs, agents, events, dashboard state:")
        run_id = manager.start_run("test goal", tmpdir)
        manager.log_event("info", "orchestrator", "wave 1 started", wave=1)
        manager.set_agent_state("task_1", 1, "running", "executing claude -p")
        manager.set_agent_state("task_1", 1, "done", "verified")
        manager.finish_run("complete", 0.0052)
        dash = manager.get_dashboard_state()
        print(f"  Run recorded: {dash['run']['goal']} -> {dash['run']['status']}")
        print(f"  Agents tracked: {len(dash['agents'])} (status: {dash['agents'][0]['status']})")
        print(f"  Events: {len(dash['events'])}")
        assert dash['run']['status'] == 'complete'
        assert dash['agents'][0]['status'] == 'done'
        assert len(dash['events']) == 1

        print("\nTest 5 - Incremental event fetch:")
        manager.log_event("info", "orchestrator", "another event")
        last_id = dash['events'][-1]['event_id']
        newer = manager.get_dashboard_state(events_after=last_id)['events']
        print(f"  Only new events returned: {len(newer) == 1 and newer[0]['message'] == 'another event'}")

    print("\nAll tests passed!")
