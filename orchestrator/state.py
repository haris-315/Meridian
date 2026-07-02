import sqlite3
import subprocess
import sys
import json
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path

RUFLO_NAMESPACE = "meridian"


class StateManager:
    def __init__(self, db_path: str = "orchestrator.db"):
        self.db_path = Path(db_path)
        self.init_db()

    def init_db(self) -> None:
        """Initialize SQLite database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                description TEXT,
                result TEXT,
                cost_usd REAL,
                verified BOOLEAN,
                executor_output TEXT,
                verifier_output TEXT,
                wave INTEGER,
                timestamp TEXT
            )
        """)

        conn.commit()
        conn.close()

    def write_task_result(
        self,
        task_id: str,
        executor_result: Dict[str, Any],
        verifier_result: Dict[str, Any],
        description: str = "",
        wave: int = 0
    ) -> None:
        """Write task execution and verification results."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        success = verifier_result.get('passed', False)
        status = "done" if success else "failed"

        cursor.execute("""
            INSERT OR REPLACE INTO tasks
            (task_id, status, description, result, cost_usd, verified,
             executor_output, verifier_output, wave, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            task_id,
            status,
            description,
            executor_result.get('result', ''),
            executor_result.get('cost_usd', 0.0),
            success,
            json.dumps(executor_result),
            json.dumps(verifier_result),
            wave,
            datetime.now().isoformat()
        ))

        conn.commit()
        conn.close()

        self._store_ruflo_memory(task_id, wave, description, executor_result, verifier_result, success)

    def _store_ruflo_memory(
        self,
        task_id: str,
        wave: int,
        description: str,
        executor_result: Dict[str, Any],
        verifier_result: Dict[str, Any],
        success: bool
    ) -> None:
        """Store a cross-wave summary of this verified task in Ruflo memory so
        later waves' executor prompts can pull real context about what earlier
        waves produced. Never raises — a memory outage must not block the
        orchestrator loop, so failures are logged and swallowed."""
        key = f"wave_{wave}_task_{task_id}"
        summary = json.dumps({
            'task_id': task_id,
            'description': description,
            'result': executor_result.get('result', ''),
            'verified': success,
            'verifier_output': (verifier_result.get('output') or '')[:500],
            'cost_usd': executor_result.get('cost_usd', 0.0),
        })

        try:
            proc = subprocess.run(
                ["ruflo", "memory", "store", "--key", key, "--value", summary,
                 "--namespace", RUFLO_NAMESPACE, "--upsert"],
                capture_output=True,
                text=True,
                timeout=30,
                stdin=subprocess.DEVNULL
            )
            if proc.returncode != 0:
                print(f"[WARN] Ruflo memory store failed for '{key}': {proc.stderr.strip()}", file=sys.stderr)
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            print(f"[WARN] Ruflo memory store failed for '{key}': {e}", file=sys.stderr)

    def get_task_status(self, task_id: str) -> Optional[str]:
        """Get current status of a task."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM tasks WHERE task_id = ?", (task_id,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None

    def get_wave_summary(self, wave: Optional[int] = None) -> Dict[str, Any]:
        """Get summary of a specific wave's results (defaults to the latest wave)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if wave is None:
            cursor.execute("SELECT MAX(wave) FROM tasks")
            row = cursor.fetchone()
            wave = row[0] if row and row[0] is not None else 0

        cursor.execute("""
            SELECT COUNT(*), SUM(cost_usd), SUM(CASE WHEN verified THEN 1 ELSE 0 END)
            FROM tasks WHERE status = 'done' AND wave = ?
        """, (wave,))
        done_count, total_cost, verified_count = cursor.fetchone()
        done_count = done_count or 0
        total_cost = total_cost or 0.0
        verified_count = verified_count or 0

        cursor.execute("""
            SELECT COUNT(*) FROM tasks WHERE status = 'failed' AND wave = ?
        """, (wave,))
        failed_count = cursor.fetchone()[0] or 0

        cursor.execute("""
            SELECT task_id, description, status FROM tasks WHERE wave = ? ORDER BY timestamp DESC LIMIT 5
        """, (wave,))
        recent = cursor.fetchall()

        cursor.execute("""
            SELECT COUNT(*), SUM(cost_usd) FROM tasks WHERE status = 'done'
        """)
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

    def clear(self) -> None:
        """Clear all data (for testing)."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tasks")
        conn.commit()
        conn.close()


if __name__ == "__main__":
    import os

    # Use temp DB for testing
    test_db = "/tmp/test_orchestrator.db"
    if os.path.exists(test_db):
        os.remove(test_db)

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
    status = manager.get_task_status('task_1')
    print(f"  Task status: {status}")

    print("\nTest 2 - Wave summary:")
    summary = manager.get_wave_summary()
    print(f"  Tasks completed: {summary['tasks_completed']}")
    print(f"  Total cost: ${summary['total_cost_usd']}")

    print("\nTest 3 - Failed task:")
    manager.write_task_result('task_2',
        {'task_id': 'task_2', 'result': 'Error', 'cost_usd': 0.001},
        {'passed': False, 'output': 'Test failed', 'failed_command': 'pytest'}
    )
    status = manager.get_task_status('task_2')
    print(f"  Task 2 status: {status}")

    # Cleanup
    os.remove(test_db)
    print("\nAll tests passed!")
