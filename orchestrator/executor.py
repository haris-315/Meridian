import shutil
import subprocess
import json
import sys
from typing import Dict, Any, Optional
from dag import TaskNode

USE_RUFLO = True  # set False to fall back to plain TaskExecutor
RUFLO_NAMESPACE = "meridian"


def is_ruflo_available() -> bool:
    """Fast PATH check for the ruflo CLI. Does not spawn a process."""
    return shutil.which("ruflo") is not None


class TaskExecutor:
    def __init__(self, working_dir: str = "."):
        self.working_dir = working_dir

    def execute(self, task: TaskNode, wave_num: int = 0) -> Dict[str, Any]:
        """Execute a single task via Claude Code CLI headlessly."""
        prompt = self._build_prompt(task, wave_num)
        return self._run_claude(prompt, task)

    def _run_claude(self, prompt: str, task: TaskNode) -> Dict[str, Any]:
        try:
            result = subprocess.run(
                ["claude", "-p", prompt, "--output-format", "json",
                 "--permission-mode", "acceptEdits"],
                capture_output=True,
                text=True,
                timeout=300,
                cwd=self.working_dir,
                stdin=subprocess.DEVNULL
            )

            if result.returncode != 0:
                return {
                    'task_id': task.id,
                    'result': f"CLI error: {result.stderr}",
                    'session_id': None,
                    'cost_usd': 0.0,
                    'success': False,
                    'error': result.stderr
                }

            try:
                output = json.loads(result.stdout)
                is_error = output.get('is_error', False)
                return {
                    'task_id': task.id,
                    'result': output.get('result', result.stdout),
                    'session_id': output.get('session_id'),
                    'cost_usd': output.get('total_cost_usd', 0.0),
                    'success': not is_error,
                    'output': output
                }
            except json.JSONDecodeError:
                return {
                    'task_id': task.id,
                    'result': result.stdout,
                    'session_id': None,
                    'cost_usd': 0.0,
                    'success': True,
                    'output': {'raw': result.stdout}
                }

        except subprocess.TimeoutExpired:
            return {
                'task_id': task.id,
                'result': "Task execution timeout",
                'session_id': None,
                'cost_usd': 0.0,
                'success': False,
                'error': 'timeout'
            }
        except FileNotFoundError:
            return {
                'task_id': task.id,
                'result': "Claude CLI not found. Install with: pip install claude-code",
                'session_id': None,
                'cost_usd': 0.0,
                'success': False,
                'error': 'claude_not_found'
            }

    def _build_prompt(self, task: TaskNode, wave_num: int = 0) -> str:
        """Build execution prompt from task description, its boundaries
        (dependency ids, which scope what this task may assume already exists),
        and any Ruflo-stored context from the wave that produced each dependency."""
        boundaries = (
            f"This task depends on: {', '.join(task.dependencies)}. "
            "Only build on work already completed by those tasks."
            if task.dependencies else
            "This task has no dependencies; do not assume any other work exists yet."
        )

        context_section = ""
        if task.dependencies:
            context_lines = []
            for dep_id in task.dependencies:
                summary = self._retrieve_prior_context(dep_id, wave_num)
                if summary:
                    context_lines.append(f"- {dep_id}: {summary}")
            if context_lines:
                context_section = "\n\nContext from prior tasks:\n" + "\n".join(context_lines)

        return f"""Complete the following task in the current working directory.

Task: {task.description}

Boundaries: {boundaries}{context_section}

Instructions:
- Execute this task completely and correctly
- Be concise and factual in your final response
- If the task involves code, write it to a file
- Do not ask for clarification; complete the task as specified
"""

    def _retrieve_prior_context(self, dep_id: str, wave_num: int) -> Optional[str]:
        """Retrieve the stored summary of a dependency task from the wave that
        produced it via Ruflo memory. Never raises — logs a warning and
        returns None on any failure so a memory outage can't block execution."""
        if not is_ruflo_available():
            return None

        key = f"wave_{wave_num - 1}_task_{dep_id}"
        try:
            result = subprocess.run(
                ["ruflo", "memory", "retrieve", "--key", key,
                 "--namespace", RUFLO_NAMESPACE, "--value-only"],
                capture_output=True,
                text=True,
                timeout=30,
                stdin=subprocess.DEVNULL
            )
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            print(f"[WARN] Ruflo memory retrieve failed for '{key}': {e}", file=sys.stderr)
            return None

        if result.returncode != 0 or not result.stdout.strip():
            print(f"[WARN] No Ruflo memory found for '{key}' (dependency context skipped)", file=sys.stderr)
            return None

        return result.stdout.strip()


class RufloExecutor(TaskExecutor):
    """TaskExecutor variant that registers each task as a Ruflo agent and
    records the raw execution result in Ruflo memory. Claude Code still does
    all the actual work — Ruflo spawn/store are best-effort coordination and
    never block or replace the underlying claude -p call."""

    def execute(self, task: TaskNode, wave_num: int = 0) -> Dict[str, Any]:
        self._spawn_ruflo_agent(task)
        result = super().execute(task, wave_num)
        self._store_execution_result(task, wave_num, result)
        return result

    def _spawn_ruflo_agent(self, task: TaskNode) -> None:
        """Best-effort registration of this task as a Ruflo agent. Failures
        are logged and swallowed — spawning is coordination bookkeeping, not
        a precondition for execution."""
        try:
            result = subprocess.run(
                ["ruflo", "agent", "spawn", "-t", "coder", "--name", f"meridian_{task.id}"],
                capture_output=True,
                text=True,
                timeout=30,
                stdin=subprocess.DEVNULL
            )
            if result.returncode != 0:
                print(f"[WARN] Ruflo agent spawn failed for {task.id}: {result.stderr.strip()}", file=sys.stderr)
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            print(f"[WARN] Ruflo agent spawn failed for {task.id}: {e}", file=sys.stderr)

    def _store_execution_result(self, task: TaskNode, wave_num: int, result: Dict[str, Any]) -> None:
        """Store the raw executor result under a '_raw' key so it never
        collides with the canonical wave_{n}_task_{id} summary that state.py
        writes after verification (which is what _retrieve_prior_context reads)."""
        key = f"wave_{wave_num}_task_{task.id}_raw"
        value = json.dumps({
            'task_id': task.id,
            'description': task.description,
            'result': result.get('result', ''),
            'cost_usd': result.get('cost_usd', 0.0),
            'success': result.get('success', False),
        })
        try:
            proc = subprocess.run(
                ["ruflo", "memory", "store", "--key", key, "--value", value,
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


if __name__ == "__main__":
    from dag import DAGBuilder

    # Test 1: Create a mock task and check prompt building
    print("Test 1 - Prompt building:")
    task = TaskNode(
        id="test_1",
        description="Create a hello world function in Python"
    )
    executor = TaskExecutor()
    prompt = executor._build_prompt(task, wave_num=1)
    print(f"  Prompt length: {len(prompt)}")
    print(f"  Prompt preview: {prompt[:150]}...")

    # Test 2: Prompt building with a dependency and no stored context (expect warning + no crash)
    print("\nTest 2 - Prompt with missing dependency context:")
    dependent_task = TaskNode(
        id="test_2",
        description="Write tests for the hello world function",
        dependencies=["test_1"]
    )
    prompt2 = executor._build_prompt(dependent_task, wave_num=1)
    print(f"  Contains 'Context from prior tasks:': {'Context from prior tasks:' in prompt2}")

    # Test 3: Execute task (uses real Claude CLI if available, else reports the error)
    print("\nTest 3 - Execution:")
    result = executor.execute(task)
    if result.get('error') == 'claude_not_found':
        print("  Claude CLI not installed - correctly reported error")
    elif result['success']:
        print("  Claude CLI available and executed successfully")
        print(f"  Cost: ${result['cost_usd']}")
    else:
        print(f"  Error: {result.get('error')}")

    # Test 4: Ruflo availability check
    print("\nTest 4 - Ruflo availability:")
    print(f"  is_ruflo_available(): {is_ruflo_available()}")
    print(f"  USE_RUFLO config flag: {USE_RUFLO}")
