import subprocess
import json
import sys
from typing import Dict, Any
from dag import TaskNode


class TaskExecutor:
    def __init__(self, working_dir: str = "."):
        self.working_dir = working_dir

    def execute(self, task: TaskNode) -> Dict[str, Any]:
        """Execute a single task via Claude Code CLI headlessly."""
        prompt = self._build_prompt(task)

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

    def _build_prompt(self, task: TaskNode) -> str:
        """Build execution prompt from task description and its boundaries
        (dependency ids, which scope what this task may assume already exists)."""
        boundaries = (
            f"This task depends on: {', '.join(task.dependencies)}. "
            "Only build on work already completed by those tasks."
            if task.dependencies else
            "This task has no dependencies; do not assume any other work exists yet."
        )
        return f"""Complete the following task in the current working directory.

Task: {task.description}

Boundaries: {boundaries}

Instructions:
- Execute this task completely and correctly
- Be concise and factual in your final response
- If the task involves code, write it to a file
- Do not ask for clarification; complete the task as specified
"""


if __name__ == "__main__":
    from dag import DAGBuilder

    # Test 1: Create a mock task and check prompt building
    print("Test 1 - Prompt building:")
    task = TaskNode(
        id="test_1",
        description="Create a hello world function in Python"
    )
    executor = TaskExecutor()
    prompt = executor._build_prompt(task)
    print(f"  Prompt length: {len(prompt)}")
    print(f"  Prompt preview: {prompt[:150]}...")

    # Test 2: Execute task (uses real Claude CLI if available, else reports the error)
    print("\nTest 2 - Execution:")
    result = executor.execute(task)
    if result.get('error') == 'claude_not_found':
        print("  Claude CLI not installed - correctly reported error")
    elif result['success']:
        print("  Claude CLI available and executed successfully")
        print(f"  Cost: ${result['cost_usd']}")
    else:
        print(f"  Error: {result.get('error')}")
