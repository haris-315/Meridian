import subprocess
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Any, Optional

from dag import TaskNode
from brain import Brain, is_ruflo_available

USE_RUFLO = True  # set False to fall back to plain TaskExecutor
MAX_CONCURRENT_TASKS = 8

# status_cb(task_id, wave, status, detail, ruflo_agent_id) - wired by main.py to
# StateManager.set_agent_state so the dashboard sees live per-agent transitions.
StatusCallback = Callable[[str, int, str, str, Optional[str]], None]


class TaskExecutor:
    def __init__(self, working_dir: str = ".", brain: Optional[Brain] = None,
                 status_cb: Optional[StatusCallback] = None, prior_context: str = ""):
        self.working_dir = working_dir
        self.brain = brain
        self.status_cb = status_cb or (lambda *args: None)
        # Summary of the most recent completed run in this project, if any -
        # injected into wave-1 prompts since those tasks have no in-DAG
        # dependency to pull context from otherwise. Set by main.py after
        # querying state.get_last_completed_run_context().
        self.prior_context = prior_context

    def execute(self, task: TaskNode, wave_num: int = 0) -> Dict[str, Any]:
        """Execute a single task via Claude Code CLI headlessly."""
        prompt = self._build_prompt(task, wave_num)
        return self._run_claude(prompt, task)

    def execute_wave(self, tasks: List[TaskNode], wave_num: int = 0) -> Dict[str, Dict[str, Any]]:
        """Execute all ready tasks in this wave. Base implementation runs them
        sequentially (used when ruflo is unavailable); RufloExecutor overrides
        this to dispatch them concurrently with real ruflo coordination."""
        results = {}
        for task in tasks:
            self.status_cb(task.id, wave_num, "running", "executing via claude -p", None)
            result = self.execute(task, wave_num)
            self.status_cb(task.id, wave_num, "executed" if result.get('success') else "errored",
                           str(result.get('result', ''))[:200], None)
            results[task.id] = result
        return results

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
                return self._build_cli_error_result(task, result)

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
        except Exception as e:
            return {
                'task_id': task.id,
                'result': f"Unexpected executor error: {e}",
                'session_id': None,
                'cost_usd': 0.0,
                'success': False,
                'error': str(e)
            }

    def _build_cli_error_result(self, task: TaskNode, result: "subprocess.CompletedProcess") -> Dict[str, Any]:
        """`claude -p --output-format json` still emits a valid JSON body on API
        errors (e.g. rate limits) even though the process exits non-zero and
        stderr is empty - using stderr alone silently drops the real reason.
        Parse stdout first and fall back to stderr only if that's all there is."""
        output = None
        try:
            output = json.loads(result.stdout) if result.stdout.strip() else None
        except json.JSONDecodeError:
            output = None

        error_type = 'cli_error'
        if output and output.get('result'):
            api_error_status = output.get('api_error_status')
            message = output['result']
            if api_error_status == 429:
                error_type = 'rate_limited'
                message = f"[RATE LIMITED] {message}"
            reason = message
        else:
            reason = result.stderr.strip() or f"claude -p exited with code {result.returncode} and no output"

        return {
            'task_id': task.id,
            'result': reason,
            'session_id': (output or {}).get('session_id'),
            'cost_usd': (output or {}).get('total_cost_usd', 0.0),
            'success': False,
            'error': error_type,
        }

    def _build_prompt(self, task: TaskNode, wave_num: int = 0) -> str:
        """Build execution prompt from task description, its boundaries
        (dependency ids, which scope what this task may assume already exists),
        and any Brain-stored context from the wave that produced each dependency
        plus an aggregate summary of the entire previous wave."""
        boundaries = (
            f"This task depends on: {', '.join(task.dependencies)}. "
            "Only build on work already completed by those tasks."
            if task.dependencies else
            "This task has no dependencies; do not assume any other work exists yet."
        )

        context_section = ""
        if task.dependencies and self.brain:
            context_lines = []
            for dep_id in task.dependencies:
                summary = self.brain.memory_retrieve(f"wave_{wave_num - 1}_task_{dep_id}")
                if summary:
                    context_lines.append(f"- {dep_id}: {summary}")
            if context_lines:
                context_section = "\n\nContext from prior tasks:\n" + "\n".join(context_lines)

        prior_run_section = ""
        if wave_num == 1 and self.prior_context:
            prior_run_section = f"\n\n{self.prior_context}"

        wave_context_section = ""
        if wave_num > 1 and self.brain:
            wave_summary = self.brain.memory_retrieve(f"wave_{wave_num - 1}_summary")
            if wave_summary:
                wave_context_section = f"\n\nFull context of the previous wave:\n{wave_summary}"

        retry_section = ""
        if wave_num > 1 and self.brain:
            own_previous = self.brain.memory_retrieve(f"wave_{wave_num - 1}_task_{task.id}")
            if own_previous:
                try:
                    prev = json.loads(own_previous)
                except json.JSONDecodeError:
                    prev = None
                if prev and not prev.get('verified', True):
                    retry_section = (
                        "\n\nIMPORTANT - this is a RETRY. Your previous attempt failed "
                        "independent verification with this output:\n"
                        f"{prev.get('verifier_output', '(no output captured)')}\n"
                        "Diagnose the root cause of that failure and fix it - do not "
                        "simply repeat the previous approach."
                    )

        return f"""Complete the following task in the current working directory.

Task: {task.description}

Boundaries: {boundaries}{prior_run_section}{context_section}{wave_context_section}{retry_section}

Instructions:
- Execute this task completely and correctly
- Be concise and factual in your final response
- If the task involves code, write it to a file
- Do not ask for clarification; complete the task as specified
"""


class RufloExecutor(TaskExecutor):
    """TaskExecutor variant that uses the project Brain (ruflo) as the real
    coordination layer for a wave: each ready task is registered as a ruflo
    task, assigned to a ruflo agent record, and dispatched concurrently. The
    actual file-editing work still runs through `claude -p` (confirmed via
    smoke test: `ruflo agent spawn` without a further execution call never
    performs real local tool-using work - it only registers a coordination
    record). Brain calls are best-effort and never block or replace the
    underlying claude -p call."""

    def execute_wave(self, tasks: List[TaskNode], wave_num: int = 0) -> Dict[str, Dict[str, Any]]:
        max_workers = min(len(tasks), MAX_CONCURRENT_TASKS) or 1

        registrations: Dict[str, Dict[str, Optional[str]]] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_id = {
                pool.submit(self._register_task, task, wave_num): task.id for task in tasks
            }
            for future in as_completed(future_to_id):
                registrations[future_to_id[future]] = future.result()

        results: Dict[str, Dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_task = {
                pool.submit(self._run_one, task, wave_num, registrations.get(task.id, {})): task
                for task in tasks
            }
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = {
                        'task_id': task.id,
                        'result': f"Unexpected wave-dispatch error: {e}",
                        'session_id': None,
                        'cost_usd': 0.0,
                        'success': False,
                        'error': str(e)
                    }
                reg = registrations.get(task.id, {})
                result['ruflo_task_id'] = reg.get('ruflo_task_id')
                result['ruflo_agent_id'] = reg.get('ruflo_agent_id')
                self._store_raw_result(task, wave_num, result)
                results[task.id] = result

        return results

    def _run_one(self, task: TaskNode, wave_num: int,
                 registration: Dict[str, Optional[str]]) -> Dict[str, Any]:
        ruflo_agent_id = registration.get('ruflo_agent_id')
        self.status_cb(task.id, wave_num, "running", "executing via claude -p", ruflo_agent_id)
        prompt = self._build_prompt(task, wave_num)
        result = self._run_claude(prompt, task)
        self.status_cb(task.id, wave_num, "executed" if result.get('success') else "errored",
                       str(result.get('result', ''))[:200], ruflo_agent_id)
        return result

    def _register_task(self, task: TaskNode, wave_num: int) -> Dict[str, Optional[str]]:
        """Best-effort registration of this task in the Brain's ruflo task/agent
        ledgers so ruflo genuinely owns wave coordination/tracking, not just
        memory storage. Failures are logged and swallowed - registration is
        coordination bookkeeping, not a precondition for real execution."""
        self.status_cb(task.id, wave_num, "spawning", "registering ruflo agent", None)
        ruflo_task_id = None
        ruflo_agent_id = None
        if self.brain:
            ruflo_task_id = self.brain.task_create(
                task.description, f"meridian,wave_{wave_num},{task.id}")
            ruflo_agent_id = self.brain.agent_spawn(
                f"meridian_{wave_num}_{task.id}", task.description)
            if ruflo_task_id and ruflo_agent_id:
                self.brain.task_assign(ruflo_task_id, ruflo_agent_id)
        return {'ruflo_task_id': ruflo_task_id, 'ruflo_agent_id': ruflo_agent_id}

    def _store_raw_result(self, task: TaskNode, wave_num: int, result: Dict[str, Any]) -> None:
        """Store the raw claude -p result in the Brain under a '_raw' key so it
        never collides with the canonical wave_{n}_task_{id} summary that
        state.py writes after verification (which _build_prompt reads). Note:
        completion state stays authoritative in Meridian's own state.py, gated
        by the verifier."""
        if not self.brain:
            return
        self.brain.memory_store(f"wave_{wave_num}_task_{task.id}_raw", {
            'task_id': task.id,
            'description': task.description,
            'result': result.get('result', ''),
            'cost_usd': result.get('cost_usd', 0.0),
            'success': result.get('success', False),
        })


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        brain = Brain(tmpdir) if is_ruflo_available() else None

        print("Test 1 - Prompt building:")
        task = TaskNode(
            id="test_1",
            description="Create a hello world function in Python"
        )
        executor = TaskExecutor(tmpdir, brain=brain)
        prompt = executor._build_prompt(task, wave_num=1)
        print(f"  Prompt length: {len(prompt)}")
        print(f"  Prompt preview: {prompt[:150]}...")

        print("\nTest 2 - Prompt with missing dependency context:")
        dependent_task = TaskNode(
            id="test_2",
            description="Write tests for the hello world function",
            dependencies=["test_1"]
        )
        prompt2 = executor._build_prompt(dependent_task, wave_num=1)
        print(f"  Contains 'Context from prior tasks:': {'Context from prior tasks:' in prompt2}")

        print("\nTest 3 - Ruflo availability:")
        print(f"  is_ruflo_available(): {is_ruflo_available()}")
        print(f"  USE_RUFLO config flag: {USE_RUFLO}")

        print("\nTest 4 - execute_wave dispatches all tasks, returns per-task results,")
        print("         and streams status callbacks (claude -p stubbed - no API cost):")
        transitions: List[tuple] = []

        def record_status(task_id, wave, status, detail, ruflo_agent_id):
            transitions.append((task_id, status))

        wave_executor = (RufloExecutor if brain else TaskExecutor)(
            tmpdir, brain=brain, status_cb=record_status)
        wave_executor._run_claude = lambda prompt, task: {
            'task_id': task.id, 'result': 'stub', 'session_id': None,
            'cost_usd': 0.0, 'success': True, 'output': {},
        }
        tasks = [
            TaskNode(id="w_1", description="No-op check A"),
            TaskNode(id="w_2", description="No-op check B"),
        ]
        results = wave_executor.execute_wave(tasks, wave_num=1)
        print(f"  Result keys: {sorted(results.keys())}")
        print(f"  All tasks reported: {set(results.keys()) == {'w_1', 'w_2'}}")
        statuses = {s for _, s in transitions}
        print(f"  Status transitions seen: {sorted(statuses)}")
        assert {'running', 'executed'} <= statuses
