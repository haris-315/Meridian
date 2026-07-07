import json
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Any, Optional

from dag import TaskNode
from brain import Brain, is_ruflo_available
from model_router import select_model
from context_digest import format_digest

USE_RUFLO = True  # set False to fall back to plain TaskExecutor
MAX_CONCURRENT_TASKS = 8
CLAUDE_TIMEOUT_SECONDS = 300

# status_cb(task_id, wave, status, detail, ruflo_agent_id) - wired by main.py to
# StateManager.set_agent_state so the dashboard sees live per-agent transitions.
StatusCallback = Callable[[str, int, str, str, Optional[str]], None]

# thought_cb(task_id, wave, kind, content) - wired by main.py to
# StateManager.log_agent_thought. kind is one of "thinking", "tool_use", "text".
ThoughtCallback = Callable[[str, int, str, str], None]


class TaskExecutor:
    def __init__(self, working_dir: str = ".", brain: Optional[Brain] = None,
                 status_cb: Optional[StatusCallback] = None, prior_context: str = "",
                 thought_cb: Optional[ThoughtCallback] = None):
        self.working_dir = working_dir
        self.brain = brain
        self.status_cb = status_cb or (lambda *args: None)
        self.thought_cb = thought_cb or (lambda *args: None)
        # Summary of the most recent completed run in this project, if any -
        # injected into wave-1 prompts since those tasks have no in-DAG
        # dependency to pull context from otherwise. Set by main.py after
        # querying state.get_last_completed_run_context().
        self.prior_context = prior_context

    def execute(self, task: TaskNode, wave_num: int = 0, retry_count: int = 0) -> Dict[str, Any]:
        """Execute a single task via Claude Code CLI headlessly, routed to a
        model tier by the task's declared complexity (escalating on retry)."""
        prompt = self._build_prompt(task, wave_num)
        model = select_model(task.complexity, retry_count)
        result = self._run_claude(prompt, task, wave_num, model)
        result['model'] = model
        return result

    def execute_wave(self, tasks: List[TaskNode], wave_num: int = 0,
                     retry_counts: Optional[Dict[str, int]] = None) -> Dict[str, Dict[str, Any]]:
        """Execute all ready tasks in this wave. Base implementation runs them
        sequentially (used when ruflo is unavailable); RufloExecutor overrides
        this to dispatch them concurrently with real ruflo coordination."""
        retry_counts = retry_counts or {}
        results = {}
        for task in tasks:
            self.status_cb(task.id, wave_num, "running", "executing via claude -p", None)
            result = self.execute(task, wave_num, retry_counts.get(task.id, 0))
            self.status_cb(task.id, wave_num, "executed" if result.get('success') else "errored",
                           str(result.get('result', ''))[:200], None)
            results[task.id] = result
        return results

    def _run_claude(self, prompt: str, task: TaskNode, wave_num: int = 0,
                    model: Optional[str] = None) -> Dict[str, Any]:
        """Run one agent turn via streaming JSON so intermediate reasoning
        (thinking blocks, tool calls, interim text) can be forwarded live to
        the dashboard through thought_cb, instead of only learning what
        happened once the whole call finishes. The final 'result' event
        carries the same fields --output-format json would have given as one
        shot, so error handling below still keys off the same structure."""
        cmd = ["claude", "-p", prompt, "--output-format", "stream-json", "--verbose",
               "--permission-mode", "acceptEdits"]
        if model:
            cmd += ["--model", model]

        try:
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                cwd=self.working_dir, stdin=subprocess.DEVNULL,
            )
        except FileNotFoundError:
            return {
                'task_id': task.id,
                'result': "Claude CLI not found. Install with: pip install claude-code",
                'session_id': None,
                'cost_usd': 0.0,
                'success': False,
                'error': 'claude_not_found',
            }

        final_result: Dict[str, Any] = {}

        def reader() -> None:
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                event_type = obj.get('type')
                if event_type == 'assistant':
                    for block in obj.get('message', {}).get('content', []):
                        block_type = block.get('type')
                        if block_type == 'thinking':
                            self.thought_cb(task.id, wave_num, 'thinking', block.get('thinking', ''))
                        elif block_type == 'tool_use':
                            summary = f"{block.get('name', '?')}({json.dumps(block.get('input', {}))[:150]})"
                            self.thought_cb(task.id, wave_num, 'tool_use', summary)
                        elif block_type == 'text':
                            self.thought_cb(task.id, wave_num, 'text', block.get('text', ''))
                elif event_type == 'result':
                    final_result.update(obj)

        reader_thread = threading.Thread(target=reader, daemon=True)
        reader_thread.start()

        try:
            proc.wait(timeout=CLAUDE_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            proc.kill()
            reader_thread.join(timeout=2)
            return {
                'task_id': task.id,
                'result': "Task execution timeout",
                'session_id': None,
                'cost_usd': 0.0,
                'success': False,
                'error': 'timeout',
            }

        reader_thread.join(timeout=5)
        stderr_output = ""
        try:
            stderr_output = proc.stderr.read()
        except Exception:
            pass

        if proc.returncode != 0 or not final_result:
            return self._build_cli_error_result(task, proc.returncode, final_result, stderr_output)

        is_error = final_result.get('is_error', False)
        return {
            'task_id': task.id,
            'result': final_result.get('result', ''),
            'session_id': final_result.get('session_id'),
            'cost_usd': final_result.get('total_cost_usd', 0.0),
            'success': not is_error,
            'output': final_result,
        }

    def _build_cli_error_result(self, task: TaskNode, returncode: int,
                                final_result: Dict[str, Any], stderr_output: str) -> Dict[str, Any]:
        """The streamed 'result' event still carries a real error message
        (e.g. rate limits) even when the process exits non-zero - fall back to
        stderr only if no result event was captured at all."""
        error_type = 'cli_error'
        if final_result and final_result.get('result'):
            api_error_status = final_result.get('api_error_status')
            message = final_result['result']
            if api_error_status == 429:
                error_type = 'rate_limited'
                message = f"[RATE LIMITED] {message}"
            reason = message
        else:
            reason = stderr_output.strip() or f"claude -p exited with code {returncode} and no output"

        return {
            'task_id': task.id,
            'result': reason,
            'session_id': final_result.get('session_id'),
            'cost_usd': final_result.get('total_cost_usd', 0.0),
            'success': False,
            'error': error_type,
        }

    def _build_prompt(self, task: TaskNode, wave_num: int = 0) -> str:
        """Build execution prompt from task description, its boundaries
        (dependency ids, which scope what this task may assume already exists),
        direct dependency context, a bounded rolling digest of the whole
        project's history (constant size regardless of how many waves/runs
        this project has accumulated), and retry diagnostics if applicable."""
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

        digest_section = ""
        if wave_num > 1:
            digest_text = format_digest(self.brain)
            if digest_text:
                digest_section = f"\n\n{digest_text}"

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

Boundaries: {boundaries}{prior_run_section}{context_section}{digest_section}{retry_section}

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

    def execute_wave(self, tasks: List[TaskNode], wave_num: int = 0,
                     retry_counts: Optional[Dict[str, int]] = None) -> Dict[str, Dict[str, Any]]:
        retry_counts = retry_counts or {}
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
                pool.submit(self._run_one, task, wave_num, registrations.get(task.id, {}),
                            retry_counts.get(task.id, 0)): task
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
                 registration: Dict[str, Optional[str]], retry_count: int = 0) -> Dict[str, Any]:
        ruflo_agent_id = registration.get('ruflo_agent_id')
        model = select_model(task.complexity, retry_count)
        self.status_cb(task.id, wave_num, "running", f"executing via claude -p ({model})", ruflo_agent_id)
        prompt = self._build_prompt(task, wave_num)
        result = self._run_claude(prompt, task, wave_num, model)
        result['model'] = model
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
        wave_executor._run_claude = lambda prompt, task, wave_num=0, model=None: {
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

        print("\nTest 5 - Model routing selects a tier per task complexity:")
        simple_task = TaskNode(id="s", description="trivial edit", complexity="simple")
        complex_task = TaskNode(id="c", description="hard design work", complexity="complex")
        wave_executor._run_claude = lambda prompt, task, wave_num=0, model=None: {
            'task_id': task.id, 'result': 'stub', 'session_id': None,
            'cost_usd': 0.0, 'success': True, 'output': {}, '_model_used': model,
        }
        r_simple = wave_executor.execute(simple_task, wave_num=1, retry_count=0)
        r_complex = wave_executor.execute(complex_task, wave_num=1, retry_count=0)
        print(f"  simple -> {r_simple['model']}, complex -> {r_complex['model']}")
        assert r_simple['model'] == 'haiku'
        assert r_complex['model'] == 'opus'

        print("\nAll tests passed!")
