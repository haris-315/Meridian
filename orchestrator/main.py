#!/usr/bin/env python3
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Any

from brain import Brain, is_ruflo_available
from dag import DAGBuilder, TaskNode, TaskStatus
from scheduler import Scheduler
from executor import TaskExecutor, RufloExecutor, USE_RUFLO
from verifier import TaskVerifier
from state import StateManager
from checkpoint import Checkpoint
from report import generate_final_report
from failure_analyzer import classify_failure, FailureType
from redecompose import redecompose_task

MAX_RETRIES_PER_TASK = 2
MAX_STAGNANT_WAVES = 2
RATE_LIMIT_BACKOFF_BASE_SECONDS = 60
RATE_LIMIT_BACKOFF_MAX_SECONDS = 600
MAX_CONSECUTIVE_RATE_LIMITED_WAVES = 5


class Orchestrator:
    def __init__(self, working_dir: str = "."):
        self.working_dir = Path(working_dir)
        self.brain = Brain(str(self.working_dir))
        self.state = StateManager(self.brain.orchestrator_db, brain=self.brain)
        self.executor = self._make_executor()
        self.verifier = TaskVerifier(str(self.working_dir))
        self.dag: Dict[str, TaskNode] = {}
        self.scheduler: Optional[Scheduler] = None
        self.wave_number = 0
        self.start_time = time.time()
        self.start_commit: Optional[str] = None
        self.retry_counts: Dict[str, int] = {}
        self.stagnant_waves = 0
        self.stalled = False
        self.stall_reason: Optional[str] = None
        self.consecutive_rate_limited_waves = 0
        self.last_wave_rate_limited_only = False
        self.redecomposed_ids: set = set()

    def _log(self, level: str, message: str, wave: int = 0) -> None:
        """Print to stdout AND persist to the events table so the dashboard's
        live log stream sees the same thing a terminal user does."""
        print(message)
        self.state.log_event(level, "orchestrator", message, wave)

    def _make_executor(self) -> TaskExecutor:
        """Pick RufloExecutor when USE_RUFLO is enabled and the ruflo CLI is
        actually on PATH; otherwise fall back to plain TaskExecutor and say why."""
        status_cb = self.state.set_agent_state
        thought_cb = self.state.log_agent_thought
        if USE_RUFLO:
            if is_ruflo_available():
                print("[INFO] Ruflo CLI detected - using RufloExecutor")
                return RufloExecutor(str(self.working_dir), brain=self.brain,
                                     status_cb=status_cb, thought_cb=thought_cb)
            print("[WARN] USE_RUFLO is True but 'ruflo' CLI was not found on PATH; "
                  "falling back to TaskExecutor", file=sys.stderr)
        return TaskExecutor(str(self.working_dir), brain=self.brain,
                            status_cb=status_cb, thought_cb=thought_cb)

    @staticmethod
    def _format_prior_context(context: Dict[str, Any]) -> str:
        """Render a prior completed run's summary as prompt text: what was
        asked for, and the outcome of every task, so a new goal in the same
        project (e.g. 'add a divide function to the calculator') can build on
        real prior work instead of guessing what already exists."""
        lines = [
            f"Context - this project already has a completed prior run:",
            f'Prior goal: "{context["goal"]}"',
            "Prior task outcomes:",
        ]
        tasks = context['tasks']
        if len(tasks) > 8:
            lines.append(f"  ...{len(tasks) - 8} earlier task(s) omitted for brevity...")
            tasks = tasks[-8:]
        for t in tasks:
            outcome = (t['result'] or '')[:200].replace('\n', ' ')
            lines.append(f"  - [{t['status']}] {t['task_id']}: {t['description']} -> {outcome}")
        lines.append(
            "Build on this existing work where relevant; do not recreate what already exists."
        )
        return "\n".join(lines)

    def build_dag(self, goal: str) -> None:
        """Build initial DAG from goal, informed by the most recent completed
        run in this project if one exists (cross-run context)."""
        prior = self.state.get_last_completed_run_context()
        prior_context_text = self._format_prior_context(prior) if prior else ""
        if prior:
            self._log("info", f"Found prior completed run (goal: \"{prior['goal'][:80]}\") - "
                              "injecting its context into this run")
        self.executor.prior_context = prior_context_text

        self._log("info", "Decomposing goal into a task DAG (LLM call)...")
        builder = DAGBuilder()
        self.dag = builder.build_from_goal(goal, prior_context=prior_context_text)
        self.scheduler = Scheduler(self.dag)
        self._log("info", f"Built DAG with {len(self.dag)} tasks")
        for task_id, node in self.dag.items():
            deps = ", ".join(node.dependencies) if node.dependencies else "none"
            self._log("info", f"  {task_id}: {node.description[:120]} (deps: {deps})")
        self.state.sync_dag(self.dag)

    def _print_dag_state(self, ready_ids: List[str]) -> None:
        """Print the full DAG state and ready queue at the start of every wave,
        so progress is visible live with no human polling required."""
        print("\nDAG state:")
        for task_id, node in self.dag.items():
            if node.dependencies:
                deps_str = ", ".join(f"{d}({self.dag[d].status.value})" for d in node.dependencies)
            else:
                deps_str = "none"
            print(f"  {task_id} [{node.status.value}] deps=[{deps_str}]")
        print(f"Ready queue ({len(ready_ids)}): {ready_ids}")

    def execute_wave(self) -> int:
        """Execute one wave of ready tasks concurrently. Return number of tasks
        that passed verification this wave."""
        self.wave_number += 1
        self._log("info", f"--- WAVE {self.wave_number} ---", self.wave_number)

        ready_ids = self.scheduler.get_ready_tasks()
        self._print_dag_state(ready_ids)
        if not ready_ids:
            return 0
        self._log("info", f"Dispatching {len(ready_ids)} task(s) in parallel: {ready_ids}",
                  self.wave_number)

        ready_tasks = [self.dag[task_id] for task_id in ready_ids]
        for task in ready_tasks:
            task.status = TaskStatus.RUNNING
        self.state.sync_dag(self.dag)

        executor_results = self.executor.execute_wave(ready_tasks, self.wave_number, self.retry_counts)

        executed = 0
        rate_limited_ids = set()
        non_rate_limited_failure_ids = set()
        for task in ready_tasks:
            executor_result = executor_results[task.id]
            print(f"\nExecuted: {task.id}")
            print(f"  Description: {task.description}")
            print(f"  Executor: {str(executor_result.get('result', ''))[:100]}...")

            self.state.set_agent_state(task.id, self.wave_number, "verifying",
                                       "running independent verification", None)
            verifier_result = self._verify_task(task)
            passed = verifier_result['passed']
            self._log("info" if passed else "warn",
                      f"{task.id} verification: {'PASSED' if passed else 'FAILED'}",
                      self.wave_number)

            if passed:
                task.status = TaskStatus.DONE
                executed += 1
                self.retry_counts.pop(task.id, None)
                self.state.set_agent_state(task.id, self.wave_number, "done", "verified", None)
            else:
                diagnosis = classify_failure(executor_result, verifier_result)
                is_rate_limited = diagnosis.failure_type == FailureType.RATE_LIMITED
                if is_rate_limited:
                    rate_limited_ids.add(task.id)
                else:
                    non_rate_limited_failure_ids.add(task.id)
                self.state.store_reasoning(
                    task.id, self.wave_number, diagnosis.failure_type,
                    diagnosis.diagnostic_message, self.retry_counts.get(task.id, 0),
                )
                self._handle_task_failure(task, rate_limited=is_rate_limited)
                self._log("error", f"{task.id} failed command: {verifier_result['failed_command']}",
                          self.wave_number)
                agent_status = "retrying" if task.status == TaskStatus.PENDING else "failed"
                self.state.set_agent_state(task.id, self.wave_number, agent_status,
                                           diagnosis.diagnostic_message[:200], None)

            self.state.write_task_result(
                task.id, executor_result, verifier_result, task.description, self.wave_number,
                ruflo_task_id=executor_result.get('ruflo_task_id'),
                ruflo_agent_id=executor_result.get('ruflo_agent_id'),
                retry_count=self.retry_counts.get(task.id, 0),
                model=executor_result.get('model'),
            )

        # Only treat the wave as "purely rate limited" (worth a silent backoff
        # instead of burning retry budget / escalating) if every failure this
        # wave was a rate limit - a mixed wave with a real failure alongside
        # still goes through the normal retry/stall path for that real failure.
        self.last_wave_rate_limited_only = (
            executed == 0 and bool(rate_limited_ids) and not non_rate_limited_failure_ids
        )

        self.scheduler.re_score()
        self.state.sync_dag(self.dag)
        self.state.save_checkpoint(self.dag, self.wave_number, self.retry_counts)
        return executed

    def _handle_task_failure(self, task: TaskNode, rate_limited: bool = False) -> None:
        """Reset a failed task to PENDING for another attempt if under the retry
        budget; otherwise leave it FAILED permanently (caught by stall escalation,
        never retried forever). Rate-limited failures never consume retry budget -
        an HTTP 429 isn't evidence the task itself is broken, so a real code bug
        should not get fewer real attempts because the API happened to throttle."""
        if rate_limited:
            task.status = TaskStatus.PENDING
            print(f"  {task.id} rate limited - will retry without spending retry budget")
            return

        used = self.retry_counts.get(task.id, 0)
        if used < MAX_RETRIES_PER_TASK:
            self.retry_counts[task.id] = used + 1
            task.status = TaskStatus.PENDING
            print(f"  Retry {used + 1}/{MAX_RETRIES_PER_TASK} scheduled for {task.id}")
        else:
            task.status = TaskStatus.FAILED
            print(f"  {task.id} FAILED permanently after {used} retries")

    @staticmethod
    def _normalize_verify_command(cmd: str) -> str:
        """LLM-generated verify_commands sometimes use `python -m pytest`, which
        fails when pytest is installed standalone (pipx) rather than into the
        interpreter. The bare `pytest` binary on PATH works in both setups."""
        stripped = cmd.strip()
        for prefix in ("python -m pytest", "python3 -m pytest"):
            if stripped.startswith(prefix):
                return "pytest" + stripped[len(prefix):]
        return cmd

    def _verify_task(self, task: TaskNode) -> Dict[str, Any]:
        """Independently verify a task via the verifier's subprocess checks.
        Never trusts the executor's self-reported success - only shell exit codes."""
        commands = [self._normalize_verify_command(c) for c in task.verify_commands]
        if not commands:
            test_files = sorted(
                str(p) for p in list(self.working_dir.glob("test_*.py"))
                + list(self.working_dir.glob("*_test.py"))
            )
            if test_files:
                commands = [f"pytest {' '.join(test_files)} -q"]
        return self.verifier.verify(commands)

    def try_resume(self) -> bool:
        """Check this project's brain for a run left 'interrupted' by a crash
        (start_run() marks any dangling 'running' row this way on the next
        startup) and rebuild the DAG from its last checkpoint. Returns True if
        a run was resumed - build_dag/the LLM decomposition call is skipped
        entirely in that case, so a crash costs at most one wave of progress,
        not the whole run's decomposition + completed waves."""
        resumable = self.state.find_resumable_run()
        if not resumable:
            return False

        self._log("info", f"Found interrupted run (id={resumable['run_id']}, "
                          f"wave {resumable['wave_number']}) - resuming instead of restarting")
        snapshot = json.loads(resumable['dag_json'])
        self.dag = {}
        for node in snapshot:
            status = TaskStatus(node['status'])
            # A task recorded RUNNING at crash time never got verified - treat
            # it as not-yet-attempted rather than assuming it finished.
            if status == TaskStatus.RUNNING:
                status = TaskStatus.PENDING
            self.dag[node['id']] = TaskNode(
                id=node['id'], description=node['description'],
                dependencies=node['dependencies'], status=status,
                verify_commands=node.get('verify_commands', []),
            )
        self.scheduler = Scheduler(self.dag)
        self.wave_number = resumable['wave_number']
        self.retry_counts = resumable['retry_counts']
        self.state.sync_dag(self.dag)
        return True

    def is_complete(self) -> bool:
        """Check if all tasks are done or skipped (skipped counts as terminal)."""
        return all(task.status in (TaskStatus.DONE, TaskStatus.SKIPPED) for task in self.dag.values())

    def has_failures(self) -> bool:
        """Check if any tasks failed permanently."""
        return any(task.status == TaskStatus.FAILED for task in self.dag.values())

    def _describe_stall(self) -> str:
        if self.consecutive_rate_limited_waves > MAX_CONSECUTIVE_RATE_LIMITED_WAVES:
            return (f"rate limited for {self.consecutive_rate_limited_waves} consecutive waves "
                    f"despite backoff - the API limit does not appear to be resetting")
        exhausted_ids = [t.id for t in self.dag.values() if t.status == TaskStatus.FAILED]
        if exhausted_ids:
            return f"{len(exhausted_ids)} task(s) exhausted their retry budget: {', '.join(exhausted_ids)}"
        return f"{self.stagnant_waves} consecutive waves produced zero progress"

    def _escalate_to_human(self) -> str:
        reason = self._describe_stall()
        stuck_ids = {t.id for t in self.dag.values() if t.status == TaskStatus.FAILED}
        all_failed = self.state.get_failed_tasks()
        stuck_tasks = [t for t in all_failed if t['task_id'] in stuck_ids] or all_failed
        cost_so_far = self.state.get_full_run_summary()['total_cost_usd']

        checkpoint = Checkpoint()
        checkpoint.show_stall_alert(reason, stuck_tasks, cost_so_far)
        return checkpoint.prompt_stall_decision()

    def _attempt_redecomposition(self, task_id: str) -> bool:
        """Try to recover a permanently-failed task by splitting it into
        smaller subtasks, informed by its failure history - attempted once per
        task, before ever escalating to a human. Returns True if the DAG was
        successfully rewired (new subtasks spliced in, dependents repointed)
        and the run can continue autonomously."""
        if task_id in self.redecomposed_ids:
            return False
        self.redecomposed_ids.add(task_id)

        task = self.dag[task_id]
        reasoning = self.state.get_reasoning_for_task(task_id)
        failure_context = "\n".join(r['diagnostic_message'] for r in reasoning[-3:]) or "No details captured."

        self._log("info", f"Attempting auto-redecomposition of stuck task {task_id}...")
        subtasks_data = redecompose_task(task, failure_context)
        if not subtasks_data:
            self._log("warn", f"Auto-redecomposition of {task_id} failed (LLM call unusable)")
            return False

        id_map = {t['id']: f"{task_id}_r{i}" for i, t in enumerate(subtasks_data)}
        new_nodes: Dict[str, TaskNode] = {}
        for t in subtasks_data:
            new_id = id_map[t['id']]
            deps = [id_map[d] for d in t.get('dependencies', []) if d in id_map]
            if not deps:
                deps = list(task.dependencies)
            new_nodes[new_id] = TaskNode(
                id=new_id, description=t['description'], dependencies=deps,
                verify_commands=t.get('verify_commands', []),
            )

        # Anything that depended on the failed task now depends on its new
        # leaf subtasks (those nothing else in the replacement group depends on).
        leaf_ids = [
            nid for nid in new_nodes
            if not any(nid in other.dependencies for other in new_nodes.values())
        ]
        for other_task in self.dag.values():
            if task_id in other_task.dependencies:
                other_task.dependencies = [d for d in other_task.dependencies if d != task_id] + leaf_ids

        del self.dag[task_id]
        self.dag.update(new_nodes)
        self.scheduler = Scheduler(self.dag)
        self._log("info", f"Replaced {task_id} with {len(new_nodes)} subtask(s): {list(new_nodes.keys())}")
        self.state.sync_dag(self.dag)
        return True

    def _reset_all_retryable_failures(self) -> None:
        for task in self.dag.values():
            if task.status == TaskStatus.FAILED:
                task.status = TaskStatus.PENDING
                self.retry_counts.pop(task.id, None)
        self.scheduler.re_score()

    def _skip_stuck_and_dependents(self) -> None:
        """Mark permanently-failed tasks SKIPPED, and transitively skip anything
        that depends on them (directly or indirectly), since they can never
        become ready otherwise - a dependent's readiness check requires its
        dependency to be DONE, not SKIPPED."""
        stuck = [t.id for t in self.dag.values() if t.status == TaskStatus.FAILED]
        to_skip = set(stuck)
        queue = list(stuck)
        while queue:
            task_id = queue.pop()
            for dependent_id in self.scheduler.dependents.get(task_id, []):
                if dependent_id not in to_skip:
                    to_skip.add(dependent_id)
                    queue.append(dependent_id)

        for task_id in to_skip:
            self.dag[task_id].status = TaskStatus.SKIPPED
        self.scheduler.re_score()
        self.state.sync_dag(self.dag)

    def run_loop(self, allow_escalation: bool = True) -> None:
        """Main orchestration loop: fully autonomous wave-to-wave, no per-wave
        human checkpoint. Escalates to a human only on a genuine stall (a task
        exhausted its retry budget, or MAX_STAGNANT_WAVES consecutive waves made
        zero progress). A wave where every failure was an API rate limit (429)
        gets a silent capped exponential backoff instead - it isn't evidence of
        a real problem, just external throttling - and only escalates if the
        limit still hasn't cleared after MAX_CONSECUTIVE_RATE_LIMITED_WAVES."""
        while not self.is_complete():
            executed = self.execute_wave()

            if self.last_wave_rate_limited_only:
                self.consecutive_rate_limited_waves += 1
                if self.consecutive_rate_limited_waves <= MAX_CONSECUTIVE_RATE_LIMITED_WAVES:
                    backoff = min(
                        RATE_LIMIT_BACKOFF_BASE_SECONDS * (2 ** (self.consecutive_rate_limited_waves - 1)),
                        RATE_LIMIT_BACKOFF_MAX_SECONDS,
                    )
                    print(f"\n[RATE LIMIT] Wave made no progress due to API rate limiting. "
                          f"Backing off {backoff}s before retrying "
                          f"(attempt {self.consecutive_rate_limited_waves}/{MAX_CONSECUTIVE_RATE_LIMITED_WAVES})...")
                    time.sleep(backoff)
                    continue
                # Rate limited beyond our patience - fall through to stall escalation below.
            else:
                self.consecutive_rate_limited_waves = 0

            self.stagnant_waves = 0 if executed else self.stagnant_waves + 1

            exhausted = self.has_failures()
            persistent_rate_limit = self.consecutive_rate_limited_waves > MAX_CONSECUTIVE_RATE_LIMITED_WAVES
            if not (exhausted or self.stagnant_waves >= MAX_STAGNANT_WAVES or persistent_rate_limit):
                continue

            if exhausted and not persistent_rate_limit:
                stuck_ids = [t.id for t in self.dag.values() if t.status == TaskStatus.FAILED]
                if any(self._attempt_redecomposition(tid) for tid in stuck_ids):
                    self.stagnant_waves = 0
                    continue

            if not allow_escalation:
                self.stalled = True
                self.stall_reason = self._describe_stall()
                print(f"\n[STALL] {self.stall_reason} - escalation disabled (non-interactive mode), stopping.")
                break

            decision = self._escalate_to_human()
            if decision == 'stop':
                self.stalled = True
                self.stall_reason = self._describe_stall()
                print("Orchestration stopped by user")
                break
            elif decision == 'retry_all':
                self._reset_all_retryable_failures()
                self.stagnant_waves = 0
                self.consecutive_rate_limited_waves = 0
            elif decision == 'skip_stuck':
                self._skip_stuck_and_dependents()
                self.stagnant_waves = 0
                self.consecutive_rate_limited_waves = 0

    def _capture_start_commit(self) -> Optional[str]:
        try:
            result = subprocess.run(
                ["git", "-C", str(self.working_dir), "rev-parse", "HEAD"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        return None

    def show_final_summary(self) -> Dict[str, Any]:
        """Build and show the final structured report: summary, confidence, test
        results, documentation files created. Prints both a human-readable form
        and raw JSON to stdout (most runs finish with no human present)."""
        duration = time.time() - self.start_time
        report = generate_final_report(
            self.state, self.dag, self.working_dir, duration,
            self.start_commit, self.start_time, self.stalled, self.stall_reason,
        )
        checkpoint = Checkpoint()
        checkpoint.show_final_summary(report)
        print(json.dumps(report, indent=2))
        return report

    def run(self, goal: str, allow_escalation: bool = True, resume: bool = True) -> Dict[str, Any]:
        """Main entry point: resume an interrupted run if one exists in this
        project's brain, otherwise build a fresh DAG from `goal` and run the
        orchestration loop. `resume` is skipped for the self-test/JSON-DAG
        call paths where a fresh DAG is explicitly wanted."""
        self.state.start_run(goal, str(self.working_dir))
        self._log("info", f"Goal: {goal}")
        self.start_commit = self._capture_start_commit()
        if not (resume and self.try_resume()):
            self.build_dag(goal)
        self.run_loop(allow_escalation=allow_escalation)
        report = self.show_final_summary()
        run_status = "stalled" if self.stalled else ("complete" if self.is_complete() else "stopped")
        self.state.finish_run(run_status, report.get('cost_usd', 0.0))
        return report


def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py '<goal>' [--non-interactive]")
        print("Example: python main.py 'create a hello world function and test for it'")
        print("  --non-interactive: on a stall, stop and report instead of prompting a human")
        sys.exit(1)

    goal = sys.argv[1]
    allow_escalation = '--non-interactive' not in sys.argv

    working_dir = Path.cwd()
    orchestrator = Orchestrator(str(working_dir))
    orchestrator.run(goal, allow_escalation=allow_escalation)


if __name__ == "__main__":
    # Test mode
    if len(sys.argv) == 1 or (len(sys.argv) == 2 and sys.argv[1] == "--test"):
        print("Running self-test...")

        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)

            orchestrator = Orchestrator(tmpdir)

            # Test 1: Build DAG from goal
            print("\nTest 1 - Build DAG:")
            goal = "Create function\nWrite test\nRun test"
            orchestrator.build_dag(goal)
            print(f"  DAG size: {len(orchestrator.dag)}")
            print(f"  Tasks: {list(orchestrator.dag.keys())}")

            # Test 2: Get ready tasks
            print("\nTest 2 - Get ready tasks:")
            ready = orchestrator.scheduler.get_ready_tasks()
            print(f"  Initially ready: {ready}")

            # Test 3: Task completion
            print("\nTest 3 - Task completion:")
            task = orchestrator.dag['task_0']
            task.status = TaskStatus.DONE
            orchestrator.state.write_task_result(
                'task_0',
                {'task_id': 'task_0', 'result': 'Done', 'cost_usd': 0.001},
                {'passed': True, 'output': 'OK', 'failed_command': None},
                'Create function'
            )
            orchestrator.scheduler.re_score()
            ready = orchestrator.scheduler.get_ready_tasks()
            print(f"  After task_0 done: {ready}")

            # Test 4: Check state summary
            print("\nTest 4 - State summary:")
            summary = orchestrator.state.get_wave_summary()
            print(f"  Completed: {summary['tasks_completed']}")
            print(f"  Cost: ${summary['total_cost_usd']}")

            # Test 5: Wave dispatch is concurrent, not sequential
            # (stubs the actual claude -p call so this test costs no real API
            # calls/time - it verifies OUR ThreadPoolExecutor dispatch code path,
            # not LLM behavior.)
            print("\nTest 5 - Concurrent wave dispatch:")
            import time as _time
            from dag import TaskNode as _TaskNode

            call_starts: List[float] = []

            def fake_run_claude(prompt, task):
                call_starts.append(_time.time())
                _time.sleep(0.3)
                return {
                    'task_id': task.id, 'result': 'stub', 'session_id': None,
                    'cost_usd': 0.0, 'success': True, 'output': {},
                }

            orchestrator.executor._run_claude = fake_run_claude
            wave_tasks = [
                _TaskNode(id="par_a", description="Independent task A"),
                _TaskNode(id="par_b", description="Independent task B"),
            ]
            wave_start = _time.time()
            results = orchestrator.executor.execute_wave(wave_tasks, wave_num=99)
            wave_duration = _time.time() - wave_start
            overlapped = len(call_starts) == 2 and abs(call_starts[0] - call_starts[1]) < 0.2
            print(f"  Both tasks reported: {set(results.keys()) == {'par_a', 'par_b'}}")
            print(f"  Wave duration ~0.3s (concurrent) not ~0.6s (sequential): {wave_duration:.2f}s")
            print(f"  Start times overlapped: {overlapped}")

            # Tests 6-7 use a small deterministic DAG (built via build_from_json,
            # not the LLM-decomposed one from Test 1) so retry/skip assertions
            # don't depend on whatever shape the LLM happens to produce this run.
            retry_orchestrator = Orchestrator(tmpdir)
            retry_orchestrator.dag = DAGBuilder().build_from_json(json.dumps([
                {"id": "r0", "description": "root", "dependencies": []},
                {"id": "r1", "description": "stuck branch", "dependencies": ["r0"]},
                {"id": "r2", "description": "dependent on stuck branch", "dependencies": ["r1"]},
                {"id": "r3", "description": "independent sibling branch", "dependencies": ["r0"]},
            ]))
            retry_orchestrator.scheduler = Scheduler(retry_orchestrator.dag)
            retry_orchestrator.dag['r0'].status = TaskStatus.DONE
            retry_orchestrator.dag['r3'].status = TaskStatus.DONE

            # Test 6: Retry policy
            print("\nTest 6 - Retry policy:")
            failing_task = retry_orchestrator.dag['r1']
            failing_task.status = TaskStatus.RUNNING
            for i in range(MAX_RETRIES_PER_TASK):
                retry_orchestrator._handle_task_failure(failing_task)
                assert failing_task.status == TaskStatus.PENDING, "should retry while under budget"
            retry_orchestrator._handle_task_failure(failing_task)
            print(f"  Status after {MAX_RETRIES_PER_TASK + 1} failures: {failing_task.status.value}")
            assert failing_task.status == TaskStatus.FAILED, "should be permanently failed after budget exhausted"

            # Test 7: Skip stuck + transitive dependents (siblings of the stuck
            # task, like r3, are untouched and stay DONE - only the stuck task and
            # things that depend on it are skipped)
            print("\nTest 7 - Skip stuck task and dependents:")
            retry_orchestrator._skip_stuck_and_dependents()
            skipped = [t.id for t in retry_orchestrator.dag.values() if t.status == TaskStatus.SKIPPED]
            print(f"  Skipped: {skipped}")
            assert set(skipped) == {'r1', 'r2'}, f"expected r1+r2 skipped, r3 untouched, got {skipped}"
            assert retry_orchestrator.dag['r3'].status == TaskStatus.DONE, "sibling branch must not be skipped"
            assert retry_orchestrator.is_complete(), "run should be complete once stuck task + dependents are skipped/done"

        print("\n✓ All self-tests passed!")
    else:
        main()
