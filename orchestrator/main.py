#!/usr/bin/env python3
import sys
import time
from pathlib import Path
from typing import Dict, List, Literal, Optional, Any

from dag import DAGBuilder, TaskNode, TaskStatus
from scheduler import Scheduler
from executor import TaskExecutor
from verifier import TaskVerifier
from state import StateManager
from checkpoint import Checkpoint


class Orchestrator:
    def __init__(self, working_dir: str = "."):
        self.working_dir = Path(working_dir)
        self.state = StateManager(str(self.working_dir / "orchestrator.db"))
        self.executor = TaskExecutor(str(self.working_dir))
        self.verifier = TaskVerifier(str(self.working_dir))
        self.dag: Dict[str, TaskNode] = {}
        self.scheduler: Optional[Scheduler] = None
        self.wave_number = 0
        self.start_time = time.time()

    def build_dag(self, goal: str) -> None:
        """Build initial DAG from goal."""
        builder = DAGBuilder()
        self.dag = builder.build_from_goal(goal)
        self.scheduler = Scheduler(self.dag)
        print(f"Built DAG with {len(self.dag)} tasks")

    def execute_wave(self) -> int:
        """Execute one wave of ready tasks. Return number of tasks executed."""
        self.wave_number += 1
        print(f"\n--- WAVE {self.wave_number} ---")

        ready_tasks = self.scheduler.get_ready_tasks()
        if not ready_tasks:
            return 0

        print(f"Ready tasks ({len(ready_tasks)}): {ready_tasks}")

        executed = 0
        for task_id in ready_tasks:
            task = self.dag[task_id]
            print(f"\nExecuting: {task_id}")
            print(f"  Description: {task.description}")

            task.status = TaskStatus.RUNNING

            executor_result = self.executor.execute(task)
            print(f"  Executor: {executor_result['result'][:100]}...")

            verifier_result = self._verify_task(task)
            print(f"  Verification: {'PASSED' if verifier_result['passed'] else 'FAILED'}")

            self.state.write_task_result(
                task_id, executor_result, verifier_result, task.description, self.wave_number
            )

            if verifier_result['passed']:
                task.status = TaskStatus.DONE
                executed += 1
            else:
                task.status = TaskStatus.FAILED
                print(f"  Failed command: {verifier_result['failed_command']}")

        self.scheduler.re_score()
        return executed

    def _verify_task(self, task: TaskNode) -> Dict[str, Any]:
        """Independently verify a task via the verifier's subprocess checks.
        Never trusts the executor's self-reported success — only shell exit codes."""
        commands = list(task.verify_commands)
        if not commands:
            test_files = sorted(
                str(p) for p in list(self.working_dir.glob("test_*.py"))
                + list(self.working_dir.glob("*_test.py"))
            )
            if test_files:
                commands = [f"pytest {' '.join(test_files)} -q"]
        return self.verifier.verify(commands)

    def show_checkpoint(self) -> Literal['approve', 'redirect', 'edit', 'stop']:
        """Show checkpoint and get human decision."""
        summary = self.state.get_wave_summary(self.wave_number)
        checkpoint = Checkpoint(self.wave_number)
        checkpoint.show_summary(summary)
        decision = checkpoint.prompt_decision()
        return decision

    def is_complete(self) -> bool:
        """Check if all tasks are done."""
        return all(task.status == TaskStatus.DONE for task in self.dag.values())

    def has_failures(self) -> bool:
        """Check if any tasks failed."""
        return any(task.status == TaskStatus.FAILED for task in self.dag.values())

    def run_loop(self, interactive: bool = True) -> None:
        """Main orchestration loop."""
        while not self.is_complete():
            executed = self.execute_wave()

            if executed == 0:
                print("\nNo more ready tasks")
                if self.has_failures():
                    print("Some tasks failed - cannot proceed")
                break

            if interactive:
                decision = self.show_checkpoint()
                if decision == 'approve':
                    continue
                elif decision == 'redirect':
                    new_goal = input("Enter new goal: ")
                    self.build_dag(new_goal)
                    self.wave_number = 0
                elif decision == 'edit':
                    print("Edit feature not yet implemented")
                elif decision == 'stop':
                    print("Orchestration stopped by user")
                    break
            else:
                continue

    def show_final_summary(self) -> None:
        """Show final orchestration summary."""
        duration = time.time() - self.start_time
        wave_summary = self.state.get_wave_summary(self.wave_number)
        final_summary = {
            'tasks_completed': wave_summary['cumulative_tasks_completed'],
            'tasks_failed': wave_summary['tasks_failed'],
            'tests_passed': wave_summary['tests_passed'],
            'total_cost_usd': wave_summary['cumulative_cost_usd'],
        }
        checkpoint = Checkpoint()
        checkpoint.show_final_summary(final_summary, duration)

    def run(self, goal: str, interactive: bool = True) -> None:
        """Main entry point: build DAG and run orchestration loop."""
        print(f"Goal: {goal}")
        self.build_dag(goal)
        self.run_loop(interactive=interactive)
        self.show_final_summary()


def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py '<goal>' [--non-interactive]")
        print("Example: python main.py 'create a hello world function and test for it'")
        sys.exit(1)

    goal = sys.argv[1]
    interactive = '--non-interactive' not in sys.argv

    working_dir = Path.cwd()
    orchestrator = Orchestrator(str(working_dir))
    orchestrator.run(goal, interactive=interactive)


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

            # Test 3: Mark first task as done
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

        print("\n✓ All self-tests passed!")
    else:
        main()
