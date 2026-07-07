import json
import subprocess
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union
from enum import Enum


class TaskStatus(Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TaskNode:
    id: str
    description: str
    dependencies: List[str] = field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    verify_commands: List[str] = field(default_factory=list)
    complexity: str = "medium"  # "simple" | "medium" | "complex" - drives model routing

    def __hash__(self) -> int:
        return hash(self.id)


class DAGBuilder:
    def __init__(self) -> None:
        self.tasks: Dict[str, TaskNode] = {}

    def build_from_goal(self, goal: str, prior_context: str = "") -> Dict[str, TaskNode]:
        """Decompose a plain text goal into a task DAG via a headless Claude call.
        `prior_context`, if given, is a summary of the most recent completed run
        in this project - so "refactor the calculator" decomposes with real
        knowledge of what add()/subtract()/etc. already exist, instead of
        re-planning from a blank slate. Falls back to a single task if
        decomposition fails for any reason."""
        tasks_data = self._decompose_goal(goal, prior_context)
        if not tasks_data:
            return self._single_task_fallback(goal)

        self.tasks = {}
        for task in tasks_data:
            task_id = task['id']
            self.tasks[task_id] = TaskNode(
                id=task_id,
                description=task['description'],
                dependencies=task.get('dependencies', []),
                verify_commands=task.get('verify_commands', []),
                complexity=task.get('complexity', 'medium'),
            )

        try:
            self.detect_cycles()
        except ValueError:
            return self._single_task_fallback(goal)

        return self.tasks

    def _decompose_goal(self, goal: str, prior_context: str = "") -> Optional[List[Dict]]:
        """Call claude -p headlessly to break the goal into subtasks. Returns
        None (never raises) if the call fails, times out, or the output can't
        be parsed into a valid task list."""
        prompt = self._build_decomposition_prompt(goal, prior_context)

        try:
            result = subprocess.run(
                ["claude", "-p", prompt, "--output-format", "json",
                 "--permission-mode", "acceptEdits"],
                capture_output=True,
                text=True,
                timeout=60,
                stdin=subprocess.DEVNULL
            )
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None

        if result.returncode != 0:
            return None

        try:
            output = json.loads(result.stdout)
        except json.JSONDecodeError:
            return None

        raw = output.get("result", "")
        return self._parse_task_list(raw)

    def _parse_task_list(self, raw: str) -> Optional[List[Dict]]:
        """Parse the JSON task list out of the model's text response, stripping
        markdown code fences if present. Returns None if invalid."""
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines).strip()

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return None

        if not isinstance(data, list) or len(data) < 1:
            return None

        for task in data:
            if not isinstance(task, dict) or 'id' not in task or 'description' not in task:
                return None

        return data

    def _build_decomposition_prompt(self, goal: str, prior_context: str = "") -> str:
        """Build the prompt instructing Claude to decompose the goal into a JSON task DAG."""
        context_block = f"\n{prior_context}\n" if prior_context else ""
        return f"""Break the following goal into the MINIMUM number of focused, independently
executable subtasks needed to accomplish it correctly.

Goal: {goal}
{context_block}

Rules:
- Use the fewest tasks that make sense. If the entire goal is simple enough for a single
  agent to complete correctly in one attempt (e.g. one small file, one function), return
  EXACTLY ONE task - do not manufacture artificial splits (like a separate "write tests"
  task for a two-line script) just to produce multiple tasks.
- Each subtask must be self-contained and describe one unit of work.
- Only add a dependency between two tasks if one genuinely cannot start before the other finishes.
- Tasks that can run in parallel must have NO dependency on each other.
- "complexity" must be one of "simple", "medium", or "complex", reflecting how much
  capability the task needs: "simple" for boilerplate/small well-defined edits, "medium"
  for typical feature work, "complex" for anything requiring careful design, multi-file
  coordination, or nontrivial algorithmic correctness. This is used to route the task to
  a cheaper or stronger model - be honest, do not default everything to "medium".
- "verify_commands" should contain real shell commands that verify this specific task's output
  (e.g. "pytest test_file.py", "node file.js"). Use an empty list [] if no command applies.
- verify_commands must invoke tools by their PATH name directly (e.g. "pytest x.py",
  NOT "python -m pytest x.py") - module-style invocation breaks when the tool is
  installed standalone rather than into the interpreter's site-packages.
- Do not write, create, or modify any files. Only output the plan below.
- Return ONLY valid JSON, no prose, no markdown fences, in exactly this format:
[{{"id": "task_0", "description": "...", "dependencies": [], "verify_commands": [], "complexity": "medium"}}, ...]
"""

    def _single_task_fallback(self, goal: str) -> Dict[str, TaskNode]:
        """Fallback: create a single task with the full goal as description."""
        self.tasks = {
            "task_0": TaskNode(id="task_0", description=goal.strip())
        }
        return self.tasks

    def build_from_json(self, tasks_json: str) -> Dict[str, TaskNode]:
        """Parse a JSON task list into a DAG."""
        data = json.loads(tasks_json)
        for task in data:
            task_id = task['id']
            deps = task.get('dependencies', [])
            verify_commands = task.get('verify_commands', [])
            self.tasks[task_id] = TaskNode(
                id=task_id,
                description=task['description'],
                dependencies=deps,
                verify_commands=verify_commands,
                complexity=task.get('complexity', 'medium'),
            )
        self.detect_cycles()
        return self.tasks

    def detect_cycles(self) -> None:
        """Raise if circular dependency found using DFS."""
        visited = set()
        rec_stack = set()

        def dfs(node_id: str) -> None:
            visited.add(node_id)
            rec_stack.add(node_id)

            node = self.tasks.get(node_id)
            if not node:
                raise ValueError(f"Task {node_id} not found")

            for dep_id in node.dependencies:
                if dep_id not in visited:
                    dfs(dep_id)
                elif dep_id in rec_stack:
                    raise ValueError(f"Circular dependency detected: {node_id} -> {dep_id}")

            rec_stack.remove(node_id)

        for task_id in self.tasks:
            if task_id not in visited:
                dfs(task_id)

    def build(self, goal: Union[str, None] = None) -> Dict[str, TaskNode]:
        """Build the DAG from a plain text goal or JSON task list, or return
        the already-built DAG if goal is None."""
        if goal is None:
            return self.tasks
        stripped = goal.strip()
        if stripped.startswith('[') or stripped.startswith('{'):
            return self.build_from_json(goal)
        return self.build_from_goal(goal)


if __name__ == "__main__":
    # Test 1: LLM-powered decomposition of a single-line goal
    builder = DAGBuilder()
    goal = "build a Python calculator module with add, subtract, multiply and divide functions, plus a pytest test file"
    dag = builder.build_from_goal(goal)
    print("Test 1 - LLM-decomposed DAG:")
    for task_id, node in dag.items():
        print(f"  {node.id}: {node.description} (deps: {node.dependencies}, verify: {node.verify_commands})")

    # Test 2: JSON DAG with explicit dependencies
    builder2 = DAGBuilder()
    json_tasks = json.dumps([
        {"id": "setup", "description": "Setup environment"},
        {"id": "code", "description": "Write code", "dependencies": ["setup"]},
        {"id": "test", "description": "Test code", "dependencies": ["code"]},
    ])
    dag2 = builder2.build_from_json(json_tasks)
    print("\nTest 2 - JSON DAG:")
    for task_id, node in dag2.items():
        print(f"  {node.id}: {node.description} (deps: {node.dependencies})")

    # Test 3: Cycle detection
    print("\nTest 3 - Cycle detection:")
    builder3 = DAGBuilder()
    try:
        json_cycle = json.dumps([
            {"id": "a", "description": "Task A", "dependencies": ["b"]},
            {"id": "b", "description": "Task B", "dependencies": ["a"]},
        ])
        dag3 = builder3.build_from_json(json_cycle)
        print("  ERROR: Should have detected cycle!")
    except ValueError as e:
        print(f"  Correctly detected: {e}")

    # Test 4: Fallback when decomposition is unusable
    print("\nTest 4 - Single-task fallback:")
    builder4 = DAGBuilder()
    fallback_dag = builder4._single_task_fallback("some goal that could not be decomposed")
    print(f"  Fallback tasks: {list(fallback_dag.keys())}")
    print(f"  task_0 description: {fallback_dag['task_0'].description}")
