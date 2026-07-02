import json
from dataclasses import dataclass, field
from typing import Dict, List, Union
from enum import Enum


class TaskStatus(Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


@dataclass
class TaskNode:
    id: str
    description: str
    dependencies: List[str] = field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    verify_commands: List[str] = field(default_factory=list)

    def __hash__(self) -> int:
        return hash(self.id)


class DAGBuilder:
    def __init__(self) -> None:
        self.tasks: Dict[str, TaskNode] = {}

    def build_from_goal(self, goal: str) -> Dict[str, TaskNode]:
        """Parse a plain text goal into tasks with implicit dependencies."""
        lines = goal.strip().split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if line:
                task_id = f"task_{i}"
                deps = [f"task_{j}" for j in range(i)]
                self.tasks[task_id] = TaskNode(
                    id=task_id,
                    description=line,
                    dependencies=deps
                )
        self.detect_cycles()
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
                verify_commands=verify_commands
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
    # Test 1: Simple linear goal
    builder = DAGBuilder()
    goal = "Create a function\nWrite tests\nRun tests"
    dag = builder.build_from_goal(goal)
    print("Test 1 - Linear DAG:")
    for task_id, node in dag.items():
        print(f"  {node.id}: {node.description} (deps: {node.dependencies})")

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
