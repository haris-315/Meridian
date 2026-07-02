from collections import deque
from typing import Dict, List
from dag import TaskNode, TaskStatus


class Scheduler:
    def __init__(self, dag: Dict[str, TaskNode]):
        self.dag = dag
        self.in_degree = self._compute_in_degree()
        self.dependents = self._compute_dependents()

    def _compute_in_degree(self) -> Dict[str, int]:
        """Compute in-degree for each task, counting each distinct dependency once."""
        in_degree = {task_id: 0 for task_id in self.dag}
        for task in self.dag.values():
            in_degree[task.id] = len(set(task.dependencies))
        return in_degree

    def _compute_dependents(self) -> Dict[str, List[str]]:
        """Build reverse-dependency map: dep_id -> [task_ids that depend on it]."""
        dependents: Dict[str, List[str]] = {task_id: [] for task_id in self.dag}
        for task in self.dag.values():
            for dep_id in set(task.dependencies):
                if dep_id in dependents:
                    dependents[dep_id].append(task.id)
        return dependents

    def topological_sort(self) -> List[str]:
        """Kahn's algorithm for topological sort."""
        in_degree = self.in_degree.copy()
        queue = deque(task_id for task_id, degree in in_degree.items() if degree == 0)
        result = []

        while queue:
            node_id = queue.popleft()
            result.append(node_id)

            for dependent_id in self.dependents.get(node_id, []):
                in_degree[dependent_id] -= 1
                if in_degree[dependent_id] == 0:
                    queue.append(dependent_id)

        return result

    def get_ready_tasks(self) -> List[str]:
        """Return tasks whose dependencies are all done."""
        ready = []
        for task_id, task in self.dag.items():
            if task.status == TaskStatus.PENDING or task.status == TaskStatus.READY:
                deps_done = all(
                    self.dag[dep_id].status == TaskStatus.DONE
                    for dep_id in task.dependencies
                )
                if deps_done:
                    ready.append(task_id)
                    task.status = TaskStatus.READY

        return ready

    def re_score(self) -> None:
        """Update task statuses based on dependency completion."""
        for task_id, task in self.dag.items():
            if task.status == TaskStatus.PENDING or task.status == TaskStatus.READY:
                deps_done = all(
                    self.dag[dep_id].status == TaskStatus.DONE
                    for dep_id in task.dependencies
                )
                if deps_done and task.status == TaskStatus.PENDING:
                    task.status = TaskStatus.READY


if __name__ == "__main__":
    from dag import DAGBuilder

    # Test 1: Topological sort
    print("Test 1 - Topological sort:")
    builder = DAGBuilder()
    goal = "Setup\nCode\nTest"
    dag = builder.build_from_goal(goal)

    scheduler = Scheduler(dag)
    topo = scheduler.topological_sort()
    print(f"  Order: {topo}")

    # Test 2: Ready tasks
    print("\nTest 2 - Get ready tasks:")
    ready = scheduler.get_ready_tasks()
    print(f"  Ready: {ready}")

    # Mark first task as done and check ready set updates
    dag['task_0'].status = TaskStatus.DONE
    scheduler.re_score()
    ready = scheduler.get_ready_tasks()
    print(f"  After task_0 done: {ready}")

    # Test 3: Complex DAG
    print("\nTest 3 - Complex DAG:")
    import json
    builder3 = DAGBuilder()
    json_tasks = json.dumps([
        {"id": "a", "description": "Task A"},
        {"id": "b", "description": "Task B", "dependencies": ["a"]},
        {"id": "c", "description": "Task C", "dependencies": ["a"]},
        {"id": "d", "description": "Task D", "dependencies": ["b", "c"]},
    ])
    dag3 = builder3.build_from_json(json_tasks)
    scheduler3 = Scheduler(dag3)
    topo3 = scheduler3.topological_sort()
    print(f"  Topological order: {topo3}")
    ready3 = scheduler3.get_ready_tasks()
    print(f"  Initially ready: {ready3}")
