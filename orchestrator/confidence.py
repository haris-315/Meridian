"""Per-task confidence scoring.

report.py already computes one heuristic confidence number for the whole run.
This module scores each task individually, from real signals only (the same
"never trust self-report" principle as verifier.py): whether it was checked by
a real shell verify_command vs. the free pass an empty list gives, how
confident its dependencies are (a task built on shaky ground inherits that
shakiness), and how many retries it took to get there.

Operates on plain dicts rather than TaskNode/DB row objects so the same code
scores a live in-memory DAG (from report.py, at end of run) and the JSON DAG
snapshot the dashboard already persists (from server.py, live during a run).
Expected shapes:
    dag_nodes: [{'id', 'dependencies': [...], 'status', 'verify_commands': [...]}]
    tasks:     [{'task_id', 'status', 'verified', 'retry_count'}]
"""
from typing import Any, Dict, List

WEIGHTS = {'verification': 0.55, 'dependency': 0.25, 'retry': 0.20}
RETRY_PENALTY_PER_ATTEMPT = 0.15
RETRY_PENALTY_FLOOR = 0.4


def compute_all_confidences(dag_nodes: List[Dict[str, Any]], tasks: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Confidence for every task_id present in dag_nodes."""
    nodes_by_id = {n['id']: n for n in dag_nodes}
    tasks_by_id = {t['task_id']: t for t in tasks}
    cache: Dict[str, Dict[str, Any]] = {}
    return {node_id: _compute_one(node_id, nodes_by_id, tasks_by_id, cache) for node_id in nodes_by_id}


def _compute_one(task_id: str, nodes_by_id: Dict[str, Dict], tasks_by_id: Dict[str, Dict],
                 cache: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    if task_id in cache:
        return cache[task_id]
    # Break potential cycles defensively (the DAG builder already rejects real
    # cycles, but a partially-rewired redecomposition shouldn't be able to hang this).
    cache[task_id] = {'score': 0.0, 'factors': {}, 'rationale': 'Computing (cycle guard).'}

    node = nodes_by_id.get(task_id)
    task = tasks_by_id.get(task_id)

    if node and node.get('status') == 'skipped':
        result = {'score': 0.0, 'factors': {}, 'rationale': 'Skipped (an upstream dependency failed).'}
        cache[task_id] = result
        return result

    if not task or task.get('status') != 'done':
        result = {'score': 0.0, 'factors': {}, 'rationale': 'Task has not completed successfully yet.'}
        cache[task_id] = result
        return result

    has_real_verify = bool(node.get('verify_commands')) if node else False
    verified = bool(task.get('verified'))
    verification_score = 1.0 if (verified and has_real_verify) else (0.6 if verified else 0.0)

    deps = list(node.get('dependencies', [])) if node else []
    if deps:
        dep_scores = [_compute_one(d, nodes_by_id, tasks_by_id, cache)['score'] for d in deps]
        dependency_score = sum(dep_scores) / len(dep_scores)
    else:
        dependency_score = 1.0

    retry_count = task.get('retry_count', 0) or 0
    retry_penalty = max(RETRY_PENALTY_FLOOR, 1.0 - RETRY_PENALTY_PER_ATTEMPT * retry_count)

    score = (WEIGHTS['verification'] * verification_score
             + WEIGHTS['dependency'] * dependency_score
             + WEIGHTS['retry'] * retry_penalty)

    rationale_parts = []
    rationale_parts.append(
        "verified by a real shell command" if has_real_verify
        else "no verify_commands defined - a free pass, weaker evidence"
    )
    if deps:
        rationale_parts.append(f"depends on {len(deps)} task(s) averaging {dependency_score:.2f} confidence")
    if retry_count:
        plural = "y" if retry_count == 1 else "ies"
        rationale_parts.append(f"took {retry_count} retr{plural}")

    result = {
        'score': round(max(0.0, min(1.0, score)), 3),
        'factors': {
            'verification': round(verification_score, 3),
            'dependency_health': round(dependency_score, 3),
            'retry_penalty': round(retry_penalty, 3),
        },
        'rationale': '; '.join(rationale_parts) + '.',
    }
    cache[task_id] = result
    return result


if __name__ == "__main__":
    print("Test 1 - Simple verified task, no deps:")
    dag_nodes = [{'id': 'a', 'dependencies': [], 'status': 'done', 'verify_commands': ['pytest x.py']}]
    tasks = [{'task_id': 'a', 'status': 'done', 'verified': True, 'retry_count': 0}]
    scores = compute_all_confidences(dag_nodes, tasks)
    print(f"  a: {scores['a']}")
    assert scores['a']['score'] == 1.0

    print("\nTest 2 - Done but no verify_commands (free pass) - lower score:")
    dag_nodes = [{'id': 'a', 'dependencies': [], 'status': 'done', 'verify_commands': []}]
    tasks = [{'task_id': 'a', 'status': 'done', 'verified': True, 'retry_count': 0}]
    scores = compute_all_confidences(dag_nodes, tasks)
    print(f"  a: {scores['a']['score']}")
    assert scores['a']['score'] < 1.0

    print("\nTest 3 - Dependent inherits a shaky dependency's confidence:")
    dag_nodes = [
        {'id': 'a', 'dependencies': [], 'status': 'done', 'verify_commands': []},
        {'id': 'b', 'dependencies': ['a'], 'status': 'done', 'verify_commands': ['pytest b.py']},
    ]
    tasks = [
        {'task_id': 'a', 'status': 'done', 'verified': True, 'retry_count': 0},
        {'task_id': 'b', 'status': 'done', 'verified': True, 'retry_count': 0},
    ]
    scores = compute_all_confidences(dag_nodes, tasks)
    print(f"  a: {scores['a']['score']}, b: {scores['b']['score']}")
    assert scores['b']['score'] < 1.0  # dragged down by a's weaker evidence
    assert scores['b']['factors']['dependency_health'] == scores['a']['score']

    print("\nTest 4 - Retries reduce confidence; skipped/failed score 0:")
    dag_nodes = [
        {'id': 'a', 'dependencies': [], 'status': 'done', 'verify_commands': ['x']},
        {'id': 'b', 'dependencies': [], 'status': 'skipped', 'verify_commands': []},
    ]
    tasks = [{'task_id': 'a', 'status': 'done', 'verified': True, 'retry_count': 2}]
    scores = compute_all_confidences(dag_nodes, tasks)
    print(f"  a (2 retries): {scores['a']['score']}, b (skipped): {scores['b']['score']}")
    assert scores['a']['score'] < 1.0
    assert scores['b']['score'] == 0.0

    print("\nAll tests passed!")
