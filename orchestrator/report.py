import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from dag import TaskNode, TaskStatus
from state import StateManager

DOC_GLOB_PATTERNS = ("*.md", "*.rst")


def generate_final_report(
    state: StateManager,
    dag: Dict[str, TaskNode],
    working_dir: Path,
    duration_seconds: float,
    start_commit: Optional[str],
    start_time: float,
    stalled: bool,
    stall_reason: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the final structured report: summary, confidence, test results, and
    documentation files created. Never trusts task/executor self-report - only
    the persisted `verified` column (set from verifier.py's real subprocess exit
    codes) and real filesystem/git evidence feed the numbers here."""
    run = state.get_full_run_summary()
    tasks = run['tasks']

    done_tasks = [t for t in tasks if t['status'] == 'done']
    failed_tasks = [t for t in tasks if t['status'] == 'failed']
    skipped_ids = [tid for tid, node in dag.items() if node.status == TaskStatus.SKIPPED]

    confidence = _compute_confidence(dag, done_tasks, len(tasks), stalled, stall_reason)
    documentation_files = _detect_new_doc_files(working_dir, start_time, start_commit)
    summary_text = _build_summary_text(dag, done_tasks, failed_tasks, skipped_ids, stalled, stall_reason)

    return {
        'summary_text': summary_text,
        'confidence': confidence,
        'test_results': {
            'total_verifications': len(tasks),
            'passed': len(done_tasks),
            'failed': len(failed_tasks),
            'details': [
                {'task_id': t['task_id'], 'description': t['description'], 'passed': t['verified']}
                for t in tasks
            ],
        },
        'documentation_files': documentation_files,
        'tasks': {
            'completed': len(done_tasks),
            'failed_permanent': len(failed_tasks),
            'skipped': len(skipped_ids),
            'total': len(dag),
        },
        'cost_usd': run['total_cost_usd'],
        'duration_seconds': round(duration_seconds, 2),
        'stalled': stalled,
        'stall_reason': stall_reason,
    }


def _compute_confidence(
    dag: Dict[str, TaskNode],
    done_tasks: List[Dict[str, Any]],
    total_tasks: int,
    stalled: bool,
    stall_reason: Optional[str],
) -> Dict[str, Any]:
    """Heuristic confidence, not an LLM self-grade - consistent with this
    codebase's 'never trust self-report' principle (see verifier.py). Weighted
    toward tasks that were actually checked by real shell verify_commands, not
    just the free pass a task gets when it declares none."""
    if total_tasks == 0:
        return {'score': 0.0, 'method': 'heuristic', 'rationale': 'No tasks were executed.'}

    completion_ratio = len(done_tasks) / total_tasks

    done_with_real_verify = [
        t for t in done_tasks
        if dag.get(t['task_id']) and dag[t['task_id']].verify_commands
    ]
    verify_ratio = (len(done_with_real_verify) / len(done_tasks)) if done_tasks else 0.0

    score = 0.6 * completion_ratio + 0.4 * verify_ratio

    weak_tasks = [t['task_id'] for t in done_tasks if t['task_id'] not in {d['task_id'] for d in done_with_real_verify}]
    rationale_parts = [
        f"{len(done_tasks)}/{total_tasks} tasks completed and verified.",
    ]
    if weak_tasks:
        rationale_parts.append(
            f"{len(weak_tasks)} completed task(s) had no real verify_commands "
            f"(free pass, weaker evidence): {', '.join(weak_tasks)}."
        )

    if stalled:
        score = min(score, 0.5)
        rationale_parts.append(f"Run ended with an unresolved stall ({stall_reason or 'unknown reason'}); capped at 0.5.")

    return {
        'score': round(max(0.0, min(1.0, score)), 3),
        'method': 'heuristic',
        'rationale': ' '.join(rationale_parts),
    }


def _detect_new_doc_files(working_dir: Path, start_time: float, start_commit: Optional[str]) -> List[str]:
    """Detect documentation files created during the run via git diff (preferred)
    or mtime-based glob fallback. Self-report is deliberately not used here -
    a task claiming it wrote docs is not evidence that it did."""
    if start_commit:
        try:
            result = subprocess.run(
                ["git", "-C", str(working_dir), "diff", "--name-only", "--diff-filter=A",
                 f"{start_commit}..HEAD", "--", "*.md", "*.rst"],
                capture_output=True,
                text=True,
                timeout=15,
            )
            if result.returncode == 0:
                files = [line.strip() for line in result.stdout.splitlines() if line.strip()]
                return sorted(files)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

    found = []
    for pattern in DOC_GLOB_PATTERNS:
        for path in working_dir.glob(pattern):
            try:
                if path.is_file() and path.stat().st_mtime > start_time:
                    found.append(str(path.relative_to(working_dir)))
            except OSError:
                continue
    return sorted(found)


def _build_summary_text(
    dag: Dict[str, TaskNode],
    done_tasks: List[Dict[str, Any]],
    failed_tasks: List[Dict[str, Any]],
    skipped_ids: List[str],
    stalled: bool,
    stall_reason: Optional[str],
) -> str:
    lines = [f"Completed {len(done_tasks)}/{len(dag)} tasks."]
    for t in done_tasks:
        lines.append(f"  DONE  {t['task_id']}: {t['description']}")
    for t in failed_tasks:
        lines.append(f"  FAILED {t['task_id']}: {t['description']} (retries: {t['retry_count']})")
    for tid in skipped_ids:
        lines.append(f"  SKIPPED {tid}: {dag[tid].description}")
    if stalled:
        lines.append(f"Run ended in an unresolved stall: {stall_reason or 'unknown reason'}.")
    return "\n".join(lines)
