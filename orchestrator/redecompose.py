"""Auto-recovery for a permanently-failed task: ask the LLM to split it into
smaller, more tractable subtasks informed by why the original attempt failed,
instead of immediately escalating to a human. This is what lets the
orchestrator push through a task that was simply too large/ambiguous for one
agent attempt, while still respecting the retry budget on each new subtask.
"""
import json
import subprocess
from typing import Dict, List, Optional

from dag import TaskNode


def redecompose_task(task: TaskNode, failure_context: str) -> Optional[List[Dict]]:
    """Ask claude -p to break one stuck task into 2-4 smaller subtasks.
    Returns a list of {id, description, dependencies, verify_commands} dicts
    with task-local (unnamespaced) ids, or None if the call fails, times out,
    or produces something unusable."""
    prompt = f"""A task in an automated build pipeline has failed repeatedly and needs to be
broken into smaller, more tractable subtasks.

Original task: {task.description}

Failure context (what went wrong on prior attempts):
{failure_context}

Break this into 2-4 smaller subtasks that together accomplish the same goal, each simpler
and more likely to succeed independently than the original. Subtasks may depend on each
other only if genuinely necessary (reference each other by the "id" you assign, e.g. "sub_0").

Return ONLY valid JSON, no prose, no markdown fences, in exactly this format:
[{{"id": "sub_0", "description": "...", "dependencies": [], "verify_commands": []}}, ...]
"""
    try:
        result = subprocess.run(
            ["claude", "-p", prompt, "--output-format", "json", "--permission-mode", "acceptEdits"],
            capture_output=True, text=True, timeout=60, stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None
    if result.returncode != 0:
        return None

    try:
        output = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None

    raw = output.get("result", "").strip()
    if raw.startswith("```"):
        lines = raw.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines).strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None

    if not isinstance(data, list) or not (1 < len(data) <= 4):
        return None
    for t in data:
        if not isinstance(t, dict) or 'id' not in t or 'description' not in t:
            return None
    return data


if __name__ == "__main__":
    from dag import TaskNode as _TaskNode

    print("Test 1 - Malformed LLM output is rejected, not raised:")
    import unittest.mock as mock
    with mock.patch("subprocess.run") as m:
        m.return_value = subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout=json.dumps({"result": "not valid json at all"}),
        )
        out = redecompose_task(_TaskNode(id="t", description="x"), "some failure")
        print(f"  Returns None on garbage: {out is None}")
        assert out is None

    print("\nTest 2 - Well-formed subtask list is accepted:")
    with mock.patch("subprocess.run") as m:
        m.return_value = subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout=json.dumps({"result": json.dumps([
                {"id": "sub_0", "description": "write the parser", "dependencies": [], "verify_commands": []},
                {"id": "sub_1", "description": "write the tests", "dependencies": ["sub_0"], "verify_commands": []},
            ])}),
        )
        out = redecompose_task(_TaskNode(id="t", description="x"), "syntax error")
        print(f"  Parsed {len(out)} subtasks: {[s['id'] for s in out]}")
        assert out is not None and len(out) == 2

    print("\nAll tests passed!")
