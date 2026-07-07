"""A bounded, project-persistent context digest - a ring buffer of task
outcomes shared with every executing agent, instead of the ever-growing
"dump the entire previous wave" context that used to be re-sent on every
prompt. Stored under one key in the project Brain (ruflo memory), which is
scoped to the *project*, not a single run - so the same digest naturally
carries across waves within a run and across separate runs on the same
project, with no separate cross-run mechanism needed.

The ring buffer keeps the last MAX_RECENT_ENTRIES task outcomes verbatim;
anything older collapses into a single rolling counter line. This means
prompt size contributed by this digest is roughly constant whether a project
has run 5 tasks total or 5,000 - the opposite of concatenating history.
"""
import json
from typing import Any, Dict, Optional

DIGEST_KEY = "context_digest"
MAX_RECENT_ENTRIES = 6


def _load(brain) -> Dict[str, Any]:
    raw = brain.memory_retrieve(DIGEST_KEY) if brain else None
    if not raw:
        return {'earlier_count': 0, 'earlier_verified': 0, 'recent': []}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {'earlier_count': 0, 'earlier_verified': 0, 'recent': []}
    data.setdefault('earlier_count', 0)
    data.setdefault('earlier_verified', 0)
    data.setdefault('recent', [])
    return data


def update_digest(brain, task_id: str, description: str, verified: bool, result_summary: str) -> None:
    """Append one task's outcome, compacting the oldest recent entry into the
    rolling counter once the ring buffer exceeds MAX_RECENT_ENTRIES. Best
    effort - never raises, matching every other Brain operation."""
    if not brain:
        return
    data = _load(brain)
    data['recent'].append({
        'task_id': task_id,
        'description': (description or '')[:120],
        'verified': bool(verified),
        'summary': (result_summary or '')[:150],
    })
    while len(data['recent']) > MAX_RECENT_ENTRIES:
        oldest = data['recent'].pop(0)
        data['earlier_count'] += 1
        if oldest['verified']:
            data['earlier_verified'] += 1
    brain.memory_store(DIGEST_KEY, data)


def format_digest(brain) -> str:
    """Render the digest as prompt text. Empty string if there's nothing yet
    (a project's first task) so callers can skip the section entirely."""
    if not brain:
        return ""
    data = _load(brain)
    if not data['recent'] and not data['earlier_count']:
        return ""

    lines = ["Project context (rolling summary of work done so far - this project persists across waves and runs):"]
    if data['earlier_count']:
        lines.append(
            f"  ...{data['earlier_count']} earlier task(s) ({data['earlier_verified']} verified) - "
            "compacted out to keep this summary short."
        )
    for entry in data['recent']:
        status = "verified" if entry['verified'] else "not verified"
        lines.append(f"  - [{status}] {entry['task_id']}: {entry['description']} -> {entry['summary']}")
    return "\n".join(lines)


if __name__ == "__main__":
    class FakeBrain:
        """In-memory stand-in for Brain, so this test doesn't need the real
        ruflo CLI to verify the ring-buffer compaction logic."""
        def __init__(self):
            self.store: Dict[str, str] = {}

        def memory_store(self, key, value):
            self.store[key] = json.dumps(value)
            return True

        def memory_retrieve(self, key):
            return self.store.get(key)

    print("Test 1 - Empty digest formats to nothing:")
    b = FakeBrain()
    assert format_digest(b) == ""
    print("  OK")

    print("\nTest 2 - Entries accumulate up to the cap without compaction:")
    for i in range(MAX_RECENT_ENTRIES):
        update_digest(b, f"task_{i}", f"do thing {i}", True, f"did thing {i}")
    data = _load(b)
    print(f"  recent entries: {len(data['recent'])}, earlier_count: {data['earlier_count']}")
    assert len(data['recent']) == MAX_RECENT_ENTRIES
    assert data['earlier_count'] == 0

    print("\nTest 3 - One more entry compacts the oldest into the rolling counter:")
    update_digest(b, "task_new", "one more", False, "failed this one")
    data = _load(b)
    print(f"  recent entries: {len(data['recent'])}, earlier_count: {data['earlier_count']}")
    assert len(data['recent']) == MAX_RECENT_ENTRIES
    assert data['earlier_count'] == 1
    assert data['recent'][0]['task_id'] == 'task_1'  # task_0 got compacted out
    assert data['recent'][-1]['task_id'] == 'task_new'

    print("\nTest 4 - Formatted digest is bounded regardless of history length:")
    for i in range(50):
        update_digest(b, f"bulk_{i}", "bulk work", True, "done")
    text = format_digest(b)
    print(f"  digest length after 57 total tasks: {len(text)} chars")
    assert len(text) < 1200  # stays small no matter how many tasks ran
    assert "earlier task(s)" in text

    print("\nAll tests passed!")
