"""Routes each task to a model tier by declared complexity, escalating one
tier per retry.

A task the DAG decomposition tagged "simple" (a small well-defined edit)
shouldn't burn an Opus-grade call any more than a "complex" task (careful
multi-file design) should be trusted to a Haiku-grade one. Escalating on
retry reflects a simple heuristic: if a task already failed once with a given
tier, that's evidence the task needs more capability, not just another
attempt with the same one - so a fixed-budget retry loop gets a strictly
better chance of success on later attempts instead of repeating the same bet.
"""
from typing import List

MODEL_TIERS: List[str] = ["haiku", "sonnet", "opus"]
COMPLEXITY_TO_TIER = {"simple": 0, "medium": 1, "complex": 2}
DEFAULT_TIER = COMPLEXITY_TO_TIER["medium"]


def select_model(complexity: str, retry_count: int = 0) -> str:
    """Pick a `claude -p --model` alias for a task at a given retry attempt."""
    base_tier = COMPLEXITY_TO_TIER.get(complexity, DEFAULT_TIER)
    tier = min(base_tier + retry_count, len(MODEL_TIERS) - 1)
    return MODEL_TIERS[tier]


if __name__ == "__main__":
    print("Test 1 - Base tiers by complexity:")
    assert select_model("simple") == "haiku"
    assert select_model("medium") == "sonnet"
    assert select_model("complex") == "opus"
    print("  simple->haiku, medium->sonnet, complex->opus: OK")

    print("\nTest 2 - Escalates on retry:")
    assert select_model("simple", retry_count=1) == "sonnet"
    assert select_model("simple", retry_count=2) == "opus"
    print("  simple + 2 retries -> opus: OK")

    print("\nTest 3 - Caps at the top tier, never errors:")
    assert select_model("complex", retry_count=5) == "opus"
    print("  complex + 5 retries -> still opus: OK")

    print("\nTest 4 - Unknown complexity falls back to medium tier:")
    assert select_model("nonsense") == "sonnet"
    print("  unknown complexity -> sonnet (medium default): OK")

    print("\nAll tests passed!")
