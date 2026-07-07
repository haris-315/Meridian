"""Classifies *why* a task failed, not just that it did.

Both the executor (agent self-report) and verifier (independent shell check)
can fail independently, and the reason changes what the orchestrator should
do next: a rate limit deserves a silent backoff and no retry-budget cost; a
timeout deserves a bigger budget on the next attempt; a genuine code bug
deserves a diagnostic handed back to the retrying agent. Centralizing the
classification here (instead of ad hoc checks scattered in main.py) means the
same diagnosis feeds retries, the dashboard's reasoning trace, and the stall
escalation report.
"""
from dataclasses import dataclass
from typing import Any, Dict


class FailureType:
    RATE_LIMITED = "rate_limited"
    TIMEOUT = "timeout"
    CLI_ERROR = "cli_error"
    CODE_ERROR = "code_error"
    UNKNOWN = "unknown"


@dataclass
class FailureDiagnosis:
    failure_type: str
    diagnostic_message: str
    consume_retry_budget: bool
    suggested_action: str  # 'retry', 'backoff', 'escalate'


def classify_failure(executor_result: Dict[str, Any], verifier_result: Dict[str, Any]) -> FailureDiagnosis:
    """Diagnose a failed task from its executor + verifier results.

    Order matters: executor-level errors (rate limit, timeout, CLI failure)
    are checked first since they mean the agent never got a real chance to
    do the work - only once the agent actually ran do we look at what the
    independent verifier found wrong with its output.
    """
    error = executor_result.get('error')

    if error == 'rate_limited':
        return FailureDiagnosis(
            failure_type=FailureType.RATE_LIMITED,
            diagnostic_message="API rate limited (HTTP 429) - not a task defect.",
            consume_retry_budget=False,
            suggested_action='backoff',
        )

    if error == 'timeout':
        return FailureDiagnosis(
            failure_type=FailureType.TIMEOUT,
            diagnostic_message="Agent execution timed out before completing the task. "
                                "Consider whether the task is too large for one attempt.",
            consume_retry_budget=True,
            suggested_action='retry',
        )

    if error in ('claude_not_found', 'cli_error'):
        return FailureDiagnosis(
            failure_type=FailureType.CLI_ERROR,
            diagnostic_message=f"Claude CLI error: {str(executor_result.get('result', ''))[:300]}",
            consume_retry_budget=True,
            suggested_action='escalate',
        )

    if not executor_result.get('success', True):
        return FailureDiagnosis(
            failure_type=FailureType.CODE_ERROR,
            diagnostic_message=f"Agent reported failure: {str(executor_result.get('result', ''))[:400]}",
            consume_retry_budget=True,
            suggested_action='retry',
        )

    if not verifier_result.get('passed', True):
        return FailureDiagnosis(
            failure_type=FailureType.CODE_ERROR,
            diagnostic_message=f"Independent verification failed:\n{str(verifier_result.get('output', ''))[:600]}",
            consume_retry_budget=True,
            suggested_action='retry',
        )

    return FailureDiagnosis(
        failure_type=FailureType.UNKNOWN,
        diagnostic_message="Task failed for an unrecognized reason.",
        consume_retry_budget=True,
        suggested_action='retry',
    )


if __name__ == "__main__":
    print("Test 1 - Rate limit:")
    d = classify_failure({'success': False, 'error': 'rate_limited'}, {'passed': False})
    assert d.failure_type == FailureType.RATE_LIMITED and not d.consume_retry_budget
    print(f"  {d.failure_type}, consumes_budget={d.consume_retry_budget}")

    print("\nTest 2 - Timeout:")
    d = classify_failure({'success': False, 'error': 'timeout'}, {'passed': False})
    assert d.failure_type == FailureType.TIMEOUT and d.consume_retry_budget
    print(f"  {d.failure_type}, action={d.suggested_action}")

    print("\nTest 3 - Verifier failure (agent succeeded, verify failed):")
    d = classify_failure(
        {'success': True, 'result': 'done'},
        {'passed': False, 'output': 'AssertionError: 2 != 3'},
    )
    assert d.failure_type == FailureType.CODE_ERROR
    print(f"  {d.failure_type}: {d.diagnostic_message[:60]}...")

    print("\nTest 4 - Executor-level code failure:")
    d = classify_failure({'success': False, 'result': 'wrote broken syntax'}, {'passed': False})
    assert d.failure_type == FailureType.CODE_ERROR
    print(f"  {d.failure_type}: {d.diagnostic_message[:60]}...")

    print("\nAll tests passed!")
