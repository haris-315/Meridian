from typing import Dict, Any, Literal
import sys


class Checkpoint:
    """Human-in-the-loop checkpoint for reviewing wave results."""

    def __init__(self, wave_number: int = 1):
        self.wave_number = wave_number

    def show_summary(self, summary: Dict[str, Any]) -> None:
        """Display wave summary to human."""
        print("\n" + "=" * 70)
        print(f"WAVE {self.wave_number} SUMMARY")
        print("=" * 70)
        print(f"Tasks completed: {summary['tasks_completed']}")
        print(f"Tasks failed: {summary['tasks_failed']}")
        print(f"Tests passed: {summary['tests_passed']}")
        print(f"Total cost: ${summary['total_cost_usd']:.4f}")

        if summary.get('recent_tasks'):
            print("\nRecent tasks:")
            for task in summary['recent_tasks']:
                status_icon = "✓" if task['status'] == 'done' else "✗"
                print(f"  {status_icon} {task['id']}: {task['description']}")

        print()

    def prompt_decision(self) -> Literal['approve', 'redirect', 'edit', 'stop']:
        """Prompt human for decision: approve, redirect, edit, or stop."""
        while True:
            response = input(
                "Decision [A]pprove / [R]edirect / [E]dit / [S]top? "
            ).strip().upper()

            if response in ['A', 'APPROVE']:
                return 'approve'
            elif response in ['R', 'REDIRECT']:
                return 'redirect'
            elif response in ['E', 'EDIT']:
                return 'edit'
            elif response in ['S', 'STOP']:
                return 'stop'
            else:
                print("Invalid response. Use A, R, E, or S")

    def get_redirect_goal(self) -> str:
        """Get redirect instructions from human."""
        print("\nEnter redirect instructions (e.g., 'fix the failing test'):")
        return input("> ").strip()

    def get_plan_edits(self) -> str:
        """Get plan edit instructions from human."""
        print("\nEnter plan edits (e.g., 'add task for documentation'):")
        return input("> ").strip()

    def show_final_summary(self, summary: Dict[str, Any], duration_seconds: float) -> None:
        """Display final orchestration summary."""
        minutes = duration_seconds / 60
        hours = minutes / 60

        print("\n" + "=" * 70)
        print("ORCHESTRATION COMPLETE")
        print("=" * 70)
        print(f"Total tasks completed: {summary['tasks_completed']}")
        print(f"Total tasks failed: {summary['tasks_failed']}")
        print(f"Total tests passed: {summary['tests_passed']}")
        print(f"Total cost: ${summary['total_cost_usd']:.4f}")

        if hours >= 1:
            print(f"Total time: {hours:.1f} hours")
        else:
            print(f"Total time: {minutes:.1f} minutes")

        if summary['tasks_failed'] > 0:
            print(f"\n⚠ {summary['tasks_failed']} tasks failed. Review logs for details.")
        else:
            print("\n✓ All tasks completed successfully!")

        print("=" * 70 + "\n")


if __name__ == "__main__":
    # Test 1: Show summary
    print("Test 1 - Show summary:")
    checkpoint = Checkpoint(1)
    test_summary = {
        'tasks_completed': 3,
        'tasks_failed': 0,
        'tests_passed': 2,
        'total_cost_usd': 0.0234,
        'recent_tasks': [
            {'id': 'task_1', 'description': 'Create function', 'status': 'done'},
            {'id': 'task_2', 'description': 'Write tests', 'status': 'done'},
        ]
    }
    checkpoint.show_summary(test_summary)

    # Test 2: Show final summary
    print("Test 2 - Final summary:")
    checkpoint.show_final_summary(test_summary, 245.5)

    # Test 3: Decision prompt (non-interactive)
    print("Test 3 - Decision options available: approve, redirect, edit, stop")
    print("(Skipping interactive prompt in test)")
