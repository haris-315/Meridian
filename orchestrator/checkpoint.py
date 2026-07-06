from typing import Dict, Any, List, Literal


class Checkpoint:
    """Human escalation for stalled runs, and printing the final report. There is
    no per-wave approval gate anymore - the orchestrator runs autonomously wave to
    wave; a human is only pulled in if a task exhausts its retry budget or a run
    produces zero progress for too many consecutive waves."""

    def show_stall_alert(self, reason: str, stuck_tasks: List[Dict[str, Any]], cost_so_far: float) -> None:
        """Display why the run stalled and what's stuck, before asking for a decision."""
        print("\n" + "=" * 70)
        print("RUN STALLED - HUMAN INPUT NEEDED")
        print("=" * 70)
        print(f"Reason: {reason}")
        print(f"Cost so far: ${cost_so_far:.4f}")

        if stuck_tasks:
            print(f"\nStuck tasks ({len(stuck_tasks)}):")
            for task in stuck_tasks:
                print(f"  ✗ {task['task_id']} (wave {task['wave']}, retries {task['retry_count']}): {task['description']}")
                if task.get('verifier_output'):
                    print(f"      last failure: {task['verifier_output'][:300]}")

        print()

    def prompt_stall_decision(self) -> Literal['stop', 'retry_all', 'skip_stuck']:
        """Prompt human for how to resolve a stall. The only remaining input()
        call site in the system."""
        while True:
            response = input(
                "Decision [S]top / [R]etry all stuck tasks / [K]skip stuck tasks and continue? "
            ).strip().upper()

            if response in ['S', 'STOP']:
                return 'stop'
            elif response in ['R', 'RETRY', 'RETRY_ALL']:
                return 'retry_all'
            elif response in ['K', 'SKIP', 'SKIP_STUCK']:
                return 'skip_stuck'
            else:
                print("Invalid response. Use S, R, or K")

    def show_final_summary(self, report: Dict[str, Any]) -> None:
        """Display the final structured report to the human (also emitted as JSON
        to stdout by main.py for machine consumption, since most runs finish with
        no human present)."""
        print("\n" + "=" * 70)
        print("ORCHESTRATION COMPLETE" if not report['stalled'] else "ORCHESTRATION STOPPED (STALLED)")
        print("=" * 70)

        print(report['summary_text'])

        tasks = report['tasks']
        print(f"\nTasks: {tasks['completed']} completed, {tasks['failed_permanent']} failed, "
              f"{tasks['skipped']} skipped, {tasks['total']} total")

        tr = report['test_results']
        print(f"Verifications: {tr['passed']}/{tr['total_verifications']} passed")

        conf = report['confidence']
        print(f"Confidence on goal achievement: {conf['score']:.2f} ({conf['method']})")
        print(f"  {conf['rationale']}")

        docs = report['documentation_files']
        if docs:
            print(f"\nDocumentation files created ({len(docs)}):")
            for f in docs:
                print(f"  - {f}")
        else:
            print("\nDocumentation files created: none")

        duration = report['duration_seconds']
        minutes = duration / 60
        hours = minutes / 60
        if hours >= 1:
            print(f"\nTotal time: {hours:.1f} hours")
        else:
            print(f"\nTotal time: {minutes:.1f} minutes")
        print(f"Total cost: ${report['cost_usd']:.4f}")

        print("=" * 70 + "\n")


if __name__ == "__main__":
    print("Test 1 - Stall alert:")
    checkpoint = Checkpoint()
    checkpoint.show_stall_alert(
        reason="2 consecutive waves with zero progress",
        stuck_tasks=[
            {'task_id': 'task_2', 'wave': 3, 'retry_count': 2, 'description': 'Write integration tests',
             'verifier_output': 'Exit code: 1\nOutput: 3 tests failed'},
        ],
        cost_so_far=0.42,
    )

    print("Test 2 - Final summary:")
    checkpoint.show_final_summary({
        'summary_text': 'Completed 2/3 tasks.\n  DONE  task_0: Create function\n  FAILED task_1: Write tests (retries: 2)',
        'confidence': {'score': 0.62, 'method': 'heuristic', 'rationale': '2/3 tasks completed and verified.'},
        'test_results': {'total_verifications': 3, 'passed': 2, 'failed': 1, 'details': []},
        'documentation_files': ['README.md'],
        'tasks': {'completed': 2, 'failed_permanent': 1, 'skipped': 0, 'total': 3},
        'cost_usd': 0.42,
        'duration_seconds': 245.5,
        'stalled': True,
        'stall_reason': '2 consecutive waves with zero progress',
    })

    print("Test 3 - Decision options available: stop, retry_all, skip_stuck")
    print("(Skipping interactive prompt in test)")
