# Meridian — End-to-End Flow

One goal in, one verified artifact out. This is the path a request takes
through the system, start to finish.

```
                              ┌─────────────────────┐
                              │   User / Dashboard   │
                              │   "build a thing"    │
                              └──────────┬───────────┘
                                         │
                                         ▼
┌────────────────────────────────────────────────────────────────────────┐
│  1. RESUME CHECK  (main.py: try_resume)                                │
│  Was there a run left "interrupted" by a crash in THIS project?        │
│    yes → reload DAG + wave + retry state from last checkpoint, skip →2 │
│    no  → continue to decomposition                                     │
└──────────────────────────┬───────────────────────────────────────────-┘
                            │ no crash to resume
                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│  2. GROUND THE PLANNER  (context_digest.py)                          │
│  Before asking the LLM to plan, gather what's ACTUALLY true:         │
│    • list_project_files()   → real file listing (can't go stale)     │
│    • format_digest()        → rolling summary of all past tasks      │
│    • get_last_completed_run_context() → the immediately prior run    │
└──────────────────────────┬─────────────────────────────────────────-┘
                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│  3. DECOMPOSE  (dag.py: DAGBuilder.build_from_goal)                  │
│  One claude -p call, given the goal + grounding from step 2:         │
│    • Fewest tasks possible - a trivial goal returns exactly 1 task,  │
│      never split just to look thorough                               │
│    • Each task tagged complexity: simple | medium | complex          │
│    • Each task gets verify_commands - real shell checks, not vibes   │
│    • Dependencies only where genuinely required (parallelism by      │
│      default)                                                        │
│  Cycle detection runs on the result; unusable output → 1-task        │
│  fallback (never blocks the run entirely).                           │
└──────────────────────────┬─────────────────────────────────────────-┘
                            ▼
                    ┌───────────────┐
                    │   TASK DAG    │   e.g.
                    │               │     task_0 (simple)
                    │   a graph of  │        │
                    │  TaskNodes    │        ▼
                    │               │     task_1 (medium) ──┐
                    └───────┬───────┘        │              │
                            │                ▼              ▼
                            │             task_2         task_3
                            │            (complex)      (simple)
                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│  4. WAVE LOOP  (main.py: run_loop → execute_wave)          repeats   │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ a. scheduler.get_ready_tasks()                                  │ │
│  │    → every task whose dependencies are all DONE, all at once    │ │
│  │                                                                  │ │
│  │ b. DISPATCH IN PARALLEL  (executor.py: ThreadPoolExecutor)       │ │
│  │    Independent tasks in the same wave run CONCURRENTLY.         │ │
│  │    Dependent tasks wait for their own wave.                     │ │
│  │                                                                  │ │
│  │    For each task, in its own thread:                            │ │
│  │      1. model_router.select_model(complexity, retry_count)      │ │
│  │           simple → haiku   medium → sonnet   complex → opus     │ │
│  │           (escalates one tier every retry)                      │ │
│  │      2. Build the prompt:                                       │ │
│  │           - task description + dependency boundaries            │ │
│  │           - direct dependency outputs (full detail)             │ │
│  │           - project grounding (files + digest, "trust files")   │ │
│  │           - retry diagnosis if this is a retry attempt          │ │
│  │      3. claude -p --output-format stream-json --model <tier>    │ │
│  │           Every thinking / tool_use / text block is captured    │ │
│  │           LIVE and streamed to the dashboard as it happens      │ │
│  │           (state.log_agent_thought) - not just the final result │ │
│  │                                                                  │ │
│  │ c. INDEPENDENT VERIFICATION  (verifier.py)                       │ │
│  │    Real subprocess execution of verify_commands.                │ │
│  │    The agent's self-report is recorded but NEVER trusted -      │ │
│  │    only a real shell exit code marks a task done.                │ │
│  │                                                                  │ │
│  │ d. ON FAILURE  (failure_analyzer.py)                             │ │
│  │    Classify WHY it failed:                                       │ │
│  │      rate_limited → free retry, silent backoff, no budget spent │ │
│  │      timeout / code_error / cli_error → costs a retry attempt   │ │
│  │    Diagnosis is persisted and fed back into the next attempt's  │ │
│  │    prompt so the agent fixes the root cause, not repeats itself │ │
│  │                                                                  │ │
│  │ e. RECORD RESULT  (state.py: write_task_result)                  │ │
│  │      - SQLite: task status, cost, model used, retry count       │ │
│  │      - Brain memory: this task's output, for direct dependents  │ │
│  │      - context_digest: append outcome to the ring buffer         │ │
│  │      - checkpoint saved (full DAG + wave + retries) → crash-safe│ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                        │
│  After the wave: has anything stalled?                                │
│    a task exhausted its retry budget                                  │
│      → try REDECOMPOSITION first (redecompose.py): split it into      │
│        2-4 smaller subtasks informed by its failure history,          │
│        splice into the DAG, keep going autonomously                   │
│      → only if that ALSO fails does a human get asked to intervene    │
│    N consecutive waves made zero progress → same stall path           │
│    otherwise → loop back to (a) for the next wave                     │
└──────────────────────────┬───────────────────────────────────────────┘
                            │ all tasks DONE or SKIPPED
                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│  5. FINAL REPORT  (report.py + confidence.py)                        │
│    • Per-task confidence score (verification quality × dependency    │
│      health × retry penalty) - not a self-grade, computed from real  │
│      signals only                                                    │
│    • Documentation files detected via git diff                       │
│    • Total cost, duration, pass/fail counts                          │
│    • state.finish_run() closes out the run row                       │
└──────────────────────────┬───────────────────────────────────────────┘
                            ▼
                   Verified artifact on disk
                 + full history in .meridian/
```

## The persistent brain (`.meridian/`)

Every project gets its own folder, created wherever the orchestrator runs:

```
your-project/
└── .meridian/
    ├── orchestrator.db     # runs, tasks, agents, events, checkpoints,
    │                       # reasoning, thoughts, DAG snapshots
    ├── memory.db           # ruflo cross-wave/cross-run memory
    ├── ruvector.db         # ruflo vector store
    └── .claude-flow/       # ruflo agent/task coordination ledgers
```

This is what makes steps 1–2 possible: a crash reloads from `orchestrator.db`'s
checkpoint table, and a second run on the same project ("now add a divide
function") sees real memory of the first run — not a cold start.

## Context strategy: index + ground truth, not one giant blob

Three layers feed every prompt, deliberately kept separate:

| Layer | Scope | Freshness |
|---|---|---|
| Direct dependency output | Just this task's declared deps | Full detail, always fresh (from Brain memory) |
| Rolling digest | Whole project, all waves & runs | Last 6 tasks verbatim, older ones compacted to a count |
| **File listing** | Whole project | **Always accurate** — a live filesystem walk, can't go stale |

The digest tells an agent *what happened and roughly when*. The file listing
tells it *what's actually true right now*. Prompts explicitly instruct: if
they disagree, trust the files.

## The dashboard's view of all this

`dashboard/server.py` + `index.html` read `.meridian/orchestrator.db` live
(polling every 1.5s) and render:

- **Task graph** — every node, dependency edges, live status, confidence badge, model badge
- **Agents panel** — per-agent lifecycle (spawning → running → verifying → done/retrying/failed)
- **Agent thinking** — tabbed by task, the actual `thinking` / `tool_use` / `text` stream as it happens
- **Ruflo swarm ledger** — coordination agents registered for this run
- **Live log** — every orchestrator-level event, streamed incrementally

Starting a run from the dashboard just launches `main.py --non-interactive`
as a subprocess and tails its `.meridian/` state — the same state a terminal
run produces, so either entry point sees the same live picture.
