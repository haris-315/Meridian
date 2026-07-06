# Meridian

An autonomous orchestration engine: give it a goal, it decomposes the goal into
a dependency graph of tasks, executes independent tasks **in parallel waves**
through headless Claude Code agents, **independently verifies** every result
with real shell commands (never trusting an agent's self-report), and persists
everything to a per-project brain. A live dashboard shows what the swarm is
doing at any moment.

```
goal ──► DAG (LLM decomposition) ──► ready waves ──► parallel agents ──► verifier ──► brain
                 ▲                                                          │
                 └────────────── re-score + retry with failure context ◄────┘
```

## Layout

```
orchestrator/
├── dag.py         # goal → TaskNode graph (LLM decomposition, cycle detection, fallback)
├── scheduler.py   # Kahn's topological sort → ready queue of dependency-satisfied tasks
├── executor.py    # parallel wave dispatch through `claude -p` + ruflo coordination
├── verifier.py    # independent subprocess verification — exit codes only
├── state.py       # SQLite: runs, tasks, agents, events, DAG snapshots
├── brain.py       # per-project .meridian/ folder + single gateway to the ruflo CLI
├── checkpoint.py  # human escalation on stalls (autonomous otherwise)
├── report.py      # final structured report with heuristic confidence
└── main.py        # the loop: DAG → wave → verify → persist → re-score

dashboard/
├── server.py      # stdlib HTTP server: dashboard page + JSON API + run control
└── index.html     # self-contained live UI (no build step, no dependencies)
```

## The per-project brain

Run Meridian from any folder and that folder gets a `.meridian/` directory —
its persistent memory across runs:

```
your-project/
└── .meridian/
    ├── orchestrator.db   # runs, tasks, agent states, event log, DAG snapshots
    ├── memory.db         # ruflo cross-wave memory (what each wave produced)
    ├── ruvector.db       # ruflo vector store
    └── .claude-flow/     # ruflo agent/task coordination ledgers
```

Wave N+1 agents receive real context about what wave N actually built (from the
brain, not from agent claims), and retried tasks are shown the verifier output
of their previous failure so they fix root causes instead of retrying blindly.

## Usage

```bash
# CLI
cd some-project/
python3 /path/to/Meridian/orchestrator/main.py "build a calculator module with tests and a README"

# Dashboard (visual)
python3 /path/to/Meridian/dashboard/server.py some-project/ --port 8787
# → open http://127.0.0.1:8787, type a goal, press Start
```

The dashboard shows, live: the task graph with dependency edges and per-task
status, every agent's state (spawning → running → verifying → done/retrying),
the ruflo swarm ledger, cost, and a streaming event log.

## Principles

- **Verification is never skippable.** A task is only `done` when a real shell
  command exits 0. Agent self-reports are recorded but never trusted.
- **Parallel where possible, ordered where required.** Tasks with no dependency
  between them run concurrently; dependents wait for verified completion.
- **Autonomous until genuinely stuck.** No per-wave approval gates; a human is
  only pulled in when a task exhausts its retry budget or waves stop making
  progress (rate-limit waves get exponential backoff instead).
- **Pure Python stdlib** (`subprocess`, `sqlite3`, `dataclasses`, `json`) — no
  frameworks anywhere, including the dashboard.

## Requirements

- Python 3.10+
- [Claude Code](https://claude.com/claude-code) CLI on PATH (does the actual work)
- [ruflo](https://www.npmjs.com/package/ruflo) CLI on PATH (optional — swarm
  coordination + cross-wave memory; Meridian degrades gracefully without it)
- `pytest` on PATH for Python verification commands
