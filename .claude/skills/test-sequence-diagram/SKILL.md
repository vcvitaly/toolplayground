---
name: test-sequence-diagram
description: Render one concurrency integration test class (e.g. isolation-playground ReadSkewTest, WriteSkewTest, LostUpdateTest) as an interactive, step-through HTML sequence diagram with per-step database state and highlighted source lines. Use when the user asks to draw, visualize, render or explain the sequence/timeline/interleaving of a specific test class as HTML.
---

# Test class → interactive sequence diagram

Turns a single test class into a self-contained HTML page (no CDN, works offline) that shows:

- the threads / transactions / database as lifelines, with transaction activation bars (BEGIN → COMMIT/ROLLBACK)
- every SQL call, phase signal (`phase.set` / `await`), sleep, assertion and note as a numbered step
- a step-through player (←/→, space to play, click any step)
- the database table at each step: committed rows plus each open transaction's uncommitted writes, changed cells highlighted
- the test's source with the lines behind the current step highlighted

You write a JSON **model** of the test; `render.py` validates it and injects it plus the Java source into `template.html`.

## Workflow

1. **Locate the class.** Argument is a simple class name (`ReadSkewTest`) or a path. Find it with
   `find . -path '*/src/test/*' -name '<Name>.java'`. If several match, ask which one.
2. **Read everything the test's behaviour depends on**, not just the test:
   - the test class itself (note line numbers — you will reference them)
   - its base class (in isolation-playground: `AbstractPostgresTest` — `phase`, `await`, `setPhase`, `getExecutor`)
   - repository methods it calls, so steps show the **real SQL** (`AccountsRepository`)
   - `@Sql` setup: inline `statements` or scripts under `src/test/resources/sql/` → this is `initialDb`
   - `schema.sql` for the column names
   - the package's `desc.md` if present — it may already explain the intended interleaving; reuse its wording for `summary`/`takeaway`, but trust the code where they disagree.
3. **Work out the actual interleaving.** The `Phases` enum + `phase.set`/`await` pairs define a total order; follow it. Where ordering relies on `sleep` (e.g. LostUpdateTest) say so in a note. Reason about Postgres semantics to fill in what the code doesn't show:
   - READ COMMITTED: each statement sees the latest committed data.
   - REPEATABLE READ: snapshot taken at the first statement of the transaction, not at BEGIN.
   - A concurrent UPDATE on a row another open TX has updated **blocks** until that TX ends; under REPEATABLE READ, if the other TX committed, it then fails with `could not serialize access due to concurrent update` (SQLSTATE 40001).
   - Spring's `TransactionTemplate.execute()` takes a connection and disables autocommit up front; pgjdbc sends the actual BEGIN with the first statement. It COMMITs when the callback returns, or ROLLBACKs on exception / `setRollbackOnly()`. Place the BEGIN step where `execute(` is entered and mention in `explain` when the snapshot is actually taken.
   Every `assertThat` becomes an `assert` step with the value it checks.
4. **Write the model** to `<module>/diagrams/<ClassName>.json` following the schema below. Keep labels short; put the "why" into `explain`.
5. **Render:**
   ```bash
   python3 .claude/skills/test-sequence-diagram/render.py <module>/diagrams/<ClassName>.json
   ```
   It writes `<ClassName>.html` next to the JSON (override with `-o`). It fails loudly on schema errors (unknown participant, bad line numbers, rows without the id column…) — fix the JSON and rerun.
6. **Check it** in the built-in browser (`mcp__Claude_Browser__navigate` to `file://<absolute path>`): step through once, confirm no console errors, the DB table changes where expected and highlighted lines match the step. Then tell the user the file path.

Keep the JSON next to the HTML: re-rendering after a template change is then just step 5.

## Model schema

```jsonc
{
  "title": "Read skew",                         // human title
  "testClass": "io.github...readcomitted.ReadSkewTest",
  "sourcePath": "isolation-playground/src/test/java/.../ReadSkewTest.java", // relative to repo root; embedded in the page
  "summary": "One or two sentences: what anomaly / guarantee the test demonstrates.",
  "takeaway": "Optional: the lesson, shown under the diagram.",
  "participants": [
    { "id": "main", "label": "Main thread", "kind": "thread" },
    { "id": "t1", "label": "TX1", "sub": "tx1-thread · READ COMMITTED", "kind": "tx" },
    { "id": "t2", "label": "TX2", "sub": "tx2-thread · REPEATABLE READ", "kind": "tx" },
    { "id": "db", "label": "PostgreSQL", "kind": "db" }
  ],
  "table": "accounts",
  "columns": ["id", "client", "amount"],       // first column is the row key
  "initialDb": [[1, "alice", 1000], [2, "bob", 100]],
  "steps": [
    // message: an arrow. `tx` marks transaction boundaries and drives activation bars on `from`.
    { "kind": "message", "from": "t1", "to": "db", "label": "BEGIN (READ COMMITTED)", "tx": "begin", "lines": [30] },
    { "kind": "message", "from": "t1", "to": "db", "label": "UPDATE accounts SET amount = amount - 100 WHERE id = 2",
      "result": "UPDATE 1", "lines": [33],
      "explain": "Row 2 is now locked by TX1; the new value is invisible to others until commit.",
      "state": { "pending": { "t1": [[2, "bob", 0]] }, "locks": ["row 2: held by TX1"] } },
    // signal: cross-thread hand-off via phase.set(...) → await(...)
    { "kind": "signal", "from": "t1", "to": "t2", "label": "_1_TX1_WITHDRAWN_FROM_BOB1", "lines": [34, 41] },
    // self: work on one lifeline (sleep, await, local computation). blocked: true draws it as waiting.
    { "kind": "self", "at": "t2", "label": "UPDATE … id = 1 blocks on TX1's row lock", "blocked": true },
    // assert: an assertThat; ok=false for assertions the test expects to fail is NOT a thing — use ok:true unless the test really fails.
    { "kind": "assert", "at": "t2", "label": "bob2.amount == 100", "ok": true, "lines": [44] },
    // note: free text over one or more participants. tone: info | ok | warn | error
    { "kind": "note", "over": ["t1", "t2"], "text": "Both read 800\nthen sleep 100 ms", "tone": "warn" },
    { "kind": "message", "from": "t1", "to": "db", "label": "COMMIT", "tx": "commit",
      "state": { "committed": [[1, "alice", 1000], [2, "bob", 0]], "pending": { "t1": [] }, "locks": [] } },
    { "kind": "message", "from": "t2", "to": "db", "label": "ROLLBACK", "tx": "rollback" },
    // reply: dashed return arrow (e.g. PG → thread with an error)
    { "kind": "message", "from": "db", "to": "t2", "label": "ERROR 40001 could not serialize access", "reply": true, "tone": "error" }
  ]
}
```

Rules:

- Every step may carry `lines` (1-based line numbers in `sourcePath`) and `explain` (shown in the side panel). Give `lines` to every step that corresponds to code; give `explain` wherever the *reason* isn't obvious from the label — that is the point of the page.
- `state` is a **delta that carries forward**: only include the keys that change. `committed` replaces the whole committed table; `pending.<txId>` replaces that transaction's uncommitted rows (use `[]` to clear it on COMMIT/ROLLBACK); `locks` and `views` replace their lists. `views` is `{ "<participantId>": "what this TX currently sees, e.g. snapshot from step 3" }`.
- On COMMIT, fold the TX's pending rows into `committed` and clear its `pending` in the same step.
- `tone` is optional on message/note/self: `info` (default), `ok`, `warn`, `error`.
- Line breaks in labels/notes: use `\n`.
- Participant order in the array is left-to-right order on screen. Put `main` first if the main thread does anything visible (setup, final assert), and the DB last.
- Don't invent behaviour: if you are unsure whether something blocks or errors, say so in `explain` instead of guessing silently.
