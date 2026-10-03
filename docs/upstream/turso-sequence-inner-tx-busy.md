# turso_core: a busy AUTOINCREMENT inner commit commits the outer transaction

Report draft for tursodatabase/turso (turso_core 0.8.1).

## Summary

Inside `BEGIN CONCURRENT`, an `INSERT` into an `AUTOINCREMENT` table allocates
the id in an inner transaction (`SequenceBeginInnerTx` /
`SequenceCommitInnerTx`). When the inner commit fails with `Busy` (another
connection holds the pager commit lock, for example an exclusive transaction
or a checkpoint), `op_sequence_commit_inner_tx` rolls back the inner
transaction, points the connection back at the outer one and returns `Busy`.
`Program::fail_step` retries busy errors at the same PC, so the retry runs
`SequenceCommitInnerTx` again. It finds no inner commit in progress, takes
the connection's current transaction as the inner one, and commits the
**outer** transaction:

- if the outer transaction wrote nothing yet, it is committed read-only and
  removed; the statement's table cursor is still bound to it, so the insert
  fails with `No such transaction ID: <outer>`;
- if it did write, those writes are committed, and a later `ROLLBACK` of the
  transaction reports `no transaction is active`: part of the transaction
  is durable although the application rolled it back;
- if the insert runs in a trigger (a subprogram), turso panics instead:
  `Transaction <id> not found while rolling back savepoint`.

## Reproduction

```rust
let a = db.connect()?;              // journal_mode = 'mvcc'
let b = db.connect()?;
a.execute("CREATE TABLE s(id INTEGER PRIMARY KEY AUTOINCREMENT, v)")?;
a.execute("CREATE TABLE p(v)")?;
a.set_busy_timeout(Duration::from_secs(5));
b.execute("BEGIN IMMEDIATE")?;
b.execute("INSERT INTO p VALUES ('b')")?;
a.execute("BEGIN CONCURRENT")?;
a.execute("INSERT INTO p VALUES ('a')")?;
let mut stmt = a.prepare("INSERT INTO s(v) VALUES (1)")?;
// step until StepResult::Sleep, then:
b.execute("COMMIT")?;
// step again: "No such transaction ID"; a.execute("ROLLBACK") fails with
// "no transaction is active", and p contains 'a'.
```

In Sediment (an Elixir binding), `stmt::tests::a_busy_autoincrement_insert_never_commits_part_of_its_transaction`
reproduces this deterministically when the workaround is disabled.

## Suggested fix

Keep the inner transaction's identity in the program state instead of
reading it back from the connection: `op_sequence_commit_inner_tx` could take
the inner id from `sequence_inner_tx_pending`, and on `Busy` either keep the
inner transaction (and its commit state machine) for the retry, or jump back
to the retry label like a conflict does, rather than returning `Busy` at the
same PC after restoring the outer transaction. The budget-exhausted `Busy`
path has the same problem.

## Workaround in Sediment (an Elixir binding)

`stmt.rs` (`busy_inner_commit`): when a statement containing
`SequenceCommitInnerTx` (itself or in a trigger) gets busy after it has written rows in the current
run, the driver resets the statement instead of letting turso retry, and
reports it busy. The transaction must then be rolled back (turso refuses to
commit it: "an unfinished write statement was abandoned"), as after a
write-write conflict. A statement that is busy at its start (autocommit and
`BEGIN` transactions, which don't use the inner transaction) still waits for
the lock.

## Related: concurrent AUTOINCREMENT inserts conflict at write time

With 4 connections inserting into one `AUTOINCREMENT` table in
`BEGIN CONCURRENT` transactions (2 inserts each, no shared user rows), about
9% of transactions fail with `WriteWriteConflict` from the `INSERT`
statement, and throughput is about 5 times lower than with
`INTEGER PRIMARY KEY` (0 conflicts). The inner transactions that allocate ids
update the same sequence row; a conflict there is apparently raised at write
time, where `SequenceCommitInnerTx`'s conflict retry doesn't apply, and it
aborts the outer transaction. Retrying the allocation (as for a conflict at
the inner commit) would keep id allocation from failing user transactions.
Sediment's probe: `s3::tests::concurrent_autoincrement_busy_probe`
(`--ignored`; `PROBE_PLAIN=1` for a local database).
