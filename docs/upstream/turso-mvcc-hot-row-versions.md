# turso_core: repeated updates of one row in one MVCC transaction are O(n)

Report draft for tursodatabase/turso (checked against turso_core 0.8.1 and
main at 36da5b2e4).

## What happens

In MVCC mode, `MvStore::update_to_table_or_index` is a delete followed by an
insert. Every update appends a new `RowVersion` to the row's chain, including
when the transaction updates a version it created itself. Reads and later
deletes walk the chain to find the visible version, so the k-th update of a
row in one transaction costs O(k).

`AUTOINCREMENT` makes every insert an update of the table's
`sqlite_sequence` row, so bulk inserts into an `AUTOINCREMENT` table in one
transaction become quadratic.

## Reproducer

Sediment (an Elixir binding): `cargo test --release autoincrement_mvcc_probe -- --ignored --nocapture`
(`native/sediment_nif/src/s3/tests/mod.rs`). Six batches of 500 statements
in one `BEGIN ... COMMIT`, `PRAGMA journal_mode = 'mvcc'`, release build:

| Workload | batch 1 | batch 6 |
| --- | --- | --- |
| `INSERT` into `INTEGER PRIMARY KEY` table | 5 ms | 4 ms |
| `INSERT` into `INTEGER PRIMARY KEY AUTOINCREMENT` table | 49 ms | 252 ms |
| `UPDATE c SET n = n + 1 WHERE k = 1` (one hot row) | 25 ms | 58 ms |

WAL mode is flat for all three. Through Ecto (whose default migration primary
key is `bigserial` -> `AUTOINCREMENT`), a 10,000-row transaction ran into a
15 s timeout.

## Suggested fix

When a transaction updates or deletes a version whose `begin` is its own
`TxID` (not yet committed), replace or drop that version instead of ending it
and appending another; no other transaction can see it. Alternatively, keep
the transaction's latest own version at the head of the chain so lookups
stop early.
